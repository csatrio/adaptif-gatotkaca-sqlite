// ============================================================================
// Import Definition
// ============================================================================

use crossbeam_utils::Backoff;

use std::{
    cell::UnsafeCell,
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll, Waker},
    thread::{self, Thread},
    time::{Duration, Instant},
};

// ============================================================================
// Protocol Overview
// ============================================================================
//
// One `Arc<GkReplyShared<T>>` (the only heap allocation) is shared by one sender and one
// receiver. All coordination goes through one `AtomicU8` state word:
//
//   VALUE       the sender wrote `value` and published it (set once, by the sender).
//   CLOSED      the sender was dropped without sending (set once, by the sender).
//   RX_DROPPED  the receiver was dropped before the channel completed (set by the receiver).
//   WAKER       the `wake` slot holds a registered waker and is published (set/cleared by the
//               receiver only, and only while neither VALUE nor CLOSED is set).
//
// Ownership of the plain (non-atomic) cells:
//
//   `value`: the sender writes it strictly before `fetch_or(VALUE, AcqRel)`. The receiver reads
//            it strictly after an Acquire load that observed VALUE. Exactly one side removes it:
//            the sender if its `fetch_or` saw RX_DROPPED, the receiver otherwise.
//   `wake`:  the receiver WRITES it only while WAKER is clear in the state word. The sender
//            READS it (shared reference, never writes) only if its completing `fetch_or`
//            returned a state with WAKER set. The two are mutually exclusive because every
//            receiver transition of WAKER is a CAS that fails once VALUE/CLOSED is set: either
//            the CAS lands before the sender's `fetch_or` (the sender then sees the final WAKER
//            bit and the Release/Acquire pair makes the slot write visible), or the `fetch_or`
//            lands first (the CAS fails, the receiver never touches the slot again). The
//            receiver may also take shared reads of the slot while WAKER is set (to call
//            `will_wake`), which cannot conflict with the sender's shared reads.
//
// Memory orderings:
//   * sender completion: `fetch_or(.., AcqRel)`. Release publishes `value`; Acquire makes the
//     receiver's slot write (published by its Release CAS on WAKER) visible before waking.
//   * receiver observation: `load(Acquire)` pairs with the sender's Release.
//   * receiver WAKER set: CAS success `Release` (publishes the slot write); WAKER clear and
//     drop paths use `AcqRel`. Failure orderings are `Acquire` so the receiver sees `value`.
//   * wakeups cannot be lost: the receiver re-checks the state after its registering CAS
//     succeeded (the CAS itself proves no completion happened yet, so the sender will see
//     WAKER); `park`/`unpark` tokens cover the window before the thread actually parks.

const REPLY_VALUE: u8 = 0b0001;
const REPLY_CLOSED: u8 = 0b0010;
const REPLY_RX_DROPPED: u8 = 0b0100;
const REPLY_WAKER: u8 = 0b1000;
const REPLY_COMPLETE: u8 = REPLY_VALUE | REPLY_CLOSED;

// ============================================================================
// Error Definition
// ============================================================================

/// Why a reply could not be obtained. A `#[repr(transparent)]` newtype over `u8`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GkReplyError(pub u8);

impl GkReplyError {
    /// The sender was dropped without sending a value (for example the writer thread died).
    pub const DISCONNECTED: Self = Self(1);
    /// The deadline passed before a value arrived; a value arriving later is dropped.
    pub const TIMEOUT: Self = Self(2);

    /// Human readable description of this error code.
    pub const fn message(self) -> &'static str {
        // BRANCH: map the code to its message
        match self.0 {
            1 => "reply sender disconnected without sending a value",
            2 => "timed out waiting for the reply",
            _ => "unknown reply error",
        }
    }
}

impl fmt::Display for GkReplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl Error for GkReplyError {}

// ============================================================================
// Shared Cell Definition
// ============================================================================

/// Who is registered to be woken when the channel completes.
enum GkReplyWake {
    /// Nothing registered.
    Empty,
    /// A blocked `wait`/`wait_timeout` caller.
    Thread(Thread),
    /// A future executor task.
    Task(Waker),
}

/// The single heap cell shared by a sender and its receiver.
struct GkReplyShared<T> {
    state: AtomicU8,
    value: UnsafeCell<Option<T>>,
    wake: UnsafeCell<GkReplyWake>,
}

// SAFETY: `value` and `wake` are only accessed under the exclusion protocol documented in the
// "Protocol Overview" above (state-word handoff with Release/Acquire), and `T: Send` lets the
// value move between the two ends. `Thread` and `Waker` are `Send + Sync`.
unsafe impl<T: Send> Sync for GkReplyShared<T> {}
unsafe impl<T: Send> Send for GkReplyShared<T> {}

impl<T> GkReplyShared<T> {
    /// Wakes the registered waiter.
    ///
    /// # Safety
    /// The caller must be the sender whose completing `fetch_or` returned a state with
    /// `REPLY_WAKER` set and without `REPLY_RX_DROPPED`: the slot is then published and the
    /// receiver will not write it again (only shared reads happen concurrently).
    unsafe fn notify(&self) {
        // SAFETY: per the contract, shared access only.
        let wake = unsafe { &*self.wake.get() };
        // BRANCH: wake whichever kind of waiter registered
        match wake {
            GkReplyWake::Empty => {}
            GkReplyWake::Thread(thread) => thread.unpark(),
            GkReplyWake::Task(waker) => waker.wake_by_ref(),
        }
    }

    /// Removes the value.
    ///
    /// # Safety
    /// The caller must have exclusive logical ownership of `value`: either the receiver after an
    /// Acquire load that observed `REPLY_VALUE`, or the sender after its `fetch_or` returned
    /// `REPLY_RX_DROPPED`.
    unsafe fn take_value(&self) -> Option<T> {
        // SAFETY: per the contract, no other access to the cell can happen.
        unsafe { (*self.value.get()).take() }
    }

    /// Converts a completed state into the result, consuming the value if there is one.
    ///
    /// # Safety
    /// `state` must be a state loaded with Acquire (or returned by a failed CAS with Acquire
    /// failure ordering) that has `REPLY_VALUE` or `REPLY_CLOSED` set, and the caller must be the
    /// receiver that has not consumed the result yet.
    unsafe fn result_of(&self, state: u8) -> Result<T, GkReplyError> {
        // BRANCH: a published value, or a closed channel
        if state & REPLY_VALUE != 0 {
            // SAFETY: VALUE observed with Acquire, we are the receiver.
            match unsafe { self.take_value() } {
                Some(value) => Ok(value),
                None => Err(GkReplyError::DISCONNECTED),
            }
        } else {
            Err(GkReplyError::DISCONNECTED)
        }
    }

    /// Clears `REPLY_WAKER` so the receiver owns the slot again.
    ///
    /// Returns `Err(state)` with the completed state if the channel completed first (the slot
    /// then stays published and must not be written). Must only be called by the receiver.
    fn unset_waker(&self, state: u8) -> Result<(), u8> {
        // BRANCH: already complete, or nothing registered, or clear the bit
        if state & REPLY_COMPLETE != 0 {
            return Err(state);
        }
        if state & REPLY_WAKER == 0 {
            return Ok(());
        }
        match self.state.compare_exchange(
            state,
            state & !REPLY_WAKER,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            // Only the sender can change the word concurrently, and only to a completed state.
            Err(current) => Err(current),
        }
    }

    /// Stores `wake` in the slot and publishes it.
    ///
    /// Returns `Err(state)` with the completed state if the channel completed first. Must only
    /// be called by the receiver while `REPLY_WAKER` is clear in `state` (see `unset_waker`).
    fn register(&self, wake: GkReplyWake, state: u8) -> Result<(), u8> {
        // SAFETY: WAKER is clear, so the sender will not read the slot until our CAS below
        // publishes it, and the receiver (this thread) is the only writer.
        unsafe { *self.wake.get() = wake };
        match self.state.compare_exchange(
            state,
            state | REPLY_WAKER,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            // Failure means the sender completed (the only concurrent mutation).
            Err(current) => Err(current),
        }
    }
}

// ============================================================================
// Channel Definition
// ============================================================================

/// Creates a single-use reply channel with exactly one heap allocation.
pub fn reply_channel<T: Send>() -> (GkReplySender<T>, GkReplyReceiver<T>) {
    let shared = Arc::new(GkReplyShared {
        state: AtomicU8::new(0),
        value: UnsafeCell::new(None),
        wake: UnsafeCell::new(GkReplyWake::Empty),
    });
    (
        GkReplySender {
            inner: Some(Arc::clone(&shared)),
        },
        GkReplyReceiver {
            shared,
            done: false,
        },
    )
}

/// Sending half of a reply channel. Dropping it without calling `send` disconnects the receiver.
pub struct GkReplySender<T> {
    inner: Option<Arc<GkReplyShared<T>>>,
}

impl<T> GkReplySender<T> {
    /// Delivers `value` to the receiver. Never blocks. If the receiver is already gone the
    /// value is dropped immediately.
    pub fn send(mut self, value: T) {
        // BRANCH: the sender is always populated until consumed
        let Some(shared) = self.inner.take() else {
            return;
        };
        // SAFETY: the receiver does not read `value` before it observes VALUE, which is only
        // set by the `fetch_or` below; the sender is the only writer.
        unsafe { *shared.value.get() = Some(value) };
        let prev = shared.state.fetch_or(REPLY_VALUE, Ordering::AcqRel);
        // BRANCH: receiver gone (drop the value now), receiver waiting (wake it), or nobody
        if prev & REPLY_RX_DROPPED != 0 {
            // SAFETY: RX_DROPPED was set before VALUE, so the receiver never reads the value.
            drop(unsafe { shared.take_value() });
        } else if prev & REPLY_WAKER != 0 {
            // SAFETY: we observed WAKER (published with Release) in the completing `fetch_or`.
            unsafe { shared.notify() };
        }
    }
}

impl<T> Drop for GkReplySender<T> {
    fn drop(&mut self) {
        // BRANCH: only an unsent sender closes the channel
        let Some(shared) = self.inner.take() else {
            return;
        };
        let prev = shared.state.fetch_or(REPLY_CLOSED, Ordering::AcqRel);
        // BRANCH: wake a registered waiter so it observes the disconnect
        if prev & REPLY_RX_DROPPED == 0 && prev & REPLY_WAKER != 0 {
            // SAFETY: we observed WAKER in the completing `fetch_or`.
            unsafe { shared.notify() };
        }
    }
}

/// Receiving half of a reply channel. Consume it with `wait`/`wait_timeout`, `.await` it, or
/// poll it with `try_take`.
pub struct GkReplyReceiver<T> {
    shared: Arc<GkReplyShared<T>>,
    /// The result was already handed out (value taken or disconnect reported).
    done: bool,
}

impl<T> GkReplyReceiver<T> {
    /// True when a value was sent or the sender disconnected (cheap, relaxed-cost check).
    pub fn is_ready(&self) -> bool {
        !self.done && self.shared.state.load(Ordering::Acquire) & REPLY_COMPLETE != 0
    }

    /// Non-blocking take. `Ok(None)` while pending, `Ok(Some(v))` once, then
    /// `Err(DISCONNECTED)` (also after a sender disconnect or after the value was taken).
    pub fn try_take(&mut self) -> Result<Option<T>, GkReplyError> {
        // BRANCH: already consumed
        if self.done {
            return Err(GkReplyError::DISCONNECTED);
        }
        let state = self.shared.state.load(Ordering::Acquire);
        // BRANCH: not complete yet, or complete
        if state & REPLY_COMPLETE == 0 {
            return Ok(None);
        }
        self.done = true;
        // SAFETY: completed state loaded with Acquire, result not consumed before.
        unsafe { self.shared.result_of(state) }.map(Some)
    }

    /// Blocks until the value arrives or the sender is dropped. Spins briefly, then parks.
    ///
    /// The sender unparks the calling thread, so a stray unpark token may remain after
    /// return; code that parks the same thread later must tolerate spurious wakeups (as
    /// `std::thread::park` always requires).
    pub fn wait(mut self) -> Result<T, GkReplyError> {
        // BRANCH: completed during the spin phase, otherwise park
        match self.spin() {
            Some(state) => {
                self.done = true;
                // SAFETY: completed state observed with Acquire, first consumption.
                unsafe { self.shared.result_of(state) }
            }
            None => self.park_until(None),
        }
    }

    /// Like `wait` but gives up at the deadline with `TIMEOUT`. The receiver is consumed, so a
    /// value that arrives later is dropped by the sender.
    pub fn wait_timeout(mut self, timeout: Duration) -> Result<T, GkReplyError> {
        // BRANCH: completed during the spin phase, otherwise park until the deadline
        match self.spin() {
            Some(state) => {
                self.done = true;
                // SAFETY: completed state observed with Acquire, first consumption.
                unsafe { self.shared.result_of(state) }
            }
            None => {
                // BRANCH: a deadline that overflows `Instant` means wait forever
                let deadline = Instant::now().checked_add(timeout);
                self.park_until(deadline)
            }
        }
    }

    /// Short adaptive spin; returns the completed state if the channel completed meanwhile.
    fn spin(&self) -> Option<u8> {
        let backoff = Backoff::new();
        loop {
            let state = self.shared.state.load(Ordering::Acquire);
            // BRANCH: completed, spin budget exhausted, or keep backing off
            if state & REPLY_COMPLETE != 0 {
                return Some(state);
            }
            if backoff.is_completed() {
                return None;
            }
            backoff.snooze();
        }
    }

    /// Registers the current thread and parks until completion or `deadline`.
    fn park_until(&mut self, deadline: Option<Instant>) -> Result<T, GkReplyError> {
        let shared = &*self.shared;
        let mut state = shared.state.load(Ordering::Acquire);
        // A previous `poll` may have left a task waker published: take the slot back first.
        // BRANCH: only act while the channel is incomplete
        if state & REPLY_COMPLETE == 0 && state & REPLY_WAKER != 0 {
            // BRANCH: slot reclaimed, or the channel completed first
            match shared.unset_waker(state) {
                Ok(()) => state &= !REPLY_WAKER,
                Err(done_state) => state = done_state,
            }
        }
        // BRANCH: register the current thread unless the channel completed already
        if state & REPLY_COMPLETE == 0
            && let Err(done_state) = shared.register(GkReplyWake::Thread(thread::current()), state)
        {
            state = done_state;
        }
        loop {
            // BRANCH: completed, or wait (forever / until the deadline)
            if state & REPLY_COMPLETE != 0 {
                self.done = true;
                // SAFETY: completed state observed with Acquire, first consumption.
                return unsafe { shared.result_of(state) };
            }
            match deadline {
                None => thread::park(),
                Some(deadline) => {
                    let now = Instant::now();
                    // BRANCH: deadline passed (Drop clears the slot and marks RX_DROPPED)
                    if now >= deadline {
                        state = shared.state.load(Ordering::Acquire);
                        // BRANCH: the value landed while the deadline passed; deliver it instead of a timeout
                        if state & REPLY_COMPLETE != 0 {
                            continue;
                        }
                        return Err(GkReplyError::TIMEOUT);
                    }
                    thread::park_timeout(deadline - now);
                }
            }
            state = shared.state.load(Ordering::Acquire);
        }
    }
}

impl<T> Drop for GkReplyReceiver<T> {
    fn drop(&mut self) {
        // BRANCH: result already consumed, nothing left to release
        if self.done {
            return;
        }
        let shared = &*self.shared;
        let mut state = shared.state.load(Ordering::Acquire);
        loop {
            // BRANCH: value waiting (drop it here), closed, or mark the receiver as gone
            if state & REPLY_VALUE != 0 {
                // SAFETY: VALUE observed with Acquire; the sender has finished writing.
                drop(unsafe { shared.take_value() });
                return;
            }
            if state & REPLY_CLOSED != 0 {
                return;
            }
            match shared.state.compare_exchange(
                state,
                (state | REPLY_RX_DROPPED) & !REPLY_WAKER,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // BRANCH: release the waker we had registered; the sender sees no WAKER now
                    if state & REPLY_WAKER != 0 {
                        // SAFETY: WAKER is cleared and the channel was incomplete at the CAS,
                        // so the sender never reads the slot; we are its only accessor.
                        unsafe { *shared.wake.get() = GkReplyWake::Empty };
                    }
                    return;
                }
                Err(current) => state = current,
            }
        }
    }
}

impl<T> Future for GkReplyReceiver<T> {
    type Output = Result<T, GkReplyError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // BRANCH: polled after completion
        if this.done {
            return Poll::Ready(Err(GkReplyError::DISCONNECTED));
        }
        let shared = &*this.shared;
        let mut state = shared.state.load(Ordering::Acquire);
        loop {
            // BRANCH: completed
            if state & REPLY_COMPLETE != 0 {
                this.done = true;
                // SAFETY: completed state observed with Acquire, first consumption.
                return Poll::Ready(unsafe { shared.result_of(state) });
            }
            // BRANCH: a waker is already published
            if state & REPLY_WAKER != 0 {
                // SAFETY: WAKER is set and the channel incomplete at the load: the sender only
                // takes shared references to the slot, the receiver (us) is the only writer.
                let same = match unsafe { &*shared.wake.get() } {
                    GkReplyWake::Task(waker) => waker.will_wake(cx.waker()),
                    _ => false,
                };
                // BRANCH: same task: the registered waker is still right. A completion after
                // our load sees WAKER and wakes it.
                if same {
                    return Poll::Pending;
                }
                // BRANCH: different waker or a thread slot: reclaim the slot, or finish
                match shared.unset_waker(state) {
                    Ok(()) => state &= !REPLY_WAKER,
                    Err(done_state) => {
                        state = done_state;
                        continue;
                    }
                }
            }
            // BRANCH: registered (the CAS proves no completion yet, so no lost wakeup) or done
            match shared.register(GkReplyWake::Task(cx.waker().clone()), state) {
                Ok(()) => return Poll::Pending,
                Err(done_state) => state = done_state,
            }
        }
    }
}
