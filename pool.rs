//! The reader thread pool: worker threads over a [`GkTreiberMpmc`] job queue, with no `Mutex`, `RwLock` or `Condvar`.
//!
//! # Moving parts
//!
//! - **Submit** ([`GkReaderExecutor::spawn`]): the calling thread owns one [`GkMpmcLane`] per pool, kept in a
//!   thread-local [`FastU64Map`] keyed by the pool's id and created on the first submit. A submit boxes the job into a
//!   `GkJobNode`, pushes it on the lane (one `SeqCst` CAS), then does one `SeqCst` read-modify-write on the pool's
//!   `state` word and, if that shows a sleeping worker, wakes exactly one.
//! - **Workers** loop on [`GkTreiberMpmc::try_reap`], run every job of a reaped chain, spin `spin_polls` empty polls
//!   when the queue runs dry, and only then sleep.
//! - **Shutdown** sets a flag in `state`, unparks everyone, lets the workers drain what is queued and joins them.
//!
//! # The parking protocol (why no wake-up is lost)
//!
//! `state` holds the `CLOSED` bit and the number of registered sleepers (`sleepers << 1`). A second word array,
//! `sleeping`, holds one bit per worker saying *which* workers are registered.
//!
//! A worker about to sleep does, in this order:
//!
//! 1. set its bit in `sleeping` (`SeqCst` RMW),
//! 2. add one sleeper to `state` (`SeqCst` RMW),
//! 3. `fence(SeqCst)`,
//! 4. re-check `has_work()` and `CLOSED`; only if both are negative, `park` (or `park_timeout`, below),
//! 5. on waking, clear its bit and remove its sleeper (so the bit and the counter are always the worker's own to
//!    undo; whoever claims a sleeper only clears the bit), then go back to polling.
//!
//! A submitter does: `push` (`SeqCst` CAS), then `state.fetch_add(0, SeqCst)`, and wakes one sleeper if the value it
//! read has `sleepers > 0`.
//!
//! Take any push `P` and any worker `S` going to sleep. Both sit in the single `SeqCst` order. Either `P`'s CAS comes
//! before `S`'s fence, and then the `has_work` load of step 4 sees the job; or `S`'s fence (and so its step 2) comes
//! before `P`'s CAS, and then `P`'s RMW on `state`, which comes after the CAS, reads a value that includes `S`'s
//! registration (RMWs on one word always read the latest write). So `P` sees `sleepers > 0`, and because step 1 comes
//! before step 2, it finds a bit to claim. Claiming is a CAS that clears the bit; the winner unparks that worker. A
//! `park`/`unpark` pair cannot lose a wake-up either: `unpark` before `park` leaves a token, so the `park` returns at
//! once (the price is an occasional spurious wake-up, which is harmless: the worker re-polls and sleeps again).
//!
//! Two ways a claim can be "wasted" on a worker that is not really asleep, and why they cost nothing:
//!
//! - *The claimed worker is already leaving* (step 5, or its step 4 found work). It is still counted in `state`
//!   until its own decrement, and `P`'s RMW came before that decrement, so `P`'s job is visible to the worker's next
//!   poll. Another job in another lane that the worker does not take is covered by the cascade: a worker that gets a
//!   chain while `sleepers > 0` and the queue still `has_work()` wakes one more sleeper.
//! - *Two submitters see the same single sleeper.* The second finds no bit and wakes nobody; the claimed worker is
//!   awake and polls every lane, so it finds both jobs (or cascades).
//!
//! # Sharing a chain, and rescuing a blocked worker
//!
//! A worker that reaps a lane keeps the first job and publishes the rest in a chain slot, where idle workers can take
//! jobs from it. With the default `rescue_after = 0` they do so at once: a producer that keeps a few dozen medium
//! jobs (tens of microseconds) in flight puts them all in its one lane, one worker reaps the lot, and the other workers
//! share it job by job instead of waiting. (With a stall threshold such as 50 µs the chain's head changes every 20 µs
//! and a rescue never fires, so those jobs ran serially on one worker: 50k reads/s against 160k in the read-path
//! bench.) An idle worker only touches a chain's slot when it has nothing else to do, so busy workers and tiny jobs
//! pay nothing measurable. With `rescue_after > 0` an idle worker instead takes a chain over (the owner stops at its
//! next take) only once its first job has not changed for that long: a pure blocked-worker rescue.
//!
//! The queue never wakes anyone for a shared chain, so the pool does two things:
//!
//! - a worker whose re-check shows `shared_chains() > 0` parks with a timeout instead of `park` (`rescue_after`, at
//!   least 10 µs; 50 µs when `rescue_after` is zero) and polls again when it comes back (with a threshold, two looks
//!   at an unchanged slot at least `rescue_after` apart make a rescue);
//! - a worker that obtains a chain which it published (`published()`), or right after a timed-out sleep (it may have
//!   just rescued one), wakes one sleeper if there is any, so that a sleeper that parked *before* the chain existed
//!   (and therefore without a timeout) starts watching it. The publish is followed by `fence(SeqCst)` before the read
//!   of `state`, which pairs with step 3 of the sleeper the same way as above (a sleeper either sees the chain in its
//!   `shared_chains()` re-check or is counted when the publisher reads `state`).
//!
//! # Allocation
//!
//! A submit allocates the job node (one `Box<GkJobNode>`); the job itself is the caller's `Box<dyn FnOnce>`, so a job
//! costs two allocations. The lane is taken from the queue without allocating. A single-allocation node would need a
//! type-erased, variable-sized node (unsafe layout code), which was left out for correctness first.
//!
//! # Counters
//!
//! `pending` counts jobs submitted and not yet picked up by a worker. It is a `Relaxed` add per submit and one
//! subtract per reaped chain (per job with `max_queue`, where accuracy decides whether a job is rejected), kept on
//! its own cache line apart from `state`. A job counts as pending until the worker takes it out of its chain.

// ==== Import Definition ====

use crate::FastU64Map;
use crate::executor::{GkReadJob, GkReaderExecutor, GkThreadOptions};

use adaptif_gatotkaca_utils::{
    GkMpmcChain, GkMpmcLane, GkTreiberMpmc, GkTreiberMpmcConfig, TreiberNode,
};
use crossbeam_utils::CachePadded;

use std::cell::RefCell;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Arc, OnceLock, Weak};
use std::thread;
use std::time::Duration;

// ==== Constants ====

/// Most worker threads a pool accepts.
pub const MAX_READER_THREADS: usize = 4096;
/// Default for [`GkReaderPoolConfig::max_lanes`].
pub const DEFAULT_READER_MAX_LANES: usize = 64;
/// Default for [`GkReaderPoolConfig::spin_polls`]. An empty `try_reap` is a few loads per lane and costs well under
/// 100 ns, so 128 polls keep an idle worker hot for about ten microseconds: long enough to catch the next job of a
/// burst without a park/unpark round trip (tens of microseconds), short enough that an idle pool does not burn a core.
pub const DEFAULT_READER_SPIN_POLLS: usize = 128;
/// Default for [`GkReaderPoolConfig::rescue_after`].
pub const DEFAULT_READER_RESCUE_AFTER: Duration = Duration::ZERO;

/// `state` bit 0: the pool is shut down.
const CLOSED: usize = 1;
/// `state` counts sleepers in the bits above `CLOSED`.
const SLEEPER: usize = 2;
/// Chain slots per worker (at least [`MIN_CHAIN_SLOTS`] in all): a worker that finds none free keeps its chain.
const CHAIN_SLOTS_PER_WORKER: usize = 2;
const MIN_CHAIN_SLOTS: usize = 64;
/// A timed park shorter than this would spin; a non-zero `rescue_after` is clamped to it for parking.
const MIN_PARK_TIMEOUT: Duration = Duration::from_micros(10);
/// The timed park while chains are shared when `rescue_after` is zero (nothing to wait for, so this is only the safety
/// net that makes a sleeper that missed a wake-up join a shared chain).
const SHARING_PARK_TIMEOUT: Duration = Duration::from_micros(50);

// ==== Error Definition ====

/// Why a pool could not be created.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct GkReaderPoolError(u8);

impl GkReaderPoolError {
    /// [`GkReaderPoolConfig::validate`] rejected the configuration.
    pub const INVALID_CONFIG: Self = Self(1);
    /// The operating system refused to start a worker thread (the workers already started were stopped again).
    pub const SPAWN_FAILED: Self = Self(2);

    /// The error code.
    pub const fn as_u8(self) -> u8 {
        self.0
    }

    /// The constant's name.
    pub const fn name(self) -> &'static str {
        // BRANCH: one name per constant
        match self.0 {
            1 => "INVALID_CONFIG",
            2 => "SPAWN_FAILED",
            _ => "UNKNOWN",
        }
    }
}

impl fmt::Debug for GkReaderPoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GkReaderPoolError::{}", self.name())
    }
}

impl fmt::Display for GkReaderPoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // BRANCH: one message per constant
        match self.0 {
            1 => f.write_str("invalid reader pool configuration"),
            2 => f.write_str("could not start a reader pool worker thread"),
            _ => f.write_str("unknown reader pool error"),
        }
    }
}

impl std::error::Error for GkReaderPoolError {}

// ==== Config Definition ====

/// Settings of a [`GkReaderPool`], fixed when it is created.
#[derive(Clone, Debug)]
pub struct GkReaderPoolConfig {
    /// Worker threads. 0 (the default) means [`std::thread::available_parallelism`].
    pub threads: usize,
    /// Name, stack size and start/stop hooks of the workers. Defaults to the name prefix `sqlite-reader`.
    pub thread: GkThreadOptions,
    /// Producer lanes of the queue: how many threads can have jobs queued at once (a worker that submits from inside a
    /// job counts too). A submit from a thread beyond that is not accepted (`Err(job)`); the caller runs it inline.
    /// Default 64. A lane is held until its thread exits.
    pub max_lanes: usize,
    /// Empty polls of the queue a worker makes before it registers as a sleeper. Default
    /// [`DEFAULT_READER_SPIN_POLLS`].
    pub spin_polls: usize,
    /// Zero (the default): an idle worker takes jobs from any chain another worker shares at once, so medium-size
    /// jobs from a few producers spread over all workers. Non-zero: an idle worker takes a chain over only once its
    /// first job has not changed for this long (a pure blocked-worker rescue; jobs shorter than this never spread),
    /// and a sleeping worker wakes about this often (at least 10 µs) while a chain is shared. Forwarded to
    /// [`GkTreiberMpmcConfig`].
    pub rescue_after: Duration,
    /// `None` (the default): unbounded. `Some(n)`: a submit is not accepted (`Err(job)`) while `n` jobs are pending.
    pub max_queue: Option<usize>,
}

impl Default for GkReaderPoolConfig {
    fn default() -> Self {
        Self {
            threads: 0,
            thread: GkThreadOptions::default().named("sqlite-reader"),
            max_lanes: DEFAULT_READER_MAX_LANES,
            spin_polls: DEFAULT_READER_SPIN_POLLS,
            rescue_after: DEFAULT_READER_RESCUE_AFTER,
            max_queue: None,
        }
    }
}

impl GkReaderPoolConfig {
    /// Sets [`threads`](Self::threads).
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    /// Sets [`thread`](Self::thread).
    pub fn with_thread(mut self, thread: GkThreadOptions) -> Self {
        self.thread = thread;
        self
    }

    /// Sets [`max_lanes`](Self::max_lanes).
    pub fn with_max_lanes(mut self, max_lanes: usize) -> Self {
        self.max_lanes = max_lanes;
        self
    }

    /// Sets [`spin_polls`](Self::spin_polls).
    pub fn with_spin_polls(mut self, spin_polls: usize) -> Self {
        self.spin_polls = spin_polls;
        self
    }

    /// Sets [`rescue_after`](Self::rescue_after).
    pub fn with_rescue_after(mut self, rescue_after: Duration) -> Self {
        self.rescue_after = rescue_after;
        self
    }

    /// Sets [`max_queue`](Self::max_queue).
    pub fn with_max_queue(mut self, max_queue: Option<usize>) -> Self {
        self.max_queue = max_queue;
        self
    }

    /// The worker count the pool will start: `threads`, or the available parallelism when it is 0.
    pub fn resolved_threads(&self) -> usize {
        // BRANCH: 0 asks for one worker per available core
        if self.threads == 0 {
            thread::available_parallelism().map_or(1, |n| n.get())
        } else {
            self.threads
        }
    }

    /// Checks the rules: `max_lanes >= 1`, at most [`MAX_READER_THREADS`] workers, `max_queue` not `Some(0)`.
    pub fn validate(&self) -> Result<(), GkReaderPoolError> {
        // BRANCH: a queue without a lane could never hold a job
        if self.max_lanes == 0 {
            return Err(GkReaderPoolError::INVALID_CONFIG);
        }
        // BRANCH: more workers than the sleeper bitmap and the sleeper count are sized for
        if self.resolved_threads() > MAX_READER_THREADS {
            return Err(GkReaderPoolError::INVALID_CONFIG);
        }
        // BRANCH: a queue of zero would reject every job
        if self.max_queue == Some(0) {
            return Err(GkReaderPoolError::INVALID_CONFIG);
        }
        Ok(())
    }
}

// ==== Job Node Definition ====

/// One queued job: the intrusive link of the queue and the boxed closure.
struct GkJobNode {
    next: AtomicPtr<GkJobNode>,
    job: Option<GkReadJob>,
}

impl TreiberNode for GkJobNode {
    type Value = Option<GkReadJob>;

    #[inline(always)]
    fn next(&self) -> &AtomicPtr<Self> {
        &self.next
    }

    #[inline(always)]
    fn value(&self) -> &Self::Value {
        &self.job
    }
}

/// Takes the job out of a node the queue handed to us and frees the node.
///
/// # Safety
/// `node` must come from `Box::into_raw` of a `GkJobNode` in [`GkPoolInner::submit`], and the caller must own it (it was
/// reaped, drained or never pushed): nothing else may touch it afterwards.
#[inline]
unsafe fn open_node(node: NonNull<GkJobNode>) -> Option<GkReadJob> {
    // Safety: forwarded from this function's contract.
    let node = unsafe { Box::from_raw(node.as_ptr()) };
    node.job
}

// ==== Submit-side Lanes Definition ====

/// This thread's lane into one pool.
struct GkLaneEntry {
    lane: GkMpmcLane<GkJobNode>,
    /// To tell, when the thread meets a new pool, which entries belong to pools that are gone or shut down.
    owner: Weak<GkPoolInner>,
}

thread_local! {
    /// Pool id -> this thread's lane. Dropped with the thread, which releases the lanes for reuse.
    static LANES: RefCell<FastU64Map<GkLaneEntry>> = RefCell::new(FastU64Map::new());
}

/// Ids of pools, the key of [`LANES`].
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

// ==== Pool State Definition ====

/// One worker's slot: how to wake it and how to join it.
struct GkWorkerSlot {
    /// Set by the worker itself before it can register as a sleeper.
    thread: OnceLock<thread::Thread>,
    /// A `Box<JoinHandle>` for the first `shutdown` to take.
    join: AtomicPtr<thread::JoinHandle<()>>,
}

/// Everything the handles and the workers share.
struct GkPoolInner {
    id: u64,
    me: Weak<GkPoolInner>,
    queue: GkTreiberMpmc<GkJobNode>,
    /// `CLOSED` bit and `sleepers << 1`. Every submit does a `SeqCst` RMW on it.
    state: CachePadded<AtomicUsize>,
    /// Jobs submitted and not yet taken by a worker (see the module docs).
    pending: CachePadded<AtomicUsize>,
    /// Bit `i % 64` of word `i / 64`: worker `i` is registered as a sleeper.
    sleeping: Box<[CachePadded<AtomicU64>]>,
    workers: Box<[GkWorkerSlot]>,
    threads: usize,
    spin_polls: usize,
    park_timeout: Duration,
    max_queue: Option<usize>,
    /// [`GkReaderPool`] handles; the last one to drop shuts the pool down.
    handles: AtomicUsize,
    /// Workers started and not yet exited.
    live: AtomicUsize,
}

impl GkPoolInner {
    #[inline(always)]
    fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) & CLOSED != 0
    }

    // ---- submit ----

    fn submit(&self, job: GkReadJob) -> Result<(), GkReadJob> {
        // BRANCH: shut down: refuse before allocating anything
        if self.state.load(Ordering::Relaxed) & CLOSED != 0 {
            return Err(job);
        }
        // Count the job before it is pushed, so a worker's subtract can never run ahead of the add.
        // BRANCH: bounded queue: reserve a place or refuse
        if let Some(max) = self.max_queue {
            let mut current = self.pending.load(Ordering::Relaxed);
            loop {
                // BRANCH: full
                if current >= max {
                    return Err(job);
                }
                match self.pending.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    // BRANCH: reserved
                    Ok(_) => break,
                    // BRANCH: lost a race with another submitter or a worker; look again
                    Err(seen) => current = seen,
                }
            }
        } else {
            self.pending.fetch_add(1, Ordering::Relaxed);
        }

        let mut slot = Some(job);
        let pushed = LANES.try_with(|cell| {
            let mut lanes = cell.borrow_mut();
            // BRANCH: this thread already has a lane into this pool
            if let Some(entry) = lanes.get_mut(&self.id) {
                return Some(Self::push(entry, &mut slot));
            }
            // First submit from this thread: drop lanes of pools that are gone or shut down, then take one.
            lanes.retain(|_, entry| entry.owner.upgrade().is_some_and(|pool| !pool.is_closed()));
            // BRANCH: no free lane (TOO_MANY_LANES): the caller keeps the job
            let lane = self.queue.lane().ok()?;
            let entry = lanes.entry(self.id).or_insert(GkLaneEntry {
                lane,
                owner: self.me.clone(),
            });
            Some(Self::push(entry, &mut slot))
        });
        let node = match pushed {
            Ok(Some(node)) => node,
            // BRANCH: lanes exhausted, or this thread's locals are already destroyed: nothing was queued
            _ => {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                return match slot {
                    Some(job) => Err(job),
                    None => Ok(()),
                };
            }
        };

        // The wake-up handshake: the push above was a SeqCst CAS; this is the SeqCst RMW after it (see module docs).
        let seen = self.state.fetch_add(0, Ordering::SeqCst);
        // BRANCH: the pool closed around our push: workers may be gone, so take the job back unless one has it
        if seen & CLOSED != 0 {
            return self.take_back(node);
        }
        // BRANCH: at least one registered sleeper: wake one
        if seen >> 1 > 0 {
            self.wake_one();
        }
        Ok(())
    }

    /// Boxes the job into a node and pushes it on `entry`'s lane. Returns the node's address (only compared, never
    /// dereferenced, by the caller).
    #[inline]
    fn push(entry: &mut GkLaneEntry, slot: &mut Option<GkReadJob>) -> *mut GkJobNode {
        let node = Box::into_raw(Box::new(GkJobNode {
            next: AtomicPtr::new(ptr::null_mut()),
            job: slot.take(),
        }));
        // Safety: a fresh node, in no other queue. A node that is never run is freed by `drop_node` (the default,
        // `Box::from_raw`), which matches the `Box::into_raw` above.
        unsafe { entry.lane.push(NonNull::new_unchecked(node)) };
        node
    }

    /// The pool closed after we pushed `ours`. Reaps back what is still in this thread's lane: our job is returned
    /// (`Err`) and older jobs of this thread run inline here. If a worker already took `ours`, it runs it (`Ok`).
    fn take_back(&self, ours: *mut GkJobNode) -> Result<(), GkReadJob> {
        let mut taken: Vec<NonNull<GkJobNode>> = Vec::new();
        let _ = LANES.try_with(|cell| {
            // BRANCH: our lane is there (we just pushed through it)
            if let Some(entry) = cell.borrow_mut().get_mut(&self.id) {
                entry.lane.drain(|node| taken.push(node));
            }
        });
        let mut mine = None;
        for node in taken {
            let is_ours = node.as_ptr() == ours;
            // Safety: drained from the lane, so ours; every node in it came from `push`.
            let job = unsafe { open_node(node) };
            self.pending.fetch_sub(1, Ordering::Relaxed);
            // BRANCH: our own job goes back to the caller; the others were accepted earlier and must still run
            match job {
                Some(job) if is_ours => mine = Some(job),
                Some(job) => Self::run(job),
                None => {}
            }
        }
        // BRANCH: a worker (or the shutdown's final drain) already took it
        match mine {
            Some(job) => Err(job),
            None => Ok(()),
        }
    }

    /// Claims one registered sleeper (clears its bit) and unparks it. Lock-free; the lowest worker index first, so a
    /// lightly loaded pool keeps its higher workers asleep.
    fn wake_one(&self) {
        for (index, word) in self.sleeping.iter().enumerate() {
            let mut current = word.load(Ordering::Acquire);
            // BRANCH: some worker of this word is registered
            while current != 0 {
                let bit = current.trailing_zeros() as usize;
                match word.compare_exchange_weak(
                    current,
                    current & !(1u64 << bit),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    // BRANCH: claimed it; the worker set its thread before registering
                    Ok(_) => {
                        // BRANCH: always set by then; a worker that was not started yet needs no wake
                        if let Some(thread) = self.workers[index * 64 + bit].thread.get() {
                            thread.unpark();
                        }
                        return;
                    }
                    // BRANCH: another submitter claimed it, or the bit moved; look at the new word
                    Err(seen) => current = seen,
                }
            }
        }
    }

    #[inline]
    fn run(job: GkReadJob) {
        // A panicking job must not kill the worker. (With `panic = "abort"` this is a plain call.)
        let _ = catch_unwind(AssertUnwindSafe(job));
    }

    // ---- workers ----

    fn worker_main(self: Arc<Self>, index: usize) {
        // Before the first registration, so a claimed bit always has a thread behind it.
        let _ = self.workers[index].thread.set(thread::current());
        let mut after_timed_sleep = false;
        loop {
            let mut polls = 0;
            loop {
                // BRANCH: a chain of jobs
                if let Some(chain) = self.queue.try_reap() {
                    self.run_chain(chain, after_timed_sleep);
                    after_timed_sleep = false;
                    polls = 0;
                    continue;
                }
                // BRANCH: keep polling a while before sleeping
                if polls < self.spin_polls {
                    polls += 1;
                    std::hint::spin_loop();
                    continue;
                }
                break;
            }
            // Read the flag first: a push that finished before the close is visible to the reap below.
            let closed = self.state.load(Ordering::SeqCst) & CLOSED != 0;
            // BRANCH: closed: one last reap, then leave
            if closed {
                // BRANCH: a late chain
                if let Some(chain) = self.queue.try_reap() {
                    self.run_chain(chain, false);
                    continue;
                }
                break;
            }
            after_timed_sleep = self.sleep(index);
        }
        self.live.fetch_sub(1, Ordering::Release);
    }

    /// Runs the jobs of one reaped chain.
    fn run_chain(&self, chain: GkMpmcChain<'_, GkJobNode>, after_timed_sleep: bool) {
        let published = chain.published();
        // BRANCH: we shared a chain that others will only take once we stall: make sure someone is watching it
        if published {
            fence(Ordering::SeqCst);
        }
        // BRANCH: somebody is asleep: wake one if there may be more work than we take, or a chain to watch
        if self.state.load(Ordering::Relaxed) >> 1 > 0
            && (published || after_timed_sleep || self.queue.has_work())
        {
            self.wake_one();
        }
        let mut taken = 0;
        for node in chain {
            // Safety: reaped from the queue, so ours; every node in it came from `push`.
            let job = unsafe { open_node(node) };
            // BRANCH: bounded queue: free the place as soon as the job is out of the queue
            if self.max_queue.is_some() {
                self.pending.fetch_sub(1, Ordering::Relaxed);
            } else {
                taken += 1;
            }
            // BRANCH: always set; the option only exists so the job can be moved out of the node
            if let Some(job) = job {
                Self::run(job);
            }
        }
        // BRANCH: unbounded queue: one subtract for the whole chain
        if taken > 0 {
            self.pending.fetch_sub(taken, Ordering::Relaxed);
        }
    }

    /// Registers worker `index` as a sleeper, re-checks, parks, and unregisters. Returns whether the park was timed.
    fn sleep(&self, index: usize) -> bool {
        let word = &self.sleeping[index / 64];
        let bit = 1u64 << (index % 64);
        word.fetch_or(bit, Ordering::SeqCst);
        self.state.fetch_add(SLEEPER, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let mut timed = false;
        // BRANCH: work or shutdown arrived before we registered: do not park
        if !self.queue.has_work() && self.state.load(Ordering::SeqCst) & CLOSED == 0 {
            // BRANCH: a shared chain needs a rescuer, and nothing will wake us for it: park for about rescue_after
            if self.queue.shared_chains() > 0 {
                timed = true;
                thread::park_timeout(self.park_timeout);
            } else {
                thread::park();
            }
        }
        // The bit may already be cleared by whoever claimed us; the counter is always ours to undo.
        word.fetch_and(!bit, Ordering::SeqCst);
        self.state.fetch_sub(SLEEPER, Ordering::SeqCst);
        timed
    }

    // ---- shutdown ----

    /// Closes the pool, wakes and joins the workers, and runs what they left behind. Idempotent.
    fn shutdown(&self) {
        self.state.fetch_or(CLOSED, Ordering::SeqCst);
        // Pairs with the SeqCst RMW + fence in `sleep` and the `OnceLock` set of a starting worker: a worker either
        // sees CLOSED before it parks or has its thread visible here.
        fence(Ordering::SeqCst);
        let me = thread::current().id();
        let mut on_worker = false;
        for slot in self.workers.iter() {
            // BRANCH: this is a worker thread (a job dropped the last handle): it cannot join itself
            if let Some(thread) = slot.thread.get() {
                on_worker |= thread.id() == me;
                thread.unpark();
            }
        }
        for slot in self.workers.iter() {
            let handle = slot.join.swap(ptr::null_mut(), Ordering::AcqRel);
            // BRANCH: the first shutdown takes the handle
            if !handle.is_null() {
                // Safety: stored by `GkReaderPool::new` from `Box::into_raw`, and the swap made us its only owner.
                let handle = unsafe { Box::from_raw(handle) };
                // BRANCH: never join ourselves
                if handle.thread().id() != me {
                    let _ = handle.join();
                }
            }
        }
        // Another shutdown may have taken the handles; wait until the workers have all exited.
        let own = usize::from(on_worker);
        while self.live.load(Ordering::Acquire) > own {
            thread::sleep(Duration::from_micros(50));
        }
        // Nothing should be left, but a late push may have slipped past the workers' last reap: run it here.
        self.queue.drain(|node| {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            // Safety: drained from the queue, so ours.
            // BRANCH: always set
            if let Some(job) = unsafe { open_node(node) } {
                Self::run(job);
            }
        });
    }
}

impl Drop for GkPoolInner {
    fn drop(&mut self) {
        for slot in self.workers.iter_mut() {
            let handle = *slot.join.get_mut();
            // BRANCH: a handle nobody joined (the pool was dropped without a shutdown, which cannot normally happen)
            if !handle.is_null() {
                // Safety: from `Box::into_raw`, and `&mut self` makes us the only owner.
                drop(unsafe { Box::from_raw(handle) });
            }
        }
    }
}

// ==== Pool Definition ====

/// A pool of reader threads over a lock-free job queue. See the [module docs](self) for the design.
///
/// Cloning makes another handle to the same pool (an `Arc` inside). The pool shuts down when
/// [`shutdown`](Self::shutdown) is called or when the last handle is dropped, whichever comes first; the workers
/// drain what is queued before they exit.
///
/// Every thread that submits holds one producer lane into the pool until that thread exits (see
/// [`GkReaderPoolConfig::max_lanes`]).
pub struct GkReaderPool {
    inner: Arc<GkPoolInner>,
}

impl GkReaderPool {
    /// Validates `config`, creates the queue and starts the workers.
    pub fn new(config: GkReaderPoolConfig) -> Result<Self, GkReaderPoolError> {
        config.validate()?;
        let threads = config.resolved_threads();
        let queue = GkTreiberMpmc::<GkJobNode>::with_config(GkTreiberMpmcConfig {
            max_lanes: config.max_lanes,
            max_chains: MIN_CHAIN_SLOTS.max(threads * CHAIN_SLOTS_PER_WORKER),
            rescue_after: config.rescue_after,
        })
        .map_err(|_| GkReaderPoolError::INVALID_CONFIG)?;
        let inner = Arc::new_cyclic(|me| GkPoolInner {
            id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            me: me.clone(),
            queue,
            state: CachePadded::new(AtomicUsize::new(0)),
            pending: CachePadded::new(AtomicUsize::new(0)),
            sleeping: (0..threads.div_ceil(64))
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
            workers: (0..threads)
                .map(|_| GkWorkerSlot {
                    thread: OnceLock::new(),
                    join: AtomicPtr::new(ptr::null_mut()),
                })
                .collect(),
            threads,
            spin_polls: config.spin_polls,
            park_timeout: if config.rescue_after.is_zero() {
                SHARING_PARK_TIMEOUT
            } else {
                config.rescue_after.max(MIN_PARK_TIMEOUT)
            },
            max_queue: config.max_queue,
            handles: AtomicUsize::new(1),
            live: AtomicUsize::new(0),
        });
        for index in 0..threads {
            inner.live.fetch_add(1, Ordering::Relaxed);
            let worker = Arc::clone(&inner);
            match config
                .thread
                .spawn_thread(index, move || worker.worker_main(index))
            {
                Ok(handle) => {
                    let handle = Box::into_raw(Box::new(handle));
                    inner.workers[index].join.store(handle, Ordering::Release);
                }
                // BRANCH: the OS refused a thread: stop the ones already running
                Err(_) => {
                    inner.live.fetch_sub(1, Ordering::Relaxed);
                    inner.shutdown();
                    return Err(GkReaderPoolError::SPAWN_FAILED);
                }
            }
        }
        Ok(Self { inner })
    }

    /// Number of worker threads.
    pub fn threads(&self) -> usize {
        self.inner.threads
    }

    /// Jobs submitted and not yet taken by a worker. Approximate: it is read without synchronization and a chain's
    /// jobs are counted until the worker takes them.
    pub fn pending(&self) -> usize {
        self.inner.pending.load(Ordering::Relaxed)
    }

    /// Whether [`shutdown`](Self::shutdown) has been called (on any handle).
    pub fn is_shutdown(&self) -> bool {
        self.inner.is_closed()
    }

    /// Stops accepting jobs, lets the workers run everything already queued, and joins them. Idempotent; callable from
    /// any handle. Called from inside a job (a worker thread), it does not wait for that worker itself.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl GkReaderExecutor for GkReaderPool {
    /// Queues `job`. `Err(job)` when the pool is shut down, `max_queue` jobs are pending, or this thread cannot get a
    /// producer lane (`max_lanes` threads already hold one): the caller then runs the job itself.
    fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob> {
        self.inner.submit(job)
    }

    fn name(&self) -> &str {
        "treiber-pool"
    }
}

impl Clone for GkReaderPool {
    fn clone(&self) -> Self {
        self.inner.handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for GkReaderPool {
    fn drop(&mut self) {
        // BRANCH: the last handle shuts the pool down
        if self.inner.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.shutdown();
        }
    }
}

impl fmt::Debug for GkReaderPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GkReaderPool")
            .field("threads", &self.threads())
            .field("pending", &self.pending())
            .field("shutdown", &self.is_shutdown())
            .finish()
    }
}
