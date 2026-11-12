//! Pluggable writer queue: the gatotkaca MPSC channel (the standard) or crossbeam (cargo feature `crossbeam-channel`).
//!
//! [`writer_queue`] returns a cloneable, `Send + Sync` producing end ([`GkWriterTx`]) and a single receiving end
//! ([`GkWriterRx`]). The message type is generic and the two backends are dispatched by a `match` on a small private
//! enum, never by generics or `dyn`, so the send path stays a direct call.
//!
//! # Treiber backend
//!
//! The gatotkaca channel wants one `GkSender` per producing thread (a slab of `capacity` nodes each; with the
//! crate's 224-byte `GkSqliteMsg` that is about 237 KiB at the default capacity of 1024). [`GkWriterTx`] hides that behind a thread-local map keyed by the queue id:
//!
//! - the first `send` from a thread allocates that thread's sender (the only allocation on the send path, once per
//!   thread and queue); every later `send` is a hash lookup in the thread-local map and the channel's own push;
//! - when a thread creates a sender for a new queue it first drops the senders it holds for queues whose receiver is
//!   gone, so the slabs of dead databases are freed (a thread that never creates another sender keeps its stale
//!   slabs until it exits);
//! - [`GkWriterTx::release_thread_sender`] frees the calling thread's sender for one queue at once (the database calls
//!   it after the writer has stopped); call it only when nothing is in flight from this thread, because a new sender is
//!   a new thread channel and the receiver may take its first message before older ones still queued in the old one;
//! - a sender dropped with its thread still delivers what it already sent.
//!
//! Ordering: Treiber keeps FIFO per producing thread; across threads there is no global arrival order (crossbeam is
//! globally ordered). Writes that must be ordered across threads need the earlier reply first.
//!
//! The Treiber capacity is a bound per producing thread, not a global bound: each thread can have `capacity` messages
//! the receiver has not taken yet, and once its slab is full `send` waits (spin, then yield) for the receiver. The
//! whole queue can therefore hold `producers * capacity` messages. Capacity is clamped to
//! `1..=65_536` nodes per thread. Crossbeam uses `bounded(capacity)` for the whole queue.
//!
//! # Where to create the receiver
//!
//! Anywhere. The default `GkThreadWaker` records the creating thread but refreshes it in `prepare()` before every
//! park, and a producer only unparks a waker that was published after `prepare()`, so the receiver may be created on
//! one thread and moved to the writer thread. Creating it on the writer thread is still the cleanest.
//!
//! # Disconnect
//!
//! The raw channel reports `DISCONNECTED` when every `GkSender` is gone, but the thread-local senders here outlive any
//! [`GkWriterTx`]. The queue therefore counts [`GkWriterTx`] handles itself: [`GkWriterRx::recv`] returns
//! [`GkQueueError::DISCONNECTED`] once every handle (and clone) is dropped and the queue is drained. For the Treiber
//! backend the receiver notices this within [`TREIBER_DISCONNECT_POLL`] while idle. The receiver also owns one
//! keep-alive sender so the raw channel never reports a disconnect before the first producer sent.

// ============================================================================
// Import Definition
// ============================================================================

use crate::SqliteFastU64Map;
use adaptif_gatotkaca_utils::{
    GkReceiver, GkSender, GkTreiberChannel, GkTreiberChannelConfig, GkTreiberChannelError,
};
use std::{
    any::Any,
    cell::RefCell,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

// ============================================================================
// Constants
// ============================================================================

/// Default queue capacity, today's `writer_queue_size`.
pub const DEFAULT_WRITER_QUEUE_CAPACITY: usize = 1024;

/// Most nodes one producing thread may own in the Treiber backend (the capacity is clamped to this).
pub const TREIBER_MAX_SLAB_NODES: usize = 65_536;

/// Longest an idle Treiber receiver sleeps before it checks whether every [`GkWriterTx`] is gone.
pub const TREIBER_DISCONNECT_POLL: Duration = Duration::from_millis(25);

static QUEUE_ID: AtomicU64 = AtomicU64::new(1);

// ============================================================================
// Error Definition
// ============================================================================

/// Why a queue call failed.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct GkQueueError(u8);

impl GkQueueError {
    /// The receiver is gone.
    pub const CLOSED: Self = Self(1);
    /// Every [`GkWriterTx`] is gone and nothing is left to receive.
    pub const DISCONNECTED: Self = Self(2);
    /// `recv_timeout` found nothing in time.
    pub const TIMEOUT: Self = Self(3);
    /// The channel's memory had no room.
    pub const OUT_OF_MEMORY: Self = Self(4);
    /// The configuration cannot be used (capacity 0, or rejected by the channel).
    pub const INVALID_CONFIG: Self = Self(5);

    /// The numeric code of the error.
    #[inline(always)]
    pub const fn code(self) -> u8 {
        self.0
    }

    /// A short description of the error.
    pub const fn message(self) -> &'static str {
        // BRANCH: one arm per named constant
        match self.0 {
            1 => "the receiver is gone",
            2 => "every sender is gone and the queue is empty",
            3 => "the receive timed out",
            4 => "the channel has no memory left",
            5 => "invalid queue configuration",
            _ => "unknown queue error",
        }
    }
}

impl fmt::Debug for GkQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GkQueueError({}: {})", self.0, self.message())
    }
}

impl fmt::Display for GkQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for GkQueueError {}

impl From<GkTreiberChannelError> for GkQueueError {
    fn from(err: GkTreiberChannelError) -> Self {
        // BRANCH: map each channel error to the queue error with the same meaning
        match err {
            GkTreiberChannelError::CLOSED => Self::CLOSED,
            GkTreiberChannelError::DISCONNECTED => Self::DISCONNECTED,
            GkTreiberChannelError::TIMEOUT => Self::TIMEOUT,
            GkTreiberChannelError::OUT_OF_MEMORY => Self::OUT_OF_MEMORY,
            // BRANCH: INVALID_CONFIG and the misuse errors (receiver or sender taken, too many lanes) cannot be
            // acted on by the caller of a queue
            _ => Self::INVALID_CONFIG,
        }
    }
}

// ============================================================================
// Configuration Definition
// ============================================================================

/// Which channel carries the writer queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum GkChannelKind {
    /// The gatotkaca MPSC channel (`GkTreiberChannel`). The default.
    #[default]
    Treiber,
    /// A crossbeam bounded channel.
    #[cfg(feature = "crossbeam-channel")]
    Crossbeam,
}

/// Writer queue configuration.
#[derive(Clone, Debug)]
pub struct GkWriterQueueConfig {
    /// The channel backend.
    pub kind: GkChannelKind,
    /// Crossbeam: the bound of the whole queue. Treiber: the bound of each producing thread (clamped to
    /// `1..=`[`TREIBER_MAX_SLAB_NODES`]). 0 is rejected with [`GkQueueError::INVALID_CONFIG`].
    pub capacity: usize,
}

impl Default for GkWriterQueueConfig {
    fn default() -> Self {
        Self {
            kind: GkChannelKind::default(),
            capacity: DEFAULT_WRITER_QUEUE_CAPACITY,
        }
    }
}

impl GkWriterQueueConfig {
    /// Sets [`kind`](Self::kind).
    pub fn with_kind(mut self, kind: GkChannelKind) -> Self {
        self.kind = kind;
        self
    }

    /// Sets [`capacity`](Self::capacity).
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }
}

// ============================================================================
// Shared State Definition
// ============================================================================

/// State shared by every handle of one queue. Touched only on handle creation, clone and drop, never per message.
struct GkQueueShared {
    id: u64,
    /// Set when the receiver drops; the thread-local sweep frees senders of closed queues.
    closed: AtomicBool,
    /// Live [`GkWriterTx`] handles.
    senders: AtomicUsize,
}

// ============================================================================
// Thread-Local Senders
// ============================================================================

/// One thread's sender for one queue, type-erased because the map holds queues of different message types.
struct GkTlsSender {
    shared: Arc<GkQueueShared>,
    sender: Box<dyn Any>,
}

thread_local! {
    static SENDERS: RefCell<SqliteFastU64Map<GkTlsSender>> = RefCell::new(SqliteFastU64Map::new());
}

/// What the thread-local send path did with the value left in the caller's slot.
enum GkTlsOutcome {
    /// Sent; the slot is empty.
    Sent,
    /// The receiver is gone; the value is still in the slot.
    Closed,
    /// No thread-local sender could be used (re-entrant call, or thread-local storage already destroyed); the value
    /// is still in the slot.
    Unusable,
}

#[inline(always)]
fn send_with<T: Send + 'static>(sender: &mut GkSender<T>, slot: &mut Option<T>) -> GkTlsOutcome {
    // BRANCH: the slot always holds the value here
    let Some(value) = slot.take() else {
        return GkTlsOutcome::Closed;
    };
    // BRANCH: the receiver is gone, take the value back
    if let Err(back) = sender.send(value) {
        *slot = Some(back);
        return GkTlsOutcome::Closed;
    }
    GkTlsOutcome::Sent
}

/// First send of this thread to this queue: sweep dead queues, create the sender, send.
#[cold]
#[inline(never)]
fn create_and_send<T: Send + 'static>(
    map: &mut SqliteFastU64Map<GkTlsSender>,
    channel: &GkTreiberChannel<T>,
    shared: &Arc<GkQueueShared>,
    slot: &mut Option<T>,
    dead: &mut Vec<GkTlsSender>,
) -> GkTlsOutcome {
    // Dead entries leave the map here but are dropped by the caller after the borrow ends: dropping a sender with a
    // closed receiver drops queued messages, and a message's `Drop` may send again on this thread.
    dead.extend(
        map.extract_if(|_, entry| entry.shared.closed.load(Ordering::Acquire))
            .map(|(_, entry)| entry),
    );
    // BRANCH: the receiver is gone or the channel has no memory
    let Ok(mut sender) = channel.sender() else {
        return GkTlsOutcome::Closed;
    };
    let outcome = send_with(&mut sender, slot);
    map.insert(
        shared.id,
        GkTlsSender {
            shared: shared.clone(),
            sender: Box::new(sender),
        },
    );
    outcome
}

#[inline]
fn send_treiber<T: Send + 'static>(
    channel: &GkTreiberChannel<T>,
    shared: &Arc<GkQueueShared>,
    value: T,
) -> Result<(), T> {
    let id = shared.id;
    let mut slot = Some(value);
    let mut dead: Vec<GkTlsSender> = Vec::new();
    let outcome = SENDERS.try_with(|cell| {
        // BRANCH: the map is borrowed already (a send re-entered from inside a send on this thread)
        let Ok(mut map) = cell.try_borrow_mut() else {
            return GkTlsOutcome::Unusable;
        };
        // BRANCH: this thread already has a sender for the queue (the hot path)
        if let Some(entry) = map.get_mut(&id) {
            return match entry.sender.downcast_mut::<GkSender<T>>() {
                Some(sender) => send_with(sender, &mut slot),
                // BRANCH: ids are unique, so the type always matches
                None => GkTlsOutcome::Unusable,
            };
        }
        create_and_send(&mut map, channel, shared, &mut slot, &mut dead)
    });
    // The borrow is over: dead senders may run message destructors now.
    drop(dead);
    match outcome {
        // BRANCH: delivered
        Ok(GkTlsOutcome::Sent) => Ok(()),
        // BRANCH: the receiver is gone
        Ok(GkTlsOutcome::Closed) => slot.map_or(Ok(()), Err),
        // BRANCH: no thread-local sender: use a short-lived one. Dropping it delivers what it sent.
        Ok(GkTlsOutcome::Unusable) | Err(_) => match slot {
            Some(value) => match channel.sender() {
                Ok(mut sender) => sender.send(value),
                Err(_) => Err(value),
            },
            None => Ok(()),
        },
    }
}

// ============================================================================
// Producing End Definition
// ============================================================================

enum GkTxInner<T: Send + 'static> {
    Treiber(GkTreiberChannel<T>),
    #[cfg(feature = "crossbeam-channel")]
    Crossbeam(crossbeam_channel::Sender<T>),
}

/// The producing end of a writer queue: `Clone + Send + Sync`, usable from any thread.
pub struct GkWriterTx<T: Send + 'static> {
    inner: GkTxInner<T>,
    shared: Arc<GkQueueShared>,
}

impl<T: Send + 'static> GkWriterTx<T> {
    /// Sends `value`, waiting while the queue (Treiber: this thread's share of it) is full. Gives the value back when
    /// the receiver is gone (and, for Treiber, when the channel has no memory for a new thread's sender).
    #[inline]
    pub fn send(&self, value: T) -> Result<(), T> {
        // BRANCH: one arm per backend
        match &self.inner {
            GkTxInner::Treiber(channel) => send_treiber(channel, &self.shared, value),
            #[cfg(feature = "crossbeam-channel")]
            GkTxInner::Crossbeam(sender) => sender.send(value).map_err(|err| err.0),
        }
    }

    /// Frees the calling thread's sender (and its slab) for this queue. Messages it already sent are still delivered.
    /// Other threads keep theirs until they exit or sweep. Does nothing for crossbeam or when the thread has none.
    pub fn release_thread_sender(&self) {
        // BRANCH: only the Treiber backend keeps per-thread state
        match &self.inner {
            GkTxInner::Treiber(_) => {
                let id = self.shared.id;
                let removed = SENDERS
                    .try_with(|cell| {
                        cell.try_borrow_mut()
                            .ok()
                            .and_then(|mut map| map.remove(&id))
                    })
                    .ok()
                    .flatten();
                // Dropped after the borrow ended: a message destructor may send on this thread.
                drop(removed);
            }
            #[cfg(feature = "crossbeam-channel")]
            GkTxInner::Crossbeam(_) => {}
        }
    }

    /// The backend of this queue.
    pub fn kind(&self) -> GkChannelKind {
        // BRANCH: one arm per backend
        match &self.inner {
            GkTxInner::Treiber(_) => GkChannelKind::Treiber,
            #[cfg(feature = "crossbeam-channel")]
            GkTxInner::Crossbeam(_) => GkChannelKind::Crossbeam,
        }
    }

    /// The unique id of the queue.
    pub fn queue_id(&self) -> u64 {
        self.shared.id
    }
}

impl<T: Send + 'static> Clone for GkWriterTx<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            // BRANCH: one arm per backend
            inner: match &self.inner {
                GkTxInner::Treiber(channel) => GkTxInner::Treiber(channel.clone()),
                #[cfg(feature = "crossbeam-channel")]
                GkTxInner::Crossbeam(sender) => GkTxInner::Crossbeam(sender.clone()),
            },
            shared: self.shared.clone(),
        }
    }
}

impl<T: Send + 'static> Drop for GkWriterTx<T> {
    fn drop(&mut self) {
        // Release: everything this handle's thread sent is visible to a receiver that sees the count reach 0.
        self.shared.senders.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T: Send + 'static> fmt::Debug for GkWriterTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GkWriterTx")
            .field("queue_id", &self.shared.id)
            .field("kind", &self.kind())
            .finish()
    }
}

// ============================================================================
// Receiving End Definition
// ============================================================================

enum GkRxInner<T: Send + 'static> {
    Treiber {
        receiver: GkReceiver<T>,
        /// Keeps the raw channel's sender count above 0, so it never reports a disconnect before the first producer
        /// thread sent. Dropped after `receiver`.
        _keepalive: GkSender<T>,
    },
    #[cfg(feature = "crossbeam-channel")]
    Crossbeam(crossbeam_channel::Receiver<T>),
}

/// The single receiving end of a writer queue. `Send`, not `Sync`; dropping it closes the queue.
pub struct GkWriterRx<T: Send + 'static> {
    inner: GkRxInner<T>,
    shared: Arc<GkQueueShared>,
}

impl<T: Send + 'static> GkWriterRx<T> {
    /// Waits for the next message. [`GkQueueError::DISCONNECTED`] once every [`GkWriterTx`] is dropped and the queue
    /// is empty.
    pub fn recv(&mut self) -> Result<T, GkQueueError> {
        // BRANCH: one arm per backend
        match &mut self.inner {
            GkRxInner::Treiber { receiver, .. } => recv_treiber(receiver, &self.shared, None),
            #[cfg(feature = "crossbeam-channel")]
            GkRxInner::Crossbeam(receiver) => {
                receiver.recv().map_err(|_| GkQueueError::DISCONNECTED)
            }
        }
    }

    /// Like [`recv`](Self::recv) but gives up with [`GkQueueError::TIMEOUT`] after `timeout`.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, GkQueueError> {
        // BRANCH: one arm per backend
        match &mut self.inner {
            GkRxInner::Treiber { receiver, .. } => {
                recv_treiber(receiver, &self.shared, Some(Instant::now() + timeout))
            }
            #[cfg(feature = "crossbeam-channel")]
            GkRxInner::Crossbeam(receiver) => {
                receiver.recv_timeout(timeout).map_err(|err| match err {
                    crossbeam_channel::RecvTimeoutError::Timeout => GkQueueError::TIMEOUT,
                    crossbeam_channel::RecvTimeoutError::Disconnected => GkQueueError::DISCONNECTED,
                })
            }
        }
    }

    /// The next message if one is ready.
    #[inline]
    pub fn try_recv(&mut self) -> Option<T> {
        // BRANCH: one arm per backend
        match &mut self.inner {
            GkRxInner::Treiber { receiver, .. } => receiver.try_recv(),
            #[cfg(feature = "crossbeam-channel")]
            GkRxInner::Crossbeam(receiver) => receiver.try_recv().ok(),
        }
    }

    /// The backend of this queue.
    pub fn kind(&self) -> GkChannelKind {
        // BRANCH: one arm per backend
        match &self.inner {
            GkRxInner::Treiber { .. } => GkChannelKind::Treiber,
            #[cfg(feature = "crossbeam-channel")]
            GkRxInner::Crossbeam(_) => GkChannelKind::Crossbeam,
        }
    }

    /// The unique id of the queue.
    pub fn queue_id(&self) -> u64 {
        self.shared.id
    }
}

impl<T: Send + 'static> Drop for GkWriterRx<T> {
    fn drop(&mut self) {
        // Release: a thread that sees `closed` in its sweep also sees everything before the drop.
        self.shared.closed.store(true, Ordering::Release);
    }
}

impl<T: Send + 'static> fmt::Debug for GkWriterRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GkWriterRx")
            .field("queue_id", &self.shared.id)
            .field("kind", &self.kind())
            .finish()
    }
}

/// Treiber receive: the raw receiver sleeps in slices so it can notice that every [`GkWriterTx`] is gone (the raw
/// channel only knows its own senders, which live as long as their threads).
fn recv_treiber<T: Send + 'static>(
    receiver: &mut GkReceiver<T>,
    shared: &GkQueueShared,
    deadline: Option<Instant>,
) -> Result<T, GkQueueError> {
    // BRANCH: a message is ready (the hot path, no clock read)
    if let Some(value) = receiver.try_recv() {
        return Ok(value);
    }
    loop {
        // BRANCH: every handle is gone; whatever they sent before is visible now
        if shared.senders.load(Ordering::Acquire) == 0 {
            return receiver.try_recv().ok_or(GkQueueError::DISCONNECTED);
        }
        let slice = match deadline {
            // BRANCH: no deadline, sleep a whole slice
            None => TREIBER_DISCONNECT_POLL,
            Some(deadline) => {
                let left = deadline.saturating_duration_since(Instant::now());
                // BRANCH: out of time; one last look
                if left.is_zero() {
                    return receiver.try_recv().ok_or(GkQueueError::TIMEOUT);
                }
                left.min(TREIBER_DISCONNECT_POLL)
            }
        };
        match receiver.recv_timeout(slice) {
            Ok(value) => return Ok(value),
            // BRANCH: nothing yet; check the handles and the deadline again
            Err(GkTreiberChannelError::TIMEOUT) => {}
            Err(err) => return Err(err.into()),
        }
    }
}

// ============================================================================
// Construction
// ============================================================================

/// Creates a writer queue. See the module documentation for the meaning of `capacity` per backend and for where to
/// create the receiver.
pub fn writer_queue<T: Send + 'static>(
    config: &GkWriterQueueConfig,
) -> Result<(GkWriterTx<T>, GkWriterRx<T>), GkQueueError> {
    // BRANCH: a queue that holds nothing can never accept a message
    if config.capacity == 0 {
        return Err(GkQueueError::INVALID_CONFIG);
    }
    let shared = Arc::new(GkQueueShared {
        id: QUEUE_ID.fetch_add(1, Ordering::Relaxed),
        closed: AtomicBool::new(false),
        senders: AtomicUsize::new(1),
    });
    // BRANCH: one arm per backend
    let (tx, rx) = match config.kind {
        GkChannelKind::Treiber => {
            let raw = GkTreiberChannelConfig::DEFAULT
                .with_bounded(true)
                .with_slab_nodes(config.capacity.clamp(1, TREIBER_MAX_SLAB_NODES));
            let channel = GkTreiberChannel::<T>::with_config(raw)?;
            let keepalive = channel.sender()?;
            let receiver = channel.receiver()?;
            (
                GkTxInner::Treiber(channel),
                GkRxInner::Treiber {
                    receiver,
                    _keepalive: keepalive,
                },
            )
        }
        #[cfg(feature = "crossbeam-channel")]
        GkChannelKind::Crossbeam => {
            let (sender, receiver) = crossbeam_channel::bounded(config.capacity);
            (GkTxInner::Crossbeam(sender), GkRxInner::Crossbeam(receiver))
        }
    };
    Ok((
        GkWriterTx {
            inner: tx,
            shared: shared.clone(),
        },
        GkWriterRx { inner: rx, shared },
    ))
}
