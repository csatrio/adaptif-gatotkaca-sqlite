// ============================================================================
// Import Definition
// ============================================================================

use std::fmt;
use std::io;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Executor Definition
// ============================================================================

/// A unit of read work: runs once, on whichever thread the executor picks.
pub type GkReadJob = Box<dyn FnOnce() + Send + 'static>;

/// Where read jobs run. The SQLite engine hands every read to one of these; the default is the lock-free
/// [`GkReaderPool`](crate::pool::GkReaderPool), and an application can plug in its own runtime or pool.
///
/// # Contract of [`spawn`](Self::spawn)
///
/// - `Ok(())`: the executor took the job and will run it exactly once, on some thread, now or later.
/// - `Err(job)`: the executor did **not** accept the job (shut down, producer lanes exhausted, queue full, ...) and
///   gave it back untouched. The caller must then run the job itself, inline on the calling thread. A job is never
///   both accepted and returned, and never dropped without running.
///
/// `spawn` must not block for long. It normally returns before the job has run (the [`GkInlineExecutor`] is the
/// deliberate exception: it runs the job before returning `Ok`).
pub trait GkReaderExecutor: Send + Sync + 'static {
    /// Offers `job` to the executor. See the [trait docs](GkReaderExecutor) for the `Err(job)` contract.
    fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob>;

    /// A short name for logs and diagnostics.
    fn name(&self) -> &str {
        "custom"
    }
}

/// Runs every job immediately on the calling thread and always accepts it. No threads, no queue: reads are as parallel
/// as the callers are.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GkInlineExecutor;

impl GkReaderExecutor for GkInlineExecutor {
    fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob> {
        job();
        Ok(())
    }

    fn name(&self) -> &str {
        "inline"
    }
}

// ============================================================================
// Thread Options Definition
// ============================================================================

/// A hook called on a worker thread with the worker's index.
pub type GkThreadHook = Arc<dyn Fn(usize) + Send + Sync + 'static>;

/// How the engine's own threads (reader workers, the writer) are created: name, stack size and start/stop hooks.
///
/// The hooks are how an application pins a worker to a CPU core, sets a priority or registers the thread with a
/// profiler, without this crate depending on an affinity library: `on_start` runs on the new thread before its work,
/// `on_stop` on the same thread after it (also when the work panics).
///
/// ```text
/// use adaptif_gatotkaca_sqlite::executor::GkThreadOptions;
/// use std::sync::Arc;
///
/// let options = GkThreadOptions::default()
///     .named("my-reader")
///     .stack_size(512 * 1024)
///     .on_start(Arc::new(|index| {
///         let _ = index; // pin to core `index` here
///     }));
/// let handle = options.spawn_thread(3, || {}).unwrap();
/// handle.join().unwrap();
/// ```
#[derive(Clone)]
pub struct GkThreadOptions {
    prefix: String,
    stack_size: Option<usize>,
    on_start: Option<GkThreadHook>,
    on_stop: Option<GkThreadHook>,
}

impl Default for GkThreadOptions {
    /// Threads named `worker-{index}`, the platform's default stack size, no hooks.
    fn default() -> Self {
        Self {
            prefix: String::from("worker"),
            stack_size: None,
            on_start: None,
            on_stop: None,
        }
    }
}

impl fmt::Debug for GkThreadOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GkThreadOptions")
            .field("prefix", &self.prefix)
            .field("stack_size", &self.stack_size)
            .field("on_start", &self.on_start.is_some())
            .field("on_stop", &self.on_stop.is_some())
            .finish()
    }
}

impl GkThreadOptions {
    /// Sets the name prefix: thread `index` is named `"{prefix}-{index}"`.
    pub fn named(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Sets the stack size in bytes (the platform may round it up).
    pub fn stack_size(mut self, bytes: usize) -> Self {
        self.stack_size = Some(bytes);
        self
    }

    /// Sets the hook run on the new thread, with its index, before its work.
    pub fn on_start(mut self, hook: GkThreadHook) -> Self {
        self.on_start = Some(hook);
        self
    }

    /// Sets the hook run on the thread, with its index, after its work (also when that unwinds).
    pub fn on_stop(mut self, hook: GkThreadHook) -> Self {
        self.on_stop = Some(hook);
        self
    }

    /// The name prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The configured stack size, if any.
    pub fn stack_size_bytes(&self) -> Option<usize> {
        self.stack_size
    }

    /// Spawns thread number `index` running `f`: named `"{prefix}-{index}"`, with the configured stack size, `on_start`
    /// before `f` and `on_stop` after it.
    pub fn spawn_thread<F: FnOnce() + Send + 'static>(
        &self,
        index: usize,
        f: F,
    ) -> io::Result<thread::JoinHandle<()>> {
        let mut builder = thread::Builder::new().name(format!("{}-{}", self.prefix, index));
        // BRANCH: a stack size was configured
        if let Some(bytes) = self.stack_size {
            builder = builder.stack_size(bytes);
        }
        let on_start = self.on_start.clone();
        let on_stop = self.on_stop.clone();
        builder.spawn(move || {
            // The stop hook runs from the guard's drop, so it also runs when `f` unwinds.
            let _stop = GkStopGuard {
                hook: on_stop,
                index,
            };
            // BRANCH: a start hook was configured
            if let Some(hook) = on_start {
                hook(index);
            }
            f();
        })
    }
}

/// Runs the stop hook when the thread's work ends, normally or by unwinding.
struct GkStopGuard {
    hook: Option<GkThreadHook>,
    index: usize,
}

impl Drop for GkStopGuard {
    fn drop(&mut self) {
        // BRANCH: a stop hook was configured
        if let Some(hook) = self.hook.take() {
            hook(self.index);
        }
    }
}
