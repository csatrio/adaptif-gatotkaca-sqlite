// ============================================================================
// Import Definition
// ============================================================================

use crate::executor::{GkReadJob, GkReaderExecutor};
use tokio::runtime::Handle;

// ============================================================================
// Tokio Executor Definition
// ============================================================================

/// Runs read jobs on a tokio runtime's blocking pool (`spawn_blocking`).
///
/// Reader connections are thread-local, and tokio's blocking threads are created on demand and exit after
/// their keep-alive, so each such thread opens its own connection lazily and closes it on exit. Bound the
/// number of connections with the runtime's `max_blocking_threads`; if reads are frequent, raise
/// `thread_keep_alive` so connections are reused.
///
/// If the runtime is shut down, the job is dropped and the awaiting future resolves to
/// `ReaderUnavailable`.
#[derive(Debug, Clone)]
pub struct GkTokioExecutor {
    handle: Handle,
}

impl GkTokioExecutor {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    /// Uses the runtime the caller is running in. Panics outside a tokio runtime, like `Handle::current`.
    pub fn current() -> Self {
        Self::new(Handle::current())
    }
}

impl GkReaderExecutor for GkTokioExecutor {
    fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob> {
        // The returned JoinHandle is dropped on purpose: the job reports through its own reply.
        drop(self.handle.spawn_blocking(job));
        Ok(())
    }

    fn name(&self) -> &str {
        "tokio-spawn_blocking"
    }
}
