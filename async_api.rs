// ============================================================================
// Import Definition
// ============================================================================

use crate::{
    GkSqlText, GkSqliteDb, GkSqliteParams, GkSqliteRowRecord, GkSqliteWriteOutcome,
    error::GkSqliteErrorEnum,
    executor::GkReadJob,
    reader, records_from_conn,
    reply::{GkReplyReceiver, reply_channel},
    router::is_explicit_transaction_sql,
    writer::{GkSqliteMsg, GkSqliteReadCommand, GkSqliteWriteCommand},
};
use rusqlite::{Connection, Row};
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};

// ============================================================================
// Future Definition
// ============================================================================

/// Which side died when a reply never arrives.
#[derive(Debug, Clone, Copy)]
enum GkDeadKind {
    Writer,
    Reader,
}

enum GkFutureState<T> {
    Ready(Option<Result<T, GkSqliteErrorEnum>>),
    Pending {
        rx: GkReplyReceiver<Result<T, GkSqliteErrorEnum>>,
        dead: GkDeadKind,
    },
}

/// Result of an `*_async` call. It is a plain `std::future::Future` driven by `std::task::Waker`, so it
/// works on tokio, smol or any executor, and `wait()` blocks a thread instead.
///
/// The work starts when the method is called, not when the future is first polled; dropping the future
/// does not cancel it.
pub struct GkSqliteFuture<T> {
    state: GkFutureState<T>,
}

impl<T> Unpin for GkSqliteFuture<T> {}

impl<T: Send> GkSqliteFuture<T> {
    fn ready(result: Result<T, GkSqliteErrorEnum>) -> Self {
        Self {
            state: GkFutureState::Ready(Some(result)),
        }
    }

    fn pending(rx: GkReplyReceiver<Result<T, GkSqliteErrorEnum>>, dead: GkDeadKind) -> Self {
        Self {
            state: GkFutureState::Pending { rx, dead },
        }
    }

    /// Blocks the calling thread until the result is available.
    pub fn wait(self) -> Result<T, GkSqliteErrorEnum> {
        // BRANCH: already finished, or wait on the reply
        match self.state {
            GkFutureState::Ready(mut r) => r
                .take()
                .unwrap_or(Err(GkSqliteErrorEnum::ReaderUnavailable)),
            GkFutureState::Pending { rx, dead } => {
                rx.wait().unwrap_or_else(|_| Err(dead_error(dead)))
            }
        }
    }
}

fn dead_error(dead: GkDeadKind) -> GkSqliteErrorEnum {
    // BRANCH: the writer died, or the reader executor dropped the job
    match dead {
        GkDeadKind::Writer => GkSqliteErrorEnum::WriterDead,
        GkDeadKind::Reader => GkSqliteErrorEnum::ReaderUnavailable,
    }
}

impl<T: Send> Future for GkSqliteFuture<T> {
    type Output = Result<T, GkSqliteErrorEnum>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // BRANCH: finished at call time, or poll the reply
        match &mut this.state {
            GkFutureState::Ready(r) => Poll::Ready(
                r.take()
                    .unwrap_or(Err(GkSqliteErrorEnum::ReaderUnavailable)),
            ),
            GkFutureState::Pending { rx, dead } => match Pin::new(rx).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(r)) => Poll::Ready(r),
                Poll::Ready(Err(_)) => Poll::Ready(Err(dead_error(*dead))),
            },
        }
    }
}

// ============================================================================
// Async Methods Definition
// ============================================================================

impl GkSqliteDb {
    /// Runs `f` on a reader connection through the configured read mode (`Inline`, the built-in pool, or a
    /// custom executor). `f` and its result must be `'static + Send` because they may cross threads.
    /// A job the executor rejects (shut down, full, lanes exhausted) runs inline on the calling thread.
    ///
    /// Async reads do not wait for earlier writes of this thread; `await` the write first (its
    /// acknowledgement already implies the commit).
    pub fn read<T, F>(&self, f: F) -> GkSqliteFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, GkSqliteErrorEnum> + Send + 'static,
    {
        let (tx, rx) = reply_channel();
        let inner = self.inner.clone();
        let job: GkReadJob = Box::new(move || {
            let result = inner.with_reader(f);
            tx.send(result);
        });
        // BRANCH: where the job runs
        match &self.inner.executor {
            // BRANCH: inline mode runs on the calling thread
            None => job(),
            Some(executor) => {
                // BRANCH: rejected jobs run on the calling thread
                if let Err(job) = executor.spawn(job) {
                    job();
                }
            }
        }
        GkSqliteFuture::pending(rx, GkDeadKind::Reader)
    }

    /// Async `execute`: write statements go to the writer thread, read-only ones to the read mode.
    pub fn execute_async(
        &self,
        sql: impl Into<GkSqlText>,
        params: impl Into<GkSqliteParams>,
    ) -> GkSqliteFuture<GkSqliteWriteOutcome> {
        let sql: GkSqlText = sql.into();
        let params: GkSqliteParams = params.into();
        // BRANCH: raw transaction control must use the transaction API
        if is_explicit_transaction_sql(&sql) {
            return GkSqliteFuture::ready(Err(GkSqliteErrorEnum::UseTransactionApi));
        }
        let readonly = match self.classify_statement(&sql) {
            Ok(r) => r,
            // BRANCH: classification failed (bad SQL)
            Err(e) => return GkSqliteFuture::ready(Err(e)),
        };
        // BRANCH: read-only statement runs on a reader
        if readonly {
            let inner = self.inner.clone();
            return self.read(move |conn| {
                let mut stmt = conn.prepare_cached(&sql)?;
                let _ = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
                Ok(GkSqliteWriteOutcome {
                    rows: 0,
                    seq: inner.committed.load(Ordering::Acquire),
                    last_insert_rowid: 0,
                })
            });
        }
        let (reply_tx, reply_rx) = reply_channel();
        let msg = GkSqliteMsg::Write {
            cmd: GkSqliteWriteCommand::Execute { sql, params },
            reply: reply_tx,
        };
        // BRANCH: queue the write, or report a dead writer
        match self.inner.writer.sender().send(msg) {
            Ok(()) => GkSqliteFuture::pending(reply_rx, GkDeadKind::Writer),
            Err(_) => GkSqliteFuture::ready(Err(GkSqliteErrorEnum::WriterDead)),
        }
    }

    /// Async single-row query. The statement must be read-only (use `execute_async` or
    /// `query_records_async` for `INSERT ... RETURNING`).
    pub fn query_row_async<T, F>(
        &self,
        sql: impl Into<GkSqlText>,
        params: impl Into<GkSqliteParams>,
        f: F,
    ) -> GkSqliteFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T> + Send + 'static,
    {
        let sql: GkSqlText = sql.into();
        let params: GkSqliteParams = params.into();
        // BRANCH: only read-only statements run on readers
        if let Err(e) = self.require_readonly(&sql) {
            return GkSqliteFuture::ready(Err(e));
        }
        self.read(move |conn| {
            reader::query_row(conn, &sql, rusqlite::params_from_iter(params.iter()), f)
        })
    }

    /// Async multi-row query. The statement must be read-only.
    pub fn query_map_async<T, F>(
        &self,
        sql: impl Into<GkSqlText>,
        params: impl Into<GkSqliteParams>,
        f: F,
    ) -> GkSqliteFuture<Vec<T>>
    where
        T: Send + 'static,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T> + Send + 'static,
    {
        let sql: GkSqlText = sql.into();
        let params: GkSqliteParams = params.into();
        // BRANCH: only read-only statements run on readers
        if let Err(e) = self.require_readonly(&sql) {
            return GkSqliteFuture::ready(Err(e));
        }
        self.read(move |conn| {
            reader::query_map(conn, &sql, rusqlite::params_from_iter(params.iter()), f)
        })
    }

    /// Async query returning concrete row records. Read-only statements run on the read mode; a
    /// write statement with `RETURNING` goes to the writer thread.
    pub fn query_records_async(
        &self,
        sql: impl Into<GkSqlText>,
        params: impl Into<GkSqliteParams>,
    ) -> GkSqliteFuture<Vec<GkSqliteRowRecord>> {
        let sql: GkSqlText = sql.into();
        let params: GkSqliteParams = params.into();
        let readonly = match self.classify_statement(&sql) {
            Ok(r) => r,
            // BRANCH: classification failed (bad SQL)
            Err(e) => return GkSqliteFuture::ready(Err(e)),
        };
        // BRANCH: read-only statement runs on a reader
        if readonly {
            return self.read(move |conn| records_from_conn(conn, &sql, &params));
        }
        let (reply_tx, reply_rx) = reply_channel();
        let msg = GkSqliteMsg::ReadFallback {
            cmd: GkSqliteReadCommand::QueryAll { sql, params },
            reply: reply_tx,
        };
        // BRANCH: queue on the writer, or report a dead writer
        match self.inner.writer.sender().send(msg) {
            Ok(()) => GkSqliteFuture::pending(reply_rx, GkDeadKind::Writer),
            Err(_) => GkSqliteFuture::ready(Err(GkSqliteErrorEnum::WriterDead)),
        }
    }

    fn require_readonly(&self, sql: &str) -> Result<(), GkSqliteErrorEnum> {
        // BRANCH: reject statements that must run on the writer
        if self.classify_statement(sql)? {
            Ok(())
        } else {
            Err(GkSqliteErrorEnum::InvalidInput(
                "async query needs a read-only statement; use execute_async or query_records_async",
            ))
        }
    }
}
