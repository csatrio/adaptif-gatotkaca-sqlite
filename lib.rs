// ============================================================================
// Import Definition
// ============================================================================

pub mod async_api;
pub mod config;
pub mod counter;
pub mod error;
pub mod executor;
pub mod pool;
pub mod pragma;
pub mod queue;
pub mod reader;
pub mod reply;
pub mod router;
pub mod sqltext;
#[cfg(feature = "tokio")]
pub mod tokio_executor;
pub mod writer;

pub use adaptif_gatotkaca_utils::{FastU64Map, SqliteFastU64Map};
pub use async_api::GkSqliteFuture;
pub use config::*;
pub use counter::*;
#[cfg(feature = "crossbeam-channel")]
pub use crossbeam_channel;
pub use crossbeam_utils;
pub use error::{GkSqliteError, GkSqliteErrorEnum};
pub use executor::{GkInlineExecutor, GkReadJob, GkReaderExecutor, GkThreadOptions};
pub use pool::{GkReaderPool, GkReaderPoolConfig, GkReaderPoolError};
pub use pragma::GkSqlitePragmas;
pub use queue::{GkChannelKind, GkQueueError};
pub use reader::{GkSqliteReader, query_map, query_row};
pub use router::{is_explicit_transaction_sql, is_readonly_batch, is_readonly_statement};
pub use rusqlite;
pub use smallvec;
pub use sqltext::GkSqlText;
#[cfg(feature = "tokio")]
pub use tokio_executor::GkTokioExecutor;
pub use writer::*;

use queue::{GkWriterQueueConfig, GkWriterTx};
use reply::reply_channel;
use rusqlite::{Row, types::ValueRef};
use smallvec::SmallVec;
use std::{
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

static DB_COUNTER: AtomicU64 = AtomicU64::new(1);

// ============================================================================
// Database Handle Definition
// ============================================================================

/// Thread-safe, cheap-to-clone database connection router.
/// Automatically routes read queries to thread-local connections and write
/// statements to a single dedicated writer thread.
#[derive(Clone)]
pub struct GkSqliteDb {
    inner: Arc<GkSqliteDbInner>,
}

struct GkSqliteDbInner {
    id: u64,
    path: Arc<str>,
    config: GkSqliteConfig,
    reader_options: GkConnectionOptions,
    /// Runs `read` / `*_async` jobs; `None` means inline on the calling thread.
    executor: Option<Arc<dyn GkReaderExecutor>>,
    writer: GkSqliteWriterHandle,
    committed: GkSqliteCommitCounter,
}

impl GkSqliteDbInner {
    /// Runs `f` on this thread's read-only connection for this database.
    #[inline]
    fn with_reader<F, R>(&self, f: F) -> Result<R, GkSqliteErrorEnum>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<R, GkSqliteErrorEnum>,
    {
        GkSqliteReader::with_connection(self.id, &self.path, &self.reader_options, f)
    }
}

impl GkSqliteDb {
    /// Opens or creates a new SQLite database in WAL mode with read/write routing.
    pub fn open(path: &str, config: GkSqliteConfig) -> Result<Self, GkSqliteErrorEnum> {
        config.validate()?;
        let id = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let normalized_path: Arc<str> = if path == ":memory:" {
            Arc::from(format!("file:mem_{}?mode=memory&cache=shared", id))
        } else {
            Arc::from(path)
        };
        let committed = GkSqliteCommitCounter::new(0);
        let writer = GkSqliteWriterHandle::new(
            &normalized_path,
            &GkWriterQueueConfig {
                kind: config.writer_channel,
                capacity: config.writer_queue_size,
            },
            committed.clone(),
            config.writer_options(),
            &config.writer_thread,
        )?;

        if !config.autocommit_mode {
            writer.set_autocommit_mode(false)?;
        }

        let executor: Option<Arc<dyn GkReaderExecutor>> = match &config.read_mode {
            // BRANCH: async reads run on the calling thread
            GkReadMode::Inline => None,
            // BRANCH: start the built-in lock-free reader pool
            GkReadMode::Pool(pool_config) => {
                let pool = GkReaderPool::new(pool_config.clone()).map_err(|_| {
                    GkSqliteErrorEnum::InvalidInput("Failed to start the reader pool")
                })?;
                Some(Arc::new(pool))
            }
            // BRANCH: the caller's own pool or runtime
            GkReadMode::Custom(executor) => Some(executor.clone()),
        };

        let inner = Arc::new(GkSqliteDbInner {
            id,
            path: normalized_path,
            reader_options: config.reader_options(),
            executor,
            config,
            writer,
            committed,
        });

        Ok(Self { inner })
    }

    #[inline(always)]
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    #[inline(always)]
    pub fn path(&self) -> &str {
        &self.inner.path
    }

    #[inline(always)]
    pub fn config(&self) -> &GkSqliteConfig {
        &self.inner.config
    }

    #[inline(always)]
    pub fn last_committed(&self) -> u64 {
        self.inner.committed.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn is_autocommit_mode(&self) -> bool {
        self.inner.writer.is_autocommit_mode()
    }

    /// Read-only classification of one statement. A thread-local cache answers repeats without
    /// borrowing the reader connection or calling into SQLite.
    #[inline]
    fn classify_statement(&self, sql: &str) -> Result<bool, GkSqliteErrorEnum> {
        if let Some(readonly) = router::cached_readonly(sql) {
            // Transaction-control SQL is never cached as classified, so this is safe to short-circuit.
            return Ok(readonly);
        }
        self.inner
            .with_reader(|conn| router::is_readonly_statement(conn, sql))
    }

    fn classify_batch(&self, sql: &str) -> Result<bool, GkSqliteErrorEnum> {
        self.inner
            .with_reader(|conn| router::is_readonly_batch(conn, sql))
    }

    /// Executes a write statement on the dedicated writer thread.
    ///
    /// Statement is classified on the thread-local reader first via `conn.prepare(sql)`.
    /// If non-readonly, the statement is immediately dropped (preventing pollution of the
    /// reader's `prepare_cached` cache) and the payload is routed to the writer channel.
    /// Returns `GkSqliteWriteOutcome` containing rows affected and assigned sequence token.
    pub fn execute(
        &self,
        sql: &str,
        params: impl Into<GkSqliteParams>,
    ) -> Result<GkSqliteWriteOutcome, GkSqliteErrorEnum> {
        let params_vec: GkSqliteParams = params.into();

        // Check for raw transaction keywords
        if is_explicit_transaction_sql(sql) {
            return Err(GkSqliteErrorEnum::UseTransactionApi);
        }

        let is_readonly = self.classify_statement(sql)?;

        if is_readonly {
            self.inner.with_reader(|conn| {
                let mut stmt = conn.prepare_cached(sql)?;
                let _ = stmt.execute(rusqlite::params_from_iter(params_vec.iter()))?;
                Ok(GkSqliteWriteOutcome {
                    rows: 0,
                    seq: self.inner.committed.load(Ordering::Acquire),
                    last_insert_rowid: 0,
                })
            })
        } else {
            let (reply_tx, reply_rx) = reply_channel();
            let cmd = GkSqliteWriteCommand::Execute {
                sql: GkSqlText::from_copy(sql),
                params: params_vec,
            };

            self.inner
                .writer
                .sender()
                .send(GkSqliteMsg::Write {
                    cmd,
                    reply: reply_tx,
                })
                .map_err(|_| GkSqliteErrorEnum::WriterDead)?;

            let outcome = reply_rx
                .wait()
                .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
            set_last_token(self.inner.id, outcome.seq);
            Ok(outcome)
        }
    }

    /// Executes an arbitrary batch of statements.
    /// Non-readonly batches are routed to the dedicated writer thread.
    pub fn execute_batch(&self, sql: &str) -> Result<GkSqliteWriteOutcome, GkSqliteErrorEnum> {
        if is_explicit_transaction_sql(sql) {
            return Err(GkSqliteErrorEnum::UseTransactionApi);
        }

        let is_readonly = self.classify_batch(sql)?;

        if is_readonly {
            self.inner.with_reader(|conn| {
                conn.execute_batch(sql)?;
                Ok(GkSqliteWriteOutcome {
                    rows: 0,
                    seq: self.inner.committed.load(Ordering::Acquire),
                    last_insert_rowid: 0,
                })
            })
        } else {
            let (reply_tx, reply_rx) = reply_channel();
            let cmd = GkSqliteWriteCommand::ExecuteBatch {
                sql: GkSqlText::from_copy(sql),
            };

            self.inner
                .writer
                .sender()
                .send(GkSqliteMsg::Write {
                    cmd,
                    reply: reply_tx,
                })
                .map_err(|_| GkSqliteErrorEnum::WriterDead)?;

            let outcome = reply_rx
                .wait()
                .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
            set_last_token(self.inner.id, outcome.seq);
            Ok(outcome)
        }
    }

    /// Executes a read query expecting a single row on a thread-local reader connection.
    /// If read-your-writes token is not satisfied within deadline, falls back to writer thread.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T, GkSqliteErrorEnum>
    where
        P: Into<GkSqliteParams> + Clone,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        // Inspect readonly status on reader first
        let is_readonly = self.classify_statement(sql)?;

        if !is_readonly {
            // Write statement returning data (e.g. INSERT ... RETURNING) -> route to writer
            return self.query_row_fallback(sql, params.into(), f);
        }

        let min_seq = get_last_token(self.inner.id);

        // Check if token deadline is satisfied
        if min_seq > 0
            && !self
                .inner
                .committed
                .wait_for(min_seq, self.inner.config.read_timeout)
        {
            // Read fallback to writer thread
            return self.query_row_fallback(sql, params.into(), f);
        }

        // Execute directly on thread-local reader connection
        self.inner.with_reader(|conn| {
            let params_converted: GkSqliteParams = params.into();
            reader::query_row(
                conn,
                sql,
                rusqlite::params_from_iter(params_converted.iter()),
                f,
            )
        })
    }

    /// Executes a read query mapping all rows on a thread-local reader connection.
    pub fn query_map<T, P, F>(
        &self,
        sql: &str,
        params: P,
        f: F,
    ) -> Result<Vec<T>, GkSqliteErrorEnum>
    where
        P: Into<GkSqliteParams> + Clone,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let is_readonly = self.classify_statement(sql)?;

        if !is_readonly {
            return self.query_map_fallback(sql, params.into(), f);
        }

        let min_seq = get_last_token(self.inner.id);

        if min_seq > 0
            && !self
                .inner
                .committed
                .wait_for(min_seq, self.inner.config.read_timeout)
        {
            return self.query_map_fallback(sql, params.into(), f);
        }

        self.inner.with_reader(|conn| {
            let params_converted: GkSqliteParams = params.into();
            reader::query_map(
                conn,
                sql,
                rusqlite::params_from_iter(params_converted.iter()),
                f,
            )
        })
    }

    fn query_row_fallback<T, F>(
        &self,
        sql: &str,
        params: GkSqliteParams,
        f: F,
    ) -> Result<T, GkSqliteErrorEnum>
    where
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        let (reply_tx, reply_rx) = reply_channel();
        let cmd = GkSqliteReadCommand::QueryRow {
            sql: GkSqlText::from_copy(sql),
            params,
        };

        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::ReadFallback {
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;

        let records = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        let record = records
            .into_iter()
            .next()
            .ok_or(GkSqliteErrorEnum::SqliteError(
                rusqlite::Error::QueryReturnedNoRows,
            ))?;

        // Re-execute closure using reader connection in memory with returned values
        self.inner.with_reader(|conn| {
            if record.column_names.is_empty() {
                let mut stmt = conn.prepare("SELECT 1 WHERE 0")?;
                let mut rows = stmt.query([])?;
                let res = match rows.next()? {
                    Some(row) => f(row).map_err(GkSqliteErrorEnum::SqliteError),
                    None => Err(GkSqliteErrorEnum::SqliteError(
                        rusqlite::Error::QueryReturnedNoRows,
                    )),
                };
                drop(rows);
                return res;
            }

            let mut select_sql = String::from("SELECT ");
            for (i, name) in record.column_names.iter().enumerate() {
                if i > 0 {
                    select_sql.push_str(", ");
                }
                select_sql.push_str(&format!("?{} AS \"{}\"", i + 1, name.replace('"', "\"\"")));
            }

            let mut stmt = conn.prepare(&select_sql)?;
            let mut rows = stmt.query(rusqlite::params_from_iter(record.values.iter()))?;
            let res = match rows.next()? {
                Some(row) => f(row).map_err(GkSqliteErrorEnum::SqliteError),
                None => Err(GkSqliteErrorEnum::SqliteError(
                    rusqlite::Error::QueryReturnedNoRows,
                )),
            };
            drop(rows);
            res
        })
    }

    fn query_map_fallback<T, F>(
        &self,
        sql: &str,
        params: GkSqliteParams,
        mut f: F,
    ) -> Result<Vec<T>, GkSqliteErrorEnum>
    where
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let (reply_tx, reply_rx) = reply_channel();
        let cmd = GkSqliteReadCommand::QueryAll {
            sql: GkSqlText::from_copy(sql),
            params,
        };

        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::ReadFallback {
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;

        let records = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        if records.is_empty() {
            return Ok(Vec::new());
        }

        self.inner.with_reader(|conn| {
            let first = &records[0];
            if first.column_names.is_empty() {
                return Ok(Vec::new());
            }

            let mut select_sql = String::from("SELECT ");
            for (i, name) in first.column_names.iter().enumerate() {
                if i > 0 {
                    select_sql.push_str(", ");
                }
                select_sql.push_str(&format!("?{} AS \"{}\"", i + 1, name.replace('"', "\"\"")));
            }

            let mut stmt = conn.prepare(&select_sql)?;
            let mut results = Vec::with_capacity(records.len());
            for rec in records {
                let mut rows = stmt.query(rusqlite::params_from_iter(rec.values.iter()))?;
                if let Some(row) = rows.next()? {
                    results.push(f(row).map_err(GkSqliteErrorEnum::SqliteError)?);
                }
                drop(rows);
            }
            Ok(results)
        })
    }

    /// Executes a read query returning concrete row records.
    /// If read-your-writes token is not satisfied within deadline, falls back to writer thread.
    pub fn query_records(
        &self,
        sql: &str,
        params: impl Into<GkSqliteParams>,
    ) -> Result<Vec<GkSqliteRowRecord>, GkSqliteErrorEnum> {
        let params_val = params.into();
        let is_readonly = self.classify_statement(sql)?;

        // BRANCH: Write statement returning data (e.g. INSERT RETURNING) routes to writer
        if !is_readonly {
            let (reply_tx, reply_rx) = reply_channel();
            let cmd = GkSqliteReadCommand::QueryAll {
                sql: GkSqlText::from_copy(sql),
                params: params_val,
            };
            self.inner
                .writer
                .sender()
                .send(GkSqliteMsg::ReadFallback {
                    cmd,
                    reply: reply_tx,
                })
                .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
            return reply_rx.wait().map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        }

        let min_seq = get_last_token(self.inner.id);
        // BRANCH: Check if token deadline is satisfied, fallback to writer if deadline exceeded
        if min_seq > 0
            && !self
                .inner
                .committed
                .wait_for(min_seq, self.inner.config.read_timeout)
        {
            let (reply_tx, reply_rx) = reply_channel();
            let cmd = GkSqliteReadCommand::QueryAll {
                sql: GkSqlText::from_copy(sql),
                params: params_val,
            };
            self.inner
                .writer
                .sender()
                .send(GkSqliteMsg::ReadFallback {
                    cmd,
                    reply: reply_tx,
                })
                .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
            return reply_rx.wait().map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        }

        // BRANCH: Execute directly on thread-local reader connection
        self.inner
            .with_reader(|conn| records_from_conn(conn, sql, &params_val))
    }

    /// Executes an explicit transaction closure entirely on the dedicated writer thread.
    ///
    /// Reads performed inside this closure execute on the writer connection and will see
    /// uncommitted changes made earlier within the transaction. Reads performed outside
    /// continue executing on thread-local readers and see only committed data.
    ///
    /// STRICT CONSTRAINT: Closures must NOT perform external I/O or acquire synchronization locks.
    /// They must also NOT call back into this database (`execute`, `query_*`, `transaction`, ...): that waits for the
    /// writer thread, which is the thread running the closure, and deadlocks. Use the connection passed in.
    pub fn transaction<F, R>(&self, f: F) -> Result<R, GkSqliteErrorEnum>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, GkSqliteErrorEnum> + Send + 'static,
        R: Send + 'static,
    {
        let (res_tx, res_rx) = reply_channel();
        let (reply_tx, reply_rx) = reply_channel();

        let closure_box = Box::new(move |conn: &mut rusqlite::Connection| {
            let res = f(conn)?;
            res_tx.send(res);
            Ok(())
        });

        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::TransactionClosure {
                closure: closure_box,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;

        let outcome = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        set_last_token(self.inner.id, outcome.seq);
        let ret = res_rx.wait().map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        Ok(ret)
    }

    pub fn begin_session(&self) -> Result<u64, GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::BeginSession { reply: reply_tx })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        reply_rx.wait().map_err(|_| GkSqliteErrorEnum::WriterDead)?
    }

    pub fn session_execute(
        &self,
        session_id: u64,
        sql: &str,
        params: impl Into<GkSqliteParams>,
    ) -> Result<GkSqliteWriteOutcome, GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        let cmd = GkSqliteWriteCommand::Execute {
            sql: GkSqlText::from_copy(sql),
            params: params.into(),
        };
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::SessionExecute {
                session_id,
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        let outcome = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        set_last_token(self.inner.id, outcome.seq);
        Ok(outcome)
    }

    pub fn session_execute_insert(
        &self,
        session_id: u64,
        sql: &str,
        params: impl Into<GkSqliteParams>,
    ) -> Result<i64, GkSqliteErrorEnum> {
        let outcome = self.session_execute(session_id, sql, params)?;
        Ok(outcome.last_insert_rowid)
    }

    pub fn session_execute_batch(
        &self,
        session_id: u64,
        sql: &str,
    ) -> Result<GkSqliteWriteOutcome, GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        let cmd = GkSqliteWriteCommand::ExecuteBatch {
            sql: GkSqlText::from_copy(sql),
        };
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::SessionExecute {
                session_id,
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        let outcome = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        set_last_token(self.inner.id, outcome.seq);
        Ok(outcome)
    }

    pub fn session_query_map<T, P, F>(
        &self,
        session_id: u64,
        sql: &str,
        params: P,
        mut f: F,
    ) -> Result<Vec<T>, GkSqliteErrorEnum>
    where
        P: Into<GkSqliteParams> + Clone,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let (reply_tx, reply_rx) = reply_channel();
        let cmd = GkSqliteReadCommand::QueryAll {
            sql: GkSqlText::from_copy(sql),
            params: params.into(),
        };
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::SessionQuery {
                session_id,
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        let records = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        if records.is_empty() {
            return Ok(Vec::new());
        }

        self.inner.with_reader(|conn| {
            let first = &records[0];
            if first.column_names.is_empty() {
                return Ok(Vec::new());
            }
            let mut select_sql = String::from("SELECT ");
            for (i, name) in first.column_names.iter().enumerate() {
                if i > 0 {
                    select_sql.push_str(", ");
                }
                select_sql.push_str(&format!("?{} AS \"{}\"", i + 1, name.replace('"', "\"\"")));
            }
            let mut stmt = conn.prepare(&select_sql)?;
            let mut results = Vec::with_capacity(records.len());
            for rec in records {
                let mut rows = stmt.query(rusqlite::params_from_iter(rec.values.iter()))?;
                if let Some(row) = rows.next()? {
                    results.push(f(row).map_err(GkSqliteErrorEnum::SqliteError)?);
                }
                drop(rows);
            }
            Ok(results)
        })
    }

    pub fn session_query_records(
        &self,
        session_id: u64,
        sql: &str,
        params: impl Into<GkSqliteParams>,
    ) -> Result<Vec<GkSqliteRowRecord>, GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        let cmd = GkSqliteReadCommand::QueryAll {
            sql: GkSqlText::from_copy(sql),
            params: params.into(),
        };
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::SessionQuery {
                session_id,
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        reply_rx.wait().map_err(|_| GkSqliteErrorEnum::WriterDead)?
    }

    pub fn session_query_row<T, P, F>(
        &self,
        session_id: u64,
        sql: &str,
        params: P,
        f: F,
    ) -> Result<T, GkSqliteErrorEnum>
    where
        P: Into<GkSqliteParams> + Clone,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        let (reply_tx, reply_rx) = reply_channel();
        let cmd = GkSqliteReadCommand::QueryRow {
            sql: GkSqlText::from_copy(sql),
            params: params.into(),
        };
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::SessionQuery {
                session_id,
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        let records = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        let record = records
            .into_iter()
            .next()
            .ok_or(GkSqliteErrorEnum::SqliteError(
                rusqlite::Error::QueryReturnedNoRows,
            ))?;

        self.inner.with_reader(|conn| {
            if record.column_names.is_empty() {
                let mut stmt = conn.prepare("SELECT 1 WHERE 0")?;
                let mut rows = stmt.query([])?;
                let res = match rows.next()? {
                    Some(row) => f(row).map_err(GkSqliteErrorEnum::SqliteError),
                    None => Err(GkSqliteErrorEnum::SqliteError(
                        rusqlite::Error::QueryReturnedNoRows,
                    )),
                };
                drop(rows);
                return res;
            }
            let mut select_sql = String::from("SELECT ");
            for (i, name) in record.column_names.iter().enumerate() {
                if i > 0 {
                    select_sql.push_str(", ");
                }
                select_sql.push_str(&format!("?{} AS \"{}\"", i + 1, name.replace('"', "\"\"")));
            }
            let mut stmt = conn.prepare(&select_sql)?;
            let mut rows = stmt.query(rusqlite::params_from_iter(record.values.iter()))?;
            let res = match rows.next()? {
                Some(row) => f(row).map_err(GkSqliteErrorEnum::SqliteError),
                None => Err(GkSqliteErrorEnum::SqliteError(
                    rusqlite::Error::QueryReturnedNoRows,
                )),
            };
            drop(rows);
            res
        })
    }

    pub fn session_savepoint(
        &self,
        session_id: u64,
        action: GkSavepointAction,
    ) -> Result<(), GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::SessionSavepoint {
                session_id,
                action,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        reply_rx.wait().map_err(|_| GkSqliteErrorEnum::WriterDead)?
    }

    pub fn end_session(
        &self,
        session_id: u64,
        commit: bool,
    ) -> Result<GkSqliteWriteOutcome, GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::EndSession {
                session_id,
                commit,
                reply: Some(reply_tx),
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        let outcome = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        set_last_token(self.inner.id, outcome.seq);
        Ok(outcome)
    }

    pub fn execute_insert(
        &self,
        sql: &str,
        params: impl Into<GkSqliteParams>,
    ) -> Result<i64, GkSqliteErrorEnum> {
        let outcome = self.execute(sql, params)?;
        Ok(outcome.last_insert_rowid)
    }

    pub fn execute_many(
        &self,
        sql: &str,
        params_vec: Vec<GkSqliteParams>,
    ) -> Result<GkSqliteWriteOutcome, GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        let sql_arc = GkSqlText::from_copy(sql);
        let statements = params_vec
            .into_iter()
            .map(|p| (sql_arc.clone(), p))
            .collect();
        let cmd = GkSqliteWriteCommand::TransactionStatements { statements };
        self.inner
            .writer
            .sender()
            .send(GkSqliteMsg::Write {
                cmd,
                reply: reply_tx,
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        let outcome = reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;
        set_last_token(self.inner.id, outcome.seq);
        Ok(outcome)
    }

    #[inline(always)]
    pub fn writer_sender(&self) -> GkWriterTx<GkSqliteMsg> {
        self.inner.writer.sender().clone()
    }

    pub fn set_autocommit_mode(&self, enabled: bool) -> Result<(), GkSqliteErrorEnum> {
        self.inner.writer.set_autocommit_mode(enabled)
    }

    pub fn checkpoint(&self) -> Result<(), GkSqliteErrorEnum> {
        self.inner.writer.checkpoint()
    }

    pub fn close(&mut self) -> Result<(), GkSqliteErrorEnum> {
        GkSqliteReader::close_thread_reader(self.inner.id);
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.writer.shutdown()?;
        }
        Ok(())
    }
}

/// Runs a read-only query on `conn` and materialises every row. Shared by the sync and async paths.
pub(crate) fn records_from_conn(
    conn: &rusqlite::Connection,
    sql: &str,
    params_val: &GkSqliteParams,
) -> Result<Vec<GkSqliteRowRecord>, GkSqliteErrorEnum> {
    let mut stmt = conn.prepare_cached(sql)?;
    let col_names: Arc<[Arc<str>]> = stmt
        .column_names()
        .map(Arc::from)
        .collect::<Vec<_>>()
        .into();
    let col_count = col_names.len();
    let mut rows = stmt.query(rusqlite::params_from_iter(params_val.iter()))?;
    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let mut values = SmallVec::with_capacity(col_count);
        for i in 0..col_count {
            let val = match row.get_ref(i)? {
                // BRANCH: Null value
                ValueRef::Null => GkSqliteValue::Null,
                // BRANCH: Integer value
                ValueRef::Integer(n) => GkSqliteValue::Integer(n),
                // BRANCH: Real float value
                ValueRef::Real(r) => GkSqliteValue::Real(r),
                // BRANCH: Text string value
                ValueRef::Text(t) => {
                    let s = std::str::from_utf8(t).unwrap_or("");
                    GkSqliteValue::Text(GkCompactStr::new(s))
                }
                // BRANCH: Blob bytes value
                ValueRef::Blob(b) => GkSqliteValue::Blob(SmallVec::from_slice(b)),
            };
            values.push(val);
        }
        results.push(GkSqliteRowRecord {
            column_names: col_names.clone(),
            values,
        });
    }
    drop(rows);
    Ok(results)
}
