// ============================================================================
// Import Definition
// ============================================================================

use crate::reply::{GkReplySender, reply_channel};
use crate::{
    config::GkConnectionOptions,
    counter::{
        GkCompactStr, GkSqliteCommitCounter, GkSqliteParams, GkSqliteValue, GkSqliteWriteOutcome,
    },
    error::GkSqliteErrorEnum,
    executor::GkThreadOptions,
    queue::{GkQueueError, GkWriterQueueConfig, GkWriterRx, GkWriterTx, writer_queue},
    sqltext::GkSqlText,
};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use smallvec::SmallVec;
use std::{
    collections::VecDeque,
    sync::Arc,
    sync::atomic::{AtomicU8, Ordering},
    thread::JoinHandle,
    time::{Duration, Instant},
};

// ============================================================================
// Control Constants Definition
// ============================================================================

pub const CTL_COMMIT: u32 = 1;
pub const CTL_SET_AUTOCOMMIT: u32 = 2;
pub const CTL_SET_BATCH: u32 = 3;
pub const CTL_CHECKPOINT: u32 = 4;
pub const CTL_SHUTDOWN: u32 = 5;

// ============================================================================
// Message & Command Definitions
// ============================================================================

#[derive(Debug, Clone)]
pub struct GkSqliteRowRecord {
    pub column_names: Arc<[Arc<str>]>,
    pub values: SmallVec<[GkSqliteValue; 8]>,
}

/// A write transaction body run on the writer thread.
type GkTransactionBody = Box<dyn FnOnce(&mut Connection) -> Result<(), GkSqliteErrorEnum> + Send>;
/// Where the writer reports the outcome of one write.
type GkWriteReply = GkReplySender<Result<GkSqliteWriteOutcome, GkSqliteErrorEnum>>;
/// A session waiting for the writer: id, reply channel, time queued.
type GkPendingSession = (u64, GkReplySender<Result<u64, GkSqliteErrorEnum>>, Instant);

#[derive(Debug, Clone)]
pub enum GkSqliteWriteCommand {
    /// Single parameterized execution (zero allocation for <= 4 params and strings <= 24 bytes)
    Execute {
        sql: GkSqlText,
        params: GkSqliteParams,
    },
    /// Raw batch execution
    ExecuteBatch { sql: GkSqlText },
    /// Multi-statement batch transaction
    TransactionStatements {
        statements: Vec<(GkSqlText, GkSqliteParams)>,
    },
}

#[derive(Debug, Clone)]
pub enum GkSqliteReadCommand {
    QueryRow {
        sql: GkSqlText,
        params: GkSqliteParams,
    },
    QueryAll {
        sql: GkSqlText,
        params: GkSqliteParams,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GkSavepointAction {
    Savepoint(GkCompactStr),
    RollbackTo(GkCompactStr),
    Release(GkCompactStr),
}

pub enum GkSqliteMsg {
    /// Zero-allocation structured write task
    Write {
        cmd: GkSqliteWriteCommand,
        reply: GkReplySender<Result<GkSqliteWriteOutcome, GkSqliteErrorEnum>>,
    },
    /// Concrete structured read fallback when min_seq timeout expires
    ReadFallback {
        cmd: GkSqliteReadCommand,
        reply: GkReplySender<Result<Vec<GkSqliteRowRecord>, GkSqliteErrorEnum>>,
    },
    /// Programmatic transaction closure.
    /// STRICT REQUIREMENT: Closures MUST NOT perform external network/disk I/O
    /// or acquire synchronization locks, preventing worker thread stalls.
    TransactionClosure {
        closure: GkTransactionBody,
        reply: GkReplySender<Result<GkSqliteWriteOutcome, GkSqliteErrorEnum>>,
    },
    /// Session transaction begin (leases writer thread for async transactions)
    BeginSession {
        reply: GkReplySender<Result<u64, GkSqliteErrorEnum>>,
    },
    /// Session write command execution
    SessionExecute {
        session_id: u64,
        cmd: GkSqliteWriteCommand,
        reply: GkReplySender<Result<GkSqliteWriteOutcome, GkSqliteErrorEnum>>,
    },
    /// Session query execution (runs on writer connection to see uncommitted writes)
    SessionQuery {
        session_id: u64,
        cmd: GkSqliteReadCommand,
        reply: GkReplySender<Result<Vec<GkSqliteRowRecord>, GkSqliteErrorEnum>>,
    },
    /// Session savepoint operations (nested transactions)
    SessionSavepoint {
        session_id: u64,
        action: GkSavepointAction,
        reply: GkReplySender<Result<(), GkSqliteErrorEnum>>,
    },
    /// Session end (commit or rollback). Reply is optional for fire-and-forget on Drop.
    EndSession {
        session_id: u64,
        commit: bool,
        reply: Option<GkReplySender<Result<GkSqliteWriteOutcome, GkSqliteErrorEnum>>>,
    },
    /// Low-overhead control command
    Control {
        cmd: u32,
        arg: u64,
        reply: Option<GkReplySender<Result<u64, GkSqliteErrorEnum>>>,
    },
}

// ============================================================================
// Writer Worker Thread
// ============================================================================

pub struct GkSqliteWriterHandle {
    sender: GkWriterTx<GkSqliteMsg>,
    thread: Option<JoinHandle<()>>,
    committed: GkSqliteCommitCounter,
    autocommit_mode: Arc<AtomicU8>,
}

impl GkSqliteWriterHandle {
    pub fn new(
        path: &str,
        queue: &GkWriterQueueConfig,
        committed: GkSqliteCommitCounter,
        options: GkConnectionOptions,
        thread_options: &GkThreadOptions,
    ) -> Result<Self, GkSqliteErrorEnum> {
        let (sender, receiver) = writer_queue::<GkSqliteMsg>(queue)
            .map_err(|_| GkSqliteErrorEnum::InvalidInput("Failed to create writer queue"))?;
        let path_string = path.to_string();
        let committed_clone = committed.clone();
        let autocommit_mode = Arc::new(AtomicU8::new(1));
        let autocommit_mode_clone = autocommit_mode.clone();

        let (ready_tx, ready_rx) = reply_channel();
        let thread_handle = thread_options
            .spawn_thread(0, move || {
                run_writer_loop(
                    &path_string,
                    receiver,
                    committed_clone,
                    autocommit_mode_clone,
                    options,
                    ready_tx,
                );
            })
            .map_err(|_e| {
                GkSqliteErrorEnum::InvalidInput("Failed to spawn sqlite writer thread")
            })?;

        ready_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)??;

        Ok(Self {
            sender,
            thread: Some(thread_handle),
            committed,
            autocommit_mode,
        })
    }

    #[inline(always)]
    pub fn sender(&self) -> &GkWriterTx<GkSqliteMsg> {
        &self.sender
    }

    #[inline(always)]
    pub fn committed(&self) -> &GkSqliteCommitCounter {
        &self.committed
    }

    pub fn set_autocommit_mode(&self, enabled: bool) -> Result<(), GkSqliteErrorEnum> {
        let arg = if enabled { 1 } else { 0 };
        let (reply_tx, reply_rx) = reply_channel();
        self.sender
            .send(GkSqliteMsg::Control {
                cmd: CTL_SET_AUTOCOMMIT,
                arg,
                reply: Some(reply_tx),
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?
            .map(|_| ())
    }

    #[inline(always)]
    pub fn is_autocommit_mode(&self) -> bool {
        self.autocommit_mode.load(Ordering::Acquire) == 1
    }

    pub fn checkpoint(&self) -> Result<(), GkSqliteErrorEnum> {
        let (reply_tx, reply_rx) = reply_channel();
        self.sender
            .send(GkSqliteMsg::Control {
                cmd: CTL_CHECKPOINT,
                arg: 0,
                reply: Some(reply_tx),
            })
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?;
        reply_rx
            .wait()
            .map_err(|_| GkSqliteErrorEnum::WriterDead)?
            .map(|_| ())
    }

    pub fn shutdown(&mut self) -> Result<(), GkSqliteErrorEnum> {
        // BRANCH: Send shutdown signal if sender is still connected
        let _ = self.sender.send(GkSqliteMsg::Control {
            cmd: CTL_SHUTDOWN,
            arg: 0,
            reply: None,
        });

        if let Some(handle) = self.thread.take() {
            // BRANCH: never join the writer thread from itself (the last handle dropped inside a job on it)
            if handle.thread().id() != std::thread::current().id() {
                let _ = handle.join();
            }
        }
        // The writer is gone; free this thread's now-useless sender slab.
        self.sender.release_thread_sender();
        Ok(())
    }
}

impl Drop for GkSqliteWriterHandle {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Statement-cache capacity for the writer and reader connections. rusqlite's default (16) thrashes
/// as soon as an application has more distinct hot statements than that.
pub const STATEMENT_CACHE_CAPACITY: usize = 256;

/// Runs a parameterless statement through the prepared-statement cache, so the control statements the
/// writer issues for every write (`SAVEPOINT`, `RELEASE`, `COMMIT`, ...) are parsed once, not per call.
#[inline]
fn exec_cached(conn: &Connection, sql: &str) -> rusqlite::Result<()> {
    conn.prepare_cached(sql)?.execute([])?;
    Ok(())
}

/// Per-write savepoint. Each one is released or rolled back before the next begins, so a single
/// fixed name is enough (and keeps the SQL text constant, hence cacheable).
#[inline]
fn sp_begin(conn: &Connection) -> rusqlite::Result<()> {
    exec_cached(conn, "SAVEPOINT gk_sp")
}

#[inline]
fn sp_release(conn: &Connection) -> rusqlite::Result<()> {
    exec_cached(conn, "RELEASE gk_sp")
}

#[inline]
fn sp_rollback(conn: &Connection) -> rusqlite::Result<()> {
    exec_cached(conn, "ROLLBACK TO gk_sp")?;
    exec_cached(conn, "RELEASE gk_sp")
}

/// Runs one write command atomically inside the open transaction.
///
/// A single `Execute` is one SQLite statement, and SQLite already undoes a failed statement on its
/// own, so it skips the `SAVEPOINT`/`RELEASE` round trip. Multi-statement commands keep the savepoint
/// so a failure part-way through leaves no partial effects.
#[inline]
fn execute_write_isolated(
    conn: &mut Connection,
    cmd: GkSqliteWriteCommand,
) -> Result<(usize, i64), GkSqliteErrorEnum> {
    // BRANCH: Single statement, statement-level atomicity is enough
    if matches!(cmd, GkSqliteWriteCommand::Execute { .. }) {
        return execute_write_command(conn, cmd);
    }
    sp_begin(conn)?;
    match execute_write_command(conn, cmd) {
        // BRANCH: Multi-statement command succeeded
        Ok(r) => {
            let _ = sp_release(conn);
            Ok(r)
        }
        // BRANCH: Multi-statement command failed, undo its partial effects
        Err(e) => {
            let _ = sp_rollback(conn);
            Err(e)
        }
    }
}

struct GkSqlitePendingAck {
    reply: GkReplySender<Result<GkSqliteWriteOutcome, GkSqliteErrorEnum>>,
    rows: usize,
    seq: u64,
    last_insert_rowid: i64,
}

fn open_writer_connection(
    path: &str,
    options: &GkConnectionOptions,
) -> Result<Connection, GkSqliteErrorEnum> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    options.initialise(&conn)?;
    Ok(conn)
}

fn run_writer_loop(
    path: &str,
    mut receiver: GkWriterRx<GkSqliteMsg>,
    committed: GkSqliteCommitCounter,
    autocommit_mode: Arc<AtomicU8>,
    options: GkConnectionOptions,
    ready_tx: GkReplySender<Result<(), GkSqliteErrorEnum>>,
) {
    let busy_timeout = options.busy_timeout;
    let mut conn = match open_writer_connection(path, &options) {
        // BRANCH: Successfully opened write connection
        Ok(c) => {
            ready_tx.send(Ok(()));
            c
        }
        // BRANCH: Failed to open writer connection
        Err(e) => {
            log::error!("SQLite writer failed to open connection: {:?}", e);
            ready_tx.send(Err(e));
            return;
        }
    };

    let mut current_seq = committed.load(Ordering::Acquire);
    let mut max_batch: usize = 64;
    let max_age: Duration = Duration::from_millis(5);
    let mut pending_acks: Vec<GkSqlitePendingAck> = Vec::with_capacity(max_batch);

    let mut in_transaction = false;
    let mut txn_start: Instant = Instant::now();

    let mut active_session: Option<u64> = None;
    let mut active_session_info: Option<(u64, Instant)> = None;
    let mut pending_sessions: VecDeque<GkPendingSession> = VecDeque::new();
    let mut pending_writes: VecDeque<(GkSqliteWriteCommand, GkWriteReply, Instant)> =
        VecDeque::new();
    let mut next_session_id: u64 = 1;
    let session_watchdog_timeout: Duration = Duration::from_secs(15);
    let mut buffered_msg: Option<GkSqliteMsg> = None;

    'main_loop: loop {
        let is_autocommit = autocommit_mode.load(Ordering::Relaxed) != 0;

        let msg = if let Some(m) = buffered_msg.take() {
            // BRANCH: a peeked message is waiting; the age limit still applies to the open transaction
            if in_transaction && active_session.is_none() && txn_start.elapsed() >= max_age {
                flush_transaction(
                    &mut conn,
                    &committed,
                    &mut current_seq,
                    &mut in_transaction,
                    &mut pending_acks,
                );
            }
            m
        } else if let Some((_, session_start)) = active_session_info {
            let elapsed = session_start.elapsed();
            if elapsed >= session_watchdog_timeout {
                log::warn!(
                    "SQLite session {:?} timed out after {:?}; rolling back",
                    active_session,
                    elapsed
                );
                let _ = exec_cached(&conn, "ROLLBACK");
                active_session = None;
                active_session_info = None;

                // Pop next queued session
                while let Some((sid, s_reply, s_start)) = pending_sessions.pop_front() {
                    if s_start.elapsed() > busy_timeout {
                        s_reply.send(Err(GkSqliteErrorEnum::Timeout));
                        continue;
                    }
                    if let Ok(()) = exec_cached(&conn, "BEGIN IMMEDIATE") {
                        active_session = Some(sid);
                        active_session_info = Some((sid, Instant::now()));
                        s_reply.send(Ok(sid));
                        break;
                    }
                }
                // BRANCH: writes buffered during the dead session were never executed; fail them instead of leaving callers blocked
                if active_session.is_none() {
                    while let Some((_, w_reply, _)) = pending_writes.pop_front() {
                        w_reply.send(Err(GkSqliteErrorEnum::Timeout));
                    }
                }
                continue 'main_loop;
            }
            let remaining = session_watchdog_timeout - elapsed;
            match receiver.recv_timeout(remaining) {
                Ok(m) => m,
                Err(GkQueueError::TIMEOUT) => continue 'main_loop,
                Err(_) => {
                    let _ = exec_cached(&conn, "ROLLBACK");
                    break 'main_loop;
                }
            }
        } else if in_transaction {
            let elapsed = txn_start.elapsed();
            if elapsed >= max_age {
                flush_transaction(
                    &mut conn,
                    &committed,
                    &mut current_seq,
                    &mut in_transaction,
                    &mut pending_acks,
                );
                continue 'main_loop;
            }
            let remaining = max_age - elapsed;
            match receiver.recv_timeout(remaining) {
                Ok(m) => m,
                Err(GkQueueError::TIMEOUT) => {
                    flush_transaction(
                        &mut conn,
                        &committed,
                        &mut current_seq,
                        &mut in_transaction,
                        &mut pending_acks,
                    );
                    continue 'main_loop;
                }
                Err(_) => {
                    flush_transaction(
                        &mut conn,
                        &committed,
                        &mut current_seq,
                        &mut in_transaction,
                        &mut pending_acks,
                    );
                    break 'main_loop;
                }
            }
        } else {
            match receiver.recv() {
                Ok(m) => m,
                Err(_) => break 'main_loop,
            }
        };

        match msg {
            GkSqliteMsg::Write { cmd, reply } => {
                if active_session.is_some() {
                    // Buffer write while session transaction is open
                    pending_writes.push_back((cmd, reply, Instant::now()));
                    continue 'main_loop;
                }

                if !in_transaction {
                    if let Err(e) = exec_cached(&conn, "BEGIN IMMEDIATE") {
                        reply.send(Err(GkSqliteErrorEnum::SqliteError(e)));
                        continue 'main_loop;
                    }
                    in_transaction = true;
                    txn_start = Instant::now();
                }

                match execute_write_isolated(&mut conn, cmd) {
                    Ok((rows, last_id)) => {
                        current_seq += 1;
                        pending_acks.push(GkSqlitePendingAck {
                            reply,
                            rows,
                            seq: current_seq,
                            last_insert_rowid: last_id,
                        });
                    }
                    Err(e) => {
                        reply.send(Err(e));
                    }
                }

                if is_autocommit {
                    buffered_msg = drain_and_flush_autocommit(
                        &mut conn,
                        &mut receiver,
                        &committed,
                        &mut current_seq,
                        &mut in_transaction,
                        &mut pending_acks,
                        max_batch,
                    );
                } else if pending_acks.len() >= max_batch || {
                    // BRANCH: peek for a queued message; flush only when the queue is empty
                    buffered_msg = receiver.try_recv();
                    buffered_msg.is_none()
                } {
                    flush_transaction(
                        &mut conn,
                        &committed,
                        &mut current_seq,
                        &mut in_transaction,
                        &mut pending_acks,
                    );
                }
            }
            GkSqliteMsg::BeginSession { reply } => {
                // Expire timed-out pending sessions
                while let Some((_, _, s_start)) = pending_sessions.front() {
                    if s_start.elapsed() > busy_timeout {
                        let (_, s_reply, _) = pending_sessions.pop_front().unwrap();
                        s_reply.send(Err(GkSqliteErrorEnum::Timeout));
                    } else {
                        break;
                    }
                }

                if active_session.is_none() {
                    if in_transaction {
                        flush_transaction(
                            &mut conn,
                            &committed,
                            &mut current_seq,
                            &mut in_transaction,
                            &mut pending_acks,
                        );
                    }
                    match exec_cached(&conn, "BEGIN IMMEDIATE") {
                        Ok(()) => {
                            let sid = next_session_id;
                            next_session_id += 1;
                            active_session = Some(sid);
                            active_session_info = Some((sid, Instant::now()));
                            reply.send(Ok(sid));
                        }
                        Err(e) => {
                            reply.send(Err(GkSqliteErrorEnum::SqliteError(e)));
                        }
                    }
                } else {
                    let sid = next_session_id;
                    next_session_id += 1;
                    pending_sessions.push_back((sid, reply, Instant::now()));
                }
            }
            GkSqliteMsg::SessionExecute {
                session_id,
                cmd,
                reply,
            } => {
                if active_session != Some(session_id) {
                    reply.send(Err(GkSqliteErrorEnum::InvalidInput(
                        "Invalid or expired session ID",
                    )));
                } else {
                    active_session_info = Some((session_id, Instant::now()));
                    match execute_write_command(&mut conn, cmd) {
                        Ok((rows, last_id)) => {
                            current_seq += 1;
                            reply.send(Ok(GkSqliteWriteOutcome {
                                rows,
                                seq: current_seq,
                                last_insert_rowid: last_id,
                            }));
                        }
                        Err(e) => {
                            reply.send(Err(e));
                        }
                    }
                }
            }
            GkSqliteMsg::SessionQuery {
                session_id,
                cmd,
                reply,
            } => {
                if active_session != Some(session_id) {
                    reply.send(Err(GkSqliteErrorEnum::InvalidInput(
                        "Invalid or expired session ID",
                    )));
                } else {
                    active_session_info = Some((session_id, Instant::now()));
                    let res = execute_read_command(&conn, cmd);
                    reply.send(res);
                }
            }
            GkSqliteMsg::SessionSavepoint {
                session_id,
                action,
                reply,
            } => {
                if active_session != Some(session_id) {
                    reply.send(Err(GkSqliteErrorEnum::InvalidInput(
                        "Invalid or expired session ID",
                    )));
                } else {
                    active_session_info = Some((session_id, Instant::now()));
                    let sql = match &action {
                        GkSavepointAction::Savepoint(name) => {
                            format!("SAVEPOINT \"{}\";", name.replace('"', "\"\""))
                        }
                        GkSavepointAction::RollbackTo(name) => {
                            format!("ROLLBACK TO \"{}\";", name.replace('"', "\"\""))
                        }
                        GkSavepointAction::Release(name) => {
                            format!("RELEASE \"{}\";", name.replace('"', "\"\""))
                        }
                    };
                    let res = conn
                        .execute_batch(&sql)
                        .map_err(GkSqliteErrorEnum::SqliteError);
                    reply.send(res);
                }
            }
            GkSqliteMsg::EndSession {
                session_id,
                commit,
                reply,
            } => {
                if active_session != Some(session_id) {
                    if let Some(r) = reply {
                        r.send(Err(GkSqliteErrorEnum::InvalidInput(
                            "Invalid or expired session ID",
                        )));
                    }
                } else {
                    let last_id = conn.last_insert_rowid();
                    let outcome = if commit {
                        match exec_cached(&conn, "COMMIT") {
                            Ok(()) => {
                                current_seq += 1;
                                committed.store(current_seq, Ordering::Release);
                                Ok(GkSqliteWriteOutcome {
                                    rows: 0,
                                    seq: current_seq,
                                    last_insert_rowid: last_id,
                                })
                            }
                            Err(e) => {
                                log::error!("SQLite session COMMIT failed: {:?}; rolling back", e);
                                let _ = exec_cached(&conn, "ROLLBACK");
                                current_seq = committed.load(Ordering::Acquire);
                                Err(GkSqliteErrorEnum::SqliteError(e))
                            }
                        }
                    } else {
                        let _ = exec_cached(&conn, "ROLLBACK");
                        current_seq = committed.load(Ordering::Acquire);
                        Ok(GkSqliteWriteOutcome {
                            rows: 0,
                            seq: current_seq,
                            last_insert_rowid: 0,
                        })
                    };

                    if let Some(r) = reply {
                        r.send(outcome);
                    }

                    active_session = None;
                    active_session_info = None;

                    // Transition: check pending_sessions first
                    let mut started_new_session = false;
                    while let Some((sid, s_reply, s_start)) = pending_sessions.pop_front() {
                        if s_start.elapsed() > busy_timeout {
                            s_reply.send(Err(GkSqliteErrorEnum::Timeout));
                            continue;
                        }
                        match exec_cached(&conn, "BEGIN IMMEDIATE") {
                            Ok(()) => {
                                active_session = Some(sid);
                                active_session_info = Some((sid, Instant::now()));
                                s_reply.send(Ok(sid));
                                started_new_session = true;
                                break;
                            }
                            Err(e) => {
                                s_reply.send(Err(GkSqliteErrorEnum::SqliteError(e)));
                            }
                        }
                    }

                    // If no session is active, drain pending_writes!
                    if !started_new_session && !pending_writes.is_empty() {
                        let mut begin_failed = false;
                        // BRANCH: open the transaction the drained writes will share
                        if !in_transaction {
                            match exec_cached(&conn, "BEGIN IMMEDIATE") {
                                Ok(()) => {
                                    in_transaction = true;
                                    txn_start = Instant::now();
                                }
                                Err(e) => {
                                    log::error!("SQLite BEGIN after session failed: {:?}", e);
                                    begin_failed = true;
                                }
                            }
                        }
                        // BRANCH: without a transaction nothing may run; answer every buffered write with a busy error
                        if begin_failed {
                            while let Some((_, w_reply, _)) = pending_writes.pop_front() {
                                w_reply.send(Err(GkSqliteErrorEnum::SqliteError(
                                    rusqlite::Error::SqliteFailure(
                                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                                        Some("could not begin a transaction for writes queued behind a session".to_owned()),
                                    ),
                                )));
                            }
                        }
                        while let Some((cmd, w_reply, w_start)) = pending_writes.pop_front() {
                            if w_start.elapsed() > busy_timeout {
                                w_reply.send(Err(GkSqliteErrorEnum::Timeout));
                                continue;
                            }
                            match execute_write_isolated(&mut conn, cmd) {
                                Ok((rows, last_id)) => {
                                    current_seq += 1;
                                    pending_acks.push(GkSqlitePendingAck {
                                        reply: w_reply,
                                        rows,
                                        seq: current_seq,
                                        last_insert_rowid: last_id,
                                    });
                                }
                                Err(e) => {
                                    w_reply.send(Err(e));
                                }
                            }
                        }
                        if is_autocommit {
                            flush_transaction(
                                &mut conn,
                                &committed,
                                &mut current_seq,
                                &mut in_transaction,
                                &mut pending_acks,
                            );
                        }
                    }
                }
            }
            GkSqliteMsg::TransactionClosure { closure, reply } => {
                if active_session.is_some() {
                    reply.send(Err(GkSqliteErrorEnum::Timeout));
                    continue 'main_loop;
                }

                if !in_transaction {
                    if let Err(e) = exec_cached(&conn, "BEGIN IMMEDIATE") {
                        reply.send(Err(GkSqliteErrorEnum::SqliteError(e)));
                        continue 'main_loop;
                    }
                    in_transaction = true;
                    txn_start = Instant::now();
                }

                if let Err(e) = sp_begin(&conn) {
                    reply.send(Err(GkSqliteErrorEnum::SqliteError(e)));
                    continue 'main_loop;
                }

                let closure_result = closure(&mut conn);
                match closure_result {
                    Ok(()) => {
                        let _ = sp_release(&conn);
                        current_seq += 1;
                        let last_id = conn.last_insert_rowid();
                        pending_acks.push(GkSqlitePendingAck {
                            reply,
                            rows: 0,
                            seq: current_seq,
                            last_insert_rowid: last_id,
                        });
                    }
                    Err(e) => {
                        let _ = sp_rollback(&conn);
                        reply.send(Err(e));
                    }
                }

                if is_autocommit || pending_acks.len() >= max_batch || {
                    // BRANCH: peek for a queued message; flush only when the queue is empty
                    buffered_msg = receiver.try_recv();
                    buffered_msg.is_none()
                } {
                    flush_transaction(
                        &mut conn,
                        &committed,
                        &mut current_seq,
                        &mut in_transaction,
                        &mut pending_acks,
                    );
                }
            }
            GkSqliteMsg::ReadFallback { cmd, reply } => {
                let res = execute_read_command(&conn, cmd);
                reply.send(res);
            }
            GkSqliteMsg::Control { cmd, arg, reply } => {
                if active_session.is_some() {
                    // BRANCH: shutdown rolls the open session back and exits; other controls wait for it to end
                    if cmd == CTL_SHUTDOWN {
                        let _ = exec_cached(&conn, "ROLLBACK");
                        active_session = None;
                        active_session_info = None;
                    } else {
                        if let Some(r) = reply {
                            r.send(Err(GkSqliteErrorEnum::Timeout));
                        }
                        continue 'main_loop;
                    }
                }

                // Ensure pending work is committed before executing control command
                if in_transaction {
                    flush_transaction(
                        &mut conn,
                        &committed,
                        &mut current_seq,
                        &mut in_transaction,
                        &mut pending_acks,
                    );
                }

                if handle_control_command(
                    &mut conn,
                    &committed,
                    &autocommit_mode,
                    &mut max_batch,
                    cmd,
                    arg,
                    reply,
                ) {
                    break 'main_loop;
                }
            }
        }
    }

    // Final shutdown flush
    if in_transaction {
        flush_transaction(
            &mut conn,
            &committed,
            &mut current_seq,
            &mut in_transaction,
            &mut pending_acks,
        );
    }
}

fn handle_control_command(
    conn: &mut Connection,
    committed: &GkSqliteCommitCounter,
    autocommit_mode: &Arc<AtomicU8>,
    max_batch: &mut usize,
    cmd: u32,
    arg: u64,
    reply: Option<GkReplySender<Result<u64, GkSqliteErrorEnum>>>,
) -> bool {
    match cmd {
        CTL_COMMIT => {
            if let Some(r) = reply {
                r.send(Ok(committed.load(Ordering::Acquire)));
            }
            false
        }
        CTL_SET_AUTOCOMMIT => {
            let mode = if arg != 0 { 1 } else { 0 };
            autocommit_mode.store(mode, Ordering::Release);
            if let Some(r) = reply {
                r.send(Ok(mode as u64));
            }
            false
        }
        CTL_SET_BATCH => {
            *max_batch = (arg as usize).max(1);
            if let Some(r) = reply {
                r.send(Ok(*max_batch as u64));
            }
            false
        }
        CTL_CHECKPOINT => {
            let res = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);");
            if let Some(r) = reply {
                r.send(
                    res.map(|_| committed.load(Ordering::Acquire))
                        .map_err(GkSqliteErrorEnum::SqliteError),
                );
            }
            false
        }
        CTL_SHUTDOWN => {
            if let Some(r) = reply {
                r.send(Ok(committed.load(Ordering::Acquire)));
            }
            true
        }
        _ => {
            if let Some(r) = reply {
                r.send(Err(GkSqliteErrorEnum::InvalidInput(
                    "Unknown control command",
                )));
            }
            false
        }
    }
}

fn drain_and_flush_autocommit(
    conn: &mut Connection,
    receiver: &mut GkWriterRx<GkSqliteMsg>,
    committed: &GkSqliteCommitCounter,
    current_seq: &mut u64,
    in_transaction: &mut bool,
    pending_acks: &mut Vec<GkSqlitePendingAck>,
    max_batch: usize,
) -> Option<GkSqliteMsg> {
    let mut next_msg = None;
    while pending_acks.len() < max_batch {
        // BRANCH: Drain available writes in autocommit batch
        match receiver.try_recv() {
            Some(GkSqliteMsg::Write { cmd, reply }) => match execute_write_isolated(conn, cmd) {
                Ok((rows, last_id)) => {
                    *current_seq += 1;
                    pending_acks.push(GkSqlitePendingAck {
                        reply,
                        rows,
                        seq: *current_seq,
                        last_insert_rowid: last_id,
                    });
                }
                Err(e) => {
                    reply.send(Err(e));
                }
            },
            Some(other) => {
                next_msg = Some(other);
                break;
            }
            None => break,
        }
    }

    flush_transaction(conn, committed, current_seq, in_transaction, pending_acks);

    next_msg
}

fn flush_transaction(
    conn: &mut Connection,
    committed: &GkSqliteCommitCounter,
    current_seq: &mut u64,
    in_transaction: &mut bool,
    pending_acks: &mut Vec<GkSqlitePendingAck>,
) {
    if !*in_transaction {
        return;
    }

    if pending_acks.is_empty() {
        let _ = exec_cached(conn, "COMMIT");
        *in_transaction = false;
        return;
    }

    // Crucial Implementation Order:
    // 1. tx.commit()?
    // 2. committed.store(seq, Ordering::Release)
    // 3. Send WriteOutcome across reply channel.
    match exec_cached(conn, "COMMIT") {
        Ok(()) => {
            let last_seq = pending_acks.last().map(|a| a.seq).unwrap_or(*current_seq);
            committed.store(last_seq, Ordering::Release);
            for ack in pending_acks.drain(..) {
                ack.reply.send(Ok(GkSqliteWriteOutcome {
                    rows: ack.rows,
                    seq: ack.seq,
                    last_insert_rowid: ack.last_insert_rowid,
                }));
            }
            *in_transaction = false;
        }
        Err(e) => {
            log::error!("SQLite COMMIT failed: {:?}; rolling back", e);
            let _ = exec_cached(conn, "ROLLBACK");
            // Reset local sequence counter to last successfully committed sequence
            *current_seq = committed.load(Ordering::Acquire);
            for ack in pending_acks.drain(..) {
                ack.reply.send(Err(GkSqliteErrorEnum::SqliteError(
                    rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                        Some(format!("Commit failed: {:?}", e)),
                    ),
                )));
            }
            *in_transaction = false;
        }
    }
}

fn execute_write_command(
    conn: &mut Connection,
    cmd: GkSqliteWriteCommand,
) -> Result<(usize, i64), GkSqliteErrorEnum> {
    match cmd {
        GkSqliteWriteCommand::Execute { sql, params } => {
            let mut stmt = conn.prepare_cached(&sql)?;
            // A statement that yields columns (INSERT/UPDATE/DELETE ... RETURNING) must be stepped
            // as a query; SQLite reports this exactly, with no scan of the SQL text.
            if stmt.column_count() > 0 {
                let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
                let mut returned_id = conn.last_insert_rowid();
                let mut count = 0usize;
                while let Some(row) = rows.next()? {
                    if count == 0
                        && let Ok(val) = row.get_ref(0)
                        && let ValueRef::Integer(i) = val
                    {
                        returned_id = i;
                    }
                    count += 1;
                }
                drop(rows);
                Ok((count, returned_id))
            } else {
                let rows = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
                let last_id = conn.last_insert_rowid();
                Ok((rows, last_id))
            }
        }
        GkSqliteWriteCommand::ExecuteBatch { sql } => {
            conn.execute_batch(&sql)?;
            Ok((0, conn.last_insert_rowid()))
        }
        GkSqliteWriteCommand::TransactionStatements { statements } => {
            let mut total_rows = 0;
            for (sql, params) in statements {
                let mut stmt = conn.prepare_cached(&sql)?;
                total_rows += stmt.execute(rusqlite::params_from_iter(params.iter()))?;
            }
            Ok((total_rows, conn.last_insert_rowid()))
        }
    }
}

fn execute_read_command(
    conn: &Connection,
    cmd: GkSqliteReadCommand,
) -> Result<Vec<GkSqliteRowRecord>, GkSqliteErrorEnum> {
    match cmd {
        GkSqliteReadCommand::QueryRow { sql, params } => {
            let mut stmt = conn.prepare_cached(&sql)?;
            let col_names: Arc<[Arc<str>]> = stmt
                .column_names()
                .map(Arc::from)
                .collect::<Vec<_>>()
                .into();
            let col_count = col_names.len();
            let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
            if let Some(row) = rows.next()? {
                let mut values = SmallVec::with_capacity(col_count);
                for i in 0..col_count {
                    let val_ref = row.get_ref(i)?;
                    values.push(sql_value_from_ref(val_ref)?);
                }
                // Ensure rows is exhausted before returning
                drop(rows);
                Ok(vec![GkSqliteRowRecord {
                    column_names: col_names,
                    values,
                }])
            } else {
                drop(rows);
                Err(GkSqliteErrorEnum::SqliteError(
                    rusqlite::Error::QueryReturnedNoRows,
                ))
            }
        }
        GkSqliteReadCommand::QueryAll { sql, params } => {
            let mut stmt = conn.prepare_cached(&sql)?;
            let col_names: Arc<[Arc<str>]> = stmt
                .column_names()
                .map(Arc::from)
                .collect::<Vec<_>>()
                .into();
            let col_count = col_names.len();
            let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
            let mut records = Vec::new();
            while let Some(row) = rows.next()? {
                let mut values = SmallVec::with_capacity(col_count);
                for i in 0..col_count {
                    let val_ref = row.get_ref(i)?;
                    values.push(sql_value_from_ref(val_ref)?);
                }
                records.push(GkSqliteRowRecord {
                    column_names: Arc::clone(&col_names),
                    values,
                });
            }
            drop(rows);
            Ok(records)
        }
    }
}

fn sql_value_from_ref(val_ref: ValueRef<'_>) -> Result<GkSqliteValue, GkSqliteErrorEnum> {
    match val_ref {
        ValueRef::Null => Ok(GkSqliteValue::Null),
        ValueRef::Integer(i) => Ok(GkSqliteValue::Integer(i)),
        ValueRef::Real(f) => Ok(GkSqliteValue::Real(f)),
        ValueRef::Text(s) => {
            let s_str = std::str::from_utf8(s).map_err(|e| {
                GkSqliteErrorEnum::SqliteError(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                ))
            })?;
            Ok(GkSqliteValue::Text(crate::counter::GkCompactStr::new(
                s_str,
            )))
        }
        ValueRef::Blob(b) => Ok(GkSqliteValue::Blob(SmallVec::from_slice(b))),
    }
}
