// ============================================================================
// Import Definition
// ============================================================================

use crate::error::GkSqliteErrorEnum;
use rusqlite::Connection;
use std::{cell::RefCell, collections::HashMap};

// ============================================================================
// Routing Classification Functions
// ============================================================================

thread_local! {
    /// Read-only classification per SQL text. Whether a statement is read-only depends on its text
    /// alone, so one parse on this thread answers every later call without touching SQLite.
    static READONLY_CACHE: RefCell<HashMap<Box<str>, bool>> = RefCell::new(HashMap::new());
}

/// Upper bound on cached classifications per thread (dynamic SQL would otherwise grow it forever).
const READONLY_CACHE_LIMIT: usize = 4096;

#[inline]
fn starts_with_ignore_ascii_case(s: &str, prefix: &str) -> bool {
    s.as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
}

/// Returns true if the statement is an explicit transaction command that must use `db.transaction()`.
pub fn is_explicit_transaction_sql(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"]
        .iter()
        .any(|kw| starts_with_ignore_ascii_case(trimmed, kw))
}

/// Returns true if the statement is a PRAGMA that mutates database or WAL state.
pub fn is_state_modifying_pragma(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if starts_with_ignore_ascii_case(trimmed, "PRAGMA") {
        let rest = trimmed[6..].trim_start();
        [
            "JOURNAL_MODE",
            "SYNCHRONOUS",
            "WAL_CHECKPOINT",
            "USER_VERSION",
        ]
        .iter()
        .any(|kw| starts_with_ignore_ascii_case(rest, kw))
    } else {
        false
    }
}

/// Returns the cached classification for `sql` on this thread, if it was classified before.
/// Needs no connection, so callers can skip opening or borrowing the reader entirely.
#[inline]
pub fn cached_readonly(sql: &str) -> Option<bool> {
    READONLY_CACHE.with(|c| c.borrow().get(sql).copied())
}

/// Classifies a statement directly via SQLite's native `sqlite3_stmt_readonly`, remembering the
/// answer per thread. Parses with `conn.prepare()` (not the statement cache) so write statements
/// never occupy a slot in the reader's compiled-statement cache.
pub fn is_readonly_statement(conn: &Connection, sql: &str) -> Result<bool, GkSqliteErrorEnum> {
    // BRANCH: Reject raw transaction control statements
    if is_explicit_transaction_sql(sql) {
        return Err(GkSqliteErrorEnum::UseTransactionApi);
    }

    // BRANCH: Always force state-modifying PRAGMAs to writer
    if is_state_modifying_pragma(sql) {
        return Ok(false);
    }

    // BRANCH: Answered by an earlier parse on this thread
    if let Some(readonly) = cached_readonly(sql) {
        return Ok(readonly);
    }

    let readonly = conn.prepare(sql)?.readonly();
    READONLY_CACHE.with(|c| {
        let mut map = c.borrow_mut();
        if map.len() >= READONLY_CACHE_LIMIT {
            map.clear();
        }
        map.insert(Box::from(sql), readonly);
    });
    Ok(readonly)
}

/// Classifies an entire batch of statements.
/// If any statement in the batch is non-readonly, returns false (route entire batch to writer).
pub fn is_readonly_batch(conn: &Connection, sql: &str) -> Result<bool, GkSqliteErrorEnum> {
    for statement_sql in sql.split(';') {
        let trimmed = statement_sql.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !is_readonly_statement(conn, trimmed)? {
            return Ok(false);
        }
    }
    Ok(true)
}
