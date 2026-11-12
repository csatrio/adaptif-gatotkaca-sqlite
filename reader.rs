// ============================================================================
// Import Definition
// ============================================================================

use crate::SqliteFastU64Map;
use crate::config::GkConnectionOptions;
use crate::error::GkSqliteErrorEnum;
use rusqlite::{Connection, OpenFlags, Params, Row};
use std::cell::RefCell;

// ============================================================================
// Thread-Local Readers Map
// ============================================================================

thread_local! {
    static READERS: RefCell<SqliteFastU64Map<Connection>> = RefCell::new(SqliteFastU64Map::new());
}

pub struct GkSqliteReader;

impl GkSqliteReader {
    /// Executes a closure using the thread-local read-only connection for `db_id`.
    /// Lazily opens and initializes the connection with query_only, mmap, and autocommit assertions.
    pub fn with_connection<F, R>(
        db_id: u64,
        path: &str,
        options: &GkConnectionOptions,
        f: F,
    ) -> Result<R, GkSqliteErrorEnum>
    where
        F: FnOnce(&Connection) -> Result<R, GkSqliteErrorEnum>,
    {
        READERS.with(|cell| {
            let mut map = cell.borrow_mut();
            if let std::collections::hash_map::Entry::Vacant(e) = map.entry(db_id) {
                let conn = open_reader_connection(path, options)?;
                e.insert(conn);
            }
            let conn = map.get(&db_id).expect("reader connection exists");

            // Invariant check: Reader must strictly be in autocommit mode before query
            if !conn.is_autocommit() {
                return Err(GkSqliteErrorEnum::NotAutocommit);
            }

            let result = f(conn);

            // Invariant check: Reader must strictly be in autocommit mode after query
            if !conn.is_autocommit() {
                return Err(GkSqliteErrorEnum::NotAutocommit);
            }

            result
        })
    }

    /// Completely closes and removes the cached reader connection for `db_id` on this thread.
    pub fn close_thread_reader(db_id: u64) {
        READERS.with(|cell| {
            cell.borrow_mut().remove(&db_id);
        });
    }

    /// Completely clears all cached reader connections on this thread.
    pub fn clear_thread_readers() {
        READERS.with(|cell| {
            cell.borrow_mut().clear();
        });
    }
}

fn open_reader_connection(
    path: &str,
    options: &GkConnectionOptions,
) -> Result<Connection, GkSqliteErrorEnum> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    options.initialise(&conn)?;
    Ok(conn)
}

// ============================================================================
// Query Helpers with Guaranteed Exhaustion
// ============================================================================

/// Executes a query expecting exactly one row.
/// Invariant: `Rows` iterator is guaranteed to be fully consumed and dropped before return.
pub fn query_row<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    f: F,
) -> Result<T, GkSqliteErrorEnum>
where
    P: Params,
    F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
{
    let mut stmt = conn.prepare_cached(sql)?;
    let mut rows = stmt.query(params)?;
    let item = match rows.next()? {
        Some(r) => f(r)?,
        None => {
            return Err(GkSqliteErrorEnum::SqliteError(
                rusqlite::Error::QueryReturnedNoRows,
            ));
        }
    };
    drop(rows);
    Ok(item)
}

/// Executes a query mapping over all rows into a vector.
/// Invariant: `Rows` iterator is guaranteed to be fully consumed and dropped before return.
pub fn query_map<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    mut f: F,
) -> Result<Vec<T>, GkSqliteErrorEnum>
where
    P: Params,
    F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
{
    let mut stmt = conn.prepare_cached(sql)?;
    let mut rows = stmt.query(params)?;
    let mut results = Vec::new();
    while let Some(r) = rows.next()? {
        results.push(f(r)?);
    }
    drop(rows);
    Ok(results)
}
