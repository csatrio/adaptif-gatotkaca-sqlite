# adaptif-gatotkaca-sqlite

A SQLite engine for Rust. Reads run lock-free on thread-local connections, and writes go through one writer thread. Your code calls one API (`GkSqliteDb`), and the crate routes each statement.

Part of the adaptif-gatotkaca engine, developed by PT Adaptif Sinergi Dinamis.

## Features

- **Automatic routing.** Read-only statements run on the caller's own thread-local read connection. Writes go to a single writer thread. `INSERT ... RETURNING` is routed to the writer.
- **Lock-free paths.** No `Mutex`, `RwLock` or `Condvar` on the read or write path. Writes use the lock-free Treiber channel by default (crossbeam is optional).
- **Write acknowledgements.** A write returns once it is committed, with rows affected, a commit sequence number (`seq`) and the last insert rowid.
- **Transactions and sessions.** `transaction(|conn| ..)` runs a closure on the writer thread. Sessions hold a transaction open across several calls.
- **Async API.** `*_async` methods return a plain `std::future::Future`. They don't depend on a runtime. You can run reads on the built-in reader pool, your own executor, or tokio.
- **Typed configuration.** Builder with validation, presets (`throughput()`, `durable()`, `low_memory()`), typed WAL/cache/mmap pragmas, and a connection hook.
- **Prepared statements.** Each connection caches 256 statements.

## Install

```toml
[dependencies]
adaptif-gatotkaca-sqlite = "0.2"

# Optional
# adaptif-gatotkaca-sqlite = { version = "0.2", features = ["tokio"] }
# adaptif-gatotkaca-sqlite = { version = "0.2", features = ["crossbeam-channel"] }
```

| Feature | Default | Adds |
| --- | --- | --- |
| `treiber-channel` | yes | the default writer channel (`GkChannelKind::Treiber`) |
| `crossbeam-channel` | no | `GkChannelKind::Crossbeam` and the `crossbeam_channel` re-export |
| `tokio` | no | `GkTokioExecutor` for running reads on a tokio runtime |

SQLite is bundled (`rusqlite` with the `bundled` feature), so no system library is needed.

## Quick start

```rust
use adaptif_gatotkaca_sqlite::{GkSqliteConfig, GkSqliteDb, GkSqliteErrorEnum};

fn main() -> Result<(), GkSqliteErrorEnum> {
    let db = GkSqliteDb::open("app.db", GkSqliteConfig::default())?;

    db.execute_batch("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;

    // Writes return once they are committed.
    let outcome = db.execute("INSERT INTO users (name) VALUES (?1)", ("alice",))?;
    println!("rows={} seq={} rowid={}", outcome.rows, outcome.seq, outcome.last_insert_rowid);

    // Reads run on the calling thread's own read connection.
    let name: String = db.query_row("SELECT name FROM users WHERE id = ?1", (1,), |row| row.get(0))?;
    let names: Vec<String> = db.query_map("SELECT name FROM users ORDER BY id", (), |row| row.get(0))?;

    println!("{name} {names:?}");
    Ok(())
}
```

`GkSqliteDb` is cheap to clone. Clone it into each thread or task that needs it.

## Parameters

Parameters accept:

- `()` for none,
- tuples of up to several values, such as `("alice", 3)`,
- arrays or slices of values,
- `Option<T>` for a nullable value.

Values convert from `i64`, `i32`, `usize`, `f64`, `&str`, `String`, `Vec<u8>` and `Option<T>`.

## Configuration

```rust
use std::time::Duration;
use adaptif_gatotkaca_sqlite::{
    GkChannelKind, GkReadMode, GkSqliteConfig, GkSqliteDb, GkSqlitePragmas, GkThreadOptions,
};

let config = GkSqliteConfig::builder()
    .busy_timeout(Duration::from_secs(5))
    .writer_queue_size(1024)                             // per producing thread (Treiber), total (crossbeam)
    .writer_channel(GkChannelKind::Treiber)
    .writer_pragmas(GkSqlitePragmas::writer_default())   // WAL + synchronous=NORMAL
    .reader_pragmas(GkSqlitePragmas::reader_default())   // query_only, mmap 256 MB, cache 64 MB
    .writer_thread(GkThreadOptions::default().named("sqlite-writer"))
    .read_mode(GkReadMode::Inline)
    .on_connection_open(|conn, role| {
        // Runs on every new connection, after its pragmas. e.g. register functions.
        Ok(())
    })
    .build()?;                                           // validates the config

let db = GkSqliteDb::open("app.db", config)?;
```

Presets, if the defaults don't fit:

- `GkSqliteConfig::throughput()`
- `GkSqliteConfig::durable()` (`synchronous=FULL`)
- `GkSqliteConfig::low_memory()`
- `GkSqliteConfig::default()` (same as the 0.1 behaviour)

`open` validates the config too.

## Sync API

Writes (`execute`, `execute_batch`, `execute_insert`, `execute_many`) run on the writer thread and return once committed:

```rust
db.execute("UPDATE users SET name = ?1 WHERE id = ?2", ("bob", 1))?;
let rowid = db.execute_insert("INSERT INTO users (name) VALUES (?1)", ("carol",))?;
db.execute_batch("CREATE INDEX IF NOT EXISTS idx_users_name ON users(name);")?;
```

`execute_many` runs one statement for each set of parameters:

```rust
use adaptif_gatotkaca_sqlite::GkSqliteParams;

let rows: Vec<GkSqliteParams> = vec![("dave",).into(), ("erin",).into()];
db.execute_many("INSERT INTO users (name) VALUES (?1)", rows)?;
```

Reads run on the calling thread:

- `query_row(sql, params, |row| ..)` returns one row.
- `query_map(sql, params, |row| ..)` returns a `Vec` of rows.
- `query_records(sql, params)` returns `GkSqliteRowRecord` values, which hold the columns without a closure.

`INSERT ... RETURNING` works with `query_row` and `query_records` and is routed to the writer.

## Transactions

Use `transaction` for multiple statements that must commit together. The closure runs on the writer thread:

```rust
let total: i64 = db.transaction(|conn| {
    conn.execute("INSERT INTO users (name) VALUES (?1)", ["frank"])?;
    conn.execute("INSERT INTO users (name) VALUES (?1)", ["grace"])?;
    let n: i64 = conn.query_row("SELECT count(*) FROM users", [], |r| r.get(0))?;
    Ok(n)
})?;
```

Inside the closure, reads see the closure's own uncommitted writes.

Rules:

- Don't call `db` again from inside the closure. The nested call waits on the writer thread, which is the thread running the closure, so it deadlocks.
- Explicit `BEGIN`, `COMMIT` and `ROLLBACK` through `execute` return `UseTransactionApi`. Use `transaction` or sessions instead.

## Sessions

A session keeps a transaction open across calls. Use it when the steps are not all known up front, such as a request that reads, decides, then writes:

```rust
let session = db.begin_session()?;
db.session_execute(session, "INSERT INTO users (name) VALUES (?1)", ("heidi",))?;
let outcome = db.end_session(session, true)?;   // true = commit, false = roll back
```

Writes that arrive on other threads while a session is open queue behind it. Keep sessions short. Dropping or closing the database rolls back an open session.

## Async API

Each `*_async` call starts the work when called and returns a `GkSqliteFuture<T>`:

```rust
let fut = db.execute_async("INSERT INTO users (name) VALUES (?1)", ("ivan",));
let outcome = fut.await?;          // any executor
// or, on a plain thread:
// let outcome = fut.wait()?;
```

Methods:

- `execute_async` (writes),
- `query_row_async` and `query_map_async` (read-only statements),
- `query_records_async` (reads, and `INSERT ... RETURNING`),
- `read(|conn| ..)` for any read work with a closure.

Async reads don't wait on this thread's pending write. `await` the write first, since its acknowledgement already means it committed.

Reads run on the `GkReadMode` executor. If the executor rejects a job, the job runs on the caller.

## Read modes

```rust
use std::sync::Arc;
use adaptif_gatotkaca_sqlite::{GkReadMode, GkReaderPoolConfig};

// Inline (default): reads run on the calling thread.
GkReadMode::Inline

// Built-in lock-free pool of reader threads.
GkReadMode::Pool(GkReaderPoolConfig::default().with_threads(8))

// Your own executor (implement GkReaderExecutor), or tokio:
// GkReadMode::Custom(Arc::new(GkTokioExecutor::current()))
```

`GkTokioExecutor::new(handle)` and `GkTokioExecutor::current()` run reads on `spawn_blocking`. Enable the `tokio` feature.

Your own executor implements `GkReaderExecutor::spawn(&self, job) -> Result<(), GkReadJob>`. Return `Err(job)` to have the caller run the job.

## Reader thread pool (`GkReaderPool`)

`GkReaderPool` is the built-in pool of worker threads for read jobs. Each worker keeps its own read-only connection, opened on first use. The job queue is lock-free (`GkTreiberMpmc`), so the pool has no `Mutex`, `RwLock` or `Condvar`.

You can get the pool two ways:

- **Through the database config.** `GkReadMode::Pool(GkReaderPoolConfig)` creates the pool with the database.
- **Yourself.** Create a `GkReaderPool` and pass it as a custom executor. Clones share the same pool.

```rust
use std::sync::Arc;
use std::time::Duration;
use adaptif_gatotkaca_sqlite::{GkReadMode, GkReaderPool, GkReaderPoolConfig, GkSqliteConfig, GkSqliteDb};

// 1. Through the database
let db = GkSqliteDb::open(
    "app.db",
    GkSqliteConfig::builder()
        .read_mode(GkReadMode::Pool(GkReaderPoolConfig::default().with_threads(8)))
        .build()?,
)?;

// 2. Your own pool, shared by reads from this database
let pool = GkReaderPool::new(
    GkReaderPoolConfig::default()
        .with_threads(8)
        .with_rescue_after(Duration::from_micros(200)),
)?;
let db = GkSqliteDb::open(
    "app.db",
    GkSqliteConfig::builder().read_mode(GkReadMode::Custom(Arc::new(pool.clone()))).build()?,
)?;

println!("{} workers, {} jobs pending", pool.threads(), pool.pending());
pool.shutdown(); // queued jobs still run; new submits run on the caller
```

### Configuration (`GkReaderPoolConfig`)

| Field | Default | What it does |
| --- | --- | --- |
| `threads` | `0` | Worker count. `0` means one per available core. Maximum `MAX_READER_THREADS` (4096). |
| `thread` | name `sqlite-reader` | Thread name prefix, stack size, `on_start` / `on_stop` hooks (`GkThreadOptions`). |
| `max_lanes` | `64` | Threads that can have jobs queued at the same time. See the lane note below. |
| `spin_polls` | `128` | Empty polls before a worker sleeps. |
| `rescue_after` | `Duration::ZERO` | `0`: idle workers share medium-size jobs from a busy worker, so they spread across the pool. Non-zero: an idle worker takes over a job only after the busy worker has been stuck on it that long (for example in `fsync`). |
| `max_queue` | `None` | `None`: unbounded. `Some(n)`: a submit is refused while `n` jobs are pending. |

Builder methods: `with_threads`, `with_thread`, `with_max_lanes`, `with_spin_polls`, `with_rescue_after`, `with_max_queue`. `validate()` checks the config, and `GkReaderPool::new` calls it.

### Methods (`GkReaderPool`)

- `GkReaderPool::new(config) -> Result<GkReaderPool, GkReaderPoolError>`: validates the config and starts the workers.
- `threads()`: worker count.
- `pending()`: jobs submitted and not yet taken by a worker. Approximate.
- `is_shutdown()`: whether `shutdown` has been called on any handle.
- `shutdown()`: stops accepting jobs, lets workers finish what is queued, and joins them. Safe to call more than once. The pool also shuts down when the last handle is dropped.

### Behaviour to plan for

- **Refused jobs run on the caller.** If a submit is refused (pool shut down, `max_queue` full, or the thread has no lane), the job runs on the calling thread. Reads still succeed, just without the pool.
- **Lanes are held per submitting thread.** Each thread that submits holds one lane until it exits. The lane is then reused by the next submitting thread. So `max_lanes` limits how many threads can submit at the same time. A server that has more concurrent submitting threads than that should raise `max_lanes`, or submit from a tokio runtime whose threads are reused.
- **Error values:** `GkReaderPoolError::INVALID_CONFIG` (bad config) and `GkReaderPoolError::SPAWN_FAILED` (the OS refused a worker thread; the workers already started are stopped).
- **Choosing `rescue_after`:** leave it at zero for mostly short reads or medium reads from a few callers. Set it (for example 50 µs to 1 ms) when reads can block for a long time, so a stuck worker does not hold up jobs queued behind it.

## Writer channel

- `GkChannelKind::Treiber` (default): FIFO per producing thread.
- `GkChannelKind::Crossbeam` (feature `crossbeam-channel`): FIFO across all threads.

## Pragmas

`GkSqlitePragmas` has typed fields for `journal_mode`, `synchronous`, `cache_size_kib`, `mmap_size`, `temp_store`, `wal_autocheckpoint` and others. Unknown pragmas go through `with_extra(name, value)`, which validates the value:

```rust
use adaptif_gatotkaca_sqlite::GkSqlitePragmas;

let pragmas = GkSqlitePragmas::writer_default();
pragmas.validate().expect("valid pragmas");
```

## Errors

`GkSqliteErrorEnum` variants:

| Variant | Meaning |
| --- | --- |
| `SqliteError(rusqlite::Error)` | the statement failed in SQLite |
| `WriterDead` | the writer thread stopped |
| `UseTransactionApi` | explicit `BEGIN`/`COMMIT`/`ROLLBACK` sent through `execute` |
| `Timeout` | a wait exceeded its limit |
| `NotAutocommit` | the operation needs autocommit mode |
| `InvalidInput(&str)` | bad input, such as an invalid config |
| `ReaderUnavailable` | the reader executor rejected or dropped a read job |

A write error is final for that statement: the crate does not retry it, and neither should your code. The reader path never retries on its own either. Reads have no side effects, so you can retry them yourself if you want.

## Checklist when adding it to a project

1. Open one `GkSqliteDb` at startup and clone it where needed.
2. Put the schema in `execute_batch` at startup.
3. Use `execute` for single writes, and `transaction` for multi-statement writes.
4. Use `query_row` and `query_map` for reads. Use `*_async` inside async code.
5. Choose a read mode: inline is fine for most apps. Use the pool or tokio when reads are heavy.
6. Don't call the database from inside a `transaction` closure.
