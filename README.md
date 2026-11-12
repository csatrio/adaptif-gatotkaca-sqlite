# adaptif-gatotkaca-sqlite

[![crates.io](https://img.shields.io/crates/v/adaptif-gatotkaca-sqlite.svg)](https://crates.io/crates/adaptif-gatotkaca-sqlite)
[![docs.rs](https://docs.rs/adaptif-gatotkaca-sqlite/badge.svg)](https://docs.rs/adaptif-gatotkaca-sqlite)

Part of the adaptif-gatotkaca engine: the SQLite storage layer,
developed by [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).

A high-performance, zero-contention, lock-free SQLite read/write routing engine for Rust built over SQLite in Write-Ahead Logging (WAL) mode.

Callers see a unified connection-like API (`GkSqliteDb`). Internally, queries and statements are automatically classified and routed:
- **Reads** execute lock-free on thread-local read-only connections with zero channel hops and zero thread contention.
- **Writes** route through a bounded queue (the gatotkaca Treiber channel by default, crossbeam on request) to a single dedicated writer thread.
- **Async reads and writes** (`execute_async`, `query_*_async`, `read`) return runtime-agnostic futures; reads run on a pool you choose: inline, the built-in lock-free reader pool, your own pool, or tokio.
- **Transactions** (`db.transaction(...)`) execute directly on the writer thread inside savepoints, enabling uncommitted read-your-own-writes isolation within the transaction.

---

## Key Architectural Principles

- **Zero Lock Primitives**: Strictly no `Mutex`, `RwLock`, `Condvar`, `parking_lot`, or `once_cell`. All concurrency is orchestrated via atomic counters (`Arc<AtomicU64>`), the lock-free writer queue, a lock-free reply oneshot (`reply_channel`) and the lock-free reader pool (`GkReaderPool`, built on `GkTreiberMpmc`).
- **Low-Allocation Hot Path** (a write still allocates its SQL `Arc` and one reply `Arc`; a `&'static str` SQL through the async API skips the copy):
  - Statements use structured command enums (`GkSqliteWriteCommand`, `GkSqliteReadCommand`) rather than arbitrary heap-boxed closures (`Box<dyn FnOnce...>`); the closure-based paths are `transaction` and the `read` / `*_async` jobs. SQL text travels as `GkSqlText`: a `&'static str` is carried without a copy.
  - A reply is one `Arc` allocation (`reply_channel`), about 10x cheaper to create than a crossbeam `bounded(1)` (17 ns against 208 ns create+send+wait on one thread).
  - Small parameters (up to 4 arguments) and small strings (up to 24 bytes) use inline stack storage via `GkSqliteParams` (`SmallVec<[GkSqliteValue; 4]>`) and `CompactString` (SSO).
- **Native Query Classification**:
  - Statements are classified directly via SQLite's native `sqlite3_stmt_readonly` on the thread-local reader (`GkSqliteReader`).
  - Non-readonly statements are dropped immediately without entering the compiled statement cache, avoiding statement cache pollution.
  - The verdict is remembered per thread, keyed by SQL text (`router::cached_readonly`, capped at 4096 entries). A repeated statement is classified with one hash lookup: no parse and no borrow of the reader connection. 
- **Prepared Statements Everywhere**:
  - Reader and writer connections keep 256 cached statements (`STATEMENT_CACHE_CAPACITY`; rusqlite's default of 16 thrashes quickly).
  - The writer's control statements (`BEGIN IMMEDIATE`, `COMMIT`, and `SAVEPOINT`/`RELEASE`/`ROLLBACK TO` for multi-statement commands) run through that cache with constant SQL text (the savepoint is always `gk_sp`). A single `Execute` runs without a savepoint: SQLite already undoes a failed single statement.
  - `execute_many`, `RETURNING` writes and the writer-side read fallbacks use `prepare_cached`.
  - SQL keyword checks (`BEGIN`/`COMMIT`/..., `PRAGMA ...`) compare ASCII case-insensitively in place instead of allocating an uppercased copy. `RETURNING` is detected from the prepared statement's column count, not by scanning the text.
  - `query_row`/`query_map` no longer clone and convert the parameters up front; they are converted once, on whichever path runs.
- **Causal Read-Your-Writes Consistency**:
  - Each write receives a monotonically increasing sequence token from the writer (`GkSqliteWriteOutcome`).
  - Writer strictly commits to the WAL and publishes the sequence with `Release` memory ordering *before* sending channel acknowledgments.
  - Reader threads check their thread-local token against `committed.load(Acquire)`. Under normal operation, the token is already globally visible, requiring 0ns wait time. If a future token is queried, a low-latency snooze/spin loop using `crossbeam_utils::Backoff` awaits satisfaction up to a configurable deadline (default 5ms), falling back to the writer thread without hanging.

---

## Quickstart

Add `adaptif-gatotkaca-sqlite` to your `Cargo.toml`:

```toml
[dependencies]
adaptif-gatotkaca-sqlite = "0.2"
```

### Basic Usage

```rust
use adaptif_gatotkaca_sqlite::{GkSqliteConfig, GkSqliteDb, GkSqliteErrorEnum, GkSqliteWriteOutcome};

fn main() -> Result<(), GkSqliteErrorEnum> {
    // Open a WAL database with read/write routing
    let db = GkSqliteDb::open("app.db", GkSqliteConfig::default())?;

    // Execute schema changes or write statements
    db.execute(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER);",
        (),
    )?;

    // Insert rows (zero allocation for small strings and parameters)
    let outcome: GkSqliteWriteOutcome = db.execute(
        "INSERT INTO users (name, balance) VALUES (?1, ?2);",
        ("Alice", 1000i64),
    )?;
    println!("Inserted {} row(s) with commit token {}", outcome.rows, outcome.seq);

    // Query a single row (executes on thread-local connection)
    let balance: i64 = db.query_row(
        "SELECT balance FROM users WHERE name = ?1;",
        ("Alice",),
        |row| row.get(0),
    )?;
    println!("Alice's balance: {}", balance);

    // Query multiple rows
    let users: Vec<(String, i64)> = db.query_map(
        "SELECT name, balance FROM users ORDER BY balance DESC;",
        (),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    println!("Users: {:?}", users);

    Ok(())
}
```

### Multi-Statement Transactions

To execute atomic transactions that view their own intermediate uncommitted modifications:

```rust
db.transaction(|conn| {
    conn.execute("UPDATE users SET balance = balance - 100 WHERE name = 'Alice';", [])?;
    conn.execute("UPDATE users SET balance = balance + 100 WHERE name = 'Bob';", [])?;
    Ok(())
})?;
```

> [!IMPORTANT]
> **Strict Transaction Requirement**: Closures passed to `db.transaction()` execute directly on the dedicated SQLite writer thread. Closures **must not** perform external network/disk I/O or acquire synchronization locks, preventing worker thread stalls. They must also **not call back into the same `GkSqliteDb`** (`execute`, `query_*`, `transaction`, ...): the nested call waits for the writer thread, which is the thread running the closure, so it deadlocks. Use the `conn` the closure receives.

---

## Configuration

`GkSqliteConfig::default()` reproduces the original behaviour. Use the builder to change it; `build()` validates:

```rust
use adaptif_gatotkaca_sqlite::{
    GkChannelKind, GkReadMode, GkReaderPoolConfig, GkSqliteConfig, GkSqliteDb, GkSqlitePragmas,
    GkThreadOptions, pragma::GkSynchronous,
};
use std::time::Duration;

let config = GkSqliteConfig::builder()
    .busy_timeout(Duration::from_secs(2))
    .writer_channel(GkChannelKind::Treiber)          // or Crossbeam (feature `crossbeam-channel`)
    .writer_queue_size(1024)
    .writer_pragmas(GkSqlitePragmas::writer_default().with_synchronous(GkSynchronous::Full))
    .reader_pragmas(GkSqlitePragmas::reader_default().with_cache_size_kib(16_384))
    .writer_thread(GkThreadOptions::default().named("db-writer").stack_size(512 * 1024))
    .reader_pool(GkReaderPoolConfig::default().with_threads(8))
    .on_connection_open(|conn, role| { /* register functions, set extra state */ Ok(()) })
    .build()?;
let db = GkSqliteDb::open("app.db", config)?;
```

Presets: `GkSqliteConfig::throughput()`, `::durable()`, `::low_memory()`.

| Option | Default | Notes |
|---|---|---|
| `writer_channel` | `Treiber` | `GkTreiberChannel` from `adaptif-gatotkaca-utils`: one sender per producing thread (created on its first write), FIFO per producer. `Crossbeam` needs the `crossbeam-channel` feature. Throughput of the two is the same within noise (SQLite dominates the writer). |
| `busy_timeout` | 5 s | SQLite busy timeout on every connection; also bounds how long queued sessions wait. |
| `read_timeout` | 5 ms | How long a read spins for its thread's last write token before falling back to the writer. |
| `autocommit_mode` | `true` | Group-commit each drained batch (`false` batches until the queue is empty or `max_batch`). |
| `writer_queue_size` | 1024 | Whole-queue bound for crossbeam; **per producing thread** for Treiber (about 237 KiB of preallocated slab per thread at 1024 messages (`GkSqliteMsg` is 224 bytes); lower it for many producer threads). |
| `writer_pragmas` | WAL, `synchronous=NORMAL` | `GkSqlitePragmas`: typed `journal_mode`, `synchronous`, `cache_size_kib`, `mmap_size`, `temp_store`, `wal_autocheckpoint`, `journal_size_limit`, `foreign_keys`, `locking_mode`, `query_only`, plus validated `extra` name/value pairs. Values are validated because `PRAGMA` text cannot bind parameters. |
| `reader_pragmas` | `query_only`, 256 MB mmap, 64 MB cache | `GkSqlitePragmas::reader_low_memory()` for small footprints. |
| `writer_thread` | `sqlite-writer-0` | `GkThreadOptions`: name prefix, stack size, `on_start`/`on_stop` hooks (pin cores there; the crate has no affinity dependency). |
| `read_mode` | `Inline` | Where `read` / `*_async` run. See below. |
| `on_connection_open` | none | Runs on every new writer and reader connection after its pragmas. |

## Async API and read modes

The synchronous `query_row` / `query_map` / `query_records` always run inline on the calling thread's own read-only connection (fastest: no handoff, closures may borrow). The async methods take `'static + Send` closures and SQL as `impl Into<GkSqlText>`, and return `GkSqliteFuture<T>`, a plain `std::future::Future` driven by `std::task::Waker` (works on tokio, smol or any executor; `.wait()` blocks a thread instead):

```rust
let n: i64 = db.query_row_async("SELECT COUNT(*) FROM users", (), |r| r.get(0)).await?;
db.execute_async("INSERT INTO users (name) VALUES (?1)", ("Bob",)).await?;
let recs = db.query_records_async("INSERT INTO users (name) VALUES ('C') RETURNING id", ()).await?;
let v = db.read(|conn| Ok(conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))?)).await?;
```

The work starts when the method is called; dropping the future does not cancel it. An acknowledged write is already committed, so `await` a write before the read that must see it (async reads do not wait on the thread-local token).

`GkReadMode` chooses where async reads run:

| Mode | Use |
|---|---|
| `Inline` | on the calling thread (no handoff) |
| `Pool(GkReaderPoolConfig)` | built-in lock-free pool on `GkTreiberMpmc` with stuck-worker rescue; `threads`, `GkThreadOptions`, `max_lanes`, `spin_polls`, `rescue_after`, `max_queue` |
| `Custom(Arc<dyn GkReaderExecutor>)` | your own pool: implement `spawn(job) -> Result<(), job>` (return `Err(job)` to have the caller run it) |
| tokio | feature `tokio`: `GkTokioExecutor::new(handle)` uses `spawn_blocking`; bound connections with `max_blocking_threads` |

Reader connections are thread-local and opened lazily, so every executor thread owns one. See Benchmarks below: inline wins for ~1 us point lookups, and a pool pays off for larger reads or when the caller must not block.

## Cargo features

| Feature | Default | Effect |
|---|---|---|
| `treiber-channel` | yes | marker; the Treiber channel is always built |
| `crossbeam-channel` | no | enables `GkChannelKind::Crossbeam` and re-exports `crossbeam_channel` |
| `tokio` | no | enables `GkTokioExecutor` |

---

## Control Commands

`GkSqliteDb` exposes runtime controls:
- `db.set_autocommit_mode(bool)`: Toggle between Autocommit batching and Non-autocommit batching.
- `db.checkpoint()`: Triggers a passive WAL checkpoint (`PRAGMA wal_checkpoint(PASSIVE);`).
- `db.close()`: closes the calling thread's reader connection and, when this is the only remaining handle, flushes queued writes and stops the writer thread. Other threads' readers close when their threads exit.

---

## Benchmarks

Measured with `--release` on one laptop (8 logical CPUs, macOS, aarch64), no other load, median of 3 runs, WAL and `synchronous=NORMAL` on both sides, same 64 MB cache and 256 MB mmap. They show the shape, not guarantees: run them on your own hardware.

### This crate vs plain `rusqlite`

`cargo test --release --test sqlite_bench_vs_rusqlite -- --ignored --nocapture`. The rusqlite baselines are the usual patterns: one connection per thread, or one `Mutex<Connection>` shared by all threads. Ratio is this crate divided by rusqlite (above 1.0 means this crate is faster).

| Scenario | This crate | rusqlite | Ratio |
|---|---:|---:|---:|
| Point reads, 1 thread (vs one connection per thread) | 692k reads/s | 714k | 0.97x |
| Point reads, 4 threads (vs one connection per thread) | 2.98M | 3.07M | 0.97x |
| Point reads, 8 threads (vs one connection per thread) | 1.41M | 1.44M | 0.98x |
| Point reads, 4 threads (vs shared `Mutex<Connection>`) | 2.96M | 405k | **7.3x** |
| Point reads, 8 threads (vs shared `Mutex<Connection>`) | 1.40M | 359k | **3.9x** |
| Writes, 1 client, every insert committed and acknowledged | 115k txn/s | 164k | 0.70x |
| Writes, 4 clients, each acknowledged (vs shared `Mutex<Connection>`) | 152k txn/s | 126k | **1.21x** |
| Writes, 4 clients, each acknowledged (vs a connection per thread, busy-waiting) | 152k txn/s | 70k | **2.18x** |
| Mixed, 1 writer + 4 readers: reads | 979k reads/s | 800k | **1.22x** |
| Mixed, 1 writer + 4 readers: writes | 24k writes/s | 45k | 0.52x |
| Mixed, 1 writer + 1 reader: reads / writes | 212k / 89k | 208k / 131k | 1.02x / 0.68x |

What this says:
- **Reads match plain rusqlite with one connection per thread** (within about 3%): routing, classification and the read-your-writes check cost almost nothing. You get that without managing connections, and against the common shared-`Mutex<Connection>` pattern reads are several times faster.
- **Concurrent writers are faster** (1.2x to 2.2x): the writer thread group-commits whatever queued up while it was busy, and callers never fight over the SQLite write lock.
- **A single in-process writer is slower than calling rusqlite directly** (0.70x), and so is the writer in the mixed runs (0.5x to 0.7x): every write is a handoff to the writer thread and back, and with busy reader threads on every core that wakeup waits for a core. If one thread does all your writing and nothing else competes, direct rusqlite autocommit is faster; what you buy here is that many threads can write and read without locks, plus the async API.
- Batching 1000 inserts per explicit transaction in rusqlite reaches about 3.0M inserts/s, but those inserts are not individually acknowledged; it is a ceiling, not a like-for-like comparison. Use `execute_many` or `transaction` for that pattern here.

### Reader pools (async reads)

`cargo test --release --test sqlite_bench_read_path -- --ignored --nocapture` compares `GkReadMode::Inline` (the synchronous API) with several pools running the same reads, 8 workers, 32 reads in flight per caller. The built-in pool is `builtin-pool`.

| Reads/s | Callers | inline | builtin-pool | std threads + crossbeam | `threadpool` | `affinitypool` | tokio `spawn_blocking` |
|---|---:|---:|---:|---:|---:|---:|---:|
| Point lookup (~1 us) | 1 | **820k** | 542k | 517k | 578k | 403k | 476k |
| Point lookup | 4 | **1.13M** | 886k | 811k | 701k | 571k | 727k |
| Point lookup | 16 | **1.06M** | 963k | 808k | 741k | 529k | 729k |
| 200-row range read (~20 us) | 1 | 48k | 145k | **164k** | 160k | 155k | 161k |
| 200-row range read | 4 | 170k | **223k** | 201k | 184k | 196k | 210k |
| 200-row range read | 16 | **209k** | 163k | 195k | 206k | 144k | 199k |

Inline always wins for tiny reads (a pool adds a queue hop and a wakeup to a one-microsecond job). Use a pool for the async API, for larger reads where one caller should use several cores, or when the calling thread must not block. The built-in pool is the fastest of the pools on point lookups at 4 and 16 callers and is within noise of the others on range reads, with no `Mutex` or `Condvar` anywhere in it.

### Write path and writer channel

`cargo test --release --features crossbeam-channel --test sqlite_bench_write_path -- --ignored --nocapture` (1 client, then 4 clients): about 110k and 157k txn/s with the Treiber writer channel, and the same within noise with crossbeam: SQLite itself dominates the writer, so the queue backend is a choice of memory and ordering behaviour, not of speed. For reference, the engine measured about 91k and 140k txn/s before the 0.2.0 work (single statements no longer run inside a savepoint; replies no longer allocate a channel).

## Tests and benchmarks

All tests live in `tests/`; every Rust file is an explicit `[[test]]` target (`autotests = false`).

| Target | File | What |
|---|---|---|
| `sqlite_routing` | `tests/test_sqlite_routing.rs` | the routing/consistency invariants (13 tests) |
| `sqlite_bench_write_path` | `tests/bench_write_path.rs` | write-path measurement; an in-tests bench, `#[ignore]`d: `cargo test --release --test sqlite_bench_write_path -- --ignored --nocapture` |
| `sqlite_api_surface` | `tests/api_surface.rs` | compile test: every public path of this crate (including `FastU64Map` from `adaptif-gatotkaca-utils`) still resolves |
| `sqlite_features` | `tests/test_sqlite_features.rs` | writer channel kinds, pragmas, connection hook, read modes, async API, executors, tokio |
| `sqlite_pragma` / `sqlite_reply` / `sqlite_queue` / `sqlite_pool` | `tests/test_{pragma,reply,queue,pool}.rs` | the pragma, reply-oneshot, writer-queue and reader-pool modules |
| `sqlite_bench_read_path` | `tests/bench_read_path.rs` | inline vs built-in pool vs std/threadpool/affinitypool/tokio (`--ignored --nocapture`) |
| `sqlite_bench_vs_rusqlite` | `tests/bench_vs_rusqlite.rs` | this crate vs plain rusqlite: reads, single and multi-client writes, mixed load (`--ignored --nocapture`) |

Run them with `cargo test` from this directory. `scripts/standalone-check.sh` copies this repo alone to a scratch directory (no sibling, no
`../adaptif-gatotkaca`) and runs fmt, `cargo build --all-targets`, clippy (`-D warnings`), `cargo test` and the Python runner there (`STANDALONE_CHECK_ONLY=1` for `cargo check`); the runner exits 0 (some passed), 77 (all skipped, reported as such) or fails the check. The scratch copy is made under `STANDALONE_TMP` (default `$HOME/standalone-check-scratch`, outside the `~/rust-dev` tree; its path is printed, it is removed on success and kept on failure or with `--keep`). The script also scans the copy for stray references to the main repo (explicit allow-list inside the script), asserts through `cargo metadata` that no `adaptif-gatotkaca-*` package comes from outside the closure, and runs `cargo clippy --all-targets -- -D warnings`. `Cargo.lock` is git-ignored in this repo, so a standalone build resolves fresh dependency versions from crates.io (main's lock file does not apply).

Python tests (`tests/python/test_sqlite_api.py`, `test_sqlite_concurrency.py`, `test_aio_sqlite.py`, `test_tortoise.py`) run against this crate only:

- `tests/pyshim/` is a dev-only pyo3 crate (its own manifest and `[workspace]`, depends on this crate by path, pyo3 0.30) that builds the extension `_sqlite_engine` exposing `GkSqliteDb` and its transaction sessions to Python (synchronous calls, GIL released while they wait). `scripts/build-pyshim.sh [--release]` builds it and installs it as `tests/python/_sqlite_engine.so`.
- `tests/python/sqlite/` is the Python layer over it: `sqlite` (sqlite3-style), `aio_sqlite` (aiosqlite-style, each call runs the sync call on the event loop's default executor, no async runtime) and `tortoise_sqlite` (Tortoise ORM). Errors are the stdlib `sqlite3` classes.
- `test_sqlite_concurrency.py` hammers `tests/python/http_server.py`, a plain-HTTP stdlib server (`/`, `/health`, `POST /sqlite/write`, `GET /sqlite/read`) with `wrk`. No TLS, no metrics endpoint, no core pinning anywhere.
- `tests/python/_main.py` loads the extension; when it is not built, `tortoise` or `wrk` is missing, the test prints `SKIP: <reason>` and exits 77. Any other failure propagates.

Run them with `scripts/build-pyshim.sh && python3 tests/python/run_all.py [name ...]` (one process each). Install the optional Python dependencies with `pip install -r tests/python/requirements.txt` into a virtualenv under `$HOME`: `tortoise-orm` for `test_tortoise.py`, `aiosqlite` for the benchmark `bench_vs_aiosqlite.py`; `wrk` for the concurrency test is a system binary. A test whose dependency is missing skips with exit 77.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Copyright (c) 2026 [PT Adaptif Sinergi Dinamis](https://adaptif.co.id/).
