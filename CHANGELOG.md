# Changelog

## 0.2.0 - pluggable writer channel, reader pool, pragmas, async API

### Breaking
- The writer queue is the gatotkaca Treiber channel by default. `crossbeam_channel` is no longer a hard dependency or an unconditional re-export: enable the `crossbeam-channel` feature for `GkChannelKind::Crossbeam` and the `crossbeam_channel` re-export.
- `GkSqliteMsg` replies are `GkReplySender<..>` (a crate-owned lock-free oneshot, `reply::reply_channel`) instead of crossbeam senders; `GkSqliteDb::writer_sender()` returns `GkWriterTx<GkSqliteMsg>`; `GkSqliteWriterHandle::new` takes a `GkWriterQueueConfig`, `GkConnectionOptions` and `GkThreadOptions`.
- `GkSqliteWriteCommand` / `GkSqliteReadCommand` carry `GkSqlText` instead of `Arc<str>`.
- `GkSqliteReader::with_connection` takes `&GkConnectionOptions` instead of a path-and-timeout pair.
- `GkSqliteErrorEnum` has a new variant `ReaderUnavailable`.
- `GkSqliteConfig` gained fields (`writer_channel`, `writer_pragmas`, `reader_pragmas`, `on_connection_open`, `read_mode`, `writer_thread`); build it with `GkSqliteConfig::builder()` or `..Default::default()`.

### Added
- `config`: `GkSqliteConfig::builder()` with validation, presets `throughput()` / `durable()` / `low_memory()`, `GkConnectionHook` (`on_connection_open`).
- `pragma`: `GkSqlitePragmas` with typed, validated writer and reader pragmas and a validated `extra` escape hatch. Defaults equal the previous hard-coded values.
- `queue`: `GkWriterQueueConfig`, `GkChannelKind`, `GkWriterTx` / `GkWriterRx` over the Treiber channel (thread-local senders, FIFO per producer) or crossbeam.
- `reply`: lock-free single-use reply channel (one allocation; sender drop is reported as `WriterDead`).
- `executor` / `pool`: `GkReaderExecutor`, `GkInlineExecutor`, `GkThreadOptions`, and the lock-free `GkReaderPool` on `GkTreiberMpmc` (stuck-worker rescue, lane fallback).
- `async_api`: `GkSqliteFuture`, `read`, `execute_async`, `query_row_async`, `query_map_async`, `query_records_async`; `GkSqlText` for allocation-free constant SQL.
- Feature `tokio`: `GkTokioExecutor` (`spawn_blocking`).
- Tests: `sqlite_features`, `sqlite_pragma`, `sqlite_reply`, `sqlite_queue`, `sqlite_pool`; benchmarks `sqlite_bench_read_path` and `sqlite_bench_vs_rusqlite` (results in the README).

### Fixed
- Non-autocommit mode: the 5 ms `max_age` flush was skipped while the queue stayed non-empty (a peeked message bypassed the age check); acknowledgements now arrive within about `max_age` again.
- Dropping or closing a database with an open session no longer hangs: `CTL_SHUTDOWN` rolls the session back and stops the writer.
- Writes queued behind a session are answered when the watchdog rolls it back, and when `BEGIN IMMEDIATE` fails while draining them; before, their callers blocked.
- `GkSqliteWriterHandle::shutdown` no longer joins its own thread when the last handle is dropped on the writer thread.
- `reply::GkReplyReceiver::wait_timeout` delivers a value that arrives as the deadline passes instead of reporting a timeout.
- Documented: `transaction` closures must not call back into the same database (the nested call waits on the writer thread and deadlocks).

### Changed (performance)
- A single `Execute` no longer runs `SAVEPOINT`/`RELEASE` around itself (multi-statement commands keep them); `RETURNING` is detected from the statement's column count instead of scanning the SQL, and its row count is the number of returned rows. Edge case: a single statement using `OR FAIL` (or a trigger `RAISE(FAIL)`) that fails part-way now keeps its partial changes inside the open transaction.
- Replies no longer allocate a crossbeam channel per call.
- The built-in reader pool takes jobs from a shared chain as soon as a worker is idle (`GkReaderPoolConfig::rescue_after` defaults to zero); medium-sized reads from one caller now spread over the workers (200-row range reads, 1 caller: 50k -> about 145k reads/s). A non-zero `rescue_after` keeps the pure blocked-worker rescue.
- Write path on the development machine (`sqlite_bench_write_path`): 90.7k -> about 110k txn/s with one client, 140k -> about 157k with four.

## 0.1.0 - extracted from adaptif-gatotkaca @ 4f1fa1762dc54a87132424e0c6072ff8f1c7512b

Moved out of `crates/gatotkaca-sqlite` of the `adaptif-gatotkaca` repository together with its tests, docs and tooling (history of the moved paths kept).

### Breaking (renames)
- Package `gatotkaca-sqlite` -> `adaptif-gatotkaca-sqlite`; library `gatotkaca_sqlite` -> `adaptif_gatotkaca_sqlite`.

### Breaking (type renames)
- Every type defined in this crate carries the `Gk` prefix: `SqliteDb` -> `GkSqliteDb`, `SqliteConfig` -> `GkSqliteConfig`, `SqliteReader` -> `GkSqliteReader`, `SqliteErrorEnum` -> `GkSqliteErrorEnum` (variant names unchanged), `SqliteError` -> `GkSqliteError`, `SqliteValue`, `SqliteParams`, `SqliteWriteOutcome`, `SqliteCommitCounter`, `SqliteRowRecord`, `SqliteWriteCommand`, `SqliteReadCommand`, `SqliteMsg`, `SqliteWriterHandle`, `SavepointAction` and `CompactStr` likewise. `FastU64Map` / `SqliteFastU64Map` come from `adaptif-gatotkaca-utils` and keep their names.

### Changed
- `adaptif-gatotkaca-support` is no longer a dependency: `FastU64Map` / `SqliteFastU64Map` come from `adaptif-gatotkaca-utils` (the local `fast_hashmap` module and its test were removed); `adaptif-gatotkaca-eventfd` was added as a dependency. Public re-exports are unchanged.
- Python tests no longer need the main repo: a dev-only pyo3 shim (`tests/pyshim`, its own manifest with `pyo3`; not a dependency of the root crate, built by `scripts/build-pyshim.sh`) exposes the engine, and the `sqlite` / `aio_sqlite` / `tortoise_sqlite` Python layer is ported to `tests/python/sqlite`. The concurrency test uses a plain-HTTP stdlib server (`tests/python/http_server.py`); all TLS, Tokio-pool and core-pinning logic was removed from the tests.
