//! End-to-end tests of the 0.2.0 surface: pluggable writer channel, pragmas, connection hook, read modes,
//! the async API and the executors. Every wait is bounded.

// ==== Import Definition ====

use adaptif_gatotkaca_sqlite::{
    GkChannelKind, GkConnectionRole, GkInlineExecutor, GkReadJob, GkReadMode, GkReaderExecutor,
    GkReaderPoolConfig, GkSqliteConfig, GkSqliteDb, GkSqliteErrorEnum, GkSqliteFuture, GkSqliteMsg,
    GkSqlitePragmas, GkSqliteValue, GkThreadOptions, pragma::GkSynchronous, writer::CTL_SHUTDOWN,
};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

// ==== Helpers ====

const LIMIT: Duration = Duration::from_secs(20);

struct GkThreadWake(thread::Thread);

impl Wake for GkThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Minimal `block_on` with a hard deadline (no runtime dependency).
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(GkThreadWake(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let deadline = std::time::Instant::now() + LIMIT;
    loop {
        // BRANCH: finished, or park until woken (bounded)
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "future did not finish"
        );
        thread::park_timeout(Duration::from_millis(50));
    }
}

/// Kills the whole test binary if it runs longer than a generous bound, so a hang can never stall CI.
fn watchdog() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        thread::spawn(|| {
            thread::sleep(Duration::from_secs(180));
            eprintln!("test_sqlite_features: watchdog expired, aborting");
            std::process::exit(1);
        });
    });
}

fn kinds() -> Vec<GkChannelKind> {
    #[allow(unused_mut)]
    let mut v = vec![GkChannelKind::Treiber];
    #[cfg(feature = "crossbeam-channel")]
    v.push(GkChannelKind::Crossbeam);
    v
}

fn open_with(config: GkSqliteConfig) -> (tempfile::TempDir, GkSqliteDb) {
    watchdog();
    let dir = tempfile::tempdir().unwrap();
    let db = GkSqliteDb::open(dir.path().join("f.db").to_str().unwrap(), config).unwrap();
    (dir, db)
}

/// Runs `f` on a thread and fails the test if it does not return in time.
fn bounded<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(LIMIT).expect("operation hung")
}

// ==== Config ====

#[test]
fn builder_rejects_invalid_config() {
    // BRANCH: zero queue size
    assert!(
        GkSqliteConfig::builder()
            .writer_queue_size(0)
            .build()
            .is_err()
    );
    // BRANCH: bad pragma extra name
    let bad = GkSqlitePragmas::writer_default().with_extra(
        "bad name;",
        adaptif_gatotkaca_sqlite::pragma::GkPragmaValue::Int(1),
    );
    assert!(
        GkSqliteConfig::builder()
            .writer_pragmas(bad)
            .build()
            .is_err()
    );
    // BRANCH: open() validates too
    let cfg = GkSqliteConfig {
        writer_queue_size: 0,
        ..GkSqliteConfig::default()
    };
    let dir = tempfile::tempdir().unwrap();
    assert!(GkSqliteDb::open(dir.path().join("x.db").to_str().unwrap(), cfg).is_err());
    // BRANCH: presets are valid
    for cfg in [
        GkSqliteConfig::throughput(),
        GkSqliteConfig::durable(),
        GkSqliteConfig::low_memory(),
    ] {
        assert!(cfg.validate().is_ok());
    }
}

#[test]
fn pragmas_and_hook_reach_both_connection_kinds() {
    let writers = Arc::new(AtomicUsize::new(0));
    let readers = Arc::new(AtomicUsize::new(0));
    let (w, r) = (writers.clone(), readers.clone());
    let config = GkSqliteConfig::builder()
        .writer_pragmas(GkSqlitePragmas::writer_default().with_synchronous(GkSynchronous::Full))
        .reader_pragmas(GkSqlitePragmas::reader_default().with_cache_size_kib(8192))
        .on_connection_open(move |_, role| {
            // BRANCH: count per role
            match role {
                GkConnectionRole::Writer => w.fetch_add(1, Ordering::SeqCst),
                GkConnectionRole::Reader => r.fetch_add(1, Ordering::SeqCst),
            };
            Ok(())
        })
        .build()
        .unwrap();
    let (_dir, db) = open_with(config);
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
        .unwrap();

    let sync_on_writer: i64 = db
        .transaction(|conn| Ok(conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?))
        .unwrap();
    assert_eq!(sync_on_writer, 2, "FULL");
    let cache: i64 = db
        .read(|conn| Ok(conn.query_row("PRAGMA cache_size", [], |r| r.get(0))?))
        .wait()
        .unwrap();
    assert_eq!(cache, -8192);
    assert_eq!(writers.load(Ordering::SeqCst), 1);
    assert!(readers.load(Ordering::SeqCst) >= 1);
}

// ==== Writer channels ====

#[test]
fn every_channel_kind_orders_writes_per_thread_and_reads_them_back() {
    for kind in kinds() {
        let (_dir, db) = open_with(
            GkSqliteConfig::builder()
                .writer_channel(kind)
                .build()
                .unwrap(),
        );
        db.execute(
            "CREATE TABLE log (id INTEGER PRIMARY KEY, th INTEGER, seq INTEGER)",
            (),
        )
        .unwrap();
        let db = Arc::new(db);
        let handles: Vec<_> = (0..6i64)
            .map(|th| {
                let db = db.clone();
                thread::spawn(move || {
                    for seq in 0..300i64 {
                        db.execute("INSERT INTO log (th, seq) VALUES (?1, ?2)", (th, seq))
                            .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let rows = db
            .query_map("SELECT th, seq FROM log ORDER BY id", (), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })
            .unwrap();
        assert_eq!(rows.len(), 6 * 300, "{kind:?}");
        let mut next = [0i64; 6];
        for (th, seq) in rows {
            assert_eq!(seq, next[th as usize], "{kind:?} per-thread order");
            next[th as usize] += 1;
        }
    }
}

#[test]
fn dead_writer_reports_writer_dead_instead_of_hanging() {
    for kind in kinds() {
        let (_dir, db) = open_with(
            GkSqliteConfig::builder()
                .writer_channel(kind)
                .build()
                .unwrap(),
        );
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ())
            .unwrap();
        let tx = db.writer_sender();
        assert!(
            tx.send(GkSqliteMsg::Control {
                cmd: CTL_SHUTDOWN,
                arg: 0,
                reply: None,
            })
            .is_ok()
        );
        let result = bounded(move || {
            // Several attempts: the first may land before the writer drains the shutdown message.
            let mut last = Ok(());
            for _ in 0..50 {
                last = db.execute("INSERT INTO t DEFAULT VALUES", ()).map(|_| ());
                // BRANCH: stop once the writer is reported dead
                if matches!(last, Err(GkSqliteErrorEnum::WriterDead)) {
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
            last
        });
        assert!(
            matches!(result, Err(GkSqliteErrorEnum::WriterDead)),
            "{kind:?}"
        );
    }
}

// ==== Async API ====

/// Counts the jobs it runs, then runs them inline.
struct GkCounting(Arc<AtomicUsize>);

impl GkReaderExecutor for GkCounting {
    fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob> {
        self.0.fetch_add(1, Ordering::SeqCst);
        job();
        Ok(())
    }
}

/// Refuses every job, so the caller must run it.
struct GkRefusing;

impl GkReaderExecutor for GkRefusing {
    fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob> {
        Err(job)
    }
}

/// Accepts a job and drops it unrun (a pool that died).
struct GkDropping;

impl GkReaderExecutor for GkDropping {
    fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob> {
        drop(job);
        Ok(())
    }
}

fn modes() -> Vec<(&'static str, GkReadMode)> {
    vec![
        ("inline", GkReadMode::Inline),
        (
            "pool",
            GkReadMode::Pool(
                GkReaderPoolConfig::default()
                    .with_threads(3)
                    .with_thread(GkThreadOptions::default().named("sqlite-reader")),
            ),
        ),
        (
            "custom-inline",
            GkReadMode::Custom(Arc::new(GkInlineExecutor)),
        ),
    ]
}

#[test]
fn async_api_round_trip_in_every_read_mode() {
    for (name, mode) in modes() {
        let (_dir, db) = open_with(GkSqliteConfig::builder().read_mode(mode).build().unwrap());
        block_on(db.execute_async(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INTEGER)",
            (),
        ))
        .unwrap();
        for i in 0..50i64 {
            let out = block_on(
                db.execute_async("INSERT INTO t (v, n) VALUES (?1, ?2)", (format!("v{i}"), i)),
            )
            .unwrap();
            assert_eq!(out.rows, 1, "{name}");
        }
        // BRANCH: single row
        let v: String =
            block_on(db.query_row_async("SELECT v FROM t WHERE n = ?1", (7i64,), |r| r.get(0)))
                .unwrap();
        assert_eq!(v, "v7", "{name}");
        // BRANCH: many rows
        let ns =
            block_on(db.query_map_async("SELECT n FROM t ORDER BY n", (), |r| r.get::<_, i64>(0)))
                .unwrap();
        assert_eq!(ns, (0..50).collect::<Vec<i64>>(), "{name}");
        // BRANCH: concrete records
        let recs =
            block_on(db.query_records_async("SELECT v, n FROM t WHERE n < ?1 ORDER BY n", (3i64,)))
                .unwrap();
        assert_eq!(recs.len(), 3, "{name}");
        assert_eq!(recs[2].values[1], GkSqliteValue::Integer(2), "{name}");
        // BRANCH: write with RETURNING goes through the writer
        let ret = block_on(
            db.query_records_async("INSERT INTO t (v, n) VALUES ('r', 99) RETURNING id", ()),
        )
        .unwrap();
        assert_eq!(ret.len(), 1, "{name}");
        // BRANCH: errors
        assert!(matches!(
            block_on(db.execute_async("BEGIN", ())),
            Err(GkSqliteErrorEnum::UseTransactionApi)
        ));
        assert!(matches!(
            block_on(
                db.query_row_async("INSERT INTO t (n) VALUES (1)", (), |r| r.get::<_, i64>(0))
            ),
            Err(GkSqliteErrorEnum::InvalidInput(_))
        ));
        assert!(
            block_on(db.query_row_async("SELECT nope FROM missing", (), |r| r.get::<_, i64>(0)))
                .is_err()
        );
    }
}

#[test]
fn pool_mode_runs_reads_on_pool_threads() {
    let config = GkSqliteConfig::builder()
        .reader_pool(
            GkReaderPoolConfig::default()
                .with_threads(2)
                .with_thread(GkThreadOptions::default().named("test-reader")),
        )
        .build()
        .unwrap();
    let (_dir, db) = open_with(config);
    let name = block_on(db.read(|_| Ok(thread::current().name().map(str::to_owned)))).unwrap();
    assert!(name.unwrap_or_default().starts_with("test-reader"));
}

#[test]
fn custom_executor_sees_the_jobs_and_refusal_falls_back_inline() {
    let seen = Arc::new(AtomicUsize::new(0));
    let (_d1, db) = open_with(
        GkSqliteConfig::builder()
            .read_mode(GkReadMode::Custom(Arc::new(GkCounting(seen.clone()))))
            .build()
            .unwrap(),
    );
    for _ in 0..5 {
        block_on(db.read(|_| Ok(()))).unwrap();
    }
    assert_eq!(seen.load(Ordering::SeqCst), 5);

    // BRANCH: a refusing executor means the calling thread runs the job
    let (_d2, db) = open_with(
        GkSqliteConfig::builder()
            .read_mode(GkReadMode::Custom(Arc::new(GkRefusing)))
            .build()
            .unwrap(),
    );
    let me = thread::current().id();
    let ran_on = block_on(db.read(|_| Ok(thread::current().id()))).unwrap();
    assert_eq!(ran_on, me);

    // BRANCH: a job the executor drops resolves to ReaderUnavailable, not a hang
    let (_d3, db) = open_with(
        GkSqliteConfig::builder()
            .read_mode(GkReadMode::Custom(Arc::new(GkDropping)))
            .build()
            .unwrap(),
    );
    let fut: GkSqliteFuture<()> = db.read(|_| Ok(()));
    assert!(matches!(
        block_on(fut),
        Err(GkSqliteErrorEnum::ReaderUnavailable)
    ));
}

#[test]
fn async_write_then_read_sees_the_write_from_many_threads() {
    let config = GkSqliteConfig::builder()
        .reader_pool(GkReaderPoolConfig::default().with_threads(4))
        .build()
        .unwrap();
    let (_dir, db) = open_with(config);
    block_on(db.execute_async("CREATE TABLE t (id INTEGER PRIMARY KEY, th INTEGER)", ())).unwrap();
    let db = Arc::new(db);
    let handles: Vec<_> = (0..4i64)
        .map(|th| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..200i64 {
                    let id = th * 1000 + i;
                    block_on(db.execute_async("INSERT INTO t (id, th) VALUES (?1, ?2)", (id, th)))
                        .unwrap();
                    // The acknowledged write must already be visible to an async read.
                    let got: i64 = block_on(db.query_row_async(
                        "SELECT th FROM t WHERE id = ?1",
                        (id,),
                        |r| r.get(0),
                    ))
                    .unwrap();
                    assert_eq!(got, th);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn futures_resolve_on_a_tokio_runtime() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let config = GkSqliteConfig::builder()
        .reader_pool(GkReaderPoolConfig::default().with_threads(2))
        .build()
        .unwrap();
    let (_dir, db) = open_with(config);
    let db = Arc::new(db);
    rt.block_on(async {
        tokio::time::timeout(LIMIT, async {
            db.execute_async("CREATE TABLE t (n INTEGER)", ())
                .await
                .unwrap();
            let tasks: Vec<_> = (0..32i64)
                .map(|i| {
                    let db = db.clone();
                    tokio::spawn(async move {
                        db.execute_async("INSERT INTO t (n) VALUES (?1)", (i,))
                            .await
                            .unwrap();
                        db.query_row_async("SELECT COUNT(*) FROM t", (), |r| r.get::<_, i64>(0))
                            .await
                            .unwrap()
                    })
                })
                .collect();
            for t in tasks {
                assert!(t.await.unwrap() >= 1);
            }
        })
        .await
        .expect("tokio test timed out");
    });
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_executor_runs_reads_on_the_blocking_pool() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let executor = adaptif_gatotkaca_sqlite::GkTokioExecutor::new(rt.handle().clone());
    let config = GkSqliteConfig::builder()
        .read_mode(GkReadMode::Custom(Arc::new(executor)))
        .build()
        .unwrap();
    let (_dir, db) = open_with(config);
    rt.block_on(async {
        tokio::time::timeout(LIMIT, async {
            db.execute_async("CREATE TABLE t (n INTEGER)", ())
                .await
                .unwrap();
            db.execute_async("INSERT INTO t VALUES (5)", ())
                .await
                .unwrap();
            let n: i64 = db
                .query_row_async("SELECT n FROM t", (), |r| r.get(0))
                .await
                .unwrap();
            assert_eq!(n, 5);
        })
        .await
        .expect("timed out");
    });
}

// ==== Writer regressions ====

#[test]
fn non_autocommit_flushes_by_age_while_the_queue_stays_busy() {
    let config = GkSqliteConfig::builder()
        .autocommit_mode(false)
        .build()
        .unwrap();
    let (_dir, db) = open_with(config);
    db.execute("CREATE TABLE t (x INTEGER)", ()).unwrap();
    // Pipeline many slow-ish writes so the queue never drains; the first ack must still arrive early.
    const N: usize = 300;
    let futures: Vec<_> = (0..N)
        .map(|_| {
            db.execute_async(
                "INSERT INTO t SELECT x FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 6000) SELECT x FROM c)",
                (),
            )
        })
        .collect();
    let start = std::time::Instant::now();
    let mut first = None;
    for f in futures {
        block_on(f).unwrap();
        // BRANCH: remember when the first acknowledgement arrived
        first.get_or_insert(start.elapsed());
    }
    let total = start.elapsed();
    let first = first.unwrap();
    assert!(
        first.as_secs_f64() < total.as_secs_f64() * 0.15,
        "first ack after {first:?} of {total:?}: commits are not bounded by max_age"
    );
}

#[test]
fn dropping_a_db_with_an_open_session_does_not_hang() {
    for kind in kinds() {
        let (_dir, db) = open_with(
            GkSqliteConfig::builder()
                .writer_channel(kind)
                .build()
                .unwrap(),
        );
        db.execute("CREATE TABLE t (x INTEGER)", ()).unwrap();
        let _session = db.begin_session().unwrap();
        bounded(move || drop(db));
    }
}

#[test]
fn writes_queued_behind_an_open_session_are_answered_when_the_db_closes() {
    let (_dir, db) = open_with(GkSqliteConfig::default());
    db.execute("CREATE TABLE t (x INTEGER)", ()).unwrap();
    let session = db.begin_session().unwrap();
    let queued = db.execute_async("INSERT INTO t VALUES (1)", ());
    let result = bounded(move || {
        drop(db);
        block_on(queued)
    });
    assert!(result.is_err());
    let _ = session;
}
