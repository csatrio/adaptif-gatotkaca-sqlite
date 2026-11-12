//! Read-path comparison benchmark: how should a read be dispatched?
//!
//! Run with:
//! `cargo test --release --test sqlite_bench_read_path -- --ignored --nocapture`
//!
//! Every strategy runs the same two workloads against the same database; only
//! the way the read call reaches a thread differs. Readers use thread-local
//! connections, so a pool worker opens its own connection lazily (covered by
//! the warm-up runs).
//!
//! Completion mechanism (identical for every strategy): each caller thread owns
//! one `std::sync::mpsc::sync_channel(WINDOW)`; every job carries a clone of
//! its `SyncSender` and sends its own submit-to-completion latency (ns) when it
//! finishes. The caller keeps up to `WINDOW` jobs in flight, then drains the
//! channel. Overhead per job: one `Instant::now` pair, one `Sender` clone, one
//! `GkSqliteDb` (Arc) clone, one `Box<dyn FnOnce>` allocation and one channel
//! send. The overhead is the same for all strategies, so it does not change the
//! ranking, but it caps the absolute numbers. The latency is stamped on the
//! executing thread at completion, so it excludes the final wake-up of the
//! caller.
//!
//! Numbers are machine-specific.

// ==== Import Definition ====

use adaptif_gatotkaca_sqlite::{GkReaderExecutor, GkSqliteConfig, GkSqliteDb, GkSqliteValue};
use crossbeam_channel as cb;
use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

// ==== Constants ====

const ROWS: i64 = 100_000;
const RANGE_LEN: i64 = 200;
/// Jobs each caller keeps in flight.
const WINDOW: usize = 32;
const TIMED_RUNS: usize = 3;
const CALLER_COUNTS: [usize; 3] = [1, 4, 16];
const POINT_JOBS: usize = 160_000;
const RANGE_JOBS: usize = 16_000;
/// A single run (all callers) must finish within this, else the test fails.
const RUN_DEADLINE: Duration = Duration::from_secs(20);
/// Whole-test hard deadline; the watchdog kills the process past it.
const TOTAL_DEADLINE: Duration = Duration::from_secs(240);

// ==== Types ====

/// One unit of work handed to a dispatcher.
type GkJob = Box<dyn FnOnce() + Send + 'static>;

/// A read workload: runs one read on `db` for the given index, returns `true` if it behaved.
type GkWorkloadFn = fn(&GkSqliteDb, u64) -> bool;

/// A way to get a job executed on some thread.
trait GkDispatch: Send + Sync {
    fn submit(&self, job: GkJob);
}

struct GkWorkload {
    name: &'static str,
    jobs: usize,
    run: GkWorkloadFn,
}

struct GkRunStats {
    reads_per_sec: f64,
    lat_ns: Vec<u64>,
}

struct GkCell {
    callers: usize,
    strategy: &'static str,
    reads_per_sec: f64,
    p50_us: f64,
    p99_us: f64,
}

// ==== Strategy: inline ====

struct GkInline;

impl GkDispatch for GkInline {
    fn submit(&self, job: GkJob) {
        job();
    }
}

// ==== Strategy: std threads fed by a crossbeam channel ====

struct GkStdThreads {
    tx: Option<cb::Sender<GkJob>>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl GkStdThreads {
    fn new(workers: usize) -> Self {
        let (tx, rx) = cb::unbounded::<GkJob>();
        let handles = (0..workers)
            .map(|_| {
                let rx = rx.clone();
                thread::spawn(move || {
                    // BRANCH: worker loop ends when every sender is dropped
                    while let Ok(job) = rx.recv() {
                        job();
                    }
                })
            })
            .collect();
        Self {
            tx: Some(tx),
            handles,
        }
    }
}

impl GkDispatch for GkStdThreads {
    fn submit(&self, job: GkJob) {
        // BRANCH: sender is present until drop
        if let Some(tx) = &self.tx {
            let _ = tx.send(job);
        }
    }
}

impl Drop for GkStdThreads {
    fn drop(&mut self) {
        self.tx = None;
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

// ==== Strategy: threadpool crate ====

struct GkThreadPoolCrate {
    pool: threadpool::ThreadPool,
}

impl GkDispatch for GkThreadPoolCrate {
    fn submit(&self, job: GkJob) {
        self.pool.execute(job);
    }
}

impl Drop for GkThreadPoolCrate {
    fn drop(&mut self) {
        self.pool.join();
    }
}

// ==== Strategy: affinitypool ====

/// `affinitypool::Threadpool::spawn` returns a future that must stay alive
/// (dropping it cancels a queued task). The caller thread invokes `spawn`
/// (which enqueues immediately) and hands the future to a tokio multi-thread
/// runtime (2 driver threads) that polls it to completion. The jobs themselves
/// run on the affinitypool workers.
struct GkAffinityPool {
    // Field order matters: the runtime is dropped before the pool.
    rt: Option<tokio::runtime::Runtime>,
    pool: affinitypool::Threadpool,
}

impl GkAffinityPool {
    fn new(workers: usize) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .expect("tokio runtime");
        Self {
            rt: Some(rt),
            pool: affinitypool::Threadpool::new(workers),
        }
    }
}

impl GkDispatch for GkAffinityPool {
    fn submit(&self, job: GkJob) {
        let fut = self.pool.spawn(job);
        // BRANCH: runtime is present until drop
        if let Some(rt) = &self.rt {
            rt.spawn(fut);
        }
    }
}

impl Drop for GkAffinityPool {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

// ==== Strategy: tokio spawn_blocking ====

struct GkTokioBlocking {
    rt: Option<tokio::runtime::Runtime>,
}

impl GkTokioBlocking {
    fn new(workers: usize) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(workers)
            .thread_keep_alive(Duration::from_secs(60))
            .build()
            .expect("tokio runtime");
        Self { rt: Some(rt) }
    }
}

impl GkDispatch for GkTokioBlocking {
    fn submit(&self, job: GkJob) {
        // BRANCH: runtime is present until drop
        if let Some(rt) = &self.rt {
            drop(rt.spawn_blocking(job));
        }
    }
}

impl Drop for GkTokioBlocking {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

// ==== Strategy: built-in GkReaderPool ====

/// Builds the dispatcher for the crate's built-in reader pool (`GkReaderPool`). `spawn` returns `Err(job)` when it
/// cannot accept the job; the dispatcher then runs it on the submitting thread. The pool joins its threads on drop.
fn reader_pool_strategy(workers: usize) -> Option<Box<dyn GkDispatch>> {
    let pool = adaptif_gatotkaca_sqlite::GkReaderPool::new(
        adaptif_gatotkaca_sqlite::GkReaderPoolConfig::default().with_threads(workers),
    )
    .ok()?;
    Some(Box::new(GkBuiltinPool(pool)))
}

/// The built-in lock-free pool; a rejected job runs on the submitting thread.
struct GkBuiltinPool(adaptif_gatotkaca_sqlite::GkReaderPool);

impl GkDispatch for GkBuiltinPool {
    fn submit(&self, job: GkJob) {
        // BRANCH: queue full / pool closed / lanes exhausted => run the job on the caller thread
        if let Err(job) = self.0.spawn(job) {
            job();
        }
    }
}

// ==== Workloads ====

fn point_lookup(db: &GkSqliteDb, idx: u64) -> bool {
    let id = 1 + (idx % ROWS as u64) as i64;
    // BRANCH: classify the lookup result
    match db.query_row("SELECT v, n FROM t WHERE id = ?1", (id,), |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }) {
        Ok((v, n)) => {
            black_box((&v, n));
            n == id * 3
        }
        Err(_) => false,
    }
}

fn range_read(db: &GkSqliteDb, idx: u64) -> bool {
    let id = 1 + (idx % (ROWS - RANGE_LEN + 1) as u64) as i64;
    // BRANCH: classify the range result
    match db.query_map(
        "SELECT id, v FROM t WHERE id BETWEEN ?1 AND ?1+199",
        (id,),
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
    ) {
        Ok(rows) => {
            let ok = rows.len() == RANGE_LEN as usize;
            black_box(&rows);
            ok
        }
        Err(_) => false,
    }
}

// ==== Harness ====

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn make_job(
    db: &GkSqliteDb,
    run: GkWorkloadFn,
    idx: u64,
    tx: &mpsc::SyncSender<u64>,
    errors: &Arc<AtomicU64>,
) -> GkJob {
    let db = db.clone();
    let tx = tx.clone();
    let errors = Arc::clone(errors);
    let submitted = Instant::now();
    Box::new(move || {
        // BRANCH: a failed read is counted, never panics on a pool thread
        if !run(&db, idx) {
            errors.fetch_add(1, Ordering::Relaxed);
        }
        let _ = tx.send(submitted.elapsed().as_nanos() as u64);
    })
}

/// Submits `count` jobs in windows of `WINDOW` and drains their completions.
/// Returns `false` if the deadline passed.
#[allow(clippy::too_many_arguments)]
fn drive_jobs(
    dispatch: &dyn GkDispatch,
    db: &GkSqliteDb,
    run: GkWorkloadFn,
    count: usize,
    seed: u64,
    errors: &Arc<AtomicU64>,
    deadline: Instant,
    lat_out: &mut Vec<u64>,
) -> bool {
    let (tx, rx) = mpsc::sync_channel::<u64>(WINDOW);
    let mut rng = seed | 1;
    let mut done = 0;
    while done < count {
        let batch = WINDOW.min(count - done);
        for _ in 0..batch {
            let idx = xorshift(&mut rng);
            dispatch.submit(make_job(db, run, idx, &tx, errors));
        }
        for _ in 0..batch {
            let left = deadline.saturating_duration_since(Instant::now());
            // BRANCH: completion arrived, or the run deadline expired
            match rx.recv_timeout(left) {
                Ok(ns) => lat_out.push(ns),
                Err(_) => return false,
            }
        }
        done += batch;
    }
    true
}

/// One run: `callers` threads, `total_jobs` split between them. Each caller
/// first warms up (opens its connection / wakes workers), then all callers
/// start together.
fn one_run(
    dispatch: &dyn GkDispatch,
    db: &GkSqliteDb,
    run: GkWorkloadFn,
    callers: usize,
    total_jobs: usize,
    run_no: u64,
) -> GkRunStats {
    let per_caller = (total_jobs / callers).max(WINDOW);
    let errors = Arc::new(AtomicU64::new(0));
    let ready = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let deadline = Instant::now() + RUN_DEADLINE;

    let results: Vec<(Instant, Instant, Vec<u64>)> = thread::scope(|s| {
        let handles: Vec<_> = (0..callers)
            .map(|c| {
                let errors = &errors;
                let ready = &ready;
                let failed = &failed;
                s.spawn(move || {
                    let seed = 0x9E37_79B9_7F4A_7C15u64
                        .wrapping_mul(run_no + 1)
                        .wrapping_add(c as u64 * 0xA24B_AED4_963E_E407);
                    let mut scratch = Vec::with_capacity(2 * WINDOW);
                    // BRANCH: warm-up failure marks the run failed
                    if !drive_jobs(
                        dispatch,
                        db,
                        run,
                        2 * WINDOW,
                        seed ^ 0x55,
                        errors,
                        deadline,
                        &mut scratch,
                    ) {
                        failed.store(true, Ordering::Relaxed);
                    }
                    ready.fetch_add(1, Ordering::AcqRel);
                    // BRANCH: spin-wait until every caller is warm (bounded by the deadline)
                    while ready.load(Ordering::Acquire) < callers && Instant::now() < deadline {
                        thread::yield_now();
                    }
                    let mut lats = Vec::with_capacity(per_caller);
                    let t0 = Instant::now();
                    // BRANCH: timed phase failure marks the whole run failed
                    if !drive_jobs(
                        dispatch, db, run, per_caller, seed, errors, deadline, &mut lats,
                    ) {
                        failed.store(true, Ordering::Relaxed);
                    }
                    (t0, Instant::now(), lats)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("caller thread panicked"))
            .collect()
    });

    assert!(
        !failed.load(Ordering::Relaxed),
        "run exceeded its {RUN_DEADLINE:?} deadline"
    );
    assert_eq!(errors.load(Ordering::Relaxed), 0, "reads returned errors");

    // The wall clock spans the earliest start to the latest finish.
    let start = results.iter().map(|r| r.0).min().expect("callers");
    let end = results.iter().map(|r| r.1).max().expect("callers");
    let jobs: usize = results.iter().map(|r| r.2.len()).sum();
    let wall = end.duration_since(start).as_secs_f64().max(1e-9);
    let mut lat_ns: Vec<u64> = results.into_iter().flat_map(|r| r.2).collect();
    lat_ns.sort_unstable();
    GkRunStats {
        reads_per_sec: jobs as f64 / wall,
        lat_ns,
    }
}

fn percentile_us(sorted_ns: &[u64], p: f64) -> f64 {
    // BRANCH: empty latency set
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ns.len() - 1) as f64 * p).round() as usize;
    sorted_ns[idx] as f64 / 1000.0
}

/// Warm-up run, then `TIMED_RUNS` timed runs; reports the median run by throughput.
fn measure_cell(
    name: &'static str,
    dispatch: &dyn GkDispatch,
    db: &GkSqliteDb,
    wl: &GkWorkload,
    callers: usize,
) -> GkCell {
    let _ = one_run(dispatch, db, wl.run, callers, wl.jobs / 8, 0);
    let mut runs: Vec<GkRunStats> = (1..=TIMED_RUNS as u64)
        .map(|n| one_run(dispatch, db, wl.run, callers, wl.jobs, n))
        .collect();
    runs.sort_by(|a, b| a.reads_per_sec.total_cmp(&b.reads_per_sec));
    let median = &runs[runs.len() / 2];
    GkCell {
        callers,
        strategy: name,
        reads_per_sec: median.reads_per_sec,
        p50_us: percentile_us(&median.lat_ns, 0.50),
        p99_us: percentile_us(&median.lat_ns, 0.99),
    }
}

fn build_strategies(workers: usize) -> Vec<(&'static str, Box<dyn GkDispatch>)> {
    let mut out: Vec<(&'static str, Box<dyn GkDispatch>)> = vec![
        ("inline", Box::new(GkInline)),
        ("std-threads-pool", Box::new(GkStdThreads::new(workers))),
        (
            "threadpool",
            Box::new(GkThreadPoolCrate {
                pool: threadpool::ThreadPool::new(workers),
            }),
        ),
        ("affinitypool", Box::new(GkAffinityPool::new(workers))),
        (
            "tokio-spawn_blocking",
            Box::new(GkTokioBlocking::new(workers)),
        ),
    ];
    // BRANCH: the built-in pool could not start (skip its row)
    if let Some(d) = reader_pool_strategy(workers) {
        out.push(("builtin-pool", d));
    }
    out
}

fn print_table(wl: &GkWorkload, cells: &[GkCell]) {
    println!();
    println!("== {} ({} reads per run) ==", wl.name, wl.jobs);
    println!(
        "{:>7}  {:<22} {:>14} {:>10} {:>10}",
        "callers", "strategy", "reads/s", "p50 us", "p99 us"
    );
    for cell in cells {
        println!(
            "{:>7}  {:<22} {:>14.0} {:>10.1} {:>10.1}",
            cell.callers, cell.strategy, cell.reads_per_sec, cell.p50_us, cell.p99_us
        );
    }
}

fn setup_db(path: &str) -> GkSqliteDb {
    let db = GkSqliteDb::open(path, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INTEGER)",
        (),
    )
    .expect("create table");
    const BATCH: i64 = 10_000;
    let mut start = 1;
    while start <= ROWS {
        let end = (start + BATCH - 1).min(ROWS);
        let params = (start..=end)
            .map(|id| {
                vec![
                    GkSqliteValue::Integer(id),
                    GkSqliteValue::from(format!("value-{id:08}-payload-payload")),
                    GkSqliteValue::Integer(id * 3),
                ]
                .into()
            })
            .collect();
        db.execute_many("INSERT INTO t (id, v, n) VALUES (?1, ?2, ?3)", params)
            .expect("insert batch");
        start = end + 1;
    }
    db.checkpoint().expect("checkpoint");
    db
}

#[test]
#[ignore = "benchmark: run with --release -- --ignored --nocapture"]
fn read_path_comparison() {
    let started = Instant::now();
    // Hard whole-test deadline: a hung pool must not hang the test run.
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let watchdog = thread::spawn(move || {
        // BRANCH: only a timeout (not a normal finish) kills the process
        if done_rx.recv_timeout(TOTAL_DEADLINE) == Err(mpsc::RecvTimeoutError::Timeout) {
            eprintln!("bench_read_path: total deadline {TOTAL_DEADLINE:?} exceeded, aborting");
            std::process::exit(1);
        }
    });

    let cpus = thread::available_parallelism().map_or(1, |n| n.get());
    let workers = cpus.min(8);
    println!();
    println!(
        "read-path comparison: cpus={cpus} workers={workers} os={} arch={}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!("in-flight window per caller={WINDOW}, timed runs={TIMED_RUNS} (median reported)");
    println!("Numbers are machine-specific; compare strategies within one run only.");
    println!("inline = each caller thread reads itself (C callers => C reader threads).");
    println!("affinitypool is driven by a 2-thread tokio multi-thread runtime.");

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bench.db");
    let db = setup_db(path.to_str().expect("utf8 path"));

    let workloads = [
        GkWorkload {
            name: "point lookup: SELECT v, n FROM t WHERE id = ?1",
            jobs: POINT_JOBS,
            run: point_lookup,
        },
        GkWorkload {
            name: "range read: SELECT id, v ... BETWEEN ?1 AND ?1+199 (200 rows)",
            jobs: RANGE_JOBS,
            run: range_read,
        },
    ];

    for wl in &workloads {
        let strategies = build_strategies(workers);
        let mut cells = Vec::new();
        for &callers in &CALLER_COUNTS {
            for (name, dispatch) in &strategies {
                cells.push(measure_cell(name, dispatch.as_ref(), &db, wl, callers));
            }
        }
        // Join / shut down every pool before the next workload.
        drop(strategies);
        print_table(wl, &cells);
    }

    println!();
    println!("total bench time: {:.1}s", started.elapsed().as_secs_f64());
    drop(db);
    drop(done_tx);
    let _ = watchdog.join();
}
