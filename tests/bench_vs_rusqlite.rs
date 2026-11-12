//! Measurement (`--ignored`): this crate against plain `rusqlite` on the same file, same pragmas (WAL,
//! `synchronous=NORMAL`, 64 MB cache, 256 MB mmap). The rusqlite baselines are the usual patterns:
//! one connection per thread, and one `Mutex<Connection>` shared by all threads. Numbers are machine-specific.
//!
//! `cargo test --release --test sqlite_bench_vs_rusqlite -- --ignored --nocapture`

// ==== Import Definition ====

use adaptif_gatotkaca_sqlite::rusqlite::{Connection, OpenFlags, params};
use adaptif_gatotkaca_sqlite::{GkSqliteConfig, GkSqliteDb};
use std::{
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const ROWS: i64 = 100_000;
const POINT_SQL: &str = "SELECT v, n FROM t WHERE id = ?1";
const INSERT_SQL: &str = "INSERT INTO t (id, v, n) VALUES (?1, ?2, ?3)";

// ==== Helpers ====

static NEXT_BASE: AtomicI64 = AtomicI64::new(1_000_000_000);

/// A fresh id range per measured run, so repeated runs on one database never collide.
fn next_base() -> i64 {
    NEXT_BASE.fetch_add(100_000_000, Ordering::Relaxed)
}

fn open_rusqlite(path: &str, read_only: bool) -> Connection {
    let flags = if read_only {
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
    };
    let conn = Connection::open_with_flags(path, flags).unwrap();
    // BRANCH: only a writer sets the journal mode
    if !read_only {
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    }
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    conn.pragma_update(None, "mmap_size", 268_435_456i64)
        .unwrap();
    conn.pragma_update(None, "cache_size", -64_000i64).unwrap();
    conn.set_prepared_statement_cache_capacity(256);
    conn
}

/// A fresh database file with `ROWS` rows; returns (dir, path).
fn seeded() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("b.db").to_str().unwrap().to_owned();
    let mut conn = open_rusqlite(&path, false);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INTEGER)")
        .unwrap();
    let tx = conn.transaction().unwrap();
    for i in 1..=ROWS {
        tx.execute(INSERT_SQL, params![i, "payload payload payload", i])
            .unwrap();
    }
    tx.commit().unwrap();
    drop(conn);
    (dir, path)
}

/// Runs `body(thread_index)` on `threads` threads released together; returns wall time of the slowest.
fn timed<F>(threads: usize, body: F) -> Duration
where
    F: Fn(usize) + Send + Sync + 'static,
{
    let body = Arc::new(body);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let handles: Vec<_> = (0..threads)
        .map(|i| {
            let (body, barrier) = (body.clone(), barrier.clone());
            thread::spawn(move || {
                barrier.wait();
                body(i);
            })
        })
        .collect();
    barrier.wait();
    let start = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    start.elapsed()
}

fn median(mut f: impl FnMut() -> f64) -> f64 {
    let mut v: Vec<f64> = (0..3).map(|_| f()).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[1]
}

fn row(label: &str, ours: f64, std: f64, unit: &str) {
    println!(
        "  {label:<44} {ours:>12.0} {std:>12.0}   {:>5.2}x  {unit}",
        ours / std
    );
}

// ==== Reads ====

fn reads_per_thread_rusqlite(path: &str, threads: usize, n: usize) -> f64 {
    let path = path.to_owned();
    let conns: Arc<Vec<Mutex<Option<Connection>>>> = Arc::new(
        (0..threads)
            .map(|_| Mutex::new(Some(open_rusqlite(&path, true))))
            .collect(),
    );
    let d = timed(threads, move |i| {
        let conn = conns[i].lock().unwrap().take().unwrap();
        for k in 0..n {
            let id = 1 + ((k * 7919 + i * 131) as i64 % ROWS);
            let mut stmt = conn.prepare_cached(POINT_SQL).unwrap();
            let _: (String, i64) = stmt
                .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap();
        }
    });
    (threads * n) as f64 / d.as_secs_f64()
}

fn reads_shared_mutex_rusqlite(path: &str, threads: usize, n: usize) -> f64 {
    let conn = Arc::new(Mutex::new(open_rusqlite(path, true)));
    let d = timed(threads, move |i| {
        for k in 0..n {
            let id = 1 + ((k * 7919 + i * 131) as i64 % ROWS);
            let guard = conn.lock().unwrap();
            let mut stmt = guard.prepare_cached(POINT_SQL).unwrap();
            let _: (String, i64) = stmt
                .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap();
        }
    });
    (threads * n) as f64 / d.as_secs_f64()
}

fn reads_ours(db: &Arc<GkSqliteDb>, threads: usize, n: usize) -> f64 {
    // Warm every thread's reader connection outside the timed region.
    let _ = timed(threads, {
        let db = db.clone();
        move |_| {
            let _: (String, i64) = db
                .query_row(POINT_SQL, (1i64,), |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap();
        }
    });
    let db = db.clone();
    let d = timed(threads, move |i| {
        for k in 0..n {
            let id = 1 + ((k * 7919 + i * 131) as i64 % ROWS);
            let _: (String, i64) = db
                .query_row(POINT_SQL, (id,), |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap();
        }
    });
    (threads * n) as f64 / d.as_secs_f64()
}

// ==== Writes ====

fn writes_one_client_rusqlite(path: &str, n: usize, batch: usize) -> f64 {
    let mut conn = open_rusqlite(path, false);
    let base = next_base();
    let start = Instant::now();
    // BRANCH: autocommit per statement, or explicit transactions of `batch` statements
    if batch <= 1 {
        for i in 0..n as i64 {
            conn.prepare_cached(INSERT_SQL)
                .unwrap()
                .execute(params![base + i, "payload payload payload", i])
                .unwrap();
        }
    } else {
        let mut i = 0i64;
        while (i as usize) < n {
            let tx = conn.transaction().unwrap();
            for _ in 0..batch {
                tx.prepare_cached(INSERT_SQL)
                    .unwrap()
                    .execute(params![base + i, "payload payload payload", i])
                    .unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        }
    }
    n as f64 / start.elapsed().as_secs_f64()
}

fn writes_one_client_ours(db: &GkSqliteDb, n: usize) -> f64 {
    let base = next_base();
    let start = Instant::now();
    for i in 0..n as i64 {
        db.execute(INSERT_SQL, (base + i, "payload payload payload", i))
            .unwrap();
    }
    n as f64 / start.elapsed().as_secs_f64()
}

fn writes_multi_mutex_rusqlite(path: &str, threads: usize, n: usize) -> f64 {
    let conn = Arc::new(Mutex::new(open_rusqlite(path, false)));
    let base = next_base();
    let d = timed(threads, move |i| {
        for k in 0..n as i64 {
            let id = base + (i as i64) * 1_000_000 + k;
            conn.lock()
                .unwrap()
                .prepare_cached(INSERT_SQL)
                .unwrap()
                .execute(params![id, "x", k])
                .unwrap();
        }
    });
    (threads * n) as f64 / d.as_secs_f64()
}

fn writes_multi_per_thread_rusqlite(path: &str, threads: usize, n: usize) -> f64 {
    let path = path.to_owned();
    let base = next_base();
    let d = timed(threads, move |i| {
        let conn = open_rusqlite(&path, false);
        for k in 0..n as i64 {
            let id = base + (i as i64) * 1_000_000 + k;
            conn.prepare_cached(INSERT_SQL)
                .unwrap()
                .execute(params![id, "x", k])
                .unwrap();
        }
    });
    (threads * n) as f64 / d.as_secs_f64()
}

fn writes_multi_ours(db: &Arc<GkSqliteDb>, threads: usize, n: usize) -> f64 {
    let db = db.clone();
    let base = next_base();
    let d = timed(threads, move |i| {
        for k in 0..n as i64 {
            let id = base + (i as i64) * 1_000_000 + k;
            db.execute(INSERT_SQL, (id, "x", k)).unwrap();
        }
    });
    (threads * n) as f64 / d.as_secs_f64()
}

// ==== Mixed: one writer + readers for a fixed time ====

fn mixed(readers: usize, secs: u64, ours: Option<Arc<GkSqliteDb>>, path: &str) -> (f64, f64) {
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(readers + 2));
    let mut handles = Vec::new();
    for r in 0..readers {
        let (stop, reads, barrier, path) = (
            stop.clone(),
            reads.clone(),
            barrier.clone(),
            path.to_owned(),
        );
        let ours = ours.clone();
        handles.push(thread::spawn(move || {
            let conn = ours.is_none().then(|| open_rusqlite(&path, true));
            barrier.wait();
            let mut k = 0usize;
            let mut local = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let id = 1 + ((k * 7919 + r * 131) as i64 % ROWS);
                // BRANCH: our engine or a per-thread rusqlite connection
                match (&ours, &conn) {
                    (Some(db), _) => {
                        let _: (String, i64) = db
                            .query_row(POINT_SQL, (id,), |x| Ok((x.get(0)?, x.get(1)?)))
                            .unwrap();
                    }
                    (None, Some(conn)) => {
                        let _: (String, i64) = conn
                            .prepare_cached(POINT_SQL)
                            .unwrap()
                            .query_row([id], |x| Ok((x.get(0)?, x.get(1)?)))
                            .unwrap();
                    }
                    (None, None) => unreachable!(),
                }
                k += 1;
                local += 1;
            }
            reads.fetch_add(local, Ordering::Relaxed);
        }));
    }
    {
        let (stop, writes, barrier, path) = (
            stop.clone(),
            writes.clone(),
            barrier.clone(),
            path.to_owned(),
        );
        let ours = ours.clone();
        handles.push(thread::spawn(move || {
            let conn = ours.is_none().then(|| open_rusqlite(&path, false));
            barrier.wait();
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                let id = 40_000_000 + i;
                // BRANCH: our engine or a dedicated rusqlite writer connection
                match (&ours, &conn) {
                    (Some(db), _) => {
                        db.execute(INSERT_SQL, (id, "x", i)).unwrap();
                    }
                    (None, Some(conn)) => {
                        conn.prepare_cached(INSERT_SQL)
                            .unwrap()
                            .execute(params![id, "x", i])
                            .unwrap();
                    }
                    (None, None) => unreachable!(),
                }
                i += 1;
            }
            writes.fetch_add(i as u64, Ordering::Relaxed);
        }));
    }
    barrier.wait();
    let start = Instant::now();
    thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let el = start.elapsed().as_secs_f64();
    (
        reads.load(Ordering::Relaxed) as f64 / el,
        writes.load(Ordering::Relaxed) as f64 / el,
    )
}

// ==== The comparison ====

#[ignore]
#[test]
fn compare_with_rusqlite() {
    // Hard stop for the whole measurement.
    thread::spawn(|| {
        thread::sleep(Duration::from_secs(300));
        eprintln!("bench_vs_rusqlite: watchdog expired");
        std::process::exit(1);
    });
    let cpus = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    println!(
        "\nthis crate vs plain rusqlite | {cpus} CPUs | {} | median of 3 | WAL, synchronous=NORMAL",
        std::env::consts::OS
    );
    println!(
        "  {:<44} {:>12} {:>12}   {:>6}",
        "scenario (higher is better)", "this crate", "rusqlite", "ratio"
    );

    let (_dir, path) = seeded();
    let db = Arc::new(GkSqliteDb::open(&path, GkSqliteConfig::default()).unwrap());

    println!("\nPoint reads, reads/s total (rusqlite: one connection per thread)");
    for threads in [1usize, 4, 8] {
        let n = 50_000;
        let ours = median(|| reads_ours(&db, threads, n));
        let std = median(|| reads_per_thread_rusqlite(&path, threads, n));
        row(&format!("{threads} thread(s)"), ours, std, "reads/s");
    }
    println!("\nPoint reads, reads/s total (rusqlite: one shared Mutex<Connection>)");
    for threads in [1usize, 4, 8] {
        let n = 50_000;
        let ours = median(|| reads_ours(&db, threads, n));
        let std = median(|| reads_shared_mutex_rusqlite(&path, threads, n));
        row(&format!("{threads} thread(s)"), ours, std, "reads/s");
    }

    println!("\nWrites, each insert committed and acknowledged, 1 client (txn/s)");
    {
        let (_d1, p1) = seeded();
        let (_d2, p2) = seeded();
        let (_d3, p3) = seeded();
        let ours_db = GkSqliteDb::open(&p1, GkSqliteConfig::default()).unwrap();
        let ours = median(|| writes_one_client_ours(&ours_db, 30_000));
        let std = median(|| writes_one_client_rusqlite(&p2, 30_000, 1));
        row(
            "autocommit insert vs rusqlite autocommit",
            ours,
            std,
            "txn/s",
        );
        let std_batched = median(|| writes_one_client_rusqlite(&p3, 30_000, 1000));
        row(
            "autocommit insert vs rusqlite 1000/txn",
            ours,
            std_batched,
            "inserts/s (ceiling: not acked per row)",
        );
    }

    println!("\nWrites, 4 clients, each insert acknowledged (txn/s total)");
    {
        let (_d1, p1) = seeded();
        let (_d2, p2) = seeded();
        let (_d3, p3) = seeded();
        let ours_db = Arc::new(GkSqliteDb::open(&p1, GkSqliteConfig::default()).unwrap());
        let ours = median(|| writes_multi_ours(&ours_db, 4, 10_000));
        let std_mutex = median(|| writes_multi_mutex_rusqlite(&p2, 4, 10_000));
        row(
            "vs rusqlite shared Mutex<Connection>",
            ours,
            std_mutex,
            "txn/s",
        );
        let std_threads = median(|| writes_multi_per_thread_rusqlite(&p3, 4, 1_000));
        row(
            "vs rusqlite connection per thread (busy)",
            ours,
            std_threads,
            "txn/s",
        );
    }

    println!("\nMixed: 1 writer + N readers for 3 s (rusqlite: separate connection per thread)");
    for readers in [1usize, 4] {
        let (_d1, p1) = seeded();
        let (_d2, p2) = seeded();
        let ours_db = Arc::new(GkSqliteDb::open(&p1, GkSqliteConfig::default()).unwrap());
        let (r_ours, w_ours) = mixed(readers, 3, Some(ours_db), &p1);
        let (r_std, w_std) = mixed(readers, 3, None, &p2);
        row(
            &format!("{readers} reader(s): reads/s"),
            r_ours,
            r_std,
            "reads/s",
        );
        row(
            &format!("{readers} reader(s): writes/s"),
            w_ours,
            w_std,
            "writes/s",
        );
    }
}
