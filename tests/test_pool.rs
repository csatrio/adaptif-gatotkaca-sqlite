// ==== Import Definition ====

use adaptif_gatotkaca_sqlite::executor::{
    GkInlineExecutor, GkReadJob, GkReaderExecutor, GkThreadOptions,
};
use adaptif_gatotkaca_sqlite::pool::{GkReaderPool, GkReaderPoolConfig, GkReaderPoolError};

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

// ==== Helpers ====

/// Upper bound of every wait in this file.
const LIMIT: Duration = Duration::from_secs(30);

/// Polls `condition` until it holds or `limit` passes. Returns whether it held.
fn wait_for(limit: Duration, condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        // BRANCH: the condition holds
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_micros(100));
    }
    condition()
}

fn pool_with(threads: usize) -> GkReaderPool {
    GkReaderPool::new(GkReaderPoolConfig::default().with_threads(threads)).unwrap()
}

fn available() -> usize {
    thread::available_parallelism().map_or(2, |n| n.get())
}

// ==== Tests ====

#[test]
fn every_job_runs_exactly_once() {
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 5_000;
    let pool = pool_with(4);
    let hits: Arc<Vec<AtomicU8>> = Arc::new(
        (0..PRODUCERS * PER_PRODUCER)
            .map(|_| AtomicU8::new(0))
            .collect(),
    );
    let done = Arc::new(AtomicUsize::new(0));
    let inline = Arc::new(AtomicUsize::new(0));

    thread::scope(|scope| {
        for producer in 0..PRODUCERS {
            let pool = &pool;
            let hits = &hits;
            let done = &done;
            let inline = &inline;
            scope.spawn(move || {
                for n in 0..PER_PRODUCER {
                    let index = producer * PER_PRODUCER + n;
                    let (hits, done) = (hits.clone(), done.clone());
                    let job: GkReadJob = Box::new(move || {
                        hits[index].fetch_add(1, Ordering::Relaxed);
                        done.fetch_add(1, Ordering::Release);
                    });
                    // BRANCH: not accepted: the caller runs it (only with lanes exhausted, which 8 < 64 rules out)
                    if let Err(job) = pool.spawn(job) {
                        inline.fetch_add(1, Ordering::Relaxed);
                        job();
                    }
                }
            });
        }
    });
    assert!(
        wait_for(LIMIT, || done.load(Ordering::Acquire)
            == PRODUCERS * PER_PRODUCER),
        "only {} of {} jobs ran",
        done.load(Ordering::Acquire),
        PRODUCERS * PER_PRODUCER
    );
    assert_eq!(inline.load(Ordering::Relaxed), 0);
    assert!(hits.iter().all(|h| h.load(Ordering::Relaxed) == 1));
    assert!(wait_for(LIMIT, || pool.pending() == 0));
}

/// One producer submits a job that sleeps, then many quick ones: with 2+ workers the quick ones must not wait for it.
fn blocked_worker_round(pool: &GkReaderPool, block: Duration, quick: usize, bound: Duration) {
    let release_at = Instant::now() + block;
    let blocker_done = Arc::new(AtomicBool::new(false));
    let quick_done = Arc::new(AtomicUsize::new(0));
    let flag = blocker_done.clone();
    let start = Instant::now();
    let job: GkReadJob = Box::new(move || {
        thread::sleep(release_at.saturating_duration_since(Instant::now()));
        flag.store(true, Ordering::Release);
    });
    assert!(pool.spawn(job).is_ok());
    for _ in 0..quick {
        let counter = quick_done.clone();
        let job: GkReadJob = Box::new(move || {
            counter.fetch_add(1, Ordering::Release);
        });
        assert!(pool.spawn(job).is_ok());
    }
    assert!(
        wait_for(bound, || quick_done.load(Ordering::Acquire) == quick),
        "quick jobs stuck behind the blocked worker: {} of {} after {:?}",
        quick_done.load(Ordering::Acquire),
        quick,
        start.elapsed()
    );
    assert!(
        !blocker_done.load(Ordering::Acquire),
        "the blocker finished first: the test proves nothing"
    );
    assert!(wait_for(LIMIT, || blocker_done.load(Ordering::Acquire)));
}

#[test]
fn blocked_worker_is_rescued() {
    // The default (idle workers share chains at once) and the stall-based rescue (a 50 us threshold).
    for (threads, rescue_after) in [
        (2, Duration::ZERO),
        (4, Duration::ZERO),
        (2, Duration::from_micros(50)),
        (4, Duration::from_micros(50)),
    ] {
        let pool = GkReaderPool::new(
            GkReaderPoolConfig::default()
                .with_threads(threads)
                .with_rescue_after(rescue_after),
        )
        .unwrap();
        // The spec case: 300 ms blocker, 200 quick jobs, all done well before it.
        blocked_worker_round(
            &pool,
            Duration::from_millis(300),
            200,
            Duration::from_millis(150),
        );
        // Many short rounds, so both ways of meeting the blocker (alone in its chain, or ahead of a published chain)
        // are hit, with workers asleep and awake.
        for round in 0..20 {
            // BRANCH: let the workers fall asleep on every other round
            if round % 2 == 0 {
                thread::sleep(Duration::from_millis(5));
            }
            blocked_worker_round(
                &pool,
                Duration::from_millis(60),
                200,
                Duration::from_millis(40),
            );
        }
    }
}

/// Busy-waits about `micros` microseconds (a stand-in for a medium-size read).
fn busy_wait(micros: u64) {
    let until = Instant::now() + Duration::from_micros(micros);
    while Instant::now() < until {
        std::hint::spin_loop();
    }
}

#[test]
fn one_producer_with_medium_jobs_spreads_over_the_workers() {
    // One producer keeps a burst of ~20 us jobs in flight: all of them sit in its single lane. They must not run
    // serially on the one worker that reaps the lane.
    const JOBS: usize = 640;
    const MICROS: u64 = 20;
    const WORKERS: usize = 4;
    let pool = pool_with(WORKERS);
    let serial = Duration::from_micros(MICROS * JOBS as u64);
    let mut best = Duration::MAX;
    // Best of several rounds (the first also warms the workers up): robust against a busy machine.
    for _ in 0..10 {
        let done = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        for _ in 0..JOBS {
            let done = done.clone();
            let job: GkReadJob = Box::new(move || {
                busy_wait(MICROS);
                done.fetch_add(1, Ordering::Release);
            });
            if let Err(job) = pool.spawn(job) {
                job();
            }
        }
        assert!(wait_for(LIMIT, || done.load(Ordering::Acquire) == JOBS));
        best = best.min(start.elapsed());
    }
    // Ideal is serial / 4; allow up to 70% of the serial time.
    assert!(
        best < serial * 7 / 10,
        "the burst ran (almost) serially: best {best:?} against {serial:?} serial with {WORKERS} workers"
    );
}

#[test]
fn shutdown_drains_queued_jobs_and_joins() {
    let pool = pool_with(2);
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..3_000 {
        let done = done.clone();
        let job: GkReadJob = Box::new(move || {
            thread::sleep(Duration::from_micros(2));
            done.fetch_add(1, Ordering::Relaxed);
        });
        assert!(pool.spawn(job).is_ok());
    }
    pool.shutdown();
    // The workers are joined: everything accepted has run.
    assert_eq!(done.load(Ordering::Relaxed), 3_000);
    assert!(pool.is_shutdown());
    assert_eq!(pool.pending(), 0);

    // BRANCH: after shutdown the job comes back and can be run by the caller
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let job: GkReadJob = Box::new(move || flag.store(true, Ordering::Relaxed));
    let job = pool
        .spawn(job)
        .expect_err("a shut down pool accepts nothing");
    assert!(!ran.load(Ordering::Relaxed));
    job();
    assert!(ran.load(Ordering::Relaxed));

    // Idempotent, from another handle too.
    pool.shutdown();
    let other = pool.clone();
    other.shutdown();
    drop(other);
    assert!(pool.is_shutdown());
}

#[test]
fn dropping_the_last_handle_drains_and_joins() {
    let pool = pool_with(3);
    let clone = pool.clone();
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..2_000 {
        let done = done.clone();
        let job: GkReadJob = Box::new(move || {
            done.fetch_add(1, Ordering::Relaxed);
        });
        assert!(pool.spawn(job).is_ok());
    }
    drop(pool);
    // BRANCH: one handle is left, so the pool is still open
    assert!(!clone.is_shutdown());
    let job: GkReadJob = Box::new(|| {});
    assert!(clone.spawn(job).is_ok());
    drop(clone);
    assert_eq!(done.load(Ordering::Relaxed), 2_000);
}

#[test]
fn shutdown_from_inside_a_job_does_not_deadlock() {
    let pool = pool_with(2);
    let inner = pool.clone();
    let (tx, rx) = mpsc::channel();
    let job: GkReadJob = Box::new(move || {
        inner.shutdown();
        let _ = tx.send(());
    });
    assert!(pool.spawn(job).is_ok());
    assert!(
        rx.recv_timeout(LIMIT).is_ok(),
        "shutdown from a worker hung"
    );
    pool.shutdown();
    assert!(pool.is_shutdown());
}

#[test]
fn lane_exhaustion_returns_the_job() {
    let pool = Arc::new(
        GkReaderPool::new(
            GkReaderPoolConfig::default()
                .with_threads(2)
                .with_max_lanes(2),
        )
        .unwrap(),
    );
    let ran = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(Barrier::new(4));
    let mut threads = Vec::new();
    let (tx, rx) = mpsc::channel::<(usize, bool)>();
    for id in 0..3 {
        let (pool, ran, hold, tx) = (pool.clone(), ran.clone(), hold.clone(), tx.clone());
        // Serialize the three submitters so the third is the one that finds both lanes taken.
        let turn = Arc::new(());
        let _ = turn;
        threads.push(thread::spawn(move || {
            thread::sleep(Duration::from_millis(100 * id as u64));
            let counter = ran.clone();
            let job: GkReadJob = Box::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            });
            let accepted = match pool.spawn(job) {
                Ok(()) => true,
                // BRANCH: not accepted: run it inline, as the contract says
                Err(job) => {
                    job();
                    false
                }
            };
            tx.send((id, accepted)).unwrap();
            // Keep the thread (and so its lane) alive until all three have tried.
            hold.wait();
        }));
    }
    let mut results = Vec::new();
    for _ in 0..3 {
        results.push(rx.recv_timeout(LIMIT).unwrap());
    }
    hold.wait();
    for t in threads {
        t.join().unwrap();
    }
    results.sort();
    assert_eq!(results, vec![(0, true), (1, true), (2, false)]);
    assert!(wait_for(LIMIT, || ran.load(Ordering::Relaxed) == 3));

    // The lanes come back when their threads exit: a new thread is accepted again.
    let again = {
        let pool = pool.clone();
        thread::spawn(move || pool.spawn(Box::new(|| {})).is_ok())
            .join()
            .unwrap()
    };
    assert!(again);
}

#[test]
fn max_queue_rejects_when_full() {
    let pool = GkReaderPool::new(
        GkReaderPoolConfig::default()
            .with_threads(1)
            .with_max_queue(Some(5)),
    )
    .unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let (s, r, d) = (started.clone(), release.clone(), done.clone());
    let blocker: GkReadJob = Box::new(move || {
        s.store(true, Ordering::Release);
        while !r.load(Ordering::Acquire) {
            thread::sleep(Duration::from_micros(100));
        }
        d.fetch_add(1, Ordering::Relaxed);
    });
    assert!(pool.spawn(blocker).is_ok());
    // The only worker has taken the blocker out of the queue: it no longer counts as pending.
    assert!(wait_for(LIMIT, || started.load(Ordering::Acquire)));
    assert!(wait_for(LIMIT, || pool.pending() == 0));

    for _ in 0..5 {
        let d = done.clone();
        let job: GkReadJob = Box::new(move || {
            d.fetch_add(1, Ordering::Relaxed);
        });
        assert!(pool.spawn(job).is_ok());
    }
    assert_eq!(pool.pending(), 5);
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let job: GkReadJob = Box::new(move || flag.store(true, Ordering::Relaxed));
    let rejected = pool.spawn(job).expect_err("the queue is full");
    assert!(!ran.load(Ordering::Relaxed));
    rejected();
    assert!(ran.load(Ordering::Relaxed));

    release.store(true, Ordering::Release);
    assert!(wait_for(LIMIT, || done.load(Ordering::Relaxed) == 6));
    assert!(wait_for(LIMIT, || pool.pending() == 0));
    // Room again.
    assert!(pool.spawn(Box::new(|| {})).is_ok());
}

/// Idle pool, one job at a time from one thread, each awaited with a hard timeout: a lost wake-up shows as a timeout.
fn no_lost_wakeup(threads: usize, spin_polls: usize) {
    const ITERATIONS: usize = 20_000;
    let pool = GkReaderPool::new(
        GkReaderPoolConfig::default()
            .with_threads(threads)
            .with_spin_polls(spin_polls),
    )
    .unwrap();
    let flag = Arc::new(AtomicBool::new(false));
    for i in 0..ITERATIONS {
        // BRANCH: vary the idle gap so workers are caught spinning, registering, parked and waking
        match i % 16 {
            0 => thread::sleep(Duration::from_micros(300)),
            1..=4 => {
                for _ in 0..(i % 7) * 200 {
                    std::hint::spin_loop();
                }
            }
            _ => {}
        }
        flag.store(false, Ordering::Relaxed);
        let f = flag.clone();
        let job: GkReadJob = Box::new(move || f.store(true, Ordering::Release));
        assert!(pool.spawn(job).is_ok());
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut spins = 0u32;
        while !flag.load(Ordering::Acquire) {
            spins += 1;
            // BRANCH: yield now and then, and check the hard timeout
            if spins.is_multiple_of(64) {
                thread::yield_now();
                assert!(
                    Instant::now() < deadline,
                    "lost wake-up: job {i} not run after 10 s with {threads} workers"
                );
            }
            std::hint::spin_loop();
        }
    }
}

#[test]
fn no_lost_wakeup_one_worker() {
    no_lost_wakeup(1, 128);
}

#[test]
fn no_lost_wakeup_two_workers() {
    no_lost_wakeup(2, 128);
}

#[test]
fn no_lost_wakeup_all_cores() {
    no_lost_wakeup(available(), 128);
}

#[test]
fn no_lost_wakeup_without_spinning() {
    // Workers register as sleepers at once, so every submit meets the parking handshake.
    no_lost_wakeup(1, 0);
    no_lost_wakeup(2, 0);
    no_lost_wakeup(available(), 0);
}

#[test]
fn no_lost_wakeup_many_producers_few_workers() {
    // Several producers, each one job at a time, against a pool that is asleep between bursts.
    let pool = pool_with(2);
    thread::scope(|scope| {
        for producer in 0..4 {
            let pool = &pool;
            scope.spawn(move || {
                let flag = Arc::new(AtomicBool::new(false));
                for i in 0..3_000 {
                    // BRANCH: let the pool go to sleep now and then
                    if (i + producer) % 50 == 0 {
                        thread::sleep(Duration::from_micros(200));
                    }
                    flag.store(false, Ordering::Relaxed);
                    let f = flag.clone();
                    let job: GkReadJob = Box::new(move || f.store(true, Ordering::Release));
                    assert!(pool.spawn(job).is_ok());
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while !flag.load(Ordering::Acquire) {
                        thread::yield_now();
                        assert!(Instant::now() < deadline, "lost wake-up");
                    }
                }
            });
        }
    });
}

#[test]
fn thread_options_name_stack_and_hooks() {
    fn deep(n: usize) -> usize {
        let buf = [n as u8; 1024];
        black_box(&buf);
        // BRANCH: recurse, then fold the frame's bytes in so the frame is not optimized away
        if n == 0 {
            0
        } else {
            deep(n - 1) + black_box(buf[0]) as usize
        }
    }

    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let seen_index = Arc::new(AtomicUsize::new(0));
    let (a, b, c) = (started.clone(), stopped.clone(), seen_index.clone());
    let options = GkThreadOptions::default()
        .named("tp-test")
        .stack_size(32 * 1024 * 1024)
        .on_start(Arc::new(move |index| {
            a.fetch_add(1, Ordering::SeqCst);
            c.fetch_or(1 << index, Ordering::SeqCst);
        }))
        .on_stop(Arc::new(move |_| {
            b.fetch_add(1, Ordering::SeqCst);
        }));
    let pool = GkReaderPool::new(
        GkReaderPoolConfig::default()
            .with_threads(3)
            .with_thread(options),
    )
    .unwrap();

    let (tx, rx) = mpsc::channel();
    for _ in 0..30 {
        let tx = tx.clone();
        let job: GkReadJob = Box::new(move || {
            let name = thread::current().name().map(str::to_owned);
            // About 8 MiB of frames: far beyond the 2 MiB default, inside the 32 MiB asked for.
            let depth = deep(8_000);
            tx.send((name, depth)).unwrap();
        });
        assert!(pool.spawn(job).is_ok());
    }
    for _ in 0..30 {
        let (name, depth) = rx.recv_timeout(LIMIT).unwrap();
        let name = name.expect("workers are named");
        assert!(
            ["tp-test-0", "tp-test-1", "tp-test-2"].contains(&name.as_str()),
            "unexpected thread name {name}"
        );
        assert!(depth > 0);
    }
    pool.shutdown();
    assert_eq!(started.load(Ordering::SeqCst), 3);
    assert_eq!(stopped.load(Ordering::SeqCst), 3);
    assert_eq!(seen_index.load(Ordering::SeqCst), 0b111);
}

#[test]
fn default_worker_name_prefix_is_sqlite_reader() {
    let pool = pool_with(2);
    let (tx, rx) = mpsc::channel();
    let job: GkReadJob = Box::new(move || {
        tx.send(thread::current().name().map(str::to_owned))
            .unwrap();
    });
    assert!(pool.spawn(job).is_ok());
    let name = rx.recv_timeout(LIMIT).unwrap().unwrap();
    assert!(name.starts_with("sqlite-reader-"), "{name}");
}

#[test]
fn spawn_thread_runs_hooks_around_the_work_even_on_panic() {
    let order = Arc::new(AtomicUsize::new(0));
    let (a, w, z) = (order.clone(), order.clone(), order.clone());
    let options = GkThreadOptions::default()
        .named("hooked")
        .on_start(Arc::new(move |index| {
            assert_eq!(index, 7);
            assert_eq!(a.fetch_add(1, Ordering::SeqCst), 0);
        }))
        .on_stop(Arc::new(move |index| {
            assert_eq!(index, 7);
            assert_eq!(z.fetch_add(1, Ordering::SeqCst), 2);
        }));
    let handle = options
        .spawn_thread(7, move || {
            assert_eq!(thread::current().name(), Some("hooked-7"));
            assert_eq!(w.fetch_add(1, Ordering::SeqCst), 1);
        })
        .unwrap();
    handle.join().unwrap();
    assert_eq!(order.load(Ordering::SeqCst), 3);

    // The stop hook also runs when the work unwinds (the test harness unwinds in every profile).
    #[cfg(panic = "unwind")]
    {
        let stopped = Arc::new(AtomicBool::new(false));
        let flag = stopped.clone();
        let options = GkThreadOptions::default().on_stop(Arc::new(move |_| {
            flag.store(true, Ordering::SeqCst);
        }));
        let handle = options
            .spawn_thread(0, || panic!("expected by the test"))
            .unwrap();
        assert!(handle.join().is_err());
        assert!(stopped.load(Ordering::SeqCst));
    }
    let debug = format!("{options:?}");
    assert!(debug.contains("hooked"));
}

#[test]
fn pool_is_usable_through_a_trait_object() {
    let executor: Arc<dyn GkReaderExecutor> = Arc::new(pool_with(2));
    assert_eq!(executor.name(), "treiber-pool");
    let (tx, rx) = mpsc::channel();
    for n in 0..100u64 {
        let tx = tx.clone();
        let job: GkReadJob = Box::new(move || tx.send(n).unwrap());
        // BRANCH: refused jobs are run inline, as the contract says
        if let Err(job) = executor.spawn(job) {
            job();
        }
    }
    let mut sum = 0;
    for _ in 0..100 {
        sum += rx.recv_timeout(LIMIT).unwrap();
    }
    assert_eq!(sum, (0..100).sum::<u64>());

    // A custom executor only needs `spawn`; the name defaults to "custom".
    struct GkRefuseAll;
    impl GkReaderExecutor for GkRefuseAll {
        fn spawn(&self, job: GkReadJob) -> Result<(), GkReadJob> {
            Err(job)
        }
    }
    let custom: Arc<dyn GkReaderExecutor> = Arc::new(GkRefuseAll);
    assert_eq!(custom.name(), "custom");
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let job: GkReadJob = Box::new(move || flag.store(true, Ordering::Relaxed));
    custom.spawn(job).expect_err("refused")();
    assert!(ran.load(Ordering::Relaxed));
}

#[test]
fn inline_executor_runs_on_the_calling_thread() {
    let executor = GkInlineExecutor;
    assert_eq!(executor.name(), "inline");
    let caller = thread::current().id();
    let ran_on = Arc::new(AtomicU64::new(0));
    let flag = ran_on.clone();
    let job: GkReadJob = Box::new(move || {
        flag.store(
            u64::from(thread::current().id() == caller) + 1,
            Ordering::Relaxed,
        );
    });
    assert!(executor.spawn(job).is_ok());
    // Already run, on this thread, before spawn returned.
    assert_eq!(ran_on.load(Ordering::Relaxed), 2);
}

#[test]
fn a_panicking_job_does_not_kill_the_pool() {
    // Verified empirically: `cargo test` (also `--release`) ignores `panic = "abort"` for the whole test build, the
    // library included, so the job's panic unwinds into the worker's catch_unwind. Gated anyway, in case a build
    // (for example -Zpanic-abort-tests) aborts instead.
    #[cfg(panic = "unwind")]
    {
        let pool = pool_with(1);
        let job: GkReadJob = Box::new(|| panic!("expected by the test"));
        assert!(pool.spawn(job).is_ok());
        let (tx, rx) = mpsc::channel();
        let job: GkReadJob = Box::new(move || tx.send(()).unwrap());
        assert!(pool.spawn(job).is_ok());
        assert!(
            rx.recv_timeout(LIMIT).is_ok(),
            "the worker died with the job"
        );
    }
}

#[test]
fn config_validation_and_error_codes() {
    assert!(GkReaderPoolConfig::default().validate().is_ok());
    assert_eq!(
        GkReaderPoolConfig::default().with_max_lanes(0).validate(),
        Err(GkReaderPoolError::INVALID_CONFIG)
    );
    assert_eq!(
        GkReaderPoolConfig::default()
            .with_max_queue(Some(0))
            .validate(),
        Err(GkReaderPoolError::INVALID_CONFIG)
    );
    assert_eq!(
        GkReaderPoolConfig::default()
            .with_threads(1_000_000)
            .validate(),
        Err(GkReaderPoolError::INVALID_CONFIG)
    );
    assert_eq!(
        GkReaderPool::new(GkReaderPoolConfig::default().with_max_lanes(0)).unwrap_err(),
        GkReaderPoolError::INVALID_CONFIG
    );
    assert_ne!(
        GkReaderPoolError::INVALID_CONFIG,
        GkReaderPoolError::SPAWN_FAILED
    );
    assert_eq!(GkReaderPoolError::SPAWN_FAILED.name(), "SPAWN_FAILED");
    assert!(!GkReaderPoolError::INVALID_CONFIG.to_string().is_empty());

    // 0 threads means one per available core.
    let pool = GkReaderPool::new(GkReaderPoolConfig::default()).unwrap();
    assert_eq!(pool.threads(), available());
    assert!(format!("{pool:?}").contains("GkReaderPool"));
    let config = GkReaderPoolConfig::default()
        .with_spin_polls(0)
        .with_rescue_after(Duration::from_micros(20));
    assert!(format!("{config:?}").contains("sqlite-reader"));
}

#[test]
fn many_short_lived_pools_on_one_thread() {
    // Each pool gives this thread a lane; lanes of finished pools are swept when the next one is first used.
    for round in 0..300 {
        let pool = GkReaderPool::new(
            GkReaderPoolConfig::default()
                .with_threads(1)
                .with_max_lanes(1),
        )
        .unwrap();
        let (tx, rx) = mpsc::channel();
        let job: GkReadJob = Box::new(move || tx.send(round).unwrap());
        assert!(pool.spawn(job).is_ok());
        assert_eq!(rx.recv_timeout(LIMIT).unwrap(), round);
    }
}

#[test]
fn jobs_can_submit_jobs() {
    let pool = Arc::new(pool_with(2));
    let done = Arc::new(AtomicUsize::new(0));
    for _ in 0..200 {
        let (inner_pool, done) = (pool.clone(), done.clone());
        let job: GkReadJob = Box::new(move || {
            let again = done.clone();
            let follow_up: GkReadJob = Box::new(move || {
                again.fetch_add(1, Ordering::Relaxed);
            });
            // BRANCH: refused: run it here
            if let Err(job) = inner_pool.spawn(follow_up) {
                job();
            }
        });
        assert!(pool.spawn(job).is_ok());
    }
    assert!(wait_for(LIMIT, || done.load(Ordering::Relaxed) == 200));
}

// ==== Micro-benchmark ====

fn percentile(sorted: &[u64], p: f64) -> f64 {
    // BRANCH: nothing measured
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index] as f64 / 1_000.0
}

#[test]
#[ignore = "micro-benchmark: cargo test --release --test sqlite_pool -- --ignored --nocapture"]
fn bench_throughput_and_latency() {
    const WORKERS: usize = 4;
    for producers in [1usize, 4] {
        let pool = pool_with(WORKERS);

        // Throughput: each producer fires trivial jobs as fast as it can.
        let per_producer = 500_000usize;
        let total = producers * per_producer;
        let done = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        thread::scope(|scope| {
            for _ in 0..producers {
                let (pool, done) = (&pool, &done);
                scope.spawn(move || {
                    for _ in 0..per_producer {
                        let done = done.clone();
                        let job: GkReadJob = Box::new(move || {
                            done.fetch_add(1, Ordering::Relaxed);
                        });
                        // BRANCH: refused: run inline
                        if let Err(job) = pool.spawn(job) {
                            job();
                        }
                    }
                });
            }
        });
        assert!(wait_for(Duration::from_secs(120), || done
            .load(Ordering::Relaxed)
            == total));
        let elapsed = start.elapsed();
        println!(
            "[bench] {producers} producer(s), {WORKERS} workers: {:.2} M jobs/s ({total} jobs in {:?})",
            total as f64 / elapsed.as_secs_f64() / 1e6,
            elapsed
        );

        // Latency: each producer submits one job at a time and waits for it; the clock runs from just before the
        // submit to the first instruction of the job. Every 8th iteration pauses first, so some jobs meet sleeping
        // workers (the wake-up path) and some meet spinning ones.
        let iterations = 20_000usize;
        let mut all: Vec<u64> = Vec::with_capacity(producers * iterations);
        let results: Vec<Vec<u64>> = thread::scope(|scope| {
            let handles: Vec<_> = (0..producers)
                .map(|_| {
                    let pool = &pool;
                    scope.spawn(move || {
                        let slot = Arc::new(AtomicU64::new(0));
                        let epoch = Instant::now();
                        let mut samples = Vec::with_capacity(iterations);
                        for i in 0..iterations {
                            // BRANCH: sometimes let the pool go idle first
                            if i % 8 == 0 {
                                thread::sleep(Duration::from_micros(100));
                            }
                            slot.store(0, Ordering::Relaxed);
                            let s = slot.clone();
                            let submitted = Instant::now();
                            let job: GkReadJob = Box::new(move || {
                                s.store(epoch.elapsed().as_nanos() as u64 + 1, Ordering::Release);
                            });
                            let sent = (submitted - epoch).as_nanos() as u64;
                            // BRANCH: refused: run inline
                            if let Err(job) = pool.spawn(job) {
                                job();
                            }
                            let deadline = Instant::now() + Duration::from_secs(10);
                            let ran_at = loop {
                                let value = slot.load(Ordering::Acquire);
                                // BRANCH: the job has run
                                if value != 0 {
                                    break value - 1;
                                }
                                assert!(Instant::now() < deadline, "bench job never ran");
                                std::hint::spin_loop();
                            };
                            samples.push(ran_at.saturating_sub(sent));
                        }
                        samples
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for samples in results {
            all.extend(samples);
        }
        all.sort_unstable();
        println!(
            "[bench] {producers} producer(s), {WORKERS} workers: submit->run latency p50 {:.1} us, p99 {:.1} us, max {:.1} us",
            percentile(&all, 0.50),
            percentile(&all, 0.99),
            percentile(&all, 1.0)
        );
    }
}
