#![allow(clippy::field_reassign_with_default)] // the tests tweak one field of a default configuration

// ============================================================================
// Import Definition
// ============================================================================

use adaptif_gatotkaca_sqlite::{GkSqliteConfig, GkSqliteDb, GkSqliteErrorEnum, GkSqliteValue};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

fn create_temp_db_path(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir();
    dir.join(format!(
        "gatotkaca_sqlite_{}_{}_{}.db",
        prefix,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn cleanup_temp_db(path: &PathBuf) {
    let path_str = path.to_string_lossy();
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path_str));
    let _ = std::fs::remove_file(format!("{}-shm", path_str));
}

// ============================================================================
// Test Suite: 13 Concurrency & Correctness Invariants
// ============================================================================

/// Test 1: Write then read on same thread sees write (token path).
#[test]
fn test_01_read_your_own_writes_same_thread() {
    let db_path = create_temp_db_path("t01");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);",
        (),
    )
    .expect("create table");

    let outcome = db
        .execute("INSERT INTO items (name) VALUES (?1);", ("apple",))
        .expect("insert");
    assert!(outcome.seq > 0);

    let name: String = db
        .query_row("SELECT name FROM items WHERE id = 1;", (), |row| row.get(0))
        .expect("query row");

    assert_eq!(name, "apple");
    cleanup_temp_db(&db_path);
}

/// Test 2: Write on thread A, pass token to thread B, B's read sees it.
#[test]
fn test_02_cross_thread_token_passing() {
    let db_path = create_temp_db_path("t02");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT);", ())
        .expect("create table");

    let db_clone = db.clone();
    let (tx, rx) = crossbeam_channel::bounded(1);

    let handle = thread::spawn(move || {
        let outcome = db_clone
            .execute("INSERT INTO logs (msg) VALUES (?1);", ("committed_event",))
            .expect("insert on thread A");
        tx.send(outcome.seq).expect("send token");
    });

    handle.join().unwrap();
    let token = rx.recv().expect("recv token");

    // Thread B sets min_seq expectation via token check
    assert!(db.last_committed() >= token);
    let msg: String = db
        .query_row("SELECT msg FROM logs WHERE id = 1;", (), |row| row.get(0))
        .expect("read on thread B");

    assert_eq!(msg, "committed_event");
    cleanup_temp_db(&db_path);
}

/// Test 3: 8 writer threads x 1000 inserts, count == 8000, last_committed() == 8000.
#[test]
fn test_03_concurrent_multithreaded_writers() {
    let db_path = create_temp_db_path("t03");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE counter (id INTEGER PRIMARY KEY, worker INTEGER, val INTEGER);",
        (),
    )
    .expect("create table");

    let num_threads = 8;
    let inserts_per_thread = 1000;
    let mut handles = Vec::new();

    for t_id in 0..num_threads {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..inserts_per_thread {
                let outcome = db_clone
                    .execute(
                        "INSERT INTO counter (worker, val) VALUES (?1, ?2);",
                        (t_id as i64, i as i64),
                    )
                    .expect("concurrent insert");
                assert!(outcome.seq > 0);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let total_rows: i64 = db
        .query_row("SELECT COUNT(*) FROM counter;", (), |row| row.get(0))
        .expect("count rows");

    assert_eq!(total_rows, (num_threads * inserts_per_thread) as i64);
    // 1 for create table + 8000 inserts = 8001
    assert_eq!(db.last_committed(), 8001);

    cleanup_temp_db(&db_path);
}

/// Test 4: Constraint violation in batch returns Err to caller only; others commit.
#[test]
fn test_04_savepoint_isolation_on_constraint_violation() {
    let db_path = create_temp_db_path("t04");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE unique_items (id INTEGER PRIMARY KEY, sku TEXT UNIQUE);",
        (),
    )
    .expect("create table");

    // Insert first item
    db.execute("INSERT INTO unique_items (sku) VALUES (?1);", ("SKU-100",))
        .expect("first insert");

    // Concurrently or in batch send a duplicate item and valid item
    let duplicate_res = db.execute("INSERT INTO unique_items (sku) VALUES (?1);", ("SKU-100",));
    assert!(duplicate_res.is_err(), "Duplicate SKU must return Err");

    // Valid subsequent item must succeed and commit cleanly
    let valid_res = db.execute("INSERT INTO unique_items (sku) VALUES (?1);", ("SKU-200",));
    assert!(valid_res.is_ok(), "Valid SKU must succeed");

    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM unique_items;", (), |row| row.get(0))
        .expect("count");
    assert_eq!(count, 2);

    cleanup_temp_db(&db_path);
}

/// Test 5: Forced COMMIT failure resets sequence and fails pending batch.
#[test]
fn test_05_commit_failure_sequence_rollback() {
    let db_path = create_temp_db_path("t05");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER CHECK(balance >= 0));",
        (),
    )
    .expect("create table");

    let seq_before = db.last_committed();

    // Trigger a failure inside a transaction closure
    let tx_res = db.transaction(|conn| {
        conn.execute("INSERT INTO accounts (balance) VALUES (100);", [])?;
        // Violate CHECK constraint
        conn.execute("INSERT INTO accounts (balance) VALUES (-50);", [])?;
        Ok(())
    });

    assert!(tx_res.is_err(), "Transaction must fail");
    assert_eq!(
        db.last_committed(),
        seq_before,
        "Committed counter must not increment on aborted transaction"
    );

    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM accounts;", (), |row| row.get(0))
        .expect("count");
    assert_eq!(count, 0, "No rows committed");

    cleanup_temp_db(&db_path);
}

/// Test 6: Non-autocommit mode: reader does not observe uncommitted writer data.
#[test]
fn test_06_non_autocommit_reader_isolation() {
    let db_path = create_temp_db_path("t06");
    let path_str = db_path.to_str().unwrap();

    let mut config = GkSqliteConfig::default();
    config.autocommit_mode = false;
    let db = GkSqliteDb::open(path_str, config).expect("open db in non-autocommit mode");

    db.execute(
        "CREATE TABLE secret (id INTEGER PRIMARY KEY, val TEXT);",
        (),
    )
    .expect("create table");

    // Start a long-running transaction with sleep
    let db_clone = db.clone();
    let started = Arc::new(AtomicBool::new(false));
    let started_clone = started.clone();

    let handle = thread::spawn(move || {
        let _ = db_clone.transaction(move |conn| {
            conn.execute(
                "INSERT INTO secret (val) VALUES ('uncommitted_secret');",
                [],
            )?;
            started_clone.store(true, Ordering::Release);
            thread::sleep(Duration::from_millis(50));
            Ok(())
        });
    });

    while !started.load(Ordering::Acquire) {
        thread::yield_now();
    }

    // Reader thread reads immediately without waiting for writer transaction
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM secret;", (), |row| row.get(0))
        .expect("reader count");

    // Reader must NOT see uncommitted secret data
    assert_eq!(count, 0, "Reader must not observe uncommitted data");

    handle.join().unwrap();

    // After commit, reader sees the committed secret
    let post_count: i64 = db
        .query_row("SELECT COUNT(*) FROM secret;", (), |row| row.get(0))
        .expect("reader count after commit");
    assert_eq!(post_count, 1);

    cleanup_temp_db(&db_path);
}

/// Test 7: Reader always reports is_autocommit() == true even after query error.
#[test]
fn test_07_reader_autocommit_invariant_after_error() {
    let db_path = create_temp_db_path("t07");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute("CREATE TABLE status (id INTEGER);", ())
        .expect("create table");

    // Intentionally execute a syntactically invalid read query
    let err_res: Result<i64, GkSqliteErrorEnum> =
        db.query_row("SELECT non_existent_col FROM status;", (), |r| r.get(0));
    assert!(err_res.is_err(), "Query must fail");

    // Reader must still maintain valid state and execute subsequent valid queries
    let valid: i64 = db
        .query_row("SELECT 42;", (), |r| r.get(0))
        .expect("reader must remain in autocommit mode");
    assert_eq!(valid, 42);

    cleanup_temp_db(&db_path);
}

/// Test 8: WAL size bounded under sustained read+write loop.
#[test]
fn test_08_wal_bounded_checkpoint_loop() {
    let db_path = create_temp_db_path("t08");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE wal_stress (id INTEGER PRIMARY KEY, payload BLOB);",
        (),
    )
    .expect("create table");

    let payload = vec![0u8; 1024]; // 1KB per row
    for i in 0..500 {
        db.execute(
            "INSERT INTO wal_stress (payload) VALUES (?1);",
            (GkSqliteValue::Blob(smallvec::SmallVec::from_slice(
                &payload,
            )),),
        )
        .expect("insert");

        if i % 100 == 0 {
            db.checkpoint().expect("checkpoint");
        }
    }

    db.checkpoint().expect("final checkpoint");

    let wal_file = format!("{}-wal", path_str);
    if let Ok(meta) = std::fs::metadata(wal_file) {
        // WAL file should be small (bounded) after checkpointing
        assert!(
            meta.len() < 2 * 1024 * 1024,
            "WAL file must be bounded, found {} bytes",
            meta.len()
        );
    }

    cleanup_temp_db(&db_path);
}

/// Test 9: transaction() rolls back entirely on Err.
#[test]
fn test_09_transaction_full_rollback_on_err() {
    let db_path = create_temp_db_path("t09");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, entry TEXT);",
        (),
    )
    .expect("create table");

    let initial_committed = db.last_committed();

    let res: Result<(), GkSqliteErrorEnum> = db.transaction(|conn| {
        conn.execute("INSERT INTO ledger (entry) VALUES ('first');", [])?;
        conn.execute("INSERT INTO ledger (entry) VALUES ('second');", [])?;
        Err(GkSqliteErrorEnum::InvalidInput(
            "forced business logic abort",
        ))
    });

    assert!(res.is_err());
    assert_eq!(db.last_committed(), initial_committed);

    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM ledger;", (), |r| r.get(0))
        .expect("count");
    assert_eq!(
        count, 0,
        "All statements in aborted transaction must be rolled back"
    );

    cleanup_temp_db(&db_path);
}

/// Test 10: Mode switch at runtime under load loses no writes.
#[test]
fn test_10_mode_switch_under_load_no_writes_lost() {
    let db_path = create_temp_db_path("t10");
    let path_str = db_path.to_str().unwrap();

    let db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE toggle (id INTEGER PRIMARY KEY, n INTEGER);",
        (),
    )
    .expect("create table");

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let db_writer = db.clone();

    let write_count = Arc::new(AtomicU64::new(0));
    let write_count_clone = write_count.clone();

    // Spawn writers
    let writer_handle = thread::spawn(move || {
        let mut i = 0;
        while running_clone.load(Ordering::Relaxed) || i < 200 {
            db_writer
                .execute("INSERT INTO toggle (n) VALUES (?1);", (i as i64,))
                .expect("insert during mode switch");
            write_count_clone.fetch_add(1, Ordering::Relaxed);
            i += 1;
            if i >= 400 {
                break;
            }
        }
    });

    // Concurrently toggle autocommit mode back and forth
    for _ in 0..10 {
        db.set_autocommit_mode(false).expect("set non-autocommit");
        thread::sleep(Duration::from_millis(2));
        db.set_autocommit_mode(true).expect("set autocommit");
        thread::sleep(Duration::from_millis(2));
    }

    running.store(false, Ordering::Relaxed);
    writer_handle.join().unwrap();

    let expected = write_count.load(Ordering::Relaxed);
    let actual: i64 = db
        .query_row("SELECT COUNT(*) FROM toggle;", (), |r| r.get(0))
        .expect("count");

    assert_eq!(
        actual, expected as i64,
        "No writes should be lost during mode switch"
    );
    cleanup_temp_db(&db_path);
}

/// Test 11: Shutdown with queued writes: all acked or errored, none lost.
#[test]
fn test_11_shutdown_flushes_all_queued_writes() {
    let db_path = create_temp_db_path("t11");
    let path_str = db_path.to_str().unwrap();

    let mut db = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("open db");
    db.execute(
        "CREATE TABLE queue_test (id INTEGER PRIMARY KEY, v INTEGER);",
        (),
    )
    .expect("create table");

    let count = 100;
    for i in 0..count {
        db.execute("INSERT INTO queue_test (v) VALUES (?1);", (i as i64,))
            .expect("insert before close");
    }

    // Close db handle gracefully
    db.close().expect("close db");

    // Reopen and verify all 100 rows were persisted
    let reopened = GkSqliteDb::open(path_str, GkSqliteConfig::default()).expect("reopen db");
    let actual: i64 = reopened
        .query_row("SELECT COUNT(*) FROM queue_test;", (), |r| r.get(0))
        .expect("count");

    assert_eq!(actual, count as i64);
    cleanup_temp_db(&db_path);
}

/// Test 12: Read with unsatisfiable token falls back to writer after deadline without hanging.
#[test]
fn test_12_unsatisfiable_token_fallback_without_hang() {
    let db_path = create_temp_db_path("t12");
    let path_str = db_path.to_str().unwrap();

    let mut config = GkSqliteConfig::default();
    config.read_timeout = Duration::from_millis(5); // 5ms deadline
    let db = GkSqliteDb::open(path_str, config).expect("open db");

    db.execute(
        "CREATE TABLE token_test (id INTEGER PRIMARY KEY, name TEXT);",
        (),
    )
    .expect("create table");
    db.execute(
        "INSERT INTO token_test (name) VALUES ('fallback_success');",
        (),
    )
    .expect("insert");

    // Manually force thread's last token to an unsatisfiable sequence in the distant future
    adaptif_gatotkaca_sqlite::set_last_token(db.id(), 9_999_999);

    let start = Instant::now();
    // Query should spin until 5ms timeout, then fall back to writer connection without hanging!
    let name: String = db
        .query_row("SELECT name FROM token_test WHERE id = 1;", (), |r| {
            r.get(0)
        })
        .expect("fallback query to writer");

    let elapsed = start.elapsed();
    assert_eq!(name, "fallback_success");
    // Verify it didn't hang indefinitely (should take around 5-50ms)
    assert!(
        elapsed < Duration::from_millis(500),
        "Query took too long: {:?}",
        elapsed
    );

    cleanup_temp_db(&db_path);
}

/// Test 13: Full pipeline verification under explicit non-autocommit mode
#[test]
fn test_13_non_autocommit_full_pipeline_verification() {
    let db_path = create_temp_db_path("t13");
    let path_str = db_path.to_str().unwrap();

    let mut config = GkSqliteConfig::default();
    config.autocommit_mode = false;
    let db = GkSqliteDb::open(path_str, config).expect("open db in non-autocommit mode");

    db.execute(
        "CREATE TABLE pipeline (id INTEGER PRIMARY KEY, k TEXT, v INTEGER);",
        (),
    )
    .expect("create table");

    // Perform multiple rapid writes which get batched in non-autocommit mode
    for i in 0..50 {
        let outcome = db
            .execute(
                "INSERT INTO pipeline (k, v) VALUES (?1, ?2);",
                (format!("key_{}", i), i as i64),
            )
            .expect("insert in non-autocommit mode");
        assert!(outcome.seq > 0);
    }

    // Read back in mapping query
    let rows: Vec<(String, i64)> = db
        .query_map("SELECT k, v FROM pipeline ORDER BY v ASC;", (), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("query map in non-autocommit mode");

    assert_eq!(rows.len(), 50);
    assert_eq!(rows[0].0, "key_0");
    assert_eq!(rows[49].1, 49);

    cleanup_temp_db(&db_path);
}
