// ============================================================================
// Import Definition
// ============================================================================

use adaptif_gatotkaca_sqlite::error::GkSqliteErrorEnum;
use adaptif_gatotkaca_sqlite::pragma::*;
use adaptif_gatotkaca_sqlite::rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;

// ============================================================================
// Helpers
// ============================================================================

fn int(conn: &Connection, name: &str) -> i64 {
    conn.pragma_query_value(None, name, |r| r.get::<_, i64>(0))
        .unwrap()
}

fn text(conn: &Connection, name: &str) -> String {
    conn.pragma_query_value(None, name, |r| r.get::<_, String>(0))
        .unwrap()
}

fn fresh() -> (TempDir, String, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.db").to_str().unwrap().to_owned();
    let conn = Connection::open(&path).unwrap();
    (dir, path, conn)
}

fn name(s: &str) -> GkCompactStr {
    GkCompactStr::new(s)
}

use adaptif_gatotkaca_sqlite::counter::GkCompactStr;

// ============================================================================
// Defaults
// ============================================================================

#[test]
fn defaults_match_literal_legacy_values() {
    let w = GkSqlitePragmas::writer_default();
    assert_eq!(w.journal_mode, Some(GkJournalMode::Wal));
    assert_eq!(w.synchronous, Some(GkSynchronous::Normal));
    assert_eq!(w.cache_size_kib, None);
    assert_eq!(w.mmap_size, None);
    assert_eq!(w.query_only, None);
    assert!(w.extra.is_empty());
    assert_eq!(
        w.to_sql_statements().unwrap(),
        ["PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"]
    );

    let r = GkSqlitePragmas::reader_default();
    assert_eq!(r.query_only, Some(true));
    assert_eq!(r.mmap_size, Some(268_435_456));
    assert_eq!(r.cache_size_kib, Some(64_000));
    assert_eq!(r.journal_mode, None);
    assert_eq!(r.synchronous, None);
    assert!(r.extra.is_empty());
    assert_eq!(
        r.to_sql_statements().unwrap(),
        [
            "PRAGMA cache_size=-64000",
            "PRAGMA mmap_size=268435456",
            "PRAGMA query_only=1"
        ]
    );
    assert_eq!(GkJournalMode::default(), GkJournalMode::Wal);
    assert_eq!(GkSqlitePragmas::default(), GkSqlitePragmas::none());
}

#[test]
fn writer_default_on_fresh_file() {
    let (_d, _p, conn) = fresh();
    GkSqlitePragmas::writer_default().apply(&conn).unwrap();
    assert_eq!(text(&conn, "journal_mode"), "wal");
    assert_eq!(int(&conn, "synchronous"), 1);
}

#[test]
fn reader_default_on_read_only_connection() {
    let (_d, path, conn) = fresh();
    GkSqlitePragmas::writer_default().apply(&conn).unwrap();
    conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
        .unwrap();
    let ro = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    GkSqlitePragmas::reader_default().apply(&ro).unwrap();
    assert_eq!(int(&ro, "query_only"), 1);
    assert_eq!(int(&ro, "cache_size"), -64000);
    assert_eq!(int(&ro, "mmap_size"), 268_435_456);
    assert!(ro.execute("INSERT INTO t VALUES (2)", []).is_err());
}

// ============================================================================
// Presets and fields
// ============================================================================

#[test]
fn presets_read_back() {
    let (_d, _p, conn) = fresh();
    GkSqlitePragmas::writer_throughput().apply(&conn).unwrap();
    assert_eq!(text(&conn, "journal_mode"), "wal");
    assert_eq!(int(&conn, "synchronous"), 1);
    assert_eq!(int(&conn, "wal_autocheckpoint"), 4000);
    assert_eq!(int(&conn, "temp_store"), 2);
    assert_eq!(int(&conn, "cache_size"), -16384);

    let (_d, _p, conn) = fresh();
    GkSqlitePragmas::writer_durable().apply(&conn).unwrap();
    assert_eq!(text(&conn, "journal_mode"), "wal");
    assert_eq!(int(&conn, "synchronous"), 2);

    let (_d, _p, conn) = fresh();
    GkSqlitePragmas::reader_low_memory().apply(&conn).unwrap();
    assert_eq!(int(&conn, "query_only"), 1);
    assert_eq!(int(&conn, "cache_size"), -2048);
    assert_eq!(int(&conn, "mmap_size"), 0);
}

#[test]
fn every_typed_field_round_trips() {
    let (_d, _p, conn) = fresh();
    let p = GkSqlitePragmas::none()
        .with_journal_mode(GkJournalMode::Truncate)
        .with_synchronous(GkSynchronous::Extra)
        .with_cache_size_kib(1234)
        .with_mmap_size(1 << 20)
        .with_temp_store(GkTempStore::File)
        .with_wal_autocheckpoint(77)
        .with_journal_size_limit(1 << 16)
        .with_foreign_keys(true)
        .with_locking_mode(GkLockingMode::Exclusive)
        .with_query_only(true);
    p.apply(&conn).unwrap();
    assert_eq!(text(&conn, "journal_mode"), "truncate");
    assert_eq!(int(&conn, "synchronous"), 3);
    assert_eq!(int(&conn, "cache_size"), -1234);
    assert_eq!(int(&conn, "mmap_size"), 1 << 20);
    assert_eq!(int(&conn, "temp_store"), 1);
    assert_eq!(int(&conn, "wal_autocheckpoint"), 77);
    assert_eq!(int(&conn, "journal_size_limit"), 1 << 16);
    assert_eq!(int(&conn, "foreign_keys"), 1);
    assert_eq!(text(&conn, "locking_mode"), "exclusive");
    assert_eq!(int(&conn, "query_only"), 1);
}

#[test]
fn every_enum_variant_applies() {
    for jm in [
        GkJournalMode::Wal,
        GkJournalMode::Delete,
        GkJournalMode::Truncate,
        GkJournalMode::Persist,
        GkJournalMode::Off,
    ] {
        let (_d, _p, conn) = fresh();
        GkSqlitePragmas::none()
            .with_journal_mode(jm)
            .apply(&conn)
            .unwrap();
        assert_eq!(text(&conn, "journal_mode"), jm.as_str().to_lowercase());
    }
    for (s, n) in [
        (GkSynchronous::Off, 0),
        (GkSynchronous::Normal, 1),
        (GkSynchronous::Full, 2),
        (GkSynchronous::Extra, 3),
    ] {
        let (_d, _p, conn) = fresh();
        GkSqlitePragmas::none()
            .with_synchronous(s)
            .apply(&conn)
            .unwrap();
        assert_eq!(int(&conn, "synchronous"), n);
    }
    for (t, n) in [
        (GkTempStore::Default, 0),
        (GkTempStore::File, 1),
        (GkTempStore::Memory, 2),
    ] {
        let (_d, _p, conn) = fresh();
        GkSqlitePragmas::none()
            .with_temp_store(t)
            .apply(&conn)
            .unwrap();
        assert_eq!(int(&conn, "temp_store"), n);
    }
    // Memory journal on its own connection.
    let conn = Connection::open_in_memory().unwrap();
    GkSqlitePragmas::none()
        .with_journal_mode(GkJournalMode::Memory)
        .apply(&conn)
        .unwrap();
    assert_eq!(text(&conn, "journal_mode"), "memory");
}

#[test]
fn extras_apply_in_order() {
    let (_d, _p, conn) = fresh();
    let p = GkSqlitePragmas::none()
        .with_extra("user_version", GkPragmaValue::Int(42))
        .with_extra("recursive_triggers", GkPragmaValue::Bool(true))
        .with_extra("secure_delete", GkPragmaValue::Text(name("fast")))
        .with_extra("application_id", GkPragmaValue::Int(7));
    p.apply(&conn).unwrap();
    assert_eq!(int(&conn, "user_version"), 42);
    assert_eq!(int(&conn, "recursive_triggers"), 1);
    assert_eq!(int(&conn, "secure_delete"), 2);
    assert_eq!(int(&conn, "application_id"), 7);
    let stmts = p.to_sql_statements().unwrap();
    assert_eq!(stmts[0], "PRAGMA user_version=42");
    assert_eq!(stmts[3], "PRAGMA application_id=7");
}

#[test]
fn none_changes_nothing() {
    let (_d, _p, conn) = fresh();
    let before = (
        text(&conn, "journal_mode"),
        int(&conn, "synchronous"),
        int(&conn, "cache_size"),
        int(&conn, "mmap_size"),
        int(&conn, "temp_store"),
        int(&conn, "wal_autocheckpoint"),
        int(&conn, "foreign_keys"),
        int(&conn, "query_only"),
    );
    let none = GkSqlitePragmas::none();
    assert!(none.to_sql_statements().unwrap().is_empty());
    none.apply(&conn).unwrap();
    let after = (
        text(&conn, "journal_mode"),
        int(&conn, "synchronous"),
        int(&conn, "cache_size"),
        int(&conn, "mmap_size"),
        int(&conn, "temp_store"),
        int(&conn, "wal_autocheckpoint"),
        int(&conn, "foreign_keys"),
        int(&conn, "query_only"),
    );
    assert_eq!(before, after);
}

#[test]
fn apply_on_shared_memory_uri() {
    let uri = "file:mem_pragma_test?mode=memory&cache=shared";
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_URI;
    let w = Connection::open_with_flags(uri, flags).unwrap();
    GkSqlitePragmas::writer_default().apply(&w).unwrap();
    // An in-memory database cannot use WAL; SQLite reports "memory" and does not fail.
    assert_eq!(text(&w, "journal_mode"), "memory");
    w.execute_batch("CREATE TABLE t(x);").unwrap();
    let r = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap();
    GkSqlitePragmas::reader_default().apply(&r).unwrap();
    assert_eq!(int(&r, "query_only"), 1);
}

// ============================================================================
// Validation
// ============================================================================

#[test]
fn validation_rejects_bad_extras() {
    let bad_names = [
        "",
        "1abc",
        "a b",
        "a;b",
        "a-b",
        "a'b",
        "x=1; DROP TABLE t",
        &"a".repeat(65),
    ];
    for n in bad_names {
        let p = GkSqlitePragmas::none().with_extra(n, GkPragmaValue::Int(1));
        assert_eq!(p.validate(), Err(GkPragmaError::INVALID_NAME), "{n:?}");
    }
    let bad_texts = [
        "",
        "a b",
        "a;b",
        "a'b",
        "a\"b",
        "a(b)",
        "x\0y",
        "a=b",
        &"a".repeat(65),
    ];
    for t in bad_texts {
        let p = GkSqlitePragmas::none().with_extra("user_version", GkPragmaValue::Text(name(t)));
        assert_eq!(p.validate(), Err(GkPragmaError::INVALID_VALUE), "{t:?}");
    }
    // Boundary: 64 chars are accepted.
    let ok = GkSqlitePragmas::none()
        .with_extra("a".repeat(64), GkPragmaValue::Text(name(&"b".repeat(64))));
    assert_eq!(ok.validate(), Ok(()));
    let ok = GkSqlitePragmas::none().with_extra("_x1", GkPragmaValue::Text(name("a-b_c.1")));
    assert_eq!(ok.validate(), Ok(()));
}

#[test]
fn validation_rejects_duplicates_of_typed_names() {
    for n in [
        "journal_mode",
        "SYNCHRONOUS",
        "cache_size",
        "cache_size_kib",
        "mmap_size",
        "temp_store",
        "wal_autocheckpoint",
        "journal_size_limit",
        "foreign_keys",
        "locking_mode",
        "Query_Only",
    ] {
        let p = GkSqlitePragmas::none().with_extra(n, GkPragmaValue::Int(1));
        assert_eq!(p.validate(), Err(GkPragmaError::FORBIDDEN), "{n}");
    }
}

#[test]
fn validation_rejects_bad_numbers() {
    let bad = [
        GkSqlitePragmas::none().with_cache_size_kib(0),
        GkSqlitePragmas::none().with_cache_size_kib(-1),
        GkSqlitePragmas::none().with_cache_size_kib(i64::MIN),
        GkSqlitePragmas::none().with_cache_size_kib(i64::from(i32::MAX) + 1),
        GkSqlitePragmas::none().with_mmap_size(-1),
        GkSqlitePragmas::none().with_wal_autocheckpoint(-1),
        GkSqlitePragmas::none().with_wal_autocheckpoint(i64::from(i32::MAX) + 1),
        GkSqlitePragmas::none().with_journal_size_limit(-2),
    ];
    for p in bad {
        assert_eq!(p.validate(), Err(GkPragmaError::INVALID_VALUE), "{p:?}");
    }
    let good = GkSqlitePragmas::none()
        .with_cache_size_kib(1)
        .with_mmap_size(0)
        .with_wal_autocheckpoint(0)
        .with_journal_size_limit(-1);
    assert_eq!(good.validate(), Ok(()));
}

#[test]
fn apply_maps_validation_failure_to_invalid_input_without_touching_connection() {
    let (_d, _p, conn) = fresh();
    let p = GkSqlitePragmas::none()
        .with_synchronous(GkSynchronous::Off)
        .with_extra("a;b", GkPragmaValue::Int(1));
    // BRANCH: expect InvalidInput carrying the static message
    match p.apply(&conn) {
        Err(GkSqliteErrorEnum::InvalidInput(msg)) => {
            assert_eq!(msg, GkPragmaError::INVALID_NAME.message());
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(int(&conn, "synchronous"), 2, "nothing may be applied");
}

#[test]
fn error_type_surface() {
    assert_eq!(std::mem::size_of::<GkPragmaError>(), 1);
    assert_ne!(GkPragmaError::INVALID_NAME, GkPragmaError::INVALID_VALUE);
    assert_ne!(GkPragmaError::INVALID_VALUE, GkPragmaError::FORBIDDEN);
    for e in [
        GkPragmaError::INVALID_NAME,
        GkPragmaError::INVALID_VALUE,
        GkPragmaError::FORBIDDEN,
    ] {
        assert_eq!(e.to_string(), e.message());
        let _: &dyn std::error::Error = &e;
    }
}
