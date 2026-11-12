// ============================================================================
// Import Definition
// ============================================================================

#[cfg(feature = "crossbeam-channel")]
use adaptif_gatotkaca_sqlite::crossbeam_channel::{Receiver, Sender, unbounded};
use adaptif_gatotkaca_sqlite::crossbeam_utils::Backoff;
use adaptif_gatotkaca_sqlite::smallvec::SmallVec;
use adaptif_gatotkaca_sqlite::{
    FastU64Map, GkCompactStr, GkSavepointAction, GkSqliteConfig, GkSqliteDb, GkSqliteErrorEnum,
    GkSqliteMsg, GkSqliteParams, GkSqliteReader, GkSqliteRowRecord, GkSqliteValue,
    SqliteFastU64Map, rusqlite,
};

// The paths below are the ones the main repository's Python glue (src/ffi/python/{sqlite,security}) imports from this crate.
// A rename or removal makes this file stop compiling; it asserts nothing at run time beyond a few cheap sanity checks.

#[test]
fn public_paths_used_by_the_python_glue_resolve() {
    // BRANCH: type names are only referenced, never constructed, where construction needs a database
    fn name_of<T>() -> &'static str {
        std::any::type_name::<T>()
    }
    let names = [
        name_of::<GkCompactStr>(),
        name_of::<GkSavepointAction>(),
        name_of::<GkSqliteConfig>(),
        name_of::<GkSqliteDb>(),
        name_of::<GkSqliteMsg>(),
        name_of::<GkSqliteParams>(),
        name_of::<GkSqliteReader>(),
        name_of::<GkSqliteRowRecord>(),
        name_of::<GkSqliteValue>(),
        name_of::<SqliteFastU64Map<u8>>(),
        name_of::<FastU64Map<u8>>(),
    ];
    assert!(names.iter().all(|n| !n.is_empty()));

    // BRANCH: GkSqliteErrorEnum::SqliteError wraps a rusqlite error (used as a map_err function in the glue)
    let wrap: fn(rusqlite::Error) -> GkSqliteErrorEnum = GkSqliteErrorEnum::SqliteError;
    let _ = wrap;

    // BRANCH: crossbeam_channel, crossbeam_utils, smallvec and rusqlite are re-exported as the same crates
    #[cfg(feature = "crossbeam-channel")]
    {
        // BRANCH: the crossbeam re-export exists only with the `crossbeam-channel` feature
        let (tx, rx): (Sender<u8>, Receiver<u8>) = unbounded();
        tx.send(7).unwrap();
        assert_eq!(rx.recv().unwrap(), 7);
    }
    let _backoff = Backoff::new();
    let sv: SmallVec<[u8; 4]> = SmallVec::new();
    assert!(sv.is_empty());
    assert!(!rusqlite::version().is_empty());
}
