//! Write-path throughput of the existing engine (measurement, `--ignored`): used to compare SQLite compiled with and
//! without the session/preupdate-hook flags (set `LIBSQLITE3_FLAGS` in the environment), and the writer channel kinds
//! (Treiber vs crossbeam, the latter with `--features crossbeam-channel`).

// ==== Import Definition ====

use adaptif_gatotkaca_sqlite::{GkChannelKind, GkSqliteConfig, GkSqliteDb};
use std::sync::Arc;
use std::time::Instant;

fn measure(kind: GkChannelKind) {
    let dir = tempfile::tempdir().unwrap();
    let config = GkSqliteConfig::builder()
        .writer_channel(kind)
        .build()
        .unwrap();
    let db = Arc::new(GkSqliteDb::open(dir.path().join("b.db").to_str().unwrap(), config).unwrap());
    db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INTEGER)",
        (),
    )
    .unwrap();
    let mut best = 0f64;
    for round in 0..5 {
        let t = Instant::now();
        let n = 40_000;
        for i in 0..n {
            db.execute(
                "INSERT INTO t (id, v, n) VALUES (?1, ?2, ?3)",
                ((round * n + i) as i64, "payload payload payload", i as i64),
            )
            .unwrap();
        }
        best = best.max(n as f64 / t.elapsed().as_secs_f64());
    }
    let mut best4 = 0f64;
    for round in 0..3i64 {
        let t = Instant::now();
        let hs: Vec<_> = (0..4)
            .map(|k| {
                let db = db.clone();
                std::thread::spawn(move || {
                    for i in 0..20_000 {
                        db.execute(
                            "INSERT INTO t (id, v, n) VALUES (?1, ?2, ?3)",
                            (1_000_000 * (k + 1) + round * 100_000 + i as i64, "x", 1i64),
                        )
                        .unwrap();
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        best4 = best4.max(80_000.0 / t.elapsed().as_secs_f64());
    }
    println!("WRITE [{kind:?}] 1-client best {best:.0} txn/s; 4-client best {best4:.0} txn/s");
}

#[ignore]
#[test]
fn write_path_throughput() {
    measure(GkChannelKind::Treiber);
    #[cfg(feature = "crossbeam-channel")]
    measure(GkChannelKind::Crossbeam);
}
