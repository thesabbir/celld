//! After a checkpoint restarts the WAL, the next capture tells that restart
//! from a missed FULL or RESTART checkpoint by the salts of the frames
//! written since. It reads those frame headers only: the rest of the file
//! is the old WAL, however large.

use celld_ltx::{CheckpointMode, Db};

#[test]
fn a_restart_is_told_apart_without_reading_the_old_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    let conn = rusqlite::Connection::open(&path).expect("open");
    conn.pragma_update(None, "journal_mode", "wal")
        .expect("wal");
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .expect("autocheckpoint");
    conn.execute_batch("CREATE TABLE t (x TEXT)")
        .expect("create");

    let mut db = Db::open(&path).expect("db");
    db.min_checkpoint_page_n = u32::MAX;
    db.sync().expect("capture");
    for _ in 0..4000 {
        conn.execute("INSERT INTO t VALUES (hex(randomblob(512)))", [])
            .expect("insert");
    }
    db.sync().expect("capture");
    let wal_len = db.last_sync_timing().wal_len_bytes;

    db.checkpoint(CheckpointMode::Passive).expect("checkpoint");
    let timing = db.last_sync_timing();
    assert_eq!(
        timing.checkpoint_restarts, 1,
        "the WAL restarted: {timing:?}"
    );
    assert!(
        timing.restart_scan_bytes > 0,
        "the restart was scanned: {timing:?}"
    );
    assert!(
        timing.restart_scan_bytes < wal_len / 16,
        "{} bytes read of a {wal_len}-byte WAL: {timing:?}",
        timing.restart_scan_bytes,
    );
}

#[test]
fn a_restart_that_hid_a_wal_generation_still_snapshots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    let conn = rusqlite::Connection::open(&path).expect("open");
    conn.pragma_update(None, "journal_mode", "wal")
        .expect("wal");
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .expect("autocheckpoint");
    conn.execute_batch("CREATE TABLE t (x TEXT)")
        .expect("create");
    let insert = |n: usize| {
        for _ in 0..n {
            conn.execute("INSERT INTO t VALUES (hex(randomblob(512)))", [])
                .expect("insert");
        }
    };

    insert(50);
    {
        let mut db = Db::open(&path).expect("db");
        db.min_checkpoint_page_n = u32::MAX;
        db.sync().expect("capture");
        db.close().expect("close");
    }
    // Two restarts behind the replicator's back: the middle generation's
    // frames reach the database file uncaptured.
    let restart = || {
        conn.query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| r.get::<_, i64>(0))
            .expect("restart");
    };
    restart();
    insert(30);
    restart();
    insert(5);

    let mut db = Db::open(&path).expect("db");
    db.min_checkpoint_page_n = u32::MAX;
    db.sync().expect("capture");
    let timing = db.last_sync_timing();
    assert!(
        timing.snapshot,
        "a hidden generation must snapshot: {timing:?}"
    );
    assert_eq!(timing.snapshot_reason, 5, "{timing:?}");
}
