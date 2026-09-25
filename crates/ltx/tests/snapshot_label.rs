//! A snapshot holds exactly the state at the txid it is labelled with: a
//! commit after the last capture is not in it.

use celld_ltx::ltx::{decode_database_image, decode_file};
use celld_ltx::Db;

fn conn(path: &std::path::Path) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.pragma_update(None, "journal_mode", "wal")
        .expect("wal");
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .expect("no checkpoint");
    conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x TEXT)")
        .expect("create");
    conn
}

fn insert(conn: &rusqlite::Connection, rows: usize) {
    for _ in 0..rows {
        conn.execute("INSERT INTO t VALUES (hex(randomblob(32)))", [])
            .expect("insert");
    }
}

fn count(path: &std::path::Path) -> i64 {
    rusqlite::Connection::open(path)
        .expect("open")
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("count")
}

#[test]
#[allow(clippy::disallowed_methods, reason = "a test's scratch file")]
fn a_commit_after_the_capture_is_not_in_the_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    let writer = conn(&path);
    insert(&writer, 5);
    let mut db = Db::open(&path).expect("db");
    db.sync().expect("capture");
    insert(&writer, 3);
    db.sync().expect("capture");
    let captured = db.pos().expect("pos");

    // Committed after the capture: belongs to the next txid.
    insert(&writer, 2);

    let mut buf = Vec::new();
    let pos = db.snapshot_to_writer(&mut buf).expect("snapshot");
    assert_eq!(pos.txid, captured.txid);
    assert_eq!(
        decode_file(&buf).expect("decode").header.max_txid,
        captured.txid
    );
    let out = dir.path().join("snap.db");
    std::fs::write(&out, decode_database_image(&buf).expect("image")).expect("write");
    assert_eq!(count(&out), 8);
}
