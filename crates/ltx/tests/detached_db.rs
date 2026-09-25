//! A replica's database can be detached, captured on another thread, and
//! attached again: capture is synchronous, and a host keeps it off its async
//! threads.

use celld_ltx::replica::restore;
use celld_ltx::{Db, FileReplicaClient, Pos, Replica, TXID};

#[tokio::test]
async fn a_detached_database_captures_on_another_thread() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    let conn = rusqlite::Connection::open(&path).expect("open");
    conn.pragma_update(None, "journal_mode", "wal")
        .expect("wal");
    conn.execute_batch("CREATE TABLE t (x TEXT); INSERT INTO t VALUES ('a'), ('b');")
        .expect("write");

    let client = FileReplicaClient::new(remote.path().to_string_lossy().into_owned());
    let mut replica = Replica::new(Db::open(&path).expect("db"), client);
    replica.seed_pos(Pos::ZERO);
    let mut db = replica.take_db().expect("attached");
    assert!(replica.sync().await.is_err(), "a sync with no database");
    let db = std::thread::spawn(move || {
        db.sync().expect("capture");
        db
    })
    .join()
    .expect("thread");
    replica.attach_db(db);
    replica.sync().await.expect("upload");
    assert!(replica.pos().txid > TXID(0));

    let out = dir.path().join("out.db");
    restore(&replica.client, &out, TXID(0))
        .await
        .expect("restore");
    let n: i64 = rusqlite::Connection::open(&out)
        .expect("open")
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .expect("count");
    assert_eq!(n, 2);
}
