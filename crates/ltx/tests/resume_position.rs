//! A replica that does not know its position takes it from every level, not
//! from L0 alone: once compaction and retention have removed L0, the position
//! lives in L1 and the snapshots.

use celld_ltx::replica_compactor::ReplicaCompactor;
use celld_ltx::{Db, FileReplicaClient, Pos, Replica, ReplicaClient, TXID};

fn write(path: &std::path::Path, rows: usize) {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.pragma_update(None, "journal_mode", "wal")
        .expect("wal");
    conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x TEXT)")
        .expect("create");
    for _ in 0..rows {
        conn.execute("INSERT INTO t VALUES (hex(randomblob(32)))", [])
            .expect("insert");
    }
}

#[tokio::test]
async fn an_unknown_position_is_read_past_a_pruned_l0() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    let client = || FileReplicaClient::new(remote.path().to_string_lossy().into_owned());

    write(&path, 1);
    let mut db = Db::open(&path).expect("db");
    db.sync().expect("capture");
    let mut replica = Replica::new(db, client());
    replica.seed_pos(Pos::ZERO);
    replica.sync().await.expect("upload");
    for _ in 0..3 {
        write(&path, 3);
        replica.db_mut().expect("db").sync().expect("capture");
        replica.sync().await.expect("upload");
    }
    let tail = replica.pos().txid;
    let db = replica.into_db().expect("db");

    // L0 compacted into L1, then every L0 file deleted.
    ReplicaCompactor::new(&client())
        .compact(1)
        .await
        .expect("compact")
        .expect("compacted");
    let l0 = client().ltx_files(0, TXID(0)).await.expect("list");
    client().delete_ltx_files(&l0).await.expect("delete");

    // A replica that must find its position, with nothing new to upload.
    let mut resumed = Replica::new(db, client());
    resumed.sync().await.expect("sync");
    assert_eq!(resumed.pos().txid, tail);
    let l0 = client().ltx_files(0, TXID(0)).await.expect("list");
    assert!(l0.is_empty(), "re-uploaded what L1 holds: {l0:?}");
}
