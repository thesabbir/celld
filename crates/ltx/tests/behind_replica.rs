//! A database restored from its replica, then written to, carries the replica
//! forward: its first capture after the check is a whole-database snapshot
//! numbered after the replica's position, whichever level that position lives
//! in.

use celld_ltx::replica::restore;
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

fn rows(path: &std::path::Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).expect("open");
    let mut stmt = conn
        .prepare("SELECT x FROM t ORDER BY rowid")
        .expect("prepare");
    stmt.query_map([], |r| r.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

async fn restored_then_written(prune_l0: bool) {
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
    drop(replica);

    if prune_l0 {
        ReplicaCompactor::new(&client())
            .compact(1)
            .await
            .expect("compact")
            .expect("compacted");
        let l0 = client().ltx_files(0, TXID(0)).await.expect("list");
        client().delete_ltx_files(&l0).await.expect("delete");
    }

    // Restore to a new file, reopen it for replication, write to it.
    let restored = dir.path().join("restored.db");
    restore(&client(), &restored, TXID(0))
        .await
        .expect("restore");
    let mut replica = Replica::new(Db::open(&restored).expect("db"), client());
    replica
        .check_database_behind_replica()
        .await
        .expect("check");
    write(&restored, 2);
    replica.db_mut().expect("db").sync().expect("capture");
    replica.sync().await.expect("upload");
    assert!(replica.pos().txid > tail, "nothing uploaded past {tail:?}");

    let out = dir.path().join("out.db");
    restore(&client(), &out, TXID(0)).await.expect("restore");
    assert_eq!(rows(&out), rows(&restored));
    assert_eq!(rows(&out).len(), 12);
}

#[tokio::test]
async fn a_restored_database_carries_the_replica_forward() {
    restored_then_written(false).await;
}

#[tokio::test]
async fn a_restored_database_carries_the_replica_forward_past_a_pruned_l0() {
    restored_then_written(true).await;
}
