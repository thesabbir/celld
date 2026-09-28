//! A capture walks each new WAL frame's checksum about once: finding the
//! valid end of the tail and mapping its pages are one walk, not three.

use celld_ltx::Db;

#[test]
fn a_capture_walks_each_new_frame_about_once() {
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

    let mut db = Db::open(&path).expect("db");
    db.min_checkpoint_page_n = u32::MAX;
    insert(200);
    db.sync().expect("capture");
    let before = db.last_sync_timing().wal_len_bytes;

    insert(2000);
    db.sync().expect("capture");
    let timing = db.last_sync_timing();
    let frame = u64::from(db.page_size()) + 24;
    let new_frames = (timing.wal_len_bytes - before) / frame;
    assert!(
        timing.wal_frames_parsed <= new_frames + new_frames / 4 + 2,
        "{} frames walked for {new_frames} new: {timing:?}",
        timing.wal_frames_parsed,
    );
}

/// Tails of every size, including one that shrinks the database, restore to
/// the source row for row: the walk resumed at each round's last commit maps
/// the same pages one walk from the start would.
#[tokio::test]
async fn tails_walked_in_rounds_restore_exactly() {
    use celld_ltx::replica::restore;
    use celld_ltx::{FileReplicaClient, Pos, Replica, TXID};

    for checkpoint_pages in [u32::MAX, 1000] {
        let dir = tempfile::tempdir().expect("tempdir");
        let remote = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("a.db");
        let client = || FileReplicaClient::new(remote.path().to_string_lossy().into_owned());
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.pragma_update(None, "journal_mode", "wal")
            .expect("wal");
        conn.pragma_update(None, "wal_autocheckpoint", 0)
            .expect("autocheckpoint");
        conn.execute_batch("CREATE TABLE t (x TEXT)")
            .expect("create");

        let mut db = Db::open(&path).expect("db");
        db.min_checkpoint_page_n = checkpoint_pages;
        db.sync().expect("capture");
        let mut replica = Replica::new(db, client());
        replica.seed_pos(Pos::ZERO);
        replica.sync().await.expect("upload");

        for (i, burst) in [1_usize, 3, 40, 700, 5, 2500, 2, 900]
            .into_iter()
            .enumerate()
        {
            // Many commits per tail, so the walk resumes mid-tail.
            for rows in (0..burst).collect::<Vec<_>>().chunks(7) {
                conn.execute_batch("BEGIN").expect("begin");
                for _ in rows {
                    conn.execute("INSERT INTO t VALUES (hex(randomblob(700)))", [])
                        .expect("insert");
                }
                conn.execute_batch("COMMIT").expect("commit");
            }
            if i == 5 {
                conn.execute_batch("DELETE FROM t WHERE rowid % 3 != 0; VACUUM;")
                    .expect("shrink");
            }
            replica.db_mut().expect("db").sync().expect("capture");
            replica.sync().await.expect("upload");
        }

        let out = dir.path().join("out.db");
        restore(&client(), &out, TXID(0)).await.expect("restore");
        let read = |p: &std::path::Path| -> Vec<String> {
            let c = rusqlite::Connection::open(p).expect("open");
            let mut stmt = c
                .prepare("SELECT x FROM t ORDER BY rowid")
                .expect("prepare");
            stmt.query_map([], |r| r.get(0))
                .expect("query")
                .collect::<Result<_, _>>()
                .expect("rows")
        };
        assert_eq!(
            read(&out),
            read(&path),
            "checkpoint_pages {checkpoint_pages}"
        );
        let ok: String = rusqlite::Connection::open(&out)
            .expect("open")
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .expect("check");
        assert_eq!(ok, "ok");
    }
}
