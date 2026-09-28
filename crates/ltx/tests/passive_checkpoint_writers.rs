//! A passive checkpoint holds the writer barrier only to seal the capture,
//! not through the backfill: a writer commits while the checkpoint copies
//! the WAL into the database. The backfill stops at the sealed frames, so a
//! commit the capture has not seen is never checkpointed away, and a restore
//! holds every row.

use std::sync::{Arc, Mutex};

use celld_ltx::db::internal;
use celld_ltx::replica::restore;
use celld_ltx::{Db, FileReplicaClient, Pos, Replica, TXID};

fn write(path: &std::path::Path, rows: usize) {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.pragma_update(None, "journal_mode", "wal")
        .expect("wal");
    conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x TEXT)")
        .expect("create");
    for _ in 0..rows {
        conn.execute("INSERT INTO t VALUES (hex(randomblob(512)))", [])
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

#[tokio::test]
async fn a_writer_commits_while_a_passive_checkpoint_backfills() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    let client = || FileReplicaClient::new(remote.path().to_string_lossy().into_owned());

    write(&path, 50);
    let mut db = Db::open(&path).expect("db");
    db.sync().expect("capture");
    let mut replica = Replica::new(db, client());
    replica.seed_pos(Pos::ZERO);
    replica.sync().await.expect("upload");

    // Uncaptured frames for the checkpoint to seal and backfill.
    write(&path, 200);

    // Right before the checkpoint copies, a writer that will not wait.
    let committed = Arc::new(Mutex::new(None));
    let hook = {
        let path = path.clone();
        let committed = committed.clone();
        Box::new(move || {
            let conn = rusqlite::Connection::open(&path).expect("open");
            conn.busy_timeout(std::time::Duration::ZERO).expect("busy");
            let result = conn.execute_batch(
                "BEGIN IMMEDIATE; \
                 INSERT INTO t VALUES ('during-the-checkpoint'); \
                 COMMIT;",
            );
            *committed.lock().expect("lock") = Some(result.map_err(|e| e.to_string()));
        })
    };
    internal::checkpoint_passive_with_barrier_hook(replica.db_mut().expect("db"), hook)
        .expect("checkpoint");
    let committed = committed
        .lock()
        .expect("lock")
        .take()
        .expect("the hook ran");
    assert_eq!(
        committed,
        Ok(()),
        "a writer must commit while the checkpoint backfills"
    );

    // It backfilled the sealed frames only: the writer's commit came after
    // the seal, so it stays in the WAL for the next capture.
    let timing = replica.db().expect("db").last_sync_timing();
    assert!(
        timing.checkpoint_backfilled < timing.checkpoint_wal_frames,
        "the checkpoint backfilled past the seal: {timing:?}"
    );
    // Later passes, the last with writers held off, copied the tail they
    // appended, so the WAL restarts instead of growing under a writer that
    // never stops.
    assert!(
        timing.checkpoint_catch_up_backfilled + timing.checkpoint_tail_backfilled > 0,
        "no tail pass: {timing:?}"
    );
    assert_eq!(
        timing.checkpoint_restarts, 1,
        "the WAL did not restart: {timing:?}"
    );

    // The commit the checkpoint did not seal still reaches the replica.
    write(&path, 20);
    replica.db_mut().expect("db").sync().expect("capture");
    replica.sync().await.expect("upload");
    let out = dir.path().join("out.db");
    restore(&client(), &out, TXID(0)).await.expect("restore");
    let restored = rows(&out);
    assert_eq!(restored, rows(&path));
    assert_eq!(restored.len(), 271);
    assert!(restored.iter().any(|r| r == "during-the-checkpoint"));
}

/// A write turn the test can see: taking it counts, and `held` says whether
/// one is out now.
#[derive(Default)]
struct Turns {
    taken: std::sync::atomic::AtomicUsize,
    held: std::sync::atomic::AtomicBool,
}

struct Held(Arc<Turns>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0
            .held
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_passive_checkpoint_takes_the_write_turn_to_seal_and_not_to_backfill() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    write(&path, 50);
    let mut db = Db::open(&path).expect("db");
    db.sync().expect("capture");

    let turns = Arc::new(Turns::default());
    db.set_write_turn(Some(Arc::new({
        let turns = turns.clone();
        move || -> Box<dyn std::any::Any> {
            use std::sync::atomic::Ordering::SeqCst;
            turns.taken.fetch_add(1, SeqCst);
            assert!(!turns.held.swap(true, SeqCst), "turns do not nest");
            Box::new(Held(turns.clone()))
        }
    })));
    write(&path, 200);

    let during = Arc::new(Mutex::new(None));
    let hook = {
        let turns = turns.clone();
        let during = during.clone();
        Box::new(move || {
            use std::sync::atomic::Ordering::SeqCst;
            *during.lock().expect("lock") =
                Some((turns.taken.load(SeqCst), turns.held.load(SeqCst)));
        })
    };
    internal::checkpoint_passive_with_barrier_hook(&mut db, hook).expect("checkpoint");

    let (sealed_under, held) = during.lock().expect("lock").take().expect("the hook ran");
    assert!(sealed_under > 0, "the sealing barrier takes the write turn");
    assert!(!held, "the backfill runs with the turn free");
    assert!(!turns.held.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn a_large_tail_is_backfilled_before_the_writer_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    let client = || FileReplicaClient::new(remote.path().to_string_lossy().into_owned());

    write(&path, 50);
    let mut db = Db::open(&path).expect("db");
    db.sync().expect("capture");
    let mut replica = Replica::new(db, client());
    replica.seed_pos(Pos::ZERO);
    replica.sync().await.expect("upload");
    write(&path, 200);

    // While the bulk pass backfills, writers append far more than a tail.
    let hook = {
        let path = path.clone();
        Box::new(move || write(&path, 400))
    };
    internal::checkpoint_passive_with_barrier_hook(replica.db_mut().expect("db"), hook)
        .expect("checkpoint");

    // The held pass copies only what the unlocked passes left, not the 400
    // commits' frames: holding the writer through those was the stall.
    let timing = replica.db().expect("db").last_sync_timing();
    assert!(
        timing.checkpoint_tail_backfilled < 100,
        "the writer was held through the whole tail: {timing:?}"
    );
    assert_eq!(
        timing.checkpoint_restarts, 1,
        "the WAL did not restart: {timing:?}"
    );

    write(&path, 20);
    replica.db_mut().expect("db").sync().expect("capture");
    replica.sync().await.expect("upload");
    let out = dir.path().join("out.db");
    restore(&client(), &out, TXID(0)).await.expect("restore");
    let restored = rows(&out);
    assert_eq!(restored, rows(&path));
    assert_eq!(restored.len(), 670);
}

/// Every file in `dir` with its size.
fn files(dir: &str) -> std::collections::BTreeMap<std::path::PathBuf, u64> {
    use celld_ltx::FileSystem as _;
    let fs = celld_ltx::DirectFileSystem;
    fs.read_dir(std::path::Path::new(dir))
        .expect("read dir")
        .into_iter()
        .map(|e| {
            let len = fs.metadata(&e.path).expect("metadata").len;
            (e.path, len)
        })
        .collect()
}

#[tokio::test]
async fn a_restart_after_our_own_full_backfill_is_not_a_boundary_image() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");
    write(&path, 50);
    let mut db = Db::open(&path).expect("db");
    db.sync().expect("capture");
    // Enough database that a boundary image is unmistakable, captured
    // first so the checkpoint's own capture is a few rows.
    write(&path, 2000);
    db.sync().expect("capture");
    write(&path, 5);

    // A writer queued behind every write turn after the first: the one a
    // barrier after the bulk pass would wait on. With no writer during the
    // bulk pass it backfills every frame, so this commit restarts the WAL.
    let before = files(&internal::ltx_level_dir(&db, 0));
    let taken = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    db.set_write_turn(Some(Arc::new({
        let path = path.clone();
        let taken = taken.clone();
        move || -> Box<dyn std::any::Any> {
            if taken.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                let conn = rusqlite::Connection::open(&path).expect("open");
                conn.execute("INSERT INTO t VALUES ('behind-the-turn')", [])
                    .expect("commit");
            }
            Box::new(())
        }
    })));
    internal::checkpoint_passive_with_barrier_hook(&mut db, Box::new(|| {})).expect("checkpoint");
    db.sync().expect("capture");

    let db_bytes = {
        use celld_ltx::FileSystem as _;
        celld_ltx::DirectFileSystem
            .metadata(&path)
            .expect("metadata")
            .len
    };
    let largest = files(&internal::ltx_level_dir(&db, 0))
        .into_iter()
        .filter(|(path, _)| !before.contains_key(path))
        .map(|(_, len)| len)
        .max()
        .unwrap_or(0);
    assert!(
        largest < db_bytes / 4,
        "a {largest}-byte L0 for a {db_bytes}-byte database: {:?}",
        db.last_sync_timing()
    );
}
