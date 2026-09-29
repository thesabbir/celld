//! A passive checkpoint whose held tail pass cannot take its barrier gives
//! back the read transaction the unlocked passes pinned. A pin left on `conn`
//! holds the WAL from restarting until some later barrier succeeds, and a
//! `conn` write in the meantime would join the stale transaction.

use std::sync::{Arc, Mutex};

use celld_ltx::db::internal;
use celld_ltx::Db;

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

#[test]
fn a_failed_tail_barrier_releases_the_pin() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("a.db");

    write(&path, 50);
    let mut db = Db::open(&path).expect("db");
    db.sync().expect("capture");
    // Uncaptured frames, so the checkpoint runs its unlocked passes and a
    // tail pass.
    write(&path, 200);

    // Right before the tail barrier, another connection takes the write lock
    // and keeps it, so the barrier's write fails busy.
    let holder: Arc<Mutex<Option<rusqlite::Connection>>> = Arc::default();
    internal::set_tail_barrier_hook(&mut db, {
        let path = path.clone();
        let holder = holder.clone();
        Box::new(move || {
            let conn = rusqlite::Connection::open(&path).expect("open");
            conn.execute_batch("BEGIN IMMEDIATE").expect("write lock");
            *holder.lock().expect("holder") = Some(conn);
        })
    });

    let result = db.checkpoint(celld_ltx::CheckpointMode::Passive);
    assert!(
        result.is_err(),
        "the tail barrier ran with the write lock taken"
    );
    assert!(
        holder.lock().expect("holder").is_some(),
        "the hook ran before the tail pass"
    );
    assert!(
        !internal::passive_pin_held(&db),
        "the failed tail pass left the unlocked passes' pin on conn"
    );
}
