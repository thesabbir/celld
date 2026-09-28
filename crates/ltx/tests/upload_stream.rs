//! An L0 upload streams from the local file: peak heap is bounded by the
//! client's chunk, not the L0, even for a database-sized boundary image.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use celld_ltx::{Db, FileReplicaClient, Pos, Replica};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

// SAFETY: forwards to `System`, only counting sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now =
                LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed) + layout.size() as isize;
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const PAGE: usize = 4096;
/// 16 MiB of incompressible rows, so the L0 is database-sized.
const ROWS: usize = 4096;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn l0_upload_peak_heap_is_bounded_by_the_chunk() {
    let dir = tempfile::tempdir().expect("dir");
    let remote = tempfile::tempdir().expect("remote");
    let path = dir.path().join("big.db");
    let mut db = Db::open(&path).expect("open");
    {
        let conn = rusqlite::Connection::open(&path).expect("conn");
        conn.execute_batch("CREATE TABLE t (b BLOB);")
            .expect("schema");
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut row = vec![0u8; PAGE - 200];
        for _ in 0..ROWS {
            for b in row.chunks_mut(8) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                b.copy_from_slice(&x.to_le_bytes()[..b.len()]);
            }
            conn.execute("INSERT INTO t VALUES (?1)", [&row])
                .expect("insert");
        }
    }
    db.sync().expect("capture");
    let pos = db.pos().expect("pos");

    let client = FileReplicaClient::new(remote.path().to_string_lossy().into_owned());
    let mut replica = Replica::new(db, client);
    replica.seed_pos(Pos::ZERO);

    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    replica.sync().await.expect("upload");
    let peak = (PEAK.load(Ordering::Relaxed) - base) as u64;
    assert_eq!(replica.pos().txid, pos.txid);

    let host = celld_ltx::LtxHost::default();
    let largest = host
        .read_dir(&remote.path().join("ltx/0"))
        .expect("remote l0")
        .iter()
        .map(|f| host.metadata(&f.path).expect("remote").len)
        .max()
        .expect("an L0");
    eprintln!("upload: largest L0 {largest} B, peak heap {peak} B");
    assert!(
        largest > (ROWS * PAGE * 3 / 4) as u64,
        "largest L0 is {largest} B"
    );
    // The file client moves 1 MiB chunks; one chunk plus slack is allowed,
    // a copy of the L0 is not.
    assert!(peak < 4 << 20, "uploading L0s peaked at {peak} B of heap");
}
