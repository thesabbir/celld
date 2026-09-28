//! A sync that snapshots streams its L0: peak heap is the WAL image plus a
//! page, not a copy of the database twice over, and the L0 bytes are what the
//! buffered encoder produces.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

use celld_ltx::ltx::{decode_file, decode_file_pages, encode_file};
use celld_ltx::{CheckpointMode, Db};

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

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

const PAGE: usize = 4096;
/// 16 MiB of rows.
const ROWS: usize = 4096;
/// Slack over the WAL image: the page map, a page, buffers. ~6% of the db.
const SLACK: u64 = 1 << 20;

fn fill(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).expect("conn");
    conn.execute_batch("CREATE TABLE t (b BLOB);")
        .expect("schema");
    let blob = vec![7u8; PAGE - 200];
    for i in 0..ROWS {
        let mut row = blob.clone();
        row[..8].copy_from_slice(&(i as u64).to_be_bytes());
        conn.execute("INSERT INTO t VALUES (?1)", [row])
            .expect("insert");
    }
}

fn measure(f: impl FnOnce()) -> u64 {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    f();
    (PEAK.load(Ordering::Relaxed) - base) as u64
}

/// The snapshot L0 (the largest one: a sync may cut more after it) decodes,
/// and equals the buffered encoding of its own header and pages.
/// Files named in `before` are skipped.
fn assert_l0_is_buffered_encoding(db: &Db, before: &[std::path::PathBuf]) -> Vec<(u32, Vec<u8>)> {
    let bytes = l0_files(db)
        .into_iter()
        .filter(|p| !before.contains(p))
        .map(|p| celld_ltx::LtxHost::default().read(&p).expect("l0"))
        .max_by_key(Vec::len)
        .expect("an L0");
    let decoded = decode_file(&bytes).expect("verifies");
    let pages = decode_file_pages(&bytes).expect("pages");
    let buffered = encode_file(&decoded.header, &pages, 0).expect("encode");
    assert!(
        bytes == buffered,
        "streamed L0 differs from buffered encoding"
    );
    pages
}

fn l0_files(db: &Db) -> Vec<std::path::PathBuf> {
    let any = db.ltx_path(0, celld_ltx::TXID(1), celld_ltx::TXID(1));
    let dir = std::path::Path::new(&any).parent().expect("l0 dir");
    celld_ltx::LtxHost::default()
        .read_dir(dir)
        .expect("l0 dir")
        .into_iter()
        .map(|e| e.path)
        .collect()
}

#[test]
fn first_sync_snapshot_streams() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("big.db");
    let mut db = Db::open(&path).expect("open");
    fill(&path);
    let host = celld_ltx::LtxHost::default();
    let wal = host.metadata(&dir.path().join("big.db-wal")).unwrap().len;
    assert!(wal > (ROWS * PAGE) as u64, "wal is {wal} bytes");

    let peak = measure(|| db.sync().expect("sync"));
    eprintln!("first sync: wal {wal} B, peak heap {peak} B");
    assert!(
        peak < wal + SLACK,
        "first sync over a {wal} B WAL peaked at {peak} B"
    );

    let pages = assert_l0_is_buffered_encoding(&db, &[]);
    assert!(pages.len() > ROWS, "snapshot holds {} pages", pages.len());
}

#[test]
fn truncate_boundary_snapshot_streams() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("big.db");
    let mut db = Db::open(&path).expect("open");
    fill(&path);
    db.sync().expect("sync");
    let before = l0_files(&db);

    let peak = measure(|| db.checkpoint(CheckpointMode::Truncate).expect("checkpoint"));
    let db_bytes = celld_ltx::LtxHost::default().metadata(&path).unwrap().len;
    eprintln!("truncate boundary: database {db_bytes} B, peak heap {peak} B");
    assert!(
        peak < SLACK,
        "boundary snapshot of a {db_bytes} B database peaked at {peak} B"
    );

    // The boundary image is every database page, in order.
    let pages = assert_l0_is_buffered_encoding(&db, &before);
    let file_pages = celld_ltx::LtxHost::default()
        .metadata(&path)
        .expect("db")
        .len as usize
        / PAGE;
    let pgnos: Vec<u32> = pages.iter().map(|(p, _)| *p).collect();
    assert_eq!(pgnos, (1..=file_pages as u32).collect::<Vec<_>>());
}
