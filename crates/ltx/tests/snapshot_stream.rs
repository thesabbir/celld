//! A snapshot streams: its peak heap is bounded by a page, not the database,
//! and the streamed bytes are exactly what the buffered encoder produces.

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

/// The heap counter is process-wide: tests take this so none runs beside the
/// measurement.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

const PAGE: usize = 4096;
/// 16 MiB of rows: 4096+ pages.
const ROWS: usize = 4096;

/// A writer that keeps only a length, so it holds no heap of its own.
struct Sink(u64);
impl std::io::Write for Sink {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0 += b.len() as u64;
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn database(dir: &std::path::Path) -> Db {
    let path = dir.join("big.db");
    let mut db = Db::open(&path).expect("open");
    {
        let conn = rusqlite::Connection::open(&path).expect("conn");
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
    db.sync().expect("sync");
    db.checkpoint(CheckpointMode::Truncate).expect("checkpoint");
    db.sync().expect("sync");
    db
}

#[test]
fn snapshot_peak_heap_is_bounded_by_pages_not_database() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().expect("dir");
    let mut db = database(dir.path());
    let db_bytes = celld_ltx::LtxHost::default()
        .metadata(&dir.path().join("big.db"))
        .unwrap()
        .len;
    assert!(
        db_bytes > (ROWS * PAGE) as u64,
        "fixture is {db_bytes} bytes"
    );

    // Measured: bytes streamed and peak heap over the call.
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let mut sink = Sink(0);
    db.snapshot_to_writer(&mut sink).expect("snapshot");
    let peak = (PEAK.load(Ordering::Relaxed) - base) as u64;
    eprintln!(
        "database {db_bytes} B, snapshot {} B, peak heap {peak} B",
        sink.0
    );

    // The page index (a few words per page) and a WAL page map are allowed;
    // a copy of the pages is not. 1 MiB is ~6% of the database.
    assert!(
        peak < 1 << 20,
        "snapshot of a {db_bytes} B database peaked at {peak} B of heap"
    );

    // Byte identity: the streamed file equals the buffered encoding of the
    // same header, pages and post-apply checksum, and it verifies.
    let mut bytes = Vec::new();
    db.snapshot_to_writer(&mut bytes).expect("snapshot");
    let decoded = decode_file(&bytes).expect("verifies");
    let pages = decode_file_pages(&bytes).expect("pages");
    let buffered =
        encode_file(&decoded.header, &pages, decoded.trailer.post_apply_checksum).expect("encode");
    assert!(
        bytes == buffered,
        "streamed snapshot differs from buffered encoding"
    );
}

/// The documented upload path: stream into a host scratch file, then upload
/// the file; the replica holds the same verified snapshot.
#[test]
fn snapshot_uploads_from_a_scratch_file() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    use celld_ltx::client::{file::FileReplicaClient, ReplicaClient};
    use std::io::Write;

    let dir = tempfile::tempdir().expect("dir");
    let mut db = database(dir.path());
    let host = celld_ltx::LtxHost::default();
    let mut file = host.filesystem().temporary_file(None).expect("scratch");
    let pos = {
        let mut out = std::io::BufWriter::new(&mut file);
        let pos = db.snapshot_to_writer(&mut out).expect("snapshot");
        out.flush().expect("flush");
        pos
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime.block_on(async move {
        let replica = dir.path().join("replica");
        let client = FileReplicaClient::new(replica.to_str().unwrap());
        let info = client
            .write_ltx_file_from_file(9, celld_ltx::TXID(1), pos.txid, file, host)
            .await
            .expect("upload");
        let bytes = client
            .open_ltx_file(9, celld_ltx::TXID(1), pos.txid)
            .await
            .expect("read back");
        assert_eq!(bytes.len() as i64, info.size);
        let decoded = decode_file(&bytes).expect("verifies");
        assert_eq!(decoded.trailer.post_apply_checksum, pos.post_apply_checksum);
    });
}
