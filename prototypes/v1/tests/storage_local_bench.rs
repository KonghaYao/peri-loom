//! Diagnostic timings of actual RemoteStorage calls. These are neither durable SQL requests
//! nor a service throughput acceptance gate; use an unchanged backend and release build for A/B.
use anyhow::Result;
use peri_loom_v1::cache::SegmentCache;
use peri_loom_v1::manifest::{WalPosition, SEGMENT_SIZE};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::MemoryObjectStore;
use peri_loom_v1::storage::RemoteStorage;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

#[test]
#[ignore = "manual release diagnostic; does not establish SQL or persistence performance"]
fn dirty_segment_partial_write_and_read() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = RemoteStorage::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("tenant", "database")?,
        Arc::new(SegmentCache::new(directory.path(), 0, 0)?),
    )?;
    storage.write_at(0, &vec![1; SEGMENT_SIZE])?;
    let iterations = 100_000usize;
    let mut page = [0u8; 4096];
    let start = Instant::now();
    for iteration in 0..iterations {
        page[0] = iteration as u8;
        let offset = (iteration % (SEGMENT_SIZE / page.len()) * page.len()) as u64;
        black_box(storage.write_at(offset, black_box(&page))?);
    }
    let write_ns = start.elapsed().as_nanos() as f64 / iterations as f64;
    let start = Instant::now();
    for iteration in 0..iterations {
        let offset = (iteration % (SEGMENT_SIZE / page.len()) * page.len()) as u64;
        black_box(storage.read_at(offset, black_box(&mut page))?);
        black_box(&page);
    }
    let read_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

    // Completion still includes real immutable body/manifest publication and a final HEAD CAS;
    // it is outside these timings because the diagnostic isolates the dirty-buffer path.
    storage.publish(WalPosition::default(), 1)?;
    assert_eq!(storage.applied_lsn(), 1);
    assert_eq!(storage.stats().dirty_segments, 0);
    println!(
        "STORAGE_DIAGNOSTIC iterations={iterations} page_bytes={} segment_bytes={SEGMENT_SIZE} write_ns_per_op={write_ns:.1} read_ns_per_op={read_ns:.1}",
        page.len()
    );
    Ok(())
}
