use super::*;
use crate::object_store::MemoryObjectStore;
use crate::object_store::StoredObject;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn storage(directory: &std::path::Path) -> Result<Arc<RemoteStorage>> {
    RemoteStorage::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("tenant", "database")?,
        Arc::new(SegmentCache::new(directory, SEGMENT_SIZE, 0)?),
    )
}

#[test]
fn dirty_read_snapshot_survives_overlapping_partial_and_full_writes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = storage(directory.path())?;
    storage.write_at(0, &vec![1; SEGMENT_SIZE])?;

    // This is the snapshot read_at copies after releasing the storage lock.
    let before_partial = storage.read_segment(0)?;
    storage.write_at(17, b"changed")?;
    assert_eq!(before_partial.as_slice(), vec![1; SEGMENT_SIZE]);
    let before_full = storage.read_segment(0)?;
    assert_eq!(&before_full[17..24], b"changed");

    storage.write_at(0, &vec![2; SEGMENT_SIZE])?;
    assert_eq!(&before_full[17..24], b"changed");
    assert_eq!(storage.read_segment(0)?.as_slice(), vec![2; SEGMENT_SIZE]);
    Ok(())
}

#[test]
fn dirty_read_snapshot_survives_truncate_and_extend() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = storage(directory.path())?;
    storage.write_at(0, &vec![3; SEGMENT_SIZE])?;
    let snapshot = storage.read_segment(0)?;

    storage.truncate(19)?;
    storage.truncate(SEGMENT_SIZE as u64)?;
    let mut actual = vec![9; SEGMENT_SIZE];
    assert_eq!(storage.read_at(0, &mut actual)?, SEGMENT_SIZE);
    assert_eq!(&actual[..19], &[3; 19]);
    assert!(actual[19..].iter().all(|byte| *byte == 0));
    assert_eq!(snapshot.as_slice(), vec![3; SEGMENT_SIZE]);
    Ok(())
}

#[test]
fn cached_published_bytes_remain_immutable_across_partial_writes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let storage = storage(directory.path())?;
    storage.write_at(0, &vec![4; SEGMENT_SIZE])?;
    storage.publish(WalPosition::default(), 1)?;
    let published = storage.read_segment(0)?;
    let hash = storage.manifest().segments[&0].clone();

    storage.write_at(31, b"private")?;
    storage.write_at(47, b"change")?;
    assert_eq!(published.as_slice(), vec![4; SEGMENT_SIZE]);
    assert_eq!(
        storage.cache.get(&hash)?.unwrap().as_slice(),
        vec![4; SEGMENT_SIZE]
    );
    let mut actual = [0; 7];
    assert_eq!(storage.read_at(31, &mut actual)?, actual.len());
    assert_eq!(&actual, b"private");
    Ok(())
}

#[derive(Default)]
struct CountedStore {
    inner: MemoryObjectStore,
    segment_gets: AtomicUsize,
    segment_puts: AtomicUsize,
    reject_segment_get: AtomicBool,
    reject_head_put: AtomicBool,
}

impl ObjectStore for CountedStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.contains("/segments/") {
            self.segment_gets.fetch_add(1, Ordering::SeqCst);
            ensure!(
                !self.reject_segment_get.load(Ordering::SeqCst),
                "injected segment GET failure"
            );
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.contains("/segments/") {
            self.segment_puts.fetch_add(1, Ordering::SeqCst);
        }
        ensure!(
            !key.ends_with("/HEAD") || !self.reject_head_put.load(Ordering::SeqCst),
            "injected HEAD failure"
        );
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn counted_storage(directory: &std::path::Path) -> Result<(Arc<RemoteStorage>, Arc<CountedStore>)> {
    let store = Arc::new(CountedStore::default());
    let storage = RemoteStorage::open(
        store.clone(),
        Namespace::new("tenant", "database")?,
        Arc::new(SegmentCache::new(directory, 0, 0)?),
    )?;
    Ok((storage, store))
}

#[test]
fn repeated_segment_bodies_are_checked_once_per_publish_and_rechecked_next_time() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (storage, store) = counted_storage(directory.path())?;
    let bytes = vec![7; SEGMENT_SIZE * 8];
    storage.write_at(0, &bytes)?;
    storage.publish(WalPosition::default(), 1)?;
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 1);
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 1);
    assert_eq!(storage.manifest().segments.len(), 8);

    storage.write_at(0, &bytes)?;
    storage.publish(WalPosition::default(), 2)?;
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 2);
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 1);

    // The first body must still be checked against storage on every publication.
    let hash = storage.manifest().segments[&0].clone();
    store.inner.put(
        &storage.namespace().key(&format!("segments/{hash}"))?,
        &vec![8; SEGMENT_SIZE],
        PutCondition::Any,
    )?;
    storage.write_at(0, &bytes)?;
    let error = storage.publish(WalPosition::default(), 3).unwrap_err();
    assert!(error
        .to_string()
        .contains("immutable object integrity failure"));
    assert_eq!(storage.applied_lsn(), 2);
    assert_eq!(storage.stats().dirty_segments, 8);
    Ok(())
}

#[test]
fn failed_publish_discards_duplicate_checks_and_preserves_dirty_image() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (storage, store) = counted_storage(directory.path())?;
    storage.write_at(0, &vec![6; SEGMENT_SIZE * 3])?;

    store.reject_segment_get.store(true, Ordering::SeqCst);
    assert!(storage.publish(WalPosition::default(), 1).is_err());
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 0);
    assert_eq!(storage.stats().dirty_segments, 3);

    store.reject_segment_get.store(false, Ordering::SeqCst);
    store.reject_head_put.store(true, Ordering::SeqCst);
    assert!(storage.publish(WalPosition::default(), 1).is_err());
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 2);
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 1);
    assert_eq!(storage.applied_lsn(), 0);
    assert_eq!(storage.stats().dirty_segments, 3);

    store.reject_head_put.store(false, Ordering::SeqCst);
    storage.publish(WalPosition::default(), 1)?;
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 3);
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 1);
    assert_eq!(storage.applied_lsn(), 1);
    assert_eq!(storage.stats().dirty_segments, 0);
    Ok(())
}
