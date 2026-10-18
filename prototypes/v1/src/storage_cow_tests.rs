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
    reject_group: AtomicBool,
    reject_head_put: AtomicBool,
}

impl ObjectStore for CountedStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.contains("/segments/") {
            self.segment_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        ensure!(
            !key.ends_with("/HEAD") || !self.reject_head_put.load(Ordering::SeqCst),
            "injected HEAD failure"
        );
        let etag = self.inner.put(key, bytes, condition)?;
        if key.contains("/segments/") {
            self.segment_puts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(etag)
    }

    fn put_group(&self, writes: &[crate::object_store::GroupWrite]) -> Result<Vec<String>> {
        ensure!(
            !self.reject_group.load(Ordering::SeqCst),
            "injected segment group failure"
        );
        writes
            .iter()
            .map(|write| self.put(&write.key, &write.bytes, write.condition.clone()))
            .collect()
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
fn repeated_segment_bodies_are_grouped_once_per_publish_and_rechecked_next_time() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (storage, store) = counted_storage(directory.path())?;
    let bytes = vec![7; SEGMENT_SIZE * 8];
    storage.write_at(0, &bytes)?;
    storage.publish(WalPosition::default(), 1)?;
    // Eight dirty segments share one body, and a fresh body is published as one
    // conditional group write: the store's `Absent` precondition replaces the read probe.
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 1);
    assert_eq!(storage.manifest().segments.len(), 8);

    // The second publication targets an existing key. The group is rejected before it
    // writes, and the body is then rechecked byte for byte instead of being rewritten.
    storage.write_at(0, &bytes)?;
    storage.publish(WalPosition::default(), 2)?;
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 1);
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
fn failed_publish_preserves_the_dirty_image_and_rechecks_published_bodies() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (storage, store) = counted_storage(directory.path())?;
    storage.write_at(0, &vec![6; SEGMENT_SIZE * 3])?;

    // A body group that fails before publishing must leave the image untouched.
    store.reject_group.store(true, Ordering::SeqCst);
    assert!(storage.publish(WalPosition::default(), 1).is_err());
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 0);
    assert_eq!(storage.stats().dirty_segments, 3);

    store.reject_group.store(false, Ordering::SeqCst);
    store.reject_head_put.store(true, Ordering::SeqCst);
    assert!(storage.publish(WalPosition::default(), 1).is_err());
    // The body landed, but the HEAD fence did not move: the image stays dirty and the
    // next publication rechecks the published body instead of uploading it again.
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 1);
    assert_eq!(storage.applied_lsn(), 0);
    assert_eq!(storage.stats().dirty_segments, 3);

    store.reject_head_put.store(false, Ordering::SeqCst);
    storage.publish(WalPosition::default(), 1)?;
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 1);
    assert_eq!(store.segment_puts.load(Ordering::SeqCst), 1);
    assert_eq!(storage.applied_lsn(), 1);
    assert_eq!(storage.stats().dirty_segments, 0);
    Ok(())
}

#[derive(Default)]
struct UploadGate {
    armed: bool,
    blocked: bool,
    released: bool,
}

/// Blocks the first immutable segment PUT until the test releases it, so a publication can
/// be observed while it is inside the remote write.
#[derive(Default)]
struct GatedStore {
    inner: MemoryObjectStore,
    gate: std::sync::Mutex<UploadGate>,
    changed: std::sync::Condvar,
}

impl GatedStore {
    fn arm(&self) {
        self.gate.lock().expect("upload gate").armed = true;
    }

    fn wait_until_blocked(&self, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut gate = self.gate.lock().expect("upload gate");
        while !gate.blocked {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            gate = self
                .changed
                .wait_timeout(gate, remaining)
                .expect("upload gate")
                .0;
        }
        true
    }

    fn release(&self) {
        self.gate.lock().expect("upload gate").released = true;
        self.changed.notify_all();
    }
}

impl ObjectStore for GatedStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.contains("/segments/") {
            let mut gate = self.gate.lock().expect("upload gate");
            if gate.armed {
                gate.armed = false;
                gate.blocked = true;
                self.changed.notify_all();
                while !gate.released {
                    gate = self.changed.wait(gate).expect("upload gate");
                }
            }
        }
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn gated_storage(
    directory: &std::path::Path,
    store: Arc<GatedStore>,
) -> Result<Arc<RemoteStorage>> {
    RemoteStorage::open(
        store,
        Namespace::new("tenant", "database")?,
        Arc::new(SegmentCache::new(directory, 0, 0)?),
    )
}

#[test]
fn publication_uploads_do_not_hold_the_storage_lock() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(GatedStore::default());
    let storage = gated_storage(directory.path(), store.clone())?;
    storage.write_at(0, &vec![5; SEGMENT_SIZE])?;
    store.arm();
    let publisher = {
        let storage = storage.clone();
        std::thread::spawn(move || storage.publish(WalPosition::default(), 1))
    };
    assert!(
        store.wait_until_blocked(std::time::Duration::from_secs(5)),
        "publication never reached the segment upload"
    );

    // The publisher is inside its first segment PUT. A reader takes the storage lock only
    // for the identity of the image, so it must not wait for the upload to finish.
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = {
        let storage = storage.clone();
        std::thread::spawn(move || {
            let mut out = [0u8; 8];
            let result = storage.read_at(0, &mut out);
            let _ = sender.send((result.is_ok(), out));
        })
    };
    let observed = receiver.recv_timeout(std::time::Duration::from_secs(2));
    store.release();
    publisher.join().expect("publisher")?;
    reader.join().expect("reader");
    let (read_ok, out) = observed.expect("a reader must not wait for a publication upload");
    assert!(read_ok);
    assert_eq!(&out, &[5u8; 8]);
    assert_eq!(storage.stats().dirty_segments, 0);
    Ok(())
}

#[test]
fn publish_keeps_a_segment_rewritten_during_the_upload_dirty() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(GatedStore::default());
    let storage = gated_storage(directory.path(), store.clone())?;
    storage.write_at(0, &vec![7; SEGMENT_SIZE])?;
    store.arm();
    let publisher = {
        let storage = storage.clone();
        std::thread::spawn(move || storage.publish(WalPosition::default(), 1))
    };
    assert!(
        store.wait_until_blocked(std::time::Duration::from_secs(5)),
        "publication never reached the segment upload"
    );
    storage.write_at(0, &vec![8; SEGMENT_SIZE])?;
    store.release();
    publisher.join().expect("publisher")?;

    // The publication settled the bytes it uploaded; the rewrite arrived afterwards and
    // must stay staged instead of being cleared with the snapshot.
    assert_eq!(storage.applied_lsn(), 1);
    assert_eq!(storage.stats().dirty_segments, 1);
    let mut actual = vec![0u8; SEGMENT_SIZE];
    assert_eq!(storage.read_at(0, &mut actual)?, SEGMENT_SIZE);
    assert!(actual.iter().all(|byte| *byte == 8));

    storage.publish(WalPosition::default(), 2)?;
    assert_eq!(storage.stats().dirty_segments, 0);
    storage.read_at(0, &mut actual)?;
    assert!(actual.iter().all(|byte| *byte == 8));
    Ok(())
}
