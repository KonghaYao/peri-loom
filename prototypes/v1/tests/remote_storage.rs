use anyhow::{bail, Result};
use peri_loom_v1::cache::SegmentCache;
use peri_loom_v1::manifest::{digest, Manifest, WalPosition, SEGMENT_SIZE};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::storage::RemoteStorage;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct InstrumentedStore {
    inner: MemoryObjectStore,
    segment_gets: AtomicU64,
    fail_get: Mutex<Option<String>>,
    fail_put: Mutex<Option<String>>,
    fail_head: AtomicBool,
}

impl ObjectStore for InstrumentedStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.contains("/segments/") {
            self.segment_gets.fetch_add(1, Ordering::Relaxed);
        }
        if self.fail_get.lock().unwrap().as_deref() == Some(key) {
            bail!("injected GET failure");
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if self.fail_put.lock().unwrap().as_deref() == Some(key)
            || (key.ends_with("/HEAD") && self.fail_head.load(Ordering::Relaxed))
        {
            bail!("injected PUT failure");
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

fn namespace() -> Namespace {
    Namespace::new("tenant", "db").unwrap()
}

fn cache(root: &std::path::Path) -> Result<Arc<SegmentCache>> {
    Ok(Arc::new(SegmentCache::new(
        root,
        SEGMENT_SIZE,
        SEGMENT_SIZE as u64 * 2,
    )?))
}

fn read(storage: &RemoteStorage, offset: u64, length: usize) -> Result<Vec<u8>> {
    let mut bytes = vec![0; length];
    let count = storage.read_at(offset, &mut bytes)?;
    bytes.truncate(count);
    Ok(bytes)
}

#[test]
fn cold_cache_delete_reopen_is_lazy_and_durable() -> Result<()> {
    let root = tempfile::tempdir()?;
    let local = root.path().join("cache");
    let store = Arc::new(InstrumentedStore::default());
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(&local)?)?;
    let data = vec![7; SEGMENT_SIZE * 3 + 19];
    storage.write_at(13, &data)?;
    storage.publish(
        WalPosition {
            salts: [3, 4],
            frame_count: 8,
        },
        9,
    )?;
    let identifier = storage.manifest_id();
    assert_eq!(read(&storage, 13, data.len())?, data);
    drop(storage);
    std::fs::remove_dir_all(&local)?;
    let before = store.segment_gets.load(Ordering::Relaxed);
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(&local)?)?;
    assert_eq!(store.segment_gets.load(Ordering::Relaxed), before);
    assert_eq!(storage.manifest_id(), identifier);
    assert_eq!(storage.manifest().applied_lsn, 9);
    assert_eq!(read(&storage, 13, 11)?, vec![7; 11]);
    assert_eq!(store.segment_gets.load(Ordering::Relaxed), before + 1);
    assert_eq!(read(&storage, 13, data.len())?, data);
    for entry in std::fs::read_dir(&local)? {
        assert!(entry?.file_name().to_string_lossy().ends_with(".seg"));
    }
    Ok(())
}

#[test]
fn cas_conflict_keeps_winner_and_loser_dirty_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let first = RemoteStorage::open(store.clone(), namespace(), cache(&root.path().join("a"))?)?;
    let second = RemoteStorage::open(store.clone(), namespace(), cache(&root.path().join("b"))?)?;
    let base = second.manifest_id();
    first.write_at(0, b"winner")?;
    first.publish(WalPosition::default(), 1)?;
    second.write_at(0, b"loser")?;
    assert!(second.publish(WalPosition::default(), 2).is_err());
    assert_eq!(second.manifest_id(), base);
    assert_eq!(second.stats().dirty_segments, 1);
    assert_eq!(read(&second, 0, 5)?, b"loser");
    let reopened = RemoteStorage::open(store, namespace(), cache(&root.path().join("c"))?)?;
    assert_eq!(read(&reopened, 0, 6)?, b"winner");
    assert_eq!(reopened.manifest().applied_lsn, 1);
    Ok(())
}

#[test]
fn failed_upload_and_head_leave_old_head_and_retry_is_safe() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(root.path())?)?;
    storage.write_at(0, b"old")?;
    storage.publish(WalPosition::default(), 1)?;
    let head = store.get(&namespace().key("HEAD")?)?.unwrap();
    storage.write_at(0, b"new")?;
    let mut bytes = vec![0; SEGMENT_SIZE];
    bytes[..3].copy_from_slice(b"new");
    *store.fail_put.lock().unwrap() =
        Some(namespace().key(&format!("segments/{}", digest(&bytes)))?);
    assert!(storage.publish(WalPosition::default(), 2).is_err());
    assert_eq!(
        store.get(&namespace().key("HEAD")?)?.unwrap().bytes,
        head.bytes
    );
    *store.fail_put.lock().unwrap() = None;
    store.fail_head.store(true, Ordering::Relaxed);
    assert!(storage.publish(WalPosition::default(), 2).is_err());
    assert_eq!(storage.manifest().applied_lsn, 1);
    assert_eq!(storage.stats().dirty_segments, 1);
    assert_eq!(
        store.get(&namespace().key("HEAD")?)?.unwrap().bytes,
        head.bytes
    );
    store.fail_head.store(false, Ordering::Relaxed);
    storage.publish(WalPosition::default(), 2)?;
    assert_eq!(storage.stats().dirty_segments, 0);
    Ok(())
}

#[test]
fn cow_reuses_untouched_segments_and_keeps_old_manifests() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(root.path())?)?;
    storage.write_at(0, &vec![3; SEGMENT_SIZE * 3])?;
    storage.publish(WalPosition::default(), 1)?;
    let base = storage.manifest();
    let base_id = storage.manifest_id();
    let old_object = store
        .get(&namespace().key(&format!("manifests/{base_id}"))?)?
        .unwrap();
    assert_eq!(store.list(&namespace().key("segments")?)?.len(), 1);
    storage.write_at(SEGMENT_SIZE as u64 + 7, b"change")?;
    storage.publish(WalPosition::default(), 2)?;
    let next = storage.manifest();
    assert_eq!(next.parent.as_deref(), Some(base_id.as_str()));
    assert_eq!(next.segments[&0], base.segments[&0]);
    assert_eq!(next.segments[&2], base.segments[&2]);
    assert_ne!(next.segments[&1], base.segments[&1]);
    assert_eq!(store.list(&namespace().key("segments")?)?.len(), 2);
    let retained = store
        .get(&namespace().key(&format!("manifests/{base_id}"))?)?
        .unwrap();
    assert_eq!(retained.bytes, old_object.bytes);
    assert_eq!(serde_json::from_slice::<Manifest>(&retained.bytes)?, base);
    Ok(())
}

#[test]
fn disk_corruption_refetches_but_remote_corruption_fails_closed() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let disk = Arc::new(SegmentCache::new(root.path(), 0, SEGMENT_SIZE as u64)?);
    let storage = RemoteStorage::open(store.clone(), namespace(), disk.clone())?;
    storage.write_at(0, b"data")?;
    storage.publish(WalPosition::default(), 1)?;
    assert_eq!(read(&storage, 0, 4)?, b"data");
    let hash = storage.manifest().segments[&0].clone();
    std::fs::write(root.path().join(format!("{hash}.seg")), b"corrupt")?;
    let before = store.segment_gets.load(Ordering::Relaxed);
    assert_eq!(read(&storage, 0, 4)?, b"data");
    assert_eq!(store.segment_gets.load(Ordering::Relaxed), before + 1);
    drop(storage);
    drop(disk);
    std::fs::remove_file(root.path().join(format!("{hash}.seg")))?;
    store.put(
        &namespace().key(&format!("segments/{hash}"))?,
        &vec![8; SEGMENT_SIZE],
        PutCondition::Any,
    )?;
    let storage = RemoteStorage::open(store, namespace(), cache(root.path())?)?;
    let mut out = [99; 4];
    assert!(storage.read_at(0, &mut out).is_err());
    assert_eq!(out, [99; 4]);
    Ok(())
}

#[test]
fn cache_limits_lru_restart_and_oversized_bypass() -> Result<()> {
    let root = tempfile::tempdir()?;
    let first = Arc::new(vec![1; SEGMENT_SIZE]);
    let second = Arc::new(vec![2; SEGMENT_SIZE]);
    let third = Arc::new(vec![3; SEGMENT_SIZE]);
    let hashes = [digest(&first), digest(&second), digest(&third)];
    let cache = SegmentCache::new(root.path(), SEGMENT_SIZE, (SEGMENT_SIZE * 2) as u64)?;
    cache.insert(&hashes[0], first)?;
    cache.insert(&hashes[1], second)?;
    assert!(cache.get(&hashes[0])?.is_some());
    cache.insert(&hashes[2], third)?;
    assert!(cache.get(&hashes[1])?.is_none());
    assert!(cache.stats().ram_bytes <= SEGMENT_SIZE);
    assert!(cache.stats().disk_bytes <= (SEGMENT_SIZE * 2) as u64);
    assert!(cache.stats().evictions >= 3);
    drop(cache);
    let cache = SegmentCache::new(root.path(), 0, SEGMENT_SIZE as u64)?;
    assert!(cache.stats().disk_bytes <= SEGMENT_SIZE as u64);
    let present = [&hashes[0], &hashes[2]]
        .into_iter()
        .find(|hash| root.path().join(format!("{hash}.seg")).exists())
        .unwrap();
    assert!(cache.get(present)?.is_some());
    assert_eq!(cache.stats().hits_disk, 1);
    drop(cache);
    let cache = SegmentCache::new(root.path(), 1, 1)?;
    let bytes = Arc::new(vec![4; SEGMENT_SIZE]);
    cache.insert(&digest(&bytes), bytes)?;
    assert_eq!(cache.stats().ram_bytes, 0);
    assert_eq!(cache.stats().disk_bytes, 0);
    Ok(())
}

#[test]
fn cross_segment_failure_is_atomic_even_with_dirty_prefix() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let disk = Arc::new(SegmentCache::new(root.path(), 0, 0)?);
    let storage = RemoteStorage::open(store.clone(), namespace(), disk)?;
    storage.write_at(0, &vec![1; SEGMENT_SIZE])?;
    storage.write_at(SEGMENT_SIZE as u64, &vec![2; SEGMENT_SIZE])?;
    storage.publish(WalPosition::default(), 1)?;
    storage.write_at(SEGMENT_SIZE as u64 - 2, b"ok")?;
    let second_hash = storage.manifest().segments[&1].clone();
    *store.fail_get.lock().unwrap() = Some(namespace().key(&format!("segments/{second_hash}"))?);
    assert!(storage.write_at(SEGMENT_SIZE as u64 - 2, b"bad!").is_err());
    assert_eq!(read(&storage, SEGMENT_SIZE as u64 - 2, 2)?, b"ok");
    assert_eq!(storage.size(), (SEGMENT_SIZE * 2) as u64);
    assert_eq!(storage.stats().dirty_segments, 1);
    assert!(storage.write_at(u64::MAX, b"overflow").is_err());
    Ok(())
}

#[test]
fn truncate_extend_never_resurrects_bytes_or_segments() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(root.path())?)?;
    storage.write_at(0, &vec![8; SEGMENT_SIZE * 3])?;
    storage.publish(WalPosition::default(), 1)?;
    storage.truncate(7)?;
    storage.truncate((SEGMENT_SIZE * 3) as u64)?;
    assert_eq!(read(&storage, 0, 7)?, vec![8; 7]);
    assert_eq!(
        read(&storage, 7, SEGMENT_SIZE * 3 - 7)?,
        vec![0; SEGMENT_SIZE * 3 - 7]
    );
    storage.publish(WalPosition::default(), 2)?;
    assert_eq!(storage.manifest().segments.len(), 1);
    storage.truncate(0)?;
    storage.truncate((SEGMENT_SIZE * 2) as u64)?;
    storage.publish(WalPosition::default(), 3)?;
    let storage = RemoteStorage::open(store, namespace(), cache(&root.path().join("fresh"))?)?;
    assert!(storage.manifest().segments.is_empty());
    assert_eq!(
        read(&storage, 0, SEGMENT_SIZE * 2)?,
        vec![0; SEGMENT_SIZE * 2]
    );
    Ok(())
}

#[test]
fn manifest_corruption_and_unknown_fields_are_rejected() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(root.path())?)?;
    let key = namespace().key(&format!("manifests/{}", storage.manifest_id()))?;
    store.put(&key, b"{}", PutCondition::Any)?;
    assert!(RemoteStorage::open(store, namespace(), cache(root.path())?).is_err());
    let mut manifest = Manifest::default();
    manifest.segments.insert(0, digest(b"bad"));
    assert!(manifest.validate().is_err());
    assert!(serde_json::from_str::<Manifest>("{\"unexpected\":true}").is_err());
    Ok(())
}

#[test]
fn dirty_buffers_survive_eviction_and_cannot_leak_to_other_readers() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let shared = cache(root.path())?;
    let storage = RemoteStorage::open(store.clone(), namespace(), shared.clone())?;
    storage.write_at(0, &vec![2; SEGMENT_SIZE * 4])?;
    storage.publish(WalPosition::default(), 1)?;
    storage.write_at(0, &vec![9; SEGMENT_SIZE * 4])?;
    let other = RemoteStorage::open(store, namespace(), shared.clone())?;
    assert_eq!(
        read(&other, 0, SEGMENT_SIZE * 4)?,
        vec![2; SEGMENT_SIZE * 4]
    );
    for value in 10..15 {
        let bytes = Arc::new(vec![value; SEGMENT_SIZE]);
        shared.insert(&digest(&bytes), bytes)?;
    }
    assert_eq!(storage.stats().dirty_segments, 4);
    assert_eq!(
        read(&storage, 0, SEGMENT_SIZE * 4)?,
        vec![9; SEGMENT_SIZE * 4]
    );
    storage.publish(WalPosition::default(), 2)?;
    assert_eq!(storage.stats().dirty_segments, 0);
    Ok(())
}

#[test]
fn restart_reuses_disk_segments_without_remote_fetch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(root.path())?)?;
    assert_eq!(serde_json::to_value(storage.stats())?["segment_loads"], 0);
    storage.write_at(0, b"warm")?;
    storage.publish(WalPosition::default(), 1)?;
    assert_eq!(read(&storage, 0, 4)?, b"warm");
    drop(storage);
    let before = store.segment_gets.load(Ordering::Relaxed);
    let storage = RemoteStorage::open(store.clone(), namespace(), cache(root.path())?)?;
    assert_eq!(read(&storage, 0, 4)?, b"warm");
    assert_eq!(store.segment_gets.load(Ordering::Relaxed), before);
    assert_eq!(storage.stats().segment_loads, 0);
    assert_eq!(storage.stats().cache.hits_disk, 1);
    assert_eq!(read(&storage, 0, 4)?, b"warm");
    assert_eq!(storage.stats().cache.hits_ram, 1);
    Ok(())
}

#[test]
fn truncate_failure_preserves_size_segments_and_dirty_data() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    let disk = Arc::new(SegmentCache::new(root.path(), 0, 0)?);
    let storage = RemoteStorage::open(store.clone(), namespace(), disk)?;
    storage.write_at(0, &vec![1; SEGMENT_SIZE])?;
    storage.write_at(SEGMENT_SIZE as u64, &vec![2; SEGMENT_SIZE])?;
    storage.publish(WalPosition::default(), 1)?;
    let manifest = storage.manifest();
    *store.fail_get.lock().unwrap() =
        Some(namespace().key(&format!("segments/{}", manifest.segments[&0]))?);
    assert!(storage.truncate(7).is_err());
    assert_eq!(storage.size(), (SEGMENT_SIZE * 2) as u64);
    assert_eq!(storage.manifest(), manifest);
    assert_eq!(storage.stats().dirty_segments, 0);
    *store.fail_get.lock().unwrap() = None;
    assert_eq!(
        read(&storage, 0, SEGMENT_SIZE * 2)?,
        [vec![1; SEGMENT_SIZE], vec![2; SEGMENT_SIZE]].concat()
    );
    Ok(())
}

#[test]
fn invalid_namespace_cannot_initialize_remote_metadata() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(InstrumentedStore::default());
    for tenant in ["..", "tenant/other", "", "tenant.db"] {
        let invalid = Namespace {
            tenant: tenant.to_owned(),
            database: "db".to_owned(),
        };
        assert!(RemoteStorage::open(store.clone(), invalid, cache(root.path())?).is_err());
    }
    assert!(store.list("")?.is_empty());
    assert!(store.get("tenant/db/../../escape").is_err());
    Ok(())
}
