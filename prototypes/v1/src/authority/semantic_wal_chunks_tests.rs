use super::*;
use crate::{
    authority::{paged_index::PagedIndex, Tombstone},
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
    wal::Entry,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Default)]
struct Probe {
    inner: MemoryObjectStore,
    logical_root_page: Mutex<Option<String>>,
    logical_lookups: AtomicUsize,
}

impl ObjectStore for Probe {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if self.logical_root_page.lock().unwrap().as_deref() == Some(key) {
            self.logical_lookups.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

struct Fixture {
    store: Arc<Probe>,
    authority: Arc<Authority>,
    root: CatalogRoot,
    chunks: Vec<ObjectEntry>,
    indexes: Vec<ObjectEntry>,
}

fn fixture(chunk_count: usize) -> Result<Fixture> {
    let store = Arc::new(Probe::default());
    let namespace = Namespace::new("walker-chunks", super::super::unique_id()?)?;
    let authority = Authority::open(store.clone(), namespace.clone())?;
    let mut root = authority.snapshot()?.root;
    let mut chunks = Vec::new();
    // Enough entries force the logical registry into real immutable pages. Counting its
    // root page reads observes actual lookup work rather than an optimization counter.
    for number in 0..chunk_count.max(96) {
        let bytes = format!("registered chunk {number}").into_bytes();
        let entry = Authority::upload_entry(&bytes)?;
        store.put(
            &authority.physical_key(&entry.blob)?,
            &bytes,
            PutCondition::Absent,
        )?;
        root.blobs.insert(entry.blob.clone(), entry.clone())?;
        root.objects
            .insert(format!("wal/chunks/{}", entry.hash), entry.clone())?;
        chunks.push(entry);
    }
    let mut indexes = Vec::new();
    for count in [2, 3] {
        let bytes = serde_json::to_vec(&WalMetadata {
            epoch: 1,
            last_lsn: count as u64,
            entries: chunks[..count]
                .iter()
                .enumerate()
                .map(|(index, entry)| Entry {
                    lsn: index as u64 + 1,
                    hash: entry.hash.clone(),
                })
                .collect(),
            ..WalMetadata::default()
        })?;
        let entry = Authority::upload_entry(&bytes)?;
        store.put(
            &authority.physical_key(&entry.blob)?,
            &bytes,
            PutCondition::Absent,
        )?;
        root.blobs.insert(entry.blob.clone(), entry.clone())?;
        indexes.push(entry);
    }
    let publishing = PagedIndex::new(store.as_ref(), &namespace);
    root.blobs.publish(&publishing)?;
    root.objects.publish(&publishing)?;
    root.seal()?;
    root.branches.get_mut("main").unwrap().history = indexes
        .iter()
        .enumerate()
        .map(|(number, entry)| RecoveryPoint {
            id: entry.etag.clone(),
            branch: "main".into(),
            timeline: 1,
            sequence: number as u64 + 1,
            timestamp_ms: number as u64 + 1,
            head: None,
            wal: Some(entry.clone()),
        })
        .collect();
    let branch = root.branches.get_mut("main").unwrap();
    branch.sequence = 2;
    branch.last_commit_ms = 2;
    *store.logical_root_page.lock().unwrap() = Some(namespace.key(&format!(
        "authority/index-pages/{}",
        root.objects.pointer().context("logical registry did not spill")?
    ))?);
    Ok(Fixture {
        store,
        authority,
        root,
        chunks,
        indexes,
    })
}

#[test]
fn different_history_indexes_share_chunk_validation_and_keep_complete_gc_reachability() -> Result<()>
{
    let fixture = fixture(3)?;
    let mut walker = Walker::new(&fixture.authority, &fixture.root, true);
    let points = &fixture.root.branches["main"].history;
    walker.point(&points[0])?;
    assert_eq!(fixture.store.logical_lookups.load(Ordering::SeqCst), 2);
    walker.point(&points[1])?;
    assert_eq!(
        fixture.store.logical_lookups.load(Ordering::SeqCst),
        3,
        "only the later index's new chunk should resolve the logical registry"
    );
    let expected: BTreeSet<_> = fixture
        .indexes
        .iter()
        .chain(fixture.chunks[..3].iter())
        .map(|entry| entry.blob.clone())
        .collect();
    assert_eq!(walker.reachable, expected);
    assert_eq!(fixture.authority.reachable(&fixture.root)?, expected);
    Ok(())
}

#[test]
fn a_new_root_rechecks_missing_unregistered_tombstoned_and_wrong_hash_chunks() -> Result<()> {
    let fixture = fixture(3)?;
    let chunk = &fixture.chunks[0];
    let key = format!("wal/chunks/{}", chunk.hash);
    let mut admitted = Walker::new(&fixture.authority, &fixture.root, false);
    admitted.wal_chunk(&chunk.hash)?;
    for failure in ["missing", "unregistered", "tombstoned", "wrong-hash"] {
        let mut root = fixture.root.clone();
        match failure {
            "missing" => root.objects.remove(&key)?,
            "unregistered" => root.blobs.remove(&chunk.blob)?,
            "tombstoned" => root.tombstones.insert(
                chunk.blob.clone(),
                Tombstone {
                    entry: chunk.clone(),
                    marked_ms: 1,
                    deleted: false,
                },
            )?,
            "wrong-hash" => root
                .objects
                .insert(key.clone(), fixture.chunks[1].clone())?,
            _ => unreachable!(),
        }
        let mut walker = Walker::new(&fixture.authority, &root, true);
        for _ in 0..2 {
            assert!(walker.wal_chunk(&chunk.hash).is_err(), "{failure}");
            assert!(walker.reachable.is_empty(), "{failure}");
        }
    }
    for _ in 0..2 {
        assert!(admitted.wal_chunk("not-a-digest").is_err());
    }
    Ok(())
}

#[test]
fn shallow_index_validation_does_not_skip_the_later_chunk_sweep() -> Result<()> {
    let fixture = fixture(3)?;
    let mut root = fixture.root.clone();
    root.objects
        .remove(&format!("wal/chunks/{}", fixture.chunks[0].hash))?;
    let mut walker = Walker::new(&fixture.authority, &root, false);
    walker.wal_index(Some(&fixture.indexes[0]))?;
    assert!(walker.wal_chunks(Some(&fixture.indexes[0])).is_err());
    assert!(walker.wal_chunks(Some(&fixture.indexes[0])).is_err());
    Ok(())
}

#[test]
fn the_chunk_fifo_evicts_and_revalidates_older_entries() -> Result<()> {
    let fixture = fixture(VERIFIED_WAL_CHUNKS + 1)?;
    let mut walker = Walker::new(&fixture.authority, &fixture.root, true);
    for chunk in &fixture.chunks[..VERIFIED_WAL_CHUNKS] {
        walker.wal_chunk(&chunk.hash)?;
    }
    let before = fixture.store.logical_lookups.load(Ordering::SeqCst);
    walker.wal_chunk(&fixture.chunks[0].hash)?;
    assert_eq!(fixture.store.logical_lookups.load(Ordering::SeqCst), before);
    walker.wal_chunk(&fixture.chunks[VERIFIED_WAL_CHUNKS].hash)?;
    walker.wal_chunk(&fixture.chunks[0].hash)?;
    assert_eq!(
        fixture.store.logical_lookups.load(Ordering::SeqCst),
        before + 2
    );
    assert_eq!(walker.verified_chunks.len(), VERIFIED_WAL_CHUNKS);
    assert_eq!(walker.reachable.len(), VERIFIED_WAL_CHUNKS + 1);
    Ok(())
}
