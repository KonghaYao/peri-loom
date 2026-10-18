use crate::cache::{CacheStats, SegmentCache};
use crate::inflight::{self, Fetches};
use crate::manifest::{
    digest, valid_hash, verify_segment, Head, Manifest, WalPosition, SEGMENT_SIZE,
};
use crate::namespace::Namespace;
use crate::object_store::{ObjectStore, PutCondition};
use crate::work_metrics::{LockDomain, LockWait, ObservedLock};
use anyhow::{bail, ensure, Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Attempts a read makes before it gives up on catching an image no concurrent mutation moved.
/// A reader on the request path serializes with the writers through the IO gate, so the bound is
/// only reached by a caller racing a mutation loop directly on the storage API.
const READ_ATTEMPTS: usize = 8;

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageStats {
    pub segment_loads: u64,
    pub cached_segments: usize,
    pub dirty_segments: usize,
    pub cache: CacheStats,
}

struct State {
    manifest: Manifest,
    manifest_id: String,
    head_etag: String,
    size: u64,
    segments: BTreeMap<u64, String>,
    dirty: BTreeMap<u64, Arc<Vec<u8>>>,
    segment_loads: u64,
    /// Bumped by every mutation that can change what a read returns. A read captures it while
    /// resolving identities under the lock and validates it before returning, so a read that
    /// raced a mutation retries instead of returning a mixture of two images.
    revision: u64,
}

pub struct RemoteStorage {
    store: Arc<dyn ObjectStore>,
    namespace: Namespace,
    cache: Arc<SegmentCache>,
    /// One in-flight remote fetch per content address, for callers that miss the same object
    /// while it is being fetched.
    inflight: Fetches,
    state: Mutex<State>,
}

impl RemoteStorage {
    /// Every manifest access in this type goes through here, so the storage lock's wait and
    /// hold time is one domain rather than one number per call site.
    fn state(&self) -> ObservedLock<'_, State> {
        let wait = LockWait::begin(LockDomain::Storage);
        wait.acquired(self.state.lock().expect("storage mutex poisoned"))
    }

    pub(crate) fn resource_usage(&self) -> Result<crate::resources::ResourceUsage> {
        // This site reports poison instead of panicking, so it keeps its own acquisition and
        // only joins the storage lock's measurement.
        let wait = LockWait::begin(LockDomain::Storage);
        let state = self
            .state
            .lock()
            .map(|guard| wait.acquired(guard))
            .map_err(|_| anyhow::anyhow!("storage mutex poisoned"))?;
        let cache = self.cache.stats();
        let segment_cache_hits = cache.hits_ram.saturating_add(cache.hits_disk);
        let segment_cache_lookups = segment_cache_hits.saturating_add(cache.misses);
        let dirty_segment_bytes = state.dirty.values().try_fold(0u64, |total, bytes| {
            total
                .checked_add(bytes.len() as u64)
                .context("dirty segment payload overflow")
        })?;
        Ok(crate::resources::ResourceUsage {
            segment_ram_bytes: cache.ram_bytes,
            segment_disk_bytes: cache.disk_bytes,
            dirty_segment_bytes,
            segment_loads: state.segment_loads,
            segment_cache_hits_ram: cache.hits_ram,
            segment_cache_hits_disk: cache.hits_disk,
            segment_cache_misses: cache.misses,
            segment_cache_evictions: cache.evictions,
            segment_cache_lookups,
            segment_cache_hit_ratio: (segment_cache_lookups > 0)
                .then(|| segment_cache_hits as f64 / segment_cache_lookups as f64),
            ..crate::resources::ResourceUsage::default()
        })
    }

    pub fn open(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache: Arc<SegmentCache>,
    ) -> Result<Arc<Self>> {
        store.validate_session()?;
        let namespace = Namespace::new(namespace.tenant, namespace.database)?;
        let head_key = namespace.key("HEAD")?;
        let mut head = store.get(&head_key)?;
        if head.is_none() {
            ensure!(
                store.access_role() == crate::ownership::types::AccessRole::Writer,
                "read-only HEAD missing"
            );
            store.validate_session()?;
            let bytes = serde_json::to_vec(&Manifest::default())?;
            let identifier = digest(&bytes);
            put_immutable(
                store.as_ref(),
                &namespace.key(&format!("manifests/{identifier}"))?,
                &bytes,
            )?;
            let bytes = serde_json::to_vec(&Head {
                manifest: identifier,
            })?;
            if let Err(error) = store.put(&head_key, &bytes, PutCondition::Absent) {
                head = store.get(&head_key)?;
                if head.is_none() {
                    return Err(error);
                }
            } else {
                head = store.get(&head_key)?;
            }
        }
        let head = head.context("HEAD missing after initialization")?;
        let pointer: Head = serde_json::from_slice(&head.bytes)?;
        ensure!(valid_hash(&pointer.manifest), "invalid manifest identifier");
        let object = store
            .get(&namespace.key(&format!("manifests/{}", pointer.manifest))?)?
            .context("manifest missing")?;
        ensure!(
            digest(&object.bytes) == pointer.manifest,
            "manifest integrity failure"
        );
        let manifest: Manifest = serde_json::from_slice(&object.bytes)?;
        manifest.validate()?;
        Ok(Arc::new(Self {
            store,
            namespace,
            cache,
            inflight: Fetches::default(),
            state: Mutex::new(State {
                size: manifest.logical_db_size,
                segments: manifest.segments.clone(),
                manifest,
                manifest_id: pointer.manifest,
                head_etag: head.etag,
                dirty: BTreeMap::new(),
                segment_loads: 0,
                revision: 0,
            }),
        }))
    }

    /// Resolves a segment while the caller holds the storage lock. The write path needs the
    /// bytes and the lock in the same critical section: it stages them over the image it read,
    /// so a publication that moved the base underneath it would be a lost update.
    fn load(&self, state: &mut State, index: u64) -> Result<Arc<Vec<u8>>> {
        match resolve_segment(state, index) {
            SegmentSource::Staged(bytes) => Ok(bytes),
            SegmentSource::Zeros => Ok(Arc::new(vec![0; SEGMENT_SIZE])),
            SegmentSource::Published(hash) => {
                if let Some(bytes) = self.cache.get(&hash)? {
                    return Ok(bytes);
                }
                let bytes = self.load_remote(&hash)?;
                state.segment_loads += 1;
                let _ = self.cache.insert(&hash, bytes.clone());
                Ok(bytes)
            }
        }
    }

    /// Fetches one immutable object and verifies it against its content address before anything
    /// can use it. It never touches the storage lock, so the caller decides the lock domain: the
    /// write path calls it with the lock held, the read path calls it with the lock released.
    fn load_remote(&self, hash: &str) -> Result<Arc<Vec<u8>>> {
        let object = self
            .store
            .get(&self.namespace.key(&format!("segments/{hash}"))?)?
            .context("remote segment missing")?;
        verify_segment(hash, &object.bytes)?;
        Ok(Arc::new(object.bytes))
    }

    /// Reads one segment for a caller that holds no storage lock: the identity of the index is
    /// fixed under the lock, the bytes are read outside it, and `read_at` re-validates the image
    /// revision before it returns them.
    fn read_segment(&self, index: u64) -> Result<Arc<Vec<u8>>> {
        let source = {
            let state = self.state();
            resolve_segment(&state, index)
        };
        match source {
            SegmentSource::Staged(bytes) => Ok(bytes),
            SegmentSource::Zeros => Ok(Arc::new(vec![0; SEGMENT_SIZE])),
            SegmentSource::Published(hash) => self.fetch_segment(&hash),
        }
    }

    /// One in-flight fetch per content address: callers that miss the same immutable object
    /// while it is being fetched wait for the first caller's outcome instead of issuing their own
    /// GET. The key is the hash, so two revisions of a page never share a fetch.
    ///
    /// The lookup above is the only cache probe this path makes, so a read of one segment is one
    /// lookup whether it hits, merges or fetches.
    fn fetch_segment(&self, hash: &str) -> Result<Arc<Vec<u8>>> {
        if let Some(bytes) = self.cache.get(hash)? {
            return Ok(bytes);
        }
        match self.inflight.join(hash) {
            inflight::Joined::Waiter(waiter) => waiter.wait().map_err(anyhow::Error::msg),
            inflight::Joined::Leader(leader) => {
                let outcome = (|| -> Result<Arc<Vec<u8>>> {
                    let bytes = self.load_remote(hash)?;
                    {
                        let mut state = self.state();
                        state.segment_loads += 1;
                    }
                    let _ = self.cache.insert(hash, bytes.clone());
                    Ok(bytes)
                })();
                leader.publish(
                    outcome
                        .as_ref()
                        .map(Arc::clone)
                        .map_err(|error| format!("{error:#}")),
                );
                outcome
            }
        }
    }

    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        self.store.validate_local()?;
        for _ in 0..READ_ATTEMPTS {
            // Identity, not bytes: the revision says which image this attempt is reading and the
            // count fixes its extent. Bytes are read outside the lock below, and the revision is
            // re-checked before anything is returned, because a mutation that landed in between
            // would otherwise let this call return a mixture of two images.
            let (revision, count) = {
                let state = self.state();
                (
                    state.revision,
                    state.size.saturating_sub(offset).min(out.len() as u64) as usize,
                )
            };
            let mut completed = 0;
            while completed < count {
                let position = offset + completed as u64;
                let index = position / SEGMENT_SIZE as u64;
                let within = (position % SEGMENT_SIZE as u64) as usize;
                let length = (SEGMENT_SIZE - within).min(count - completed);
                let bytes = self.read_segment(index)?;
                out[completed..completed + length].copy_from_slice(&bytes[within..within + length]);
                completed += length;
            }
            if self.state().revision == revision {
                return Ok(count);
            }
        }
        bail!("read raced with {READ_ATTEMPTS} consecutive image mutations")
    }

    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        self.check_local_writer()?;
        let end = offset
            .checked_add(data.len() as u64)
            .context("write offset overflow")?;
        if data.is_empty() {
            return Ok(0);
        }
        let mut state = self.state();
        // Resolve every fallible base read before changing any dirty buffer. Otherwise a
        // failed fetch at the end of a spanning write could leave its dirty prefix modified.
        let mut bases = BTreeMap::new();
        let mut completed = 0;
        while completed < data.len() {
            let position = offset + completed as u64;
            let index = position / SEGMENT_SIZE as u64;
            let within = (position % SEGMENT_SIZE as u64) as usize;
            let length = (SEGMENT_SIZE - within).min(data.len() - completed);
            if (within != 0 || length != SEGMENT_SIZE) && !state.dirty.contains_key(&index) {
                bases.insert(index, self.load(&mut state, index)?);
            }
            completed += length;
        }
        let mut completed = 0;
        while completed < data.len() {
            let position = offset + completed as u64;
            let index = position / SEGMENT_SIZE as u64;
            let within = (position % SEGMENT_SIZE as u64) as usize;
            let length = (SEGMENT_SIZE - within).min(data.len() - completed);
            if within == 0 && length == SEGMENT_SIZE {
                state.dirty.insert(
                    index,
                    Arc::new(data[completed..completed + length].to_vec()),
                );
            } else {
                let bytes = state.dirty.entry(index).or_insert_with(|| {
                    bases
                        .remove(&index)
                        .expect("partial write base was resolved")
                });
                // Readers retain an immutable Arc outside the lock. Only an overlapping read
                // or a shared published cache entry requires a copy; subsequent writes to an
                // exclusively owned dirty segment change just the requested range.
                Arc::make_mut(bytes)[within..within + length]
                    .copy_from_slice(&data[completed..completed + length]);
            }
            completed += length;
        }
        state.size = state.size.max(end);
        state.revision += 1;
        Ok(data.len())
    }

    pub fn size(&self) -> u64 {
        self.state().size
    }

    pub fn truncate(&self, len: u64) -> Result<()> {
        self.check_local_writer()?;
        let mut state = self.state();
        if len < state.size {
            let boundary = len / SEGMENT_SIZE as u64;
            let within = (len % SEGMENT_SIZE as u64) as usize;
            if within != 0
                && (state.dirty.contains_key(&boundary) || state.segments.contains_key(&boundary))
            {
                let mut bytes = self.load(&mut state, boundary)?;
                Arc::make_mut(&mut bytes)[within..].fill(0);
                state.dirty.insert(boundary, bytes);
            }
            let keep = boundary + u64::from(within != 0);
            state.dirty.retain(|index, _| *index < keep);
            state.segments.retain(|index, _| *index < keep);
        }
        state.size = len;
        state.revision += 1;
        Ok(())
    }

    /// Publishes the current dirty image and advances HEAD.
    ///
    /// The remote writes run without the storage lock: a checkpoint backfill can upload
    /// hundreds of megabytes, and holding the lock across those writes stalls every reader
    /// on work it never touches. The caller serializes mutations against this method (the
    /// IO layer's append gate), so the dirty snapshot and the HEAD etag cannot move while
    /// the upload is in flight; the conditional HEAD put remains the cross-process fence.
    /// A segment rewritten during the upload keeps its newer bytes for the next publication
    /// instead of being cleared together with the snapshot.
    pub fn publish(&self, position: WalPosition, applied_lsn: u64) -> Result<()> {
        self.check_writer()?;
        let (mut next, head_etag, dirty) = {
            let state = self.state();
            ensure!(
                applied_lsn >= state.manifest.applied_lsn,
                "applied LSN cannot regress"
            );
            let mut next = state.manifest.clone();
            next.generation = next
                .generation
                .checked_add(1)
                .context("generation overflow")?;
            next.logical_db_size = state.size;
            next.segments.clone_from(&state.segments);
            next.applied_wal_position = position;
            next.applied_lsn = applied_lsn;
            next.parent = Some(state.manifest_id.clone());
            if let Some(header) = state.dirty.get(&0) {
                if state.size >= 18 && header.starts_with(b"SQLite format 3\0") {
                    let encoded = u16::from_be_bytes([header[16], header[17]]);
                    next.page_size = if encoded == 1 {
                        65536
                    } else {
                        u32::from(encoded)
                    };
                }
            }
            next.validate()?;
            let dirty: Vec<(u64, Arc<Vec<u8>>)> = state
                .dirty
                .iter()
                .map(|(index, bytes)| (*index, bytes.clone()))
                .collect();
            (next, state.head_etag.clone(), dirty)
        };
        // An immutable body only needs one successful existence/integrity check in this
        // publication. Keep the bytes too: sharing a hash must never hide a collision. This
        // table is discarded on error or return; a later publication must check again.
        let mut checked = BTreeMap::<String, &[u8]>::new();
        for (index, bytes) in &dirty {
            let hash = digest(bytes);
            let key = self.namespace.key(&format!("segments/{hash}"))?;
            if let Some(previous) = checked.get(&hash) {
                ensure!(
                    *previous == bytes.as_slice(),
                    "immutable object integrity failure: {key}"
                );
            } else {
                put_immutable(self.store.as_ref(), &key, bytes)?;
                checked.insert(hash.clone(), bytes.as_slice());
            }
            next.segments.insert(*index, hash);
        }
        next.validate()?;
        let bytes = serde_json::to_vec(&next)?;
        let identifier = digest(&bytes);
        put_immutable(
            self.store.as_ref(),
            &self.namespace.key(&format!("manifests/{identifier}"))?,
            &bytes,
        )?;
        let head = serde_json::to_vec(&Head {
            manifest: identifier.clone(),
        })?;
        let etag = self.store.put(
            &self.namespace.key("HEAD")?,
            &head,
            PutCondition::Match(head_etag.clone()),
        )?;
        // No fallible step after the HEAD comparison: the durable image has advanced, so
        // the in-memory settlement must not be able to report failure afterwards.
        let mut state = self.state();
        for (index, bytes) in &dirty {
            if state
                .dirty
                .get(index)
                .is_some_and(|current| Arc::ptr_eq(current, bytes))
            {
                state.dirty.remove(index);
            }
        }
        state.segments = next.segments.clone();
        state.manifest = next;
        state.manifest_id = identifier;
        state.head_etag = etag;
        state.revision += 1;
        Ok(())
    }

    // The whole manifest is only for callers that publish it. Anything on a
    // per-request path reads a scalar: cloning the segment index for one field is
    // unbounded work on a request that never touches a page.
    pub fn manifest(&self) -> Manifest {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .manifest
            .clone()
    }

    pub fn generation(&self) -> u64 {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .manifest
            .generation
    }

    pub fn applied_lsn(&self) -> u64 {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .manifest
            .applied_lsn
    }

    pub fn applied_wal_position(&self) -> WalPosition {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .manifest
            .applied_wal_position
    }

    pub fn manifest_id(&self) -> String {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .manifest_id
            .clone()
    }

    /// `fresh` opens the store the comparison reads HEAD from. It is only called
    /// once the local manifest proved it can be the pristine initial image: a
    /// generation-zero image that already carries pages needs no store session at
    /// all, and opening one is a remote round trip.
    pub(crate) fn initial_image_replaced<F>(&self, fresh: F) -> Result<bool>
    where
        F: FnOnce() -> Result<Arc<dyn ObjectStore>>,
    {
        let manifest_id = {
            let state = self.state();
            if state.manifest.generation != 0
                || state.manifest.logical_db_size != 0
                || !state.manifest.segments.is_empty()
                || state.manifest.applied_lsn != 0
            {
                return Ok(false);
            }
            state.manifest_id.clone()
        };
        let fresh = fresh()?;
        let head = fresh
            .get(&self.namespace.key("HEAD")?)?
            .context("initial image HEAD missing")?;
        let pointer: Head = serde_json::from_slice(&head.bytes)?;
        ensure!(valid_hash(&pointer.manifest), "invalid initial image HEAD");
        Ok(pointer.manifest != manifest_id)
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    fn check_local_writer(&self) -> Result<()> {
        ensure!(
            self.store.access_role() == crate::ownership::types::AccessRole::Writer,
            "read-only storage mutation"
        );
        self.store.validate_local()
    }

    fn check_writer(&self) -> Result<()> {
        self.check_local_writer()?;
        self.store.validate_session()
    }

    pub fn namespace(&self) -> Namespace {
        self.namespace.clone()
    }

    pub fn stats(&self) -> StorageStats {
        let state = self.state();
        let cache = self.cache.stats();
        StorageStats {
            segment_loads: state.segment_loads,
            cached_segments: cache.ram_bytes / SEGMENT_SIZE,
            dirty_segments: state.dirty.len(),
            cache,
        }
    }

    /// Test-only: the in-flight table must hold nothing once a fetch returned, so a later caller
    /// starts a fresh fetch instead of reading a retired outcome.
    #[cfg(test)]
    pub(crate) fn inflight_entries(&self) -> usize {
        self.inflight.len()
    }
}

/// What one index resolves to under the storage lock: identity, not bytes. A published segment is
/// named by its content address and read later; staged bytes retain an immutable snapshot.
/// Writers copy a staged buffer only while such a snapshot is still in use.
enum SegmentSource {
    Staged(Arc<Vec<u8>>),
    Published(String),
    Zeros,
}

fn resolve_segment(state: &State, index: u64) -> SegmentSource {
    if let Some(bytes) = state.dirty.get(&index) {
        return SegmentSource::Staged(bytes.clone());
    }
    match state.segments.get(&index) {
        Some(hash) => SegmentSource::Published(hash.clone()),
        None => SegmentSource::Zeros,
    }
}

fn put_immutable(store: &dyn ObjectStore, key: &str, bytes: &[u8]) -> Result<()> {
    if let Some(existing) = store.get(key)? {
        ensure!(
            existing.bytes == bytes,
            "immutable object integrity failure: {key}"
        );
        return Ok(());
    }
    if let Err(error) = store.put(key, bytes, PutCondition::Absent) {
        let existing = store.get(key)?.context(error)?;
        ensure!(
            existing.bytes == bytes,
            "immutable object integrity failure: {key}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod image_tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;

    #[test]
    fn detects_external_initial_image_publication() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("tenant", "database")?;
        let cache = Arc::new(SegmentCache::new(directory.path(), 0, 0)?);
        let storage = RemoteStorage::open(store.clone(), namespace.clone(), cache)?;
        assert!(!storage.initial_image_replaced(|| Ok(store.clone()))?);

        let next = Manifest {
            generation: 1,
            parent: Some(storage.manifest_id()),
            ..Manifest::default()
        };
        let bytes = serde_json::to_vec(&next)?;
        let next_id = digest(&bytes);
        store.put(
            &namespace.key(&format!("manifests/{next_id}"))?,
            &bytes,
            PutCondition::Absent,
        )?;
        let head = store.get(&namespace.key("HEAD")?)?.unwrap();
        store.put(
            &namespace.key("HEAD")?,
            &serde_json::to_vec(&Head { manifest: next_id })?,
            PutCondition::Match(head.etag),
        )?;
        assert!(storage.initial_image_replaced(|| Ok(store.clone()))?);
        Ok(())
    }

    // A generation-zero image that already carries pages can never be the pristine
    // image an imported publication replaces, so the entry path must decide that
    // without opening a store session.
    #[test]
    fn skips_the_initial_image_probe_once_the_image_is_published() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("tenant", "database")?;
        let cache = Arc::new(SegmentCache::new(directory.path(), 0, 0)?);
        let storage = RemoteStorage::open(store.clone(), namespace, cache)?;
        let probes = std::sync::atomic::AtomicUsize::new(0);
        let probe = || -> Result<Arc<dyn ObjectStore>> {
            probes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(store.clone())
        };
        assert!(!storage.initial_image_replaced(probe)?);
        assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);

        storage.write_at(0, b"page")?;
        storage.publish(WalPosition::default(), 1)?;
        assert_eq!(storage.generation(), storage.manifest().generation);
        assert_eq!(storage.applied_lsn(), 1);
        assert_eq!(
            storage.applied_wal_position(),
            storage.manifest().applied_wal_position
        );
        assert!(!storage.initial_image_replaced(probe)?);
        assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);
        Ok(())
    }
}

#[cfg(test)]
mod read_path_tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;

    /// A fetch that returned — successfully or not — must leave no entry behind: a retired
    /// outcome in the table would answer a caller that never asked for that object, and the table
    /// would grow with every miss. A later revision is a different content address, so it is
    /// fetched again rather than answered from the retired one.
    #[test]
    fn a_finished_fetch_leaves_no_entry_in_the_in_flight_table() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("tenant", "database")?;
        let cache = Arc::new(SegmentCache::new(directory.path(), 0, 0)?);
        let storage = RemoteStorage::open(store.clone(), namespace.clone(), cache)?;
        storage.write_at(0, &vec![5; SEGMENT_SIZE])?;
        storage.publish(WalPosition::default(), 1)?;

        let mut out = [0u8; 8];
        storage.read_at(0, &mut out)?;
        assert_eq!(out, [5; 8]);
        assert_eq!(storage.inflight_entries(), 0);

        storage.write_at(0, &vec![6; SEGMENT_SIZE])?;
        storage.publish(WalPosition::default(), 2)?;
        storage.read_at(0, &mut out)?;
        assert_eq!(out, [6; 8]);
        assert_eq!(storage.inflight_entries(), 0);

        let hash = storage.manifest().segments[&0].clone();
        store.delete(&namespace.key(&format!("segments/{hash}"))?, None)?;
        assert!(storage.read_at(0, &mut out).is_err());
        assert_eq!(storage.inflight_entries(), 0);
        Ok(())
    }
}

#[cfg(test)]
#[path = "storage_cow_tests.rs"]
mod cow_tests;
