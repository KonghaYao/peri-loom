use crate::cache::{CacheStats, SegmentCache};
use crate::manifest::{
    digest, valid_hash, verify_segment, Head, Manifest, WalPosition, SEGMENT_SIZE,
};
use crate::namespace::Namespace;
use crate::object_store::{ObjectStore, PutCondition};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

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
    dirty: BTreeMap<u64, Vec<u8>>,
    segment_loads: u64,
}

pub struct RemoteStorage {
    store: Arc<dyn ObjectStore>,
    namespace: Namespace,
    cache: Arc<SegmentCache>,
    state: Mutex<State>,
}

impl RemoteStorage {
    pub(crate) fn resource_usage(&self) -> Result<crate::resources::ResourceUsage> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("storage mutex poisoned"))?;
        let cache = self.cache.stats();
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
            ..crate::resources::ResourceUsage::default()
        })
    }

    pub fn open(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache: Arc<SegmentCache>,
    ) -> Result<Arc<Self>> {
        let namespace = Namespace::new(namespace.tenant, namespace.database)?;
        let head_key = namespace.key("HEAD")?;
        let mut head = store.get(&head_key)?;
        if head.is_none() {
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
            state: Mutex::new(State {
                size: manifest.logical_db_size,
                segments: manifest.segments.clone(),
                manifest,
                manifest_id: pointer.manifest,
                head_etag: head.etag,
                dirty: BTreeMap::new(),
                segment_loads: 0,
            }),
        }))
    }

    fn load(&self, state: &mut State, index: u64) -> Result<Arc<Vec<u8>>> {
        if let Some(bytes) = state.dirty.get(&index) {
            return Ok(Arc::new(bytes.clone()));
        }
        let Some(hash) = state.segments.get(&index) else {
            return Ok(Arc::new(vec![0; SEGMENT_SIZE]));
        };
        if let Some(bytes) = self.cache.get(hash)? {
            return Ok(bytes);
        }
        let object = self
            .store
            .get(&self.namespace.key(&format!("segments/{hash}"))?)?
            .context("remote segment missing")?;
        verify_segment(hash, &object.bytes)?;
        state.segment_loads += 1;
        let bytes = Arc::new(object.bytes);
        let _ = self.cache.insert(hash, bytes.clone());
        Ok(bytes)
    }

    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        let mut state = self.state.lock().expect("storage mutex poisoned");
        let count = state.size.saturating_sub(offset).min(out.len() as u64) as usize;
        let mut completed = 0;
        while completed < count {
            let position = offset + completed as u64;
            let index = position / SEGMENT_SIZE as u64;
            let within = (position % SEGMENT_SIZE as u64) as usize;
            let length = (SEGMENT_SIZE - within).min(count - completed);
            let bytes = self.load(&mut state, index)?;
            out[completed..completed + length].copy_from_slice(&bytes[within..within + length]);
            completed += length;
        }
        Ok(count)
    }

    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let end = offset
            .checked_add(data.len() as u64)
            .context("write offset overflow")?;
        if data.is_empty() {
            return Ok(0);
        }
        let mut state = self.state.lock().expect("storage mutex poisoned");
        let mut staged = BTreeMap::new();
        let mut completed = 0;
        while completed < data.len() {
            let position = offset + completed as u64;
            let index = position / SEGMENT_SIZE as u64;
            let within = (position % SEGMENT_SIZE as u64) as usize;
            let length = (SEGMENT_SIZE - within).min(data.len() - completed);
            let mut bytes = if within == 0 && length == SEGMENT_SIZE {
                vec![0; SEGMENT_SIZE]
            } else {
                self.load(&mut state, index)?.as_ref().clone()
            };
            bytes[within..within + length].copy_from_slice(&data[completed..completed + length]);
            staged.insert(index, bytes);
            completed += length;
        }
        state.dirty.extend(staged);
        state.size = state.size.max(end);
        Ok(data.len())
    }

    pub fn size(&self) -> u64 {
        self.state.lock().expect("storage mutex poisoned").size
    }

    pub fn truncate(&self, len: u64) -> Result<()> {
        let mut state = self.state.lock().expect("storage mutex poisoned");
        if len < state.size {
            let boundary = len / SEGMENT_SIZE as u64;
            let within = (len % SEGMENT_SIZE as u64) as usize;
            if within != 0
                && (state.dirty.contains_key(&boundary) || state.segments.contains_key(&boundary))
            {
                let mut bytes = self.load(&mut state, boundary)?.as_ref().clone();
                bytes[within..].fill(0);
                state.dirty.insert(boundary, bytes);
            }
            let keep = boundary + u64::from(within != 0);
            state.dirty.retain(|index, _| *index < keep);
            state.segments.retain(|index, _| *index < keep);
        }
        state.size = len;
        Ok(())
    }

    pub fn publish(&self, position: WalPosition, applied_lsn: u64) -> Result<()> {
        let mut state = self.state.lock().expect("storage mutex poisoned");
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
        next.segments = state.segments.clone();
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
        for (index, bytes) in &state.dirty {
            let hash = digest(bytes);
            put_immutable(
                self.store.as_ref(),
                &self.namespace.key(&format!("segments/{hash}"))?,
                bytes,
            )?;
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
            PutCondition::Match(state.head_etag.clone()),
        )?;
        state.segments = next.segments.clone();
        state.manifest = next;
        state.manifest_id = identifier;
        state.head_etag = etag;
        state.dirty.clear();
        Ok(())
    }

    pub fn manifest(&self) -> Manifest {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .manifest
            .clone()
    }

    pub fn manifest_id(&self) -> String {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .manifest_id
            .clone()
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub fn namespace(&self) -> Namespace {
        self.namespace.clone()
    }

    pub fn stats(&self) -> StorageStats {
        let state = self.state.lock().expect("storage mutex poisoned");
        let cache = self.cache.stats();
        StorageStats {
            segment_loads: state.segment_loads,
            cached_segments: cache.ram_bytes / SEGMENT_SIZE,
            dirty_segments: state.dirty.len(),
            cache,
        }
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
