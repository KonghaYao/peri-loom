use super::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

const MAX_CACHED_PACKS: usize = 128;
const MAX_CACHED_BYTES: usize = 2 * 1024 * 1024;

/// Header plus identity/timestamps binds a cached directory to the file actually
/// opened through the current alias. Names are never resolved through this cache.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key {
    device: u64,
    inode: u64,
    length: u64,
    modified: (i64, i64),
    changed: (i64, i64),
    header: [u8; codec::HEADER_LEN],
}

impl Key {
    #[cfg(unix)]
    pub(super) fn new(metadata: &fs::Metadata, header: &[u8; codec::HEADER_LEN]) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
            header: *header,
        }
    }

    #[cfg(not(unix))]
    pub(super) fn new(metadata: &fs::Metadata, header: &[u8; codec::HEADER_LEN]) -> Self {
        Self {
            device: 0,
            inode: 0,
            length: metadata.len(),
            modified: (0, 0),
            changed: (0, 0),
            header: *header,
        }
    }
}

#[derive(Default)]
struct State {
    entries: BTreeMap<Key, (Arc<codec::Directory>, usize)>,
    order: VecDeque<Key>,
    bytes: usize,
    hits: u64,
    misses: u64,
}

/// No pack payload is cached. Count and allocated-directory estimates are both
/// bounded, and a too-large decoded value is simply not admitted.
#[derive(Default)]
pub(super) struct DirectoryCache(Mutex<State>);

impl DirectoryCache {
    pub(super) fn get(&self, key: &Key) -> Result<Option<Arc<codec::Directory>>> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("pack directory cache poisoned"))?;
        if let Some((directory, _)) = state.entries.get(key) {
            let directory = directory.clone();
            state.hits += 1;
            return Ok(Some(directory));
        }
        state.misses += 1;
        Ok(None)
    }

    pub(super) fn insert(&self, key: Key, directory: Arc<codec::Directory>) -> Result<()> {
        // Account the Vec/String allocations and conservatively cover Arc/map/
        // queue bookkeeping. Entries have no unaccounted nested allocations.
        let weight = std::mem::size_of::<codec::Directory>()
            + directory.entries.capacity() * std::mem::size_of::<codec::Entry>()
            + directory
                .entries
                .iter()
                .map(|entry| {
                    entry.key.capacity() + entry.etag.capacity() + entry.checksum.capacity()
                })
                .sum::<usize>()
            + 2 * std::mem::size_of::<Key>()
            + 256;
        if weight > MAX_CACHED_BYTES {
            return Ok(());
        }
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("pack directory cache poisoned"))?;
        if state.entries.contains_key(&key) {
            return Ok(());
        }
        while state.entries.len() >= MAX_CACHED_PACKS || state.bytes + weight > MAX_CACHED_BYTES {
            let oldest = state
                .order
                .pop_front()
                .context("pack cache accounting diverged")?;
            if let Some((_, removed)) = state.entries.remove(&oldest) {
                state.bytes -= removed;
            }
        }
        state.bytes += weight;
        state.order.push_back(key.clone());
        state.entries.insert(key, (directory, weight));
        Ok(())
    }

    pub(super) fn snapshot(&self) -> serde_json::Value {
        match self.0.lock() {
            Ok(state) => serde_json::json!({
                "entries": state.entries.len(), "estimated_bytes": state.bytes,
                "entry_limit": MAX_CACHED_PACKS, "estimated_byte_limit": MAX_CACHED_BYTES,
                "hits": state.hits, "misses": state.misses
            }),
            Err(_) => serde_json::json!({"error": "pack directory cache poisoned"}),
        }
    }
}
