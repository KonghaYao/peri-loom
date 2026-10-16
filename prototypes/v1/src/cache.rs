use crate::manifest::{valid_hash, verify_segment};
use crate::work_metrics::{LockDomain, LockWait, ObservedLock};
use anyhow::{ensure, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default, Serialize)]
pub struct CacheStats {
    pub ram_bytes: usize,
    pub disk_bytes: u64,
    pub hits_ram: u64,
    pub hits_disk: u64,
    pub misses: u64,
    pub evictions: u64,
}

struct RamEntry {
    bytes: Arc<Vec<u8>>,
    used: u64,
}

struct DiskEntry {
    size: u64,
    used: u64,
    /// Identity of this registration. A reader that captured it before releasing the lock can
    /// tell whether the entry it observed is still the one in the index when it comes back, so
    /// a read that raced an eviction or a re-insert never edits the accounting of the new one.
    token: u64,
}

#[derive(Default)]
struct State {
    ram: BTreeMap<String, RamEntry>,
    disk: BTreeMap<String, DiskEntry>,
    clock: u64,
    tokens: u64,
    stats: CacheStats,
}

/// The disk residency one lookup observed before releasing the lock.
struct DiskIdentity {
    used: u64,
    token: u64,
}

pub struct SegmentCache {
    root: PathBuf,
    ram_limit: usize,
    disk_limit: u64,
    state: Mutex<State>,
}

impl SegmentCache {
    /// Every index access in this type goes through here, so the cache lock's wait and hold
    /// time is one domain rather than one number per call site.
    fn state(&self) -> ObservedLock<'_, State> {
        let wait = LockWait::begin(LockDomain::Cache);
        wait.acquired(self.state.lock().expect("cache mutex poisoned"))
    }

    pub fn new(root: impl AsRef<Path>, ram_limit: usize, disk_limit: u64) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let cache = Self {
            root: root.as_ref().to_path_buf(),
            ram_limit,
            disk_limit,
            state: Mutex::new(State::default()),
        };
        let mut state = cache.state();
        let mut existing = Vec::new();
        for entry in fs::read_dir(&cache.root)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(hash) = name.strip_suffix(".seg").filter(|hash| valid_hash(hash)) {
                let metadata = entry.metadata()?;
                if metadata.is_file() {
                    existing.push((metadata.modified().ok(), hash.to_owned(), metadata.len()));
                }
            }
        }
        existing.sort();
        for (_, hash, size) in existing {
            state.clock += 1;
            let used = state.clock;
            cache.register_disk(&mut state, &hash, size, used);
        }
        cache.evict_disk(&mut state)?;
        drop(state);
        Ok(cache)
    }

    fn path(&self, hash: &str) -> PathBuf {
        self.root.join(format!("{hash}.seg"))
    }

    pub fn get(&self, hash: &str) -> Result<Option<Arc<Vec<u8>>>> {
        ensure!(valid_hash(hash), "invalid cache identifier");
        // Identity, not bytes: the lookup only fixes which RAM or disk entry this call is about,
        // and the file is read after the lock is released. Holding the lock across that read
        // would put one reader's disk latency in front of every other cache access, including
        // lookups of unrelated segments.
        let observed = {
            let mut state = self.state();
            state.clock += 1;
            let used = state.clock;
            if let Some(entry) = state.ram.get_mut(hash) {
                entry.used = used;
                let bytes = entry.bytes.clone();
                state.stats.hits_ram += 1;
                return Ok(Some(bytes));
            }
            state.disk.get(hash).map(|entry| DiskIdentity {
                used,
                token: entry.token,
            })
        };
        let Some(observed) = observed else {
            let mut state = self.state();
            state.stats.misses += 1;
            return Ok(None);
        };
        let path = self.path(hash);
        match fs::read(&path) {
            // The digest is the identity check: bytes that verify against the requested hash are
            // that object, whether or not the entry that pointed at them is still in the index.
            Ok(bytes) if verify_segment(hash, &bytes).is_ok() => {
                let bytes = Arc::new(bytes);
                let mut state = self.state();
                state.stats.hits_disk += 1;
                if let Some(entry) = state.disk.get_mut(hash) {
                    if entry.token == observed.token {
                        entry.used = observed.used;
                    }
                }
                self.insert_ram(&mut state, hash, bytes.clone());
                Ok(Some(bytes))
            }
            // A miss is a miss for the caller either way: the store is asked again and its copy
            // is verified. The corrupt file is dropped only while the entry is still the one
            // observed here, so a reader never evicts a registration it did not look at.
            _ => {
                let mut state = self.state();
                state.stats.misses += 1;
                let removed = match state.disk.get(hash) {
                    Some(entry) if entry.token == observed.token => {
                        let entry = state.disk.remove(hash).expect("disk entry exists");
                        state.stats.disk_bytes -= entry.size;
                        true
                    }
                    _ => false,
                };
                drop(state);
                if removed {
                    let _ = fs::remove_file(&path);
                }
                Ok(None)
            }
        }
    }

    pub fn insert(&self, hash: &str, bytes: Arc<Vec<u8>>) -> Result<()> {
        ensure!(valid_hash(hash), "invalid cache identifier");
        verify_segment(hash, &bytes)?;
        let size = bytes.len() as u64;
        let used = {
            let mut state = self.state();
            state.clock += 1;
            let used = state.clock;
            self.insert_ram(&mut state, hash, bytes.clone());
            if size > self.disk_limit {
                return Ok(());
            }
            if let Some(previous) = state.disk.remove(hash) {
                state.stats.disk_bytes -= previous.size;
            }
            // Room is reserved before the file is written, so a full disk is still discovered
            // with the space already freed instead of after a failed write.
            while state.stats.disk_bytes + size > self.disk_limit {
                self.remove_oldest_disk(&mut state)?;
            }
            used
        };
        fs::write(self.path(hash), bytes.as_slice())?;
        let mut state = self.state();
        // Inserts that ran while this one was writing may have taken the space reserved above.
        while state.stats.disk_bytes + size > self.disk_limit {
            self.remove_oldest_disk(&mut state)?;
        }
        self.register_disk(&mut state, hash, size, used);
        Ok(())
    }

    fn insert_ram(&self, state: &mut State, hash: &str, bytes: Arc<Vec<u8>>) {
        if bytes.len() > self.ram_limit {
            return;
        }
        if let Some(previous) = state.ram.remove(hash) {
            state.stats.ram_bytes -= previous.bytes.len();
        }
        while state.stats.ram_bytes + bytes.len() > self.ram_limit {
            let oldest = state
                .ram
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(hash, _)| hash.clone())
                .expect("RAM eviction candidate");
            let entry = state.ram.remove(&oldest).expect("RAM entry exists");
            state.stats.ram_bytes -= entry.bytes.len();
            state.stats.evictions += 1;
        }
        state.stats.ram_bytes += bytes.len();
        state.ram.insert(
            hash.to_owned(),
            RamEntry {
                bytes,
                used: state.clock,
            },
        );
    }

    /// Registers the disk residency of `hash`, taking the next entry token and accounting its
    /// bytes in one place, so the size a reader sees always belongs to the registration it read.
    fn register_disk(&self, state: &mut State, hash: &str, size: u64, used: u64) {
        state.tokens += 1;
        let token = state.tokens;
        state
            .disk
            .insert(hash.to_owned(), DiskEntry { size, used, token });
        state.stats.disk_bytes += size;
    }

    fn remove_oldest_disk(&self, state: &mut State) -> Result<()> {
        let oldest = state
            .disk
            .iter()
            .min_by_key(|(_, entry)| entry.used)
            .map(|(hash, _)| hash.clone())
            .expect("disk eviction candidate");
        match fs::remove_file(self.path(&oldest)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let entry = state.disk.remove(&oldest).expect("disk entry exists");
        state.stats.disk_bytes -= entry.size;
        state.stats.evictions += 1;
        Ok(())
    }

    fn evict_disk(&self, state: &mut State) -> Result<()> {
        while state.stats.disk_bytes > self.disk_limit {
            self.remove_oldest_disk(state)?;
        }
        Ok(())
    }

    pub fn stats(&self) -> CacheStats {
        self.state().stats.clone()
    }
}
