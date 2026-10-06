use crate::manifest::{valid_hash, verify_segment};
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
}

#[derive(Default)]
struct State {
    ram: BTreeMap<String, RamEntry>,
    disk: BTreeMap<String, DiskEntry>,
    clock: u64,
    stats: CacheStats,
}

pub struct SegmentCache {
    root: PathBuf,
    ram_limit: usize,
    disk_limit: u64,
    state: Mutex<State>,
}

impl SegmentCache {
    pub fn new(root: impl AsRef<Path>, ram_limit: usize, disk_limit: u64) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        let cache = Self {
            root: root.as_ref().to_path_buf(),
            ram_limit,
            disk_limit,
            state: Mutex::new(State::default()),
        };
        let mut state = cache.state.lock().expect("cache mutex poisoned");
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
            state.stats.disk_bytes += size;
            state.disk.insert(hash, DiskEntry { size, used });
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
        let mut state = self.state.lock().expect("cache mutex poisoned");
        state.clock += 1;
        let used = state.clock;
        if let Some(entry) = state.ram.get_mut(hash) {
            entry.used = used;
            let bytes = entry.bytes.clone();
            state.stats.hits_ram += 1;
            return Ok(Some(bytes));
        }
        if state.disk.contains_key(hash) {
            match fs::read(self.path(hash)) {
                Ok(bytes) if verify_segment(hash, &bytes).is_ok() => {
                    state.disk.get_mut(hash).expect("disk entry exists").used = used;
                    state.stats.hits_disk += 1;
                    let bytes = Arc::new(bytes);
                    self.insert_ram(&mut state, hash, bytes.clone());
                    return Ok(Some(bytes));
                }
                _ => {
                    let entry = state.disk.remove(hash).expect("disk entry exists");
                    state.stats.disk_bytes -= entry.size;
                    let _ = fs::remove_file(self.path(hash));
                }
            }
        }
        state.stats.misses += 1;
        Ok(None)
    }

    pub fn insert(&self, hash: &str, bytes: Arc<Vec<u8>>) -> Result<()> {
        ensure!(valid_hash(hash), "invalid cache identifier");
        verify_segment(hash, &bytes)?;
        let mut state = self.state.lock().expect("cache mutex poisoned");
        state.clock += 1;
        self.insert_ram(&mut state, hash, bytes.clone());
        if bytes.len() as u64 <= self.disk_limit {
            if let Some(previous) = state.disk.remove(hash) {
                state.stats.disk_bytes -= previous.size;
            }
            while state.stats.disk_bytes + bytes.len() as u64 > self.disk_limit {
                self.remove_oldest_disk(&mut state)?;
            }
            fs::write(self.path(hash), bytes.as_slice())?;
            let size = bytes.len() as u64;
            let used = state.clock;
            state.disk.insert(hash.to_owned(), DiskEntry { size, used });
            state.stats.disk_bytes += size;
        }
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
        self.state
            .lock()
            .expect("cache mutex poisoned")
            .stats
            .clone()
    }
}
