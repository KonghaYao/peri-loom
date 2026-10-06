use crate::manifest::{self, Manifest, WalPosition, SEGMENT_SIZE};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

#[derive(Serialize, Debug, Clone, Default)]
pub struct StorageStats {
    pub segment_loads: u64,
    pub cached_segments: usize,
    pub dirty_segments: usize,
}

pub struct SegmentedStorage {
    root: PathBuf,
    state: Mutex<State>,
}

struct State {
    manifest: Manifest,
    logical_size: u64,
    segments: BTreeMap<u64, String>,
    cache: BTreeMap<u64, Vec<u8>>,
    dirty: BTreeSet<u64>,
    segment_loads: u64,
}

impl SegmentedStorage {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let manifest = manifest::initialize(&root)?;
        Ok(Self {
            root,
            state: Mutex::new(State {
                logical_size: manifest.logical_db_size,
                segments: manifest.segments.clone(),
                manifest,
                cache: BTreeMap::new(),
                dirty: BTreeSet::new(),
                segment_loads: 0,
            }),
        })
    }

    fn load_segment(&self, state: &mut State, index: u64) -> Result<()> {
        if state.cache.contains_key(&index) {
            return Ok(());
        }
        let bytes = if let Some(name) = state.segments.get(&index) {
            let bytes = fs::read(self.root.join("segments").join(name))
                .with_context(|| format!("read segment {index}: {name}"))?;
            ensure!(
                bytes.len() == SEGMENT_SIZE as usize,
                "invalid segment length: {index}"
            );
            ensure!(
                format!("{}.seg", manifest::digest(&bytes)) == *name,
                "segment checksum mismatch: {index}"
            );
            state.segment_loads += 1;
            bytes
        } else {
            vec![0; SEGMENT_SIZE as usize]
        };
        state.cache.insert(index, bytes);
        Ok(())
    }

    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        let mut state = self.state.lock().expect("storage mutex poisoned");
        if offset >= state.logical_size || out.is_empty() {
            return Ok(0);
        }
        let count = (state.logical_size - offset).min(out.len() as u64) as usize;
        let mut completed = 0;
        while completed < count {
            let position = offset + completed as u64;
            let index = position / u64::from(SEGMENT_SIZE);
            let within = (position % u64::from(SEGMENT_SIZE)) as usize;
            let length = (SEGMENT_SIZE as usize - within).min(count - completed);
            if state.segments.contains_key(&index) || state.cache.contains_key(&index) {
                self.load_segment(&mut state, index)?;
                out[completed..completed + length]
                    .copy_from_slice(&state.cache[&index][within..within + length]);
            } else {
                out[completed..completed + length].fill(0);
            }
            completed += length;
        }
        Ok(count)
    }

    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let end = offset
            .checked_add(data.len() as u64)
            .context("write offset overflow")?;
        let first = offset / u64::from(SEGMENT_SIZE);
        let last = (end - 1) / u64::from(SEGMENT_SIZE);
        let mut state = self.state.lock().expect("storage mutex poisoned");
        for index in first..=last {
            self.load_segment(&mut state, index)?;
        }
        let mut completed = 0;
        while completed < data.len() {
            let position = offset + completed as u64;
            let index = position / u64::from(SEGMENT_SIZE);
            let within = (position % u64::from(SEGMENT_SIZE)) as usize;
            let length = (SEGMENT_SIZE as usize - within).min(data.len() - completed);
            state.cache.get_mut(&index).expect("segment loaded")[within..within + length]
                .copy_from_slice(&data[completed..completed + length]);
            state.dirty.insert(index);
            completed += length;
        }
        state.logical_size = state.logical_size.max(end);
        Ok(data.len())
    }

    pub fn size(&self) -> u64 {
        self.state
            .lock()
            .expect("storage mutex poisoned")
            .logical_size
    }

    pub fn truncate(&self, len: u64) -> Result<()> {
        let mut state = self.state.lock().expect("storage mutex poisoned");
        if len < state.logical_size {
            let boundary = len / u64::from(SEGMENT_SIZE);
            let within = (len % u64::from(SEGMENT_SIZE)) as usize;
            if within != 0
                && (state.segments.contains_key(&boundary) || state.cache.contains_key(&boundary))
            {
                self.load_segment(&mut state, boundary)?;
                state.cache.get_mut(&boundary).expect("segment loaded")[within..].fill(0);
                state.dirty.insert(boundary);
            }
            let keep = boundary + u64::from(within != 0);
            state.segments.retain(|index, _| *index < keep);
            state.cache.retain(|index, _| *index < keep);
            state.dirty.retain(|index| *index < keep);
        }
        state.logical_size = len;
        Ok(())
    }

    pub fn publish(&self, applied_wal: WalPosition) -> Result<()> {
        let mut state = self.state.lock().expect("storage mutex poisoned");
        let generation = state
            .manifest
            .generation
            .checked_add(1)
            .context("generation overflow")?;
        let mut segments = state.segments.clone();
        for index in &state.dirty {
            let bytes = &state.cache[index];
            let name = format!("{}.seg", manifest::digest(bytes));
            manifest::write_immutable(&self.root.join("segments"), &name, bytes)?;
            segments.insert(*index, name);
        }
        manifest::sync_directory(&self.root.join("segments"))?;
        let mut page_size = state.manifest.page_size;
        if state.logical_size >= 18 {
            if let Some(header) = state.cache.get(&0) {
                if header.starts_with(b"SQLite format 3\0") {
                    let encoded = u16::from_be_bytes([header[16], header[17]]);
                    page_size = if encoded == 1 {
                        65536
                    } else {
                        u32::from(encoded)
                    };
                }
            }
        }
        let next = Manifest {
            generation,
            logical_db_size: state.logical_size,
            page_size,
            segment_size: SEGMENT_SIZE,
            segments,
            applied_wal_position: applied_wal,
        };
        manifest::publish(&self.root, &next)?;
        state.segments = next.segments.clone();
        state.manifest = next;
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

    pub fn stats(&self) -> StorageStats {
        let state = self.state.lock().expect("storage mutex poisoned");
        StorageStats {
            segment_loads: state.segment_loads,
            cached_segments: state.cache.len(),
            dirty_segments: state.dirty.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(storage: &SegmentedStorage, offset: u64, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0; length];
        let count = storage.read_at(offset, &mut bytes)?;
        bytes.truncate(count);
        Ok(bytes)
    }

    #[test]
    fn cross_segment_io_is_lazy_and_durable() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        let bytes: Vec<u8> = (0..SEGMENT_SIZE as usize * 3 + 37)
            .map(|index| (index % 251) as u8)
            .collect();
        assert_eq!(storage.write_at(17, &bytes)?, bytes.len());
        assert_eq!(storage.stats().dirty_segments, 4);
        assert_eq!(read(&storage, 0, 17)?, vec![0; 17]);
        assert_eq!(read(&storage, 17, bytes.len())?, bytes);
        let wal = WalPosition {
            salts: [7, 9],
            frame_count: 123,
        };
        storage.publish(wal.clone())?;
        assert_eq!(storage.manifest().applied_wal_position, wal);
        assert_eq!(storage.stats().dirty_segments, 0);
        drop(storage);
        let storage = SegmentedStorage::open(root.path())?;
        assert_eq!(storage.stats().segment_loads, 0);
        assert_eq!(storage.stats().cached_segments, 0);
        assert_eq!(read(&storage, 17, 3)?, bytes[..3]);
        assert_eq!(storage.stats().segment_loads, 1);
        assert_eq!(read(&storage, 18, 2)?, bytes[1..3]);
        assert_eq!(storage.stats().segment_loads, 1);
        assert_eq!(read(&storage, 17, bytes.len())?, bytes);
        assert_eq!(storage.stats().segment_loads, 4);
        Ok(())
    }

    #[test]
    fn checkpoint_only_changes_dirty_segments_and_preserves_old_generation() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        storage.write_at(0, &vec![42; SEGMENT_SIZE as usize * 3])?;
        storage.publish(WalPosition::default())?;
        let previous = storage.manifest();
        let old_head = fs::read(root.path().join("HEAD"))?;
        storage.write_at(u64::from(SEGMENT_SIZE) + 10, &[99])?;
        assert_eq!(storage.stats().dirty_segments, 1);
        storage.publish(WalPosition::default())?;
        let current = storage.manifest();
        assert_eq!(current.generation, previous.generation + 1);
        assert_eq!(current.segments[&0], previous.segments[&0]);
        assert_ne!(current.segments[&1], previous.segments[&1]);
        assert_eq!(current.segments[&2], previous.segments[&2]);
        for name in current.segments.values() {
            assert_eq!(
                fs::metadata(root.path().join("segments").join(name))?.len(),
                u64::from(SEGMENT_SIZE)
            );
        }
        fs::write(root.path().join("HEAD"), old_head)?;
        let old = SegmentedStorage::open(root.path())?;
        assert_eq!(read(&old, u64::from(SEGMENT_SIZE) + 10, 1)?, vec![42]);
        Ok(())
    }

    #[test]
    fn unpublished_changes_are_not_recovered() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        storage.write_at(0, b"published")?;
        storage.publish(WalPosition::default())?;
        storage.write_at(0, b"discarded")?;
        storage.truncate(3)?;
        drop(storage);
        let recovered = SegmentedStorage::open(root.path())?;
        assert_eq!(read(&recovered, 0, 100)?, b"published");
        Ok(())
    }

    #[test]
    fn sparse_writes_and_truncation_never_resurrect_bytes() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        let offset = u64::from(SEGMENT_SIZE) * 8 + 10;
        storage.write_at(offset, b"tail")?;
        storage.publish(WalPosition::default())?;
        assert_eq!(storage.manifest().segments.len(), 1);
        assert_eq!(read(&storage, 0, 100)?, vec![0; 100]);
        storage.truncate(offset + 2)?;
        storage.truncate(offset + 20)?;
        assert_eq!(
            read(&storage, offset, 20)?,
            [b"ta".as_slice(), &[0; 18]].concat()
        );
        storage.publish(WalPosition::default())?;
        drop(storage);
        let storage = SegmentedStorage::open(root.path())?;
        assert_eq!(
            read(&storage, offset, 20)?,
            [b"ta".as_slice(), &[0; 18]].concat()
        );
        storage.truncate(u64::from(SEGMENT_SIZE))?;
        storage.truncate(offset + 20)?;
        assert_eq!(read(&storage, offset, 20)?, vec![0; 20]);
        storage.publish(WalPosition::default())?;
        assert!(storage.manifest().segments.is_empty());
        storage.truncate(0)?;
        storage.publish(WalPosition::default())?;
        assert_eq!(SegmentedStorage::open(root.path())?.size(), 0);
        Ok(())
    }

    #[test]
    fn missing_corrupt_and_short_segments_fail_on_lazy_load() -> Result<()> {
        for corruption in 0..3 {
            let root = tempfile::tempdir()?;
            let storage = SegmentedStorage::open(root.path())?;
            storage.write_at(0, b"data")?;
            storage.publish(WalPosition::default())?;
            let path = root
                .path()
                .join("segments")
                .join(&storage.manifest().segments[&0]);
            drop(storage);
            match corruption {
                0 => fs::remove_file(path)?,
                1 => fs::write(path, vec![17; SEGMENT_SIZE as usize])?,
                _ => fs::write(path, b"short")?,
            }
            let storage = SegmentedStorage::open(root.path())?;
            assert_eq!(storage.stats().segment_loads, 0);
            assert!(storage.read_at(0, &mut [0; 4]).is_err());
            assert!(storage.write_at(0, b"x").is_err());
            assert!(storage.truncate(1).is_err());
            assert_eq!(storage.size(), 4);
            assert_eq!(storage.stats().cached_segments, 0);
            assert_eq!(storage.stats().dirty_segments, 0);
        }
        Ok(())
    }

    #[test]
    fn failed_publish_keeps_head_and_pending_changes() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        storage.write_at(0, b"before")?;
        storage.publish(WalPosition::default())?;
        let before = storage.manifest();
        let head = fs::read(root.path().join("HEAD"))?;
        storage.write_at(0, b"after!")?;
        let manifests = root.path().join("manifests");
        let backup = root.path().join("manifest-backup");
        fs::rename(&manifests, &backup)?;
        fs::write(&manifests, b"not a directory")?;
        assert!(storage.publish(WalPosition::default()).is_err());
        assert_eq!(fs::read(root.path().join("HEAD"))?, head);
        assert_eq!(storage.manifest(), before);
        assert_eq!(storage.stats().dirty_segments, 1);
        fs::remove_file(&manifests)?;
        fs::rename(&backup, &manifests)?;
        storage.publish(WalPosition::default())?;
        assert_eq!(
            read(&SegmentedStorage::open(root.path())?, 0, 6)?,
            b"after!"
        );
        Ok(())
    }

    #[test]
    fn bounds_eof_empty_io_and_sqlite_page_size() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        assert_eq!(storage.read_at(u64::MAX, &mut [0; 1])?, 0);
        assert_eq!(storage.write_at(u64::MAX, &[])?, 0);
        assert!(storage.write_at(u64::MAX, &[1]).is_err());
        let mut header = b"SQLite format 3\0".to_vec();
        header.extend_from_slice(&1_u16.to_be_bytes());
        storage.write_at(0, &header)?;
        storage.publish(WalPosition::default())?;
        assert_eq!(storage.manifest().page_size, 65536);
        let mut out = [77; 4];
        assert_eq!(storage.read_at(16, &mut out)?, 2);
        assert_eq!(out, [0, 1, 77, 77]);
        Ok(())
    }

    #[test]
    fn corrupt_later_segment_does_not_partially_apply_a_write() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        storage.write_at(0, &vec![42; SEGMENT_SIZE as usize * 2])?;
        storage.publish(WalPosition::default())?;
        let path = root
            .path()
            .join("segments")
            .join(&storage.manifest().segments[&1]);
        drop(storage);
        let storage = SegmentedStorage::open(root.path())?;
        assert_eq!(read(&storage, 0, 1)?, vec![42]);
        fs::remove_file(path)?;
        assert!(storage
            .write_at(u64::from(SEGMENT_SIZE) - 1, &[99, 99])
            .is_err());
        assert_eq!(read(&storage, u64::from(SEGMENT_SIZE) - 1, 1)?, vec![42]);
        assert_eq!(storage.stats().dirty_segments, 0);
        Ok(())
    }

    #[test]
    fn immutable_object_is_never_overwritten_to_hide_corruption() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        storage.write_at(0, b"data")?;
        storage.publish(WalPosition::default())?;
        let previous = storage.manifest();
        let path = root.path().join("segments").join(&previous.segments[&0]);
        fs::write(&path, b"corrupted")?;
        storage.write_at(0, b"data")?;
        assert!(storage.publish(WalPosition::default()).is_err());
        assert_eq!(fs::read(path)?, b"corrupted");
        assert_eq!(storage.manifest(), previous);
        assert_eq!(storage.stats().dirty_segments, 1);
        Ok(())
    }

    #[test]
    fn random_operations_match_a_flat_byte_vector() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut storage = SegmentedStorage::open(root.path())?;
        let mut expected = Vec::new();
        let mut random = 0x1234_5678_9abc_def0_u64;
        for step in 0..100 {
            random = random
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let offset = (random as usize) % (SEGMENT_SIZE as usize * 3);
            if step % 3 == 0 {
                storage.truncate(offset as u64)?;
                expected.resize(offset, 0);
            } else {
                let length = ((random >> 32) as usize % 2048) + 1;
                let bytes: Vec<u8> = (0..length)
                    .map(|index| (index as u64 ^ random) as u8)
                    .collect();
                storage.write_at(offset as u64, &bytes)?;
                if expected.len() < offset + length {
                    expected.resize(offset + length, 0);
                }
                expected[offset..offset + length].copy_from_slice(&bytes);
            }
            assert_eq!(storage.size(), expected.len() as u64);
            assert_eq!(read(&storage, 0, expected.len() + 10)?, expected);
            if step % 10 == 0 {
                let wal = WalPosition {
                    salts: [1, 2],
                    frame_count: step,
                };
                storage.publish(wal.clone())?;
                storage = SegmentedStorage::open(root.path())?;
                assert_eq!(storage.stats().cached_segments, 0);
                assert_eq!(storage.manifest().applied_wal_position, wal);
                assert_eq!(read(&storage, 0, expected.len() + 10)?, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn shared_reference_supports_concurrent_writers() -> Result<()> {
        let root = tempfile::tempdir()?;
        let storage = SegmentedStorage::open(root.path())?;
        std::thread::scope(|scope| {
            let mut writers = Vec::new();
            for index in 0..8_u64 {
                let shared = &storage;
                writers.push(scope.spawn(move || {
                    shared.write_at(index * u64::from(SEGMENT_SIZE), &[index as u8 + 1; 32])
                }));
            }
            for writer in writers {
                assert_eq!(writer.join().expect("writer thread failed")?, 32);
            }
            Ok::<_, anyhow::Error>(())
        })?;
        storage.publish(WalPosition::default())?;
        let recovered = SegmentedStorage::open(root.path())?;
        for index in 0..8_u64 {
            assert_eq!(
                read(&recovered, index * u64::from(SEGMENT_SIZE), 32)?,
                vec![index as u8 + 1; 32]
            );
        }
        Ok(())
    }

    #[test]
    fn abrupt_process_exit_recovers_only_the_published_generation() -> Result<()> {
        let root = tempfile::tempdir()?;
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "storage::tests::abrupt_exit_helper",
                "--nocapture",
            ])
            .env("PERI_LOOM_STORAGE_TEST_ROOT", root.path())
            .status()?;
        assert_eq!(status.code(), Some(73));
        let storage = SegmentedStorage::open(root.path())?;
        assert_eq!(storage.stats().segment_loads, 0);
        assert_eq!(storage.stats().cached_segments, 0);
        assert_eq!(storage.manifest().applied_wal_position.frame_count, 17);
        assert_eq!(read(&storage, 0, 9)?, b"published");
        assert_eq!(read(&storage, u64::from(SEGMENT_SIZE) * 2, 4)?, b"tail");
        assert_eq!(storage.stats().segment_loads, 2);
        Ok(())
    }

    #[test]
    fn abrupt_exit_helper() -> Result<()> {
        let Some(root) = std::env::var_os("PERI_LOOM_STORAGE_TEST_ROOT") else {
            return Ok(());
        };
        let storage = SegmentedStorage::open(PathBuf::from(root))?;
        storage.write_at(0, b"published")?;
        storage.write_at(u64::from(SEGMENT_SIZE) * 2, b"tail")?;
        storage.publish(WalPosition {
            salts: [5, 6],
            frame_count: 17,
        })?;
        storage.write_at(0, b"discarded")?;
        storage.truncate(1)?;
        std::process::exit(73);
    }
}
