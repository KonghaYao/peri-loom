use super::{
    check_condition, fresh_etag, validate_key, validate_prefix, ObjectStore, PreconditionFailed,
    PutCondition, StoredObject,
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub struct LocalObjectStore {
    root: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    key: String,
    #[serde(with = "super::payload")]
    bytes: Vec<u8>,
    etag: String,
    checksum: String,
}

impl LocalObjectStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        ensure!(
            !fs::symlink_metadata(root)?.file_type().is_symlink(),
            "symlink store root rejected"
        );
        let store = Self {
            root: root.canonicalize()?,
        };
        let _guard = store.lock()?;
        sync_directory(&store.root)?;
        if let Some(parent) = store.root.parent() {
            sync_directory(parent)?;
        }
        Ok(store)
    }

    fn temporary_path(&self, etag: &str) -> PathBuf {
        self.root.join(format!(".{etag}.tmp"))
    }

    fn lock(&self) -> Result<File> {
        let path = self.root.join(".lock");
        reject_symlink(&path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        lock.lock()?;
        Ok(lock)
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root
            .join(format!("{}.obj", blake3::hash(key.as_bytes()).to_hex()))
    }

    fn read(&self, key: &str) -> Result<Option<StoredObject>> {
        let started = std::time::Instant::now();
        let path = self.path(key);
        reject_symlink(&path)?;
        let encoded = match fs::read(path) {
            Ok(encoded) => encoded,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let envelope: Envelope =
            serde_json::from_slice(&encoded).context("invalid local object envelope")?;
        ensure!(envelope.key == key, "local object key mismatch");
        ensure!(
            envelope.checksum == blake3::hash(&envelope.bytes).to_hex().as_str(),
            "local object checksum mismatch"
        );
        ensure!(!envelope.etag.is_empty(), "missing local object etag");
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "NATIVE_LOCAL_READ pid={} key={key} bytes={} encoded_bytes={} elapsed_us={}",
                std::process::id(),
                envelope.bytes.len(),
                encoded.len(),
                started.elapsed().as_micros()
            );
        }
        Ok(Some(StoredObject {
            bytes: envelope.bytes,
            etag: envelope.etag,
        }))
    }
}

thread_local! {
    /// Durability barriers issued by the current thread. `sync_file` and
    /// `sync_directory` are the only paths that reach the filesystem with a
    /// durability request, so this counter is the barrier count a commit pays.
    static BARRIERS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Barriers issued by the current thread; thread-local so a concurrent test
/// cannot shift the count a measurement is reading. Test-only: production code
/// never reads the counter.
#[cfg(test)]
pub(crate) fn barrier_count() -> u64 {
    BARRIERS.with(std::cell::Cell::get)
}

fn note_barrier() {
    BARRIERS.with(|count| count.set(count.get().saturating_add(1)));
}

fn sync_file(file: &File) -> std::io::Result<()> {
    note_barrier();
    file.sync_all()
}

fn sync_directory(path: &Path) -> Result<()> {
    note_barrier();
    File::open(path)?.sync_all()?;
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "nonregular store file rejected"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

impl ObjectStore for LocalObjectStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let started = std::time::Instant::now();
        validate_key(key)?;
        let result = self.read(key);
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "NATIVE_LOCAL_GET pid={} key={key} elapsed_us={} ok={}",
                std::process::id(),
                started.elapsed().as_micros(),
                result.is_ok()
            );
        }
        result
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let started = std::time::Instant::now();
        validate_key(key)?;
        let _guard = self.lock()?;
        check_condition(self.read(key)?.as_ref(), &condition)?;
        let etag = fresh_etag(bytes);
        let envelope = Envelope {
            key: key.to_owned(),
            bytes: bytes.to_vec(),
            etag: etag.clone(),
            checksum: blake3::hash(bytes).to_hex().to_string(),
        };
        let temporary = self.temporary_path(&etag);
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            let started = std::time::Instant::now();
            let encoded = serde_json::to_vec(&envelope)?;
            if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
                eprintln!(
                    "NATIVE_LOCAL_ENCODE pid={} key={key} bytes={} encoded_bytes={} elapsed_us={}",
                    std::process::id(),
                    bytes.len(),
                    encoded.len(),
                    started.elapsed().as_micros()
                );
            }
            file.write_all(&encoded)?;
            sync_file(&file)?;
            fs::rename(&temporary, self.path(key))?;
            sync_directory(&self.root)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "NATIVE_LOCAL_PUT pid={} key={key} bytes={} elapsed_us={}",
                std::process::id(),
                bytes.len(),
                started.elapsed().as_micros()
            );
        }
        Ok(etag)
    }

    /// Group publish with one directory barrier for the whole group.
    ///
    /// Each body still syncs its own data (the group must be durable, not just
    /// visible), but the rename step costs a single directory sync instead of
    /// one per write. Barrier count: `writes.len() + 1` instead of
    /// `2 * writes.len()`.
    fn put_group(&self, writes: &[super::GroupWrite]) -> Result<Vec<String>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        if writes.len() == 1 {
            let write = &writes[0];
            return Ok(vec![self.put(
                &write.key,
                &write.bytes,
                write.condition.clone(),
            )?]);
        }
        for write in writes {
            validate_key(&write.key)?;
        }
        let _guard = self.lock()?;
        // Evaluate every condition against the pre-group state first, so a
        // rejected group leaves no partially visible write behind.
        let mut etags = Vec::with_capacity(writes.len());
        for write in writes {
            check_condition(self.read(&write.key)?.as_ref(), &write.condition)?;
            etags.push(fresh_etag(&write.bytes));
        }
        let mut staged = Vec::with_capacity(writes.len());
        let result = (|| -> Result<()> {
            for (write, etag) in writes.iter().zip(etags.iter()) {
                let envelope = Envelope {
                    key: write.key.clone(),
                    bytes: write.bytes.clone(),
                    etag: etag.clone(),
                    checksum: blake3::hash(&write.bytes).to_hex().to_string(),
                };
                let temporary = self.temporary_path(etag);
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)?;
                staged.push((temporary, self.path(&write.key)));
                file.write_all(&serde_json::to_vec(&envelope)?)?;
                sync_file(&file)?;
            }
            for (temporary, target) in &staged {
                fs::rename(temporary, target)?;
            }
            sync_directory(&self.root)?;
            Ok(())
        })();
        if result.is_err() {
            for (temporary, _) in &staged {
                let _ = fs::remove_file(temporary);
            }
        }
        result?;
        Ok(etags)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        validate_key(key)?;
        let _guard = self.lock()?;
        let Some(current) = self.read(key)? else {
            return Ok(());
        };
        if let Some(expected) = expected {
            ensure!(current.etag == expected, PreconditionFailed);
        }
        fs::remove_file(self.path(key))?;
        sync_directory(&self.root)?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        let _guard = self.lock()?;
        let mut keys = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "obj")
            {
                reject_symlink(&entry.path())?;
                let envelope: Envelope = serde_json::from_slice(&fs::read(entry.path())?)?;
                validate_key(&envelope.key)?;
                ensure!(
                    self.path(&envelope.key) == entry.path(),
                    "local object filename mismatch"
                );
                self.read(&envelope.key)?;
                if envelope.key.starts_with(prefix) {
                    keys.push(envelope.key);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}

#[cfg(test)]
mod group_tests {
    use super::*;
    use crate::object_store::GroupWrite;

    #[test]
    fn group_publish_pays_one_directory_barrier_for_the_whole_group() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes: Vec<GroupWrite> = (0..6)
            .map(|index| GroupWrite::absent(format!("pages/{index:02}"), vec![index as u8; 4096]))
            .collect();

        let before = barrier_count();
        let etags = store.put_group(&writes).unwrap();
        let grouped = barrier_count() - before;

        let before = barrier_count();
        for (index, write) in writes.iter().enumerate() {
            store
                .put(
                    &format!("sequential/{index:02}"),
                    &write.bytes,
                    PutCondition::Absent,
                )
                .unwrap();
        }
        let sequential = barrier_count() - before;

        assert_eq!(etags.len(), 6);
        assert_eq!(grouped, 7, "6 body syncs + 1 directory sync");
        assert_eq!(sequential, 12, "2 barriers per write");
        for (write, etag) in writes.iter().zip(etags) {
            let stored = store.get(&write.key).unwrap().unwrap();
            assert_eq!(stored.bytes, write.bytes);
            assert_eq!(stored.etag, etag);
        }
    }

    #[test]
    fn rejected_group_leaves_no_write_behind() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        store.put("root", b"current", PutCondition::Any).unwrap();
        let writes = vec![
            GroupWrite::absent("pages/00", vec![7; 64]),
            GroupWrite::new(
                "root",
                b"next".to_vec(),
                PutCondition::Match("stale".into()),
            ),
        ];
        assert!(store.put_group(&writes).is_err());
        assert!(store.get("pages/00").unwrap().is_none());
        assert_eq!(store.get("root").unwrap().unwrap().bytes, b"current");
    }
}

#[cfg(test)]
mod group_latency_tests {
    use super::*;
    use crate::object_store::GroupWrite;
    use std::time::Instant;

    /// Prints the wall cost of one directory barrier on this host, so the
    /// reduction can be turned into milliseconds without re-running the fixture.
    #[test]
    fn group_publish_latency_delta() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes: Vec<GroupWrite> = (0..6)
            .map(|index| GroupWrite::absent(format!("pages/{index:02}"), vec![index as u8; 4096]))
            .collect();
        let mut grouped = Vec::new();
        let mut sequential = Vec::new();
        for round in 0..12 {
            let prefix = format!("g{round}");
            let batch: Vec<GroupWrite> = writes
                .iter()
                .enumerate()
                .map(|(index, write)| {
                    GroupWrite::absent(format!("{prefix}/{index:02}"), write.bytes.clone())
                })
                .collect();
            let started = Instant::now();
            store.put_group(&batch).unwrap();
            grouped.push(started.elapsed().as_micros());

            let started = Instant::now();
            for (index, write) in batch.iter().enumerate() {
                store
                    .put(
                        &format!("{prefix}/s{index:02}"),
                        &write.bytes,
                        PutCondition::Absent,
                    )
                    .unwrap();
            }
            sequential.push(started.elapsed().as_micros());
        }
        grouped.sort_unstable();
        sequential.sort_unstable();
        let median = |values: &[u128]| values[values.len() / 2];
        println!(
            "GROUP_LATENCY writes=6 grouped_us_median={} grouped_us_min={} sequential_us_median={} sequential_us_min={} saved_us_median={} per_barrier_us={}",
            median(&grouped),
            grouped[0],
            median(&sequential),
            sequential[0],
            median(&sequential) - median(&grouped),
            (median(&sequential) - median(&grouped)) / 5,
        );
    }
}
