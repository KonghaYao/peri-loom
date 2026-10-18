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

/// One body staged for a group publication. `file` is `None` when an earlier attempt
/// already landed identical bytes: nothing to flush, and the stored etag is kept.
struct StagedBody {
    temporary: Option<PathBuf>,
    target: PathBuf,
    file: Option<File>,
    etag: String,
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
        flush_directory(&store.root)?;
        if let Some(parent) = store.root.parent() {
            flush_directory(parent)?;
        }
        publish_directory_barrier(&store.root)?;
        Ok(store)
    }

    fn temporary_path(&self, etag: &str) -> PathBuf {
        self.root.join(format!(".{etag}.tmp"))
    }

    /// Encode and flush one group body. Called from one staging thread at a time, so it
    /// only touches the file and the immutable `write` it was handed.
    fn stage_body(&self, write: &super::GroupWrite, etag: String) -> Result<StagedBody> {
        let envelope = Envelope {
            key: write.key.clone(),
            bytes: write.bytes.clone(),
            etag: etag.clone(),
            checksum: blake3::hash(&write.bytes).to_hex().to_string(),
        };
        let target = self.path(&write.key);
        let bytes = serde_json::to_vec(&envelope)?;
        // Content-addressed bodies with `Any` cannot collide with a live key, so they
        // are written straight to their final name: the rename and the pre-group read
        // bought no condition and the body bytes never change under an identical key.
        // Anything else keeps the temp+rename path, because `Match` *is* the head CAS
        // and must stay atomic.
        if matches!(write.condition, PutCondition::Any) {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target)
            {
                Ok(mut file) => {
                    file.write_all(&bytes)?;
                    flush_file(&file)?;
                    return Ok(StagedBody {
                        temporary: None,
                        target,
                        file: Some(file),
                        etag,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    match self.read(&write.key) {
                        Ok(Some(existing)) if existing.bytes == write.bytes => {
                            // A body an earlier attempt already landed: keep its etag so
                            // a caller that later fences on this key uses the value the
                            // store actually holds.
                            return Ok(StagedBody {
                                temporary: None,
                                target,
                                file: None,
                                etag: existing.etag,
                            });
                        }
                        Ok(Some(_)) => {
                            anyhow::bail!("immutable object conflict: {}", write.key)
                        }
                        Ok(None) => {}
                        // A partial body from a crashed attempt fails its envelope, so
                        // no durable head can reference it.
                        Err(_) => {
                            fs::remove_file(&target)?;
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        let temporary = self.temporary_path(&etag);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        flush_file(&file)?;
        Ok(StagedBody {
            temporary: Some(temporary),
            target,
            file: Some(file),
            etag,
        })
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
    /// Hardware barriers issued by the current thread. `publish_barrier` is the only
    /// path that asks the platform to push the device cache to media, so this counter
    /// is the barrier count a publication pays.
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

/// Bytes reached the device. Cheap, and never a media flush: the publication still
/// needs its `publish_barrier`.
fn flush_file(file: &File) -> std::io::Result<()> {
    barrier::flush(file)
}

fn flush_directory(path: &Path) -> Result<()> {
    barrier::flush(&File::open(path)?)?;
    Ok(())
}

/// The one hardware barrier a publication pays, after every write and rename it
/// flushed. `F_FULLFSYNC` is device-wide on macOS, so one call covers the whole
/// publication: without this split a group of N objects pays N media flushes, which
/// is what made one committing statement cost tens of milliseconds of pure barrier.
fn publish_barrier(file: &File) -> std::io::Result<()> {
    note_barrier();
    barrier::durable(file)
}

fn publish_directory_barrier(path: &Path) -> Result<()> {
    let directory = File::open(path)?;
    publish_barrier(&directory)?;
    Ok(())
}

#[cfg(target_os = "macos")]
mod barrier {
    use std::fs::File;
    use std::io;
    use std::os::fd::AsRawFd;

    /// `File::sync_all` and `File::sync_data` both reach `F_FULLFSYNC` on macOS, so a
    /// group of writes pays one media flush per object. A publication wants the opposite
    /// order instead: cheap `fsync(2)` per object to order bytes into the device cache,
    /// then one device-wide flush at the end.
    pub(super) fn flush(file: &File) -> io::Result<()> {
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn durable(file: &File) -> io::Result<()> {
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == 0 {
            Ok(())
        } else {
            // A volume that cannot flush its own cache still needs the write ordered.
            flush(file)
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod barrier {
    use std::fs::File;
    use std::io;

    /// `fsync` is already the media barrier on these platforms.
    pub(super) fn flush(file: &File) -> io::Result<()> {
        file.sync_all()
    }

    pub(super) fn durable(_file: &File) -> io::Result<()> {
        Ok(())
    }
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

    fn group_rejection_publishes_nothing(&self) -> bool {
        // `put_group` checks every condition before it stages the first body, so a
        // rejected group leaves nothing behind on this backend.
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
        // `Any` imposes no condition, so the existing object cannot change the outcome and
        // reading it would only decode and checksum bytes the publication overwrites.
        if !matches!(condition, PutCondition::Any) {
            check_condition(self.read(key)?.as_ref(), &condition)?;
        }
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
            flush_file(&file)?;
            fs::rename(&temporary, self.path(key))?;
            flush_directory(&self.root)?;
            publish_barrier(&file)?;
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

    /// Group publish that reaches media with one barrier.
    ///
    /// Each body flushes its own data (the group must be durable, not just visible) and
    /// the rename step costs a single directory flush, but the media barrier is paid
    /// once for the whole group: `F_FULLFSYNC` is device-wide, so it covers every body
    /// and rename the group already flushed. Barrier count: 1, whatever the group size.
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
            // A group is mostly content-addressed bodies with `Any`, and for those the
            // pre-group check is vacuous: skipping it avoids one open/decode per body.
            if !matches!(write.condition, PutCondition::Any) {
                check_condition(self.read(&write.key)?.as_ref(), &write.condition)?;
            }
            etags.push(fresh_etag(&write.bytes));
        }
        // A publication is 25-30 bodies on the steady path and several hundred when a
        // receipt roll lands. The per-body cost is mostly encoding: serde, base64 and
        // blake3 run on one core while the drive sits idle. The bodies are independent
        // and content addressed, so they are staged on a bounded set of threads; the
        // renames, the one directory flush and the one media barrier still happen in
        // publication order on this thread, after every body is durable.
        let workers = if writes.len() < 8 {
            1
        } else {
            std::thread::available_parallelism()
                .map(|parallel| parallel.get())
                .unwrap_or(4)
                .clamp(2, 8)
        };
        let staged: Vec<Result<StagedBody>> = if workers == 1 {
            (0..writes.len())
                .map(|index| self.stage_body(&writes[index], etags[index].clone()))
                .collect()
        } else {
            std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(workers);
                for offset in 0..workers {
                    let etags = &etags;
                    let writes = writes;
                    handles.push(scope.spawn(move || {
                        (offset..writes.len())
                            .step_by(workers)
                            .map(|index| {
                                (index, self.stage_body(&writes[index], etags[index].clone()))
                            })
                            .collect::<Vec<_>>()
                    }));
                }
                let mut staged: Vec<(usize, Result<StagedBody>)> = handles
                    .into_iter()
                    .flat_map(|handle| handle.join().unwrap())
                    .collect();
                staged.sort_by_key(|(index, _)| *index);
                staged.into_iter().map(|(_, result)| result).collect()
            })
        };
        let mut bodies = Vec::with_capacity(staged.len());
        let mut failure = None;
        for result in staged {
            match result {
                Ok(body) => bodies.push(body),
                Err(error) => failure = failure.or(Some(error)),
            }
        }
        if let Some(error) = failure {
            for body in &bodies {
                if let Some(temporary) = &body.temporary {
                    let _ = fs::remove_file(temporary);
                }
            }
            return Err(error);
        }
        let result = (|| -> Result<()> {
            for body in &bodies {
                if let Some(temporary) = &body.temporary {
                    fs::rename(temporary, &body.target)?;
                }
            }
            if let Some(file) = bodies.iter().find_map(|body| body.file.as_ref()) {
                flush_directory(&self.root)?;
                publish_barrier(file)?;
            }
            Ok(())
        })();
        if result.is_err() {
            for body in &bodies {
                if let Some(temporary) = &body.temporary {
                    let _ = fs::remove_file(temporary);
                }
            }
        }
        result?;
        Ok(bodies.into_iter().map(|body| body.etag).collect())
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
        flush_directory(&self.root)?;
        publish_directory_barrier(&self.root)?;
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
    fn group_publish_pays_one_media_barrier_for_the_whole_group() {
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
        assert_eq!(grouped, 1, "the whole group reaches media with one barrier");
        assert_eq!(sequential, 6, "one barrier per single write");
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

    #[test]
    fn repeated_content_addressed_body_keeps_its_stored_etag() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let stored = store.put("pages/a", b"body", PutCondition::Any).unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"body".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/b", b"other".to_vec(), PutCondition::Any),
        ];
        let etags = store.put_group(&writes).unwrap();
        assert_eq!(etags[0], stored, "a skip reports the etag the store holds");
        assert_eq!(store.get("pages/a").unwrap().unwrap().etag, stored);
        assert_eq!(store.get("pages/b").unwrap().unwrap().bytes, b"other");
    }

    #[test]
    fn partial_body_from_a_crashed_attempt_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let target = store.path("pages/a");
        fs::write(&target, b"partial envelope").unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"complete body".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/b", b"another body".to_vec(), PutCondition::Any),
        ];
        store.put_group(&writes).unwrap();
        assert_eq!(
            store.get("pages/a").unwrap().unwrap().bytes,
            b"complete body"
        );
        assert_eq!(
            store.get("pages/b").unwrap().unwrap().bytes,
            b"another body"
        );
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

    /// Samples the shape a publication actually writes: 25 immutable bodies per group.
    /// `Any` takes the direct-write path; `Absent` keeps the temp+rename path, so the
    /// difference is the rename the direct path removes for content-addressed bodies.
    #[test]
    fn content_addressed_group_latency_sample() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let mut direct = Vec::new();
        let mut renamed = Vec::new();
        for round in 0..20 {
            let batch: Vec<GroupWrite> = (0..25)
                .map(|index| {
                    GroupWrite::new(
                        format!("d{round}/{index:02}"),
                        vec![(round * 25 + index) as u8; 4096],
                        PutCondition::Any,
                    )
                })
                .collect();
            let started = Instant::now();
            store.put_group(&batch).unwrap();
            direct.push(started.elapsed().as_micros());

            let batch: Vec<GroupWrite> = (0..25)
                .map(|index| {
                    GroupWrite::absent(
                        format!("a{round}/{index:02}"),
                        vec![(round * 25 + index) as u8; 4096],
                    )
                })
                .collect();
            let started = Instant::now();
            store.put_group(&batch).unwrap();
            renamed.push(started.elapsed().as_micros());
        }
        direct.sort_unstable();
        renamed.sort_unstable();
        let median = |values: &[u128]| values[values.len() / 2];
        println!(
            "GROUP_ANY_LATENCY writes=25 direct_us_median={} direct_us_min={} renamed_us_median={} renamed_us_min={} saved_us_median={}",
            median(&direct),
            direct[0],
            median(&renamed),
            renamed[0],
            median(&renamed) - median(&direct),
        );
    }
}
