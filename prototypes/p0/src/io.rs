use std::fs::{File as HostFile, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{ensure, Context};
use turso_core::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use turso_core::io::{Buffer, Completion, File, FileId, FileSyncType, OpenFlags, PlatformIO, IO};
use turso_core::{CompletionError, LimboError};

use crate::manifest::WalPosition;
use crate::storage::SegmentedStorage;

pub struct SegmentedIO {
    platform: Arc<PlatformIO>,
    main: Arc<SegmentedFile>,
    path: String,
}

struct Shared {
    storage: Arc<SegmentedStorage>,
    wal_path: PathBuf,
    gate: Mutex<PublicationState>,
    _directory_lock: HostFile,
}

struct SegmentedFile {
    shared: Arc<Shared>,
    readonly: bool,
}

struct WalFile {
    inner: Arc<dyn File>,
    shared: Arc<Shared>,
}

#[derive(Default)]
struct PublicationState {
    dirty: bool,
    failed: bool,
    pending: Option<WalPosition>,
}

impl SegmentedIO {
    pub fn open(root: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        std::fs::create_dir_all(root.as_ref())?;
        let root = root.as_ref().canonicalize()?;
        let path = root.join("database");
        ensure!(!path.exists(), "virtual database path already exists");
        let path = path
            .to_str()
            .context("database path is not UTF-8")?
            .to_owned();
        let directory_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".peri-loom.lock"))?;
        directory_lock
            .try_lock()
            .context("segmented directory is already locked")?;
        let storage = Arc::new(SegmentedStorage::open(&root)?);
        let shared = Arc::new(Shared {
            storage,
            wal_path: root.join("database-wal"),
            gate: Mutex::new(PublicationState::default()),
            _directory_lock: directory_lock,
        });
        Ok(Arc::new(Self {
            platform: Arc::new(PlatformIO::new()?),
            main: Arc::new(SegmentedFile {
                shared,
                readonly: false,
            }),
            path,
        }))
    }

    pub fn storage(&self) -> Arc<SegmentedStorage> {
        self.main.shared.storage.clone()
    }

    pub fn database_path(&self) -> String {
        self.path.clone()
    }

    fn is_main(&self, path: &str) -> bool {
        matches_path(path, Path::new(&self.path))
    }

    fn is_wal(&self, path: &str) -> bool {
        matches_path(path, &self.main.shared.wal_path)
    }
}

fn matches_path(path: &str, expected: &Path) -> bool {
    let path = Path::new(path);
    if path == expected {
        return true;
    }
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) => parent
            .canonicalize()
            .is_ok_and(|parent| parent.join(name) == expected),
        _ => false,
    }
}

impl Clock for SegmentedIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.platform.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.platform.current_time_wall_clock()
    }
}

impl IO for SegmentedIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        if self.is_main(path) {
            return Ok(Arc::new(SegmentedFile {
                shared: self.main.shared.clone(),
                readonly: flags.contains(OpenFlags::ReadOnly),
            }));
        }
        if self.is_wal(path) {
            let _guard = self.main.shared.lock()?;
            return Ok(Arc::new(WalFile {
                inner: self.platform.open_file(path, flags, direct)?,
                shared: self.main.shared.clone(),
            }));
        }
        self.platform.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        if self.is_main(path) {
            return Err(adapter_error("cannot remove virtual database"));
        }
        if self.is_wal(path) {
            let dirty = self.main.shared.lock()?;
            self.main
                .shared
                .allow_wal_reset(&dirty)
                .map_err(adapter_error)?;
            return self.platform.remove_file(path);
        }
        self.platform.remove_file(path)
    }

    fn step(&self) -> turso_core::Result<()> {
        self.platform.step()
    }

    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        if self.is_main(path) {
            Ok(FileId::from_path_hash(&self.path))
        } else {
            self.platform.file_id(path)
        }
    }
}

impl Shared {
    fn lock(&self) -> turso_core::Result<MutexGuard<'_, PublicationState>> {
        self.gate
            .lock()
            .map_err(|_| adapter_error("segmented IO mutex poisoned"))
    }

    fn publish(&self, state: &mut PublicationState) -> anyhow::Result<()> {
        state.failed = true;
        match OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.wal_path)
        {
            Ok(wal) => {
                ensure!(
                    !state.dirty || state.pending.is_some() || wal.metadata()?.len() <= 32,
                    "database writes with live WAL require checkpoint position hook"
                );
                wal.sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let position = state
            .pending
            .clone()
            .unwrap_or_else(|| self.storage.manifest().applied_wal_position);
        self.storage.publish(position)?;
        state.pending = None;
        state.dirty = false;
        state.failed = false;
        Ok(())
    }

    fn allow_wal_reset(&self, state: &PublicationState) -> anyhow::Result<()> {
        ensure!(
            !state.dirty && !state.failed && state.pending.is_none(),
            "cannot reset WAL before successful database publication"
        );
        Ok(())
    }
}

fn adapter_error(error: impl std::fmt::Display) -> LimboError {
    LimboError::InternalError(format!("segmented IO: {error}"))
}

fn finish(completion: Completion, result: anyhow::Result<usize>) -> turso_core::Result<Completion> {
    match result {
        Ok(bytes) => match i32::try_from(bytes) {
            Ok(bytes) => completion.complete(bytes),
            Err(_) => completion.error(CompletionError::IOError(
                std::io::ErrorKind::InvalidInput,
                "segmented completion byte count overflow",
            )),
        },
        Err(error) => {
            let kind = error
                .downcast_ref::<std::io::Error>()
                .map_or(std::io::ErrorKind::Other, std::io::Error::kind);
            completion.error(CompletionError::IOError(
                kind,
                "segmented IO operation failed",
            ));
            return Err(adapter_error(error));
        }
    }
    Ok(completion)
}

impl File for SegmentedFile {
    fn checkpoint_wal_position(&self, salts: [u32; 2], frame_count: u64) -> turso_core::Result<()> {
        let mut state = self.shared.lock()?;
        state.pending = Some(WalPosition { salts, frame_count });
        Ok(())
    }

    fn lock_file(&self, _exclusive: bool) -> turso_core::Result<()> {
        Ok(())
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, completion: Completion) -> turso_core::Result<Completion> {
        let result = (|| {
            let _guard = self.shared.lock()?;
            let buffer = completion.as_read().buf();
            buffer.as_mut_slice().fill(0);
            self.shared.storage.read_at(pos, buffer.as_mut_slice())
        })();
        finish(completion, result)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let result = (|| {
            ensure!(!self.readonly, "database opened read-only");
            ensure!(
                buffer.len() <= i32::MAX as usize,
                "write exceeds completion byte limit"
            );
            let mut dirty = self.shared.lock()?;
            dirty.dirty = true;
            let bytes = self.shared.storage.write_at(pos, buffer.as_slice())?;
            ensure!(bytes == buffer.len(), "short segmented write");
            Ok(bytes)
        })();
        finish(completion, result)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let result = (|| {
            ensure!(!self.readonly, "database opened read-only");
            let total = buffers
                .iter()
                .try_fold(0usize, |total, buffer| total.checked_add(buffer.len()))
                .context("vectored byte count overflow")?;
            ensure!(
                total <= i32::MAX as usize,
                "vectored write exceeds completion byte limit"
            );
            let mut dirty = self.shared.lock()?;
            let mut offset = pos;
            for buffer in buffers {
                dirty.dirty = true;
                let bytes = self.shared.storage.write_at(offset, buffer.as_slice())?;
                ensure!(bytes == buffer.len(), "short segmented vectored write");
                offset = offset
                    .checked_add(bytes as u64)
                    .context("write offset overflow")?;
            }
            Ok(total)
        })();
        finish(completion, result)
    }

    fn sync(
        &self,
        completion: Completion,
        _sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        let result = (|| {
            let mut dirty = self.shared.lock()?;
            self.shared.publish(&mut dirty)?;
            Ok(0)
        })();
        finish(completion, result)
    }

    fn size(&self) -> turso_core::Result<u64> {
        let _guard = self.shared.lock()?;
        Ok(self.shared.storage.size())
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        let result = (|| {
            ensure!(!self.readonly, "database opened read-only");
            let mut dirty = self.shared.lock()?;
            dirty.dirty = true;
            self.shared.storage.truncate(len)?;
            Ok(0)
        })();
        finish(completion, result)
    }
}

impl File for WalFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, completion: Completion) -> turso_core::Result<Completion> {
        let _guard = self.shared.lock()?;
        self.inner.pread(pos, completion)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let dirty = self.shared.lock()?;
        if pos < 32 {
            if let Err(error) = self.shared.allow_wal_reset(&dirty) {
                return finish(completion, Err(error));
            }
        }
        self.inner.pwrite(pos, buffer, completion)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        let dirty = self.shared.lock()?;
        if pos < 32 {
            if let Err(error) = self.shared.allow_wal_reset(&dirty) {
                return finish(completion, Err(error));
            }
        }
        self.inner.pwritev(pos, buffers, completion)
    }

    fn sync(
        &self,
        completion: Completion,
        sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        let mut state = self.shared.lock()?;
        let result = self.inner.sync(completion, sync_type);
        if result
            .as_ref()
            .map_or(true, |completion| completion.get_error().is_some())
        {
            state.failed = true;
        }
        result
    }

    fn size(&self) -> turso_core::Result<u64> {
        let _guard = self.shared.lock()?;
        self.inner.size()
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        let dirty = self.shared.lock()?;
        if let Err(error) = self.shared.allow_wal_reset(&dirty) {
            return finish(completion, Err(error));
        }
        self.inner.truncate(len, completion)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn main_file(io: &SegmentedIO) -> Arc<dyn File> {
        io.open_file(&io.database_path(), OpenFlags::Create, false)
            .unwrap()
    }

    fn write(file: &dyn File, offset: u64, bytes: &[u8]) -> i32 {
        let count = Arc::new(Mutex::new(None));
        let observed = count.clone();
        let completion = Completion::new_write(move |result| {
            *observed.lock().unwrap() = Some(result.unwrap());
        });
        let buffer = Arc::new(Buffer::new_temporary(bytes.len()));
        buffer.as_mut_slice().copy_from_slice(bytes);
        let completion = file.pwrite(offset, buffer, completion).unwrap();
        assert!(completion.finished());
        let bytes = count.lock().unwrap().unwrap();
        bytes
    }

    fn sync(file: &dyn File) -> turso_core::Result<()> {
        let completion = file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?;
        assert!(completion.finished());
        Ok(())
    }

    #[test]
    fn virtual_database_read_counts_and_truncate_sync() {
        let directory = tempfile::tempdir().unwrap();
        let io = SegmentedIO::open(directory.path()).unwrap();
        let file = main_file(&io);
        assert_eq!(write(file.as_ref(), 5, b"abcdef"), 6);
        let buffer = Arc::new(Buffer::new_temporary(16));
        buffer.as_mut_slice().fill(255);
        let count = Arc::new(Mutex::new(None));
        let observed = count.clone();
        let completion = Completion::new_read(
            buffer.clone(),
            Box::new(move |result: Result<(Arc<Buffer>, i32), CompletionError>| {
                let (_, bytes) = result.unwrap();
                *observed.lock().unwrap() = Some(bytes);
                None
            }),
        );
        assert!(file.pread(5, completion).unwrap().finished());
        assert_eq!(*count.lock().unwrap(), Some(6));
        assert_eq!(&buffer.as_slice()[..6], b"abcdef");
        assert!(buffer.as_slice()[6..].iter().all(|byte| *byte == 0));
        sync(file.as_ref()).unwrap();
        let completion = file
            .truncate(
                7,
                Completion::new_trunc(|result| assert_eq!(result.unwrap(), 0)),
            )
            .unwrap();
        assert!(completion.finished());
        assert_eq!(io.storage().size(), 7);
        assert_eq!(io.storage().manifest().logical_db_size, 11);
        sync(file.as_ref()).unwrap();
        assert_eq!(io.storage().manifest().logical_db_size, 7);
        assert!(!Path::new(&io.database_path()).exists());
        assert_eq!(
            io.file_id(&io.database_path()).unwrap(),
            FileId::from_path_hash(&io.database_path())
        );
        drop(file);
        drop(io);
        let reopened = SegmentedIO::open(directory.path()).unwrap();
        assert_eq!(reopened.storage().size(), 7);
        let mut bytes = [0; 7];
        assert_eq!(reopened.storage().read_at(0, &mut bytes).unwrap(), 7);
        assert_eq!(&bytes, b"\0\0\0\0\0ab");
    }

    #[test]
    fn exact_partial_position_and_invalid_tail_are_not_inferred() {
        let directory = tempfile::tempdir().unwrap();
        let io = SegmentedIO::open(directory.path()).unwrap();
        std::fs::write(directory.path().join("database-wal"), [17; 200]).unwrap();
        let file = main_file(&io);
        write(file.as_ref(), 0, b"checkpointed prefix");
        file.checkpoint_wal_position([11, 22], 3).unwrap();
        sync(file.as_ref()).unwrap();
        assert_eq!(
            io.storage().manifest().applied_wal_position,
            WalPosition {
                salts: [11, 22],
                frame_count: 3
            }
        );
        assert_eq!(
            std::fs::metadata(directory.path().join("database-wal"))
                .unwrap()
                .len(),
            200
        );
    }

    #[test]
    fn failed_publication_blocks_wal_reset_until_successful_retry() {
        let directory = tempfile::tempdir().unwrap();
        let io = SegmentedIO::open(directory.path()).unwrap();
        let wal_path = directory.path().join("database-wal");
        let wal = io
            .open_file(wal_path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();
        assert_eq!(write(wal.as_ref(), 0, &[0; 64]), 64);
        let file = main_file(&io);
        write(file.as_ref(), 0, b"unpublished");
        let completion = Completion::new_sync(|result| assert!(result.is_err()));
        assert!(file.sync(completion.clone(), FileSyncType::Fsync).is_err());
        assert!(completion.finished());
        assert!(completion.get_error().is_some());
        assert!(wal
            .truncate(0, Completion::new_trunc(|result| assert!(result.is_err())))
            .is_err());
        assert!(io.remove_file(wal_path.to_str().unwrap()).is_err());
        assert_eq!(wal.size().unwrap(), 64);
        file.checkpoint_wal_position([9, 10], 2).unwrap();
        sync(file.as_ref()).unwrap();
        assert!(file
            .truncate(3, Completion::new_trunc(|_| {}))
            .unwrap()
            .finished());
        assert_eq!(io.storage().manifest().logical_db_size, 11);
        assert!(wal.truncate(0, Completion::new_trunc(|_| {})).is_err());
        file.checkpoint_wal_position([9, 10], 2).unwrap();
        sync(file.as_ref()).unwrap();
        assert_eq!(io.storage().manifest().logical_db_size, 3);
        let completion = wal
            .truncate(
                0,
                Completion::new_trunc(|result| assert_eq!(result.unwrap(), 0)),
            )
            .unwrap();
        assert!(completion.finished());
        assert_eq!(wal.size().unwrap(), 0);
        assert_eq!(io.storage().manifest().applied_wal_position.frame_count, 2);
    }

    #[test]
    fn readonly_and_vectored_completion_errors_are_finished() {
        let directory = tempfile::tempdir().unwrap();
        let io = SegmentedIO::open(directory.path()).unwrap();
        let readonly = io
            .open_file(&io.database_path(), OpenFlags::ReadOnly, false)
            .unwrap();
        let completion = Completion::new_write(|result| assert!(result.is_err()));
        assert!(readonly
            .pwritev(
                0,
                vec![Arc::new(Buffer::new_temporary(3))],
                completion.clone()
            )
            .is_err());
        assert!(completion.finished());
        assert_eq!(io.storage().size(), 0);
        let file = main_file(&io);
        let first = Arc::new(Buffer::new_temporary(3));
        first.as_mut_slice().copy_from_slice(b"abc");
        let second = Arc::new(Buffer::new_temporary(2));
        second.as_mut_slice().copy_from_slice(b"de");
        let completion = file
            .pwritev(
                0,
                vec![first, second],
                Completion::new_write(|result| assert_eq!(result.unwrap(), 5)),
            )
            .unwrap();
        assert!(completion.finished());
        assert_eq!(file.size().unwrap(), 5);
        io.step().unwrap();
        let _monotonic = io.current_time_monotonic();
        let _wall = io.current_time_wall_clock();
    }

    #[test]
    fn directory_lock_covers_same_process_and_file_handle_lifetime() {
        let directory = tempfile::tempdir().unwrap();
        let io = SegmentedIO::open(directory.path()).unwrap();
        assert!(SegmentedIO::open(directory.path()).is_err());
        let file = main_file(&io);
        drop(io);
        assert!(SegmentedIO::open(directory.path()).is_err());
        file.unlock_file().unwrap();
        assert!(SegmentedIO::open(directory.path()).is_err());
        drop(file);
        assert!(SegmentedIO::open(directory.path()).is_ok());
    }

    #[test]
    fn directory_lock_process_probe() {
        if let Some(root) = std::env::var_os("PERI_LOOM_IO_LOCK_PROBE") {
            assert!(SegmentedIO::open(root).is_err());
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let _io = SegmentedIO::open(directory.path()).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "io::tests::directory_lock_process_probe",
                "--nocapture",
            ])
            .env("PERI_LOOM_IO_LOCK_PROBE", directory.path())
            .env("LIMBO_DISABLE_FILE_LOCK", "1")
            .status()
            .unwrap();
        assert!(status.success());
    }
}
