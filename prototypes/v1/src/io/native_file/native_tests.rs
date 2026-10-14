use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLockWriteGuard};

use anyhow::{bail, Result};
use turso_core::io::clock::{MonotonicInstant, WallClockInstant};
use turso_core::io::{Buffer, Clock, Completion, File, FileId, FileSyncType, OpenFlags, IO};
use turso_core::{
    CompletionError, Database, DatabaseOpts, LimboError, MemoryIO, NativeIoRetirementGuard,
    NoncommittingAbort, NoncommittingRetirement, SqliteDialect, StepResult, SyncMode,
};

use super::{NativeFile, NativeIoGate};

struct FaultIo {
    inner: MemoryIO,
    gate: Arc<NativeIoGate>,
    armed: Arc<AtomicBool>,
    failed: Arc<AtomicUsize>,
}

struct SyncFault {
    inner: Arc<dyn File>,
    armed: Arc<AtomicBool>,
    failed: Arc<AtomicUsize>,
}

impl File for SyncFault {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn pread(&self, position: u64, completion: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(position, completion)
    }
    fn pwrite(
        &self,
        position: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.pwrite(position, buffer, completion)
    }
    fn sync(
        &self,
        completion: Completion,
        sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        if self.armed.load(Ordering::Acquire) {
            self.failed.fetch_add(1, Ordering::SeqCst);
            Err(
                CompletionError::IOError(std::io::ErrorKind::Other, "fixture native sync failure")
                    .into(),
            )
        } else {
            self.inner.sync(completion, sync_type)
        }
    }
    fn truncate(&self, length: u64, completion: Completion) -> turso_core::Result<Completion> {
        self.inner.truncate(length, completion)
    }
}

impl Clock for FaultIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for FaultIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let _ticket = self.gate.enter()?;
        let inner = self.inner.open_file(path, flags, direct)?;
        Ok(NativeFile::wrap_synchronous(
            Arc::new(SyncFault {
                inner,
                armed: self.armed.clone(),
                failed: self.failed.clone(),
            }),
            self.gate.clone(),
        ))
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.remove_file(path)
    }
    fn step(&self) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.step()
    }
    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        let _ticket = self.gate.enter()?;
        self.inner.file_id(path)
    }
}

struct LocalGuard<'gate> {
    io: Arc<dyn IO>,
    _ticket: RwLockWriteGuard<'gate, ()>,
}

impl NativeIoRetirementGuard for LocalGuard<'_> {
    fn io(&self) -> &Arc<dyn IO> {
        &self.io
    }
}

#[test]
fn actual_native_failed_sync_retires_after_real_gate_without_permanent_pending() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let gate = Arc::new(NativeIoGate::default());
    let armed = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicUsize::new(0));
    let io: Arc<dyn IO> = Arc::new(FaultIo {
        inner: MemoryIO::new(),
        gate: gate.clone(),
        armed: armed.clone(),
        failed: failed.clone(),
    });
    let engine = Database::open_file_with_flags(
        io.clone(),
        directory.path().join("isolated-native").to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new().with_views(true),
        None,
        Arc::new(SqliteDialect),
    )?;
    let connection = engine.connect()?;
    connection.set_load_extension_enabled(false);
    connection.set_sync_mode(SyncMode::Full);
    connection.wal_auto_actions_disable();
    connection.execute("CREATE TABLE ledger(value INTEGER)")?;
    armed.store(true, Ordering::Release);
    let mut statement = connection.prepare("INSERT INTO ledger VALUES(17) RETURNING value")?;
    let mut error = None;
    for _attempt in 0..1000 {
        match statement.step_with_admission_check(|_| true) {
            Ok(StepResult::Row) => {}
            Ok(StepResult::IO | StepResult::Yield) => io.step()?,
            Err(failure) => {
                error = Some(failure);
                break;
            }
            other => bail!("fixture expected terminal native sync error, got {other:?}"),
        }
    }
    assert!(matches!(error, Some(LimboError::CompletionError(_))));
    assert_eq!(failed.load(Ordering::SeqCst), 1);
    assert!(!matches!(
        statement.abort_noncommitting(),
        Ok(NoncommittingAbort::StatementStopped)
    ));
    gate.disable();
    let guard = LocalGuard {
        io: io.clone(),
        _ticket: gate.calls.try_write().unwrap(),
    };
    assert!(matches!(
        statement.retire_noncommitting(&guard)?,
        NoncommittingRetirement::RetiredUnsettled(_)
    ));
    assert!(connection.is_closed());
    assert!(!connection.get_pager().holds_write_lock());
    assert!(!connection.get_pager().holds_read_lock());
    assert_eq!(failed.load(Ordering::SeqCst), 1);
    drop(statement);
    drop(connection);
    drop(engine);
    assert!(io.step().is_err());
    Ok(())
}
