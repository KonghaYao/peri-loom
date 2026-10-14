use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use turso_core::io::{Buffer, Completion, CompletionGroup, File, FileSyncType, OpenFlags, IO};
use turso_core::{CompletionError, LimboError, MemoryIO};

use super::{NativeFile, NativeIoGate};
use crate::io::RemoteIO;
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;

#[derive(Clone, Copy)]
enum Failure {
    NoCallback,
    PartialWrite,
    AlreadyCompleted,
}

struct FaultFile {
    inner: Arc<dyn File>,
    failure: Failure,
    calls: Arc<AtomicUsize>,
}

fn original_failure() -> LimboError {
    CompletionError::IOError(
        std::io::ErrorKind::PermissionDenied,
        "fixture synchronous failure",
    )
    .into()
}

impl FaultFile {
    fn fail(&self, completion: Completion) -> turso_core::Result<Completion> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if matches!(self.failure, Failure::AlreadyCompleted) {
            completion.error(CompletionError::ShortWrite);
        }
        Err(original_failure())
    }
}

impl File for FaultFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn pread(&self, _position: u64, completion: Completion) -> turso_core::Result<Completion> {
        self.fail(completion)
    }
    fn pwrite(
        &self,
        position: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        if matches!(self.failure, Failure::PartialWrite) {
            let _completion = self
                .inner
                .pwrite(position, buffer, Completion::new_write(|_| {}))?;
        }
        self.fail(completion)
    }
    fn sync(
        &self,
        completion: Completion,
        _sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        self.fail(completion)
    }
    fn truncate(&self, _length: u64, completion: Completion) -> turso_core::Result<Completion> {
        self.fail(completion)
    }
}

type WrappedFileFixture = (Arc<dyn File>, Arc<dyn File>, Arc<AtomicUsize>);

fn wrapped(failure: Failure, gate: Arc<NativeIoGate>) -> Result<WrappedFileFixture> {
    let memory = MemoryIO::new();
    let underlying = memory.open_file("private-provider-fixture", OpenFlags::Create, false)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let file = NativeFile::wrap_synchronous(
        Arc::new(FaultFile {
            inner: underlying.clone(),
            failure,
            calls: calls.clone(),
        }),
        gate,
    );
    Ok((file, underlying, calls))
}

fn assert_original(error: LimboError) {
    assert!(matches!(
        error,
        LimboError::CompletionError(CompletionError::IOError(
            std::io::ErrorKind::PermissionDenied,
            "fixture synchronous failure"
        ))
    ));
}

#[test]
fn synchronous_error_without_callback_finishes_leaf_preserving_original_error() -> Result<()> {
    let gate = Arc::new(NativeIoGate::default());
    let (file, _, calls) = wrapped(Failure::NoCallback, gate.clone())?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let counted = callbacks.clone();
    let completion = Completion::new_write(move |result| {
        assert_eq!(
            result.unwrap_err(),
            CompletionError::IOError(
                std::io::ErrorKind::PermissionDenied,
                "fixture synchronous failure"
            )
        );
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert_original(
        file.pwrite(0, Arc::new(Buffer::new(vec![1; 8])), completion.clone())
            .unwrap_err(),
    );
    assert!(completion.finished());
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    gate.disable();
    let _guard = gate.calls.try_write().unwrap();
    Ok(())
}

#[test]
fn partial_write_error_stays_error_and_does_not_claim_no_effects() -> Result<()> {
    let gate = Arc::new(NativeIoGate::default());
    let (file, underlying, _) = wrapped(Failure::PartialWrite, gate)?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let counted = callbacks.clone();
    let completion = Completion::new_write(move |result| {
        assert!(result.is_err());
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert_original(
        file.pwrite(0, Arc::new(Buffer::new(vec![7; 8])), completion.clone())
            .unwrap_err(),
    );
    assert_eq!(underlying.size()?, 8);
    assert!(completion.finished());
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn callback_error_then_method_error_does_not_double_finish_leaf_or_group() -> Result<()> {
    let gate = Arc::new(NativeIoGate::default());
    let (file, _, _) = wrapped(Failure::AlreadyCompleted, gate)?;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let counted = callbacks.clone();
    let completion = Completion::new_write(move |result| {
        assert_eq!(result.unwrap_err(), CompletionError::ShortWrite);
        counted.fetch_add(1, Ordering::SeqCst);
    });
    let groups = Arc::new(AtomicUsize::new(0));
    let group_count = groups.clone();
    let mut group = CompletionGroup::new(move |result| {
        assert_eq!(result.unwrap_err(), CompletionError::ShortWrite);
        group_count.fetch_add(1, Ordering::SeqCst);
    });
    group.add(&completion);
    let group = group.build();
    assert_original(
        file.pwrite(0, Arc::new(Buffer::new(vec![3; 8])), completion.clone())
            .unwrap_err(),
    );
    assert!(completion.finished());
    assert!(group.finished());
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(groups.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn error_bridge_ticket_remains_held_until_real_callback_return() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let io = RemoteIO::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("bridge_fixture", "isolated")?,
        directory.path().join("cache"),
        1,
        1024 * 1024,
        1024 * 1024,
    )?;
    let (file, _, _) = wrapped(Failure::NoCallback, io.native.clone())?;
    let latch = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_latch = latch.clone();
    let (entered, callback_entered) = mpsc::channel();
    let completion = Completion::new_write(move |result| {
        assert!(result.is_err());
        entered.send(()).unwrap();
        let (lock, changed) = &*callback_latch;
        let released = lock.lock().unwrap();
        let (released, _) = changed
            .wait_timeout_while(released, Duration::from_secs(2), |released| !*released)
            .unwrap();
        assert!(*released);
    });
    thread::scope(|scope| -> Result<()> {
        let writer =
            scope.spawn(|| file.pwrite(0, Arc::new(Buffer::new(vec![9; 8])), completion.clone()));
        callback_entered.recv_timeout(Duration::from_secs(1))?;
        assert!(io.retirement_token(Instant::now()).is_err());
        let (lock, changed) = &*latch;
        *lock.lock().unwrap() = true;
        changed.notify_all();
        assert_original(writer.join().unwrap().unwrap_err());
        assert!(completion.finished());
        let _token = io.retirement_token(Instant::now())?;
        Ok(())
    })
}

#[test]
fn undispatched_rejected_leaf_is_terminal_error_not_success() -> Result<()> {
    let gate = Arc::new(NativeIoGate::default());
    let (file, _, calls) = wrapped(Failure::NoCallback, gate.clone())?;
    gate.disable();
    let _token = gate.calls.try_write().unwrap();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let counted = callbacks.clone();
    let completion = Completion::new_write(move |result| {
        assert!(result.is_err());
        counted.fetch_add(1, Ordering::SeqCst);
    });
    assert!(file
        .pwrite(0, Arc::new(Buffer::new(vec![1; 8])), completion.clone())
        .is_err());
    assert!(completion.finished());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    Ok(())
}
