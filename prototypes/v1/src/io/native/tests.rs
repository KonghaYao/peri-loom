use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use turso_core::io::{Buffer, Completion, OpenFlags, IO};

use super::{NativeIoGate, RemoteIO};
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
use crate::session::AdmissionError;

#[derive(Default)]
struct CallbackLatch {
    released: Mutex<bool>,
    changed: Condvar,
}

impl CallbackLatch {
    fn wait(&self) {
        let released = self.released.lock().unwrap();
        let (released, _) = self
            .changed
            .wait_timeout_while(released, Duration::from_secs(2), |released| !*released)
            .unwrap();
        assert!(*released, "callback release deadline");
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

fn fixture() -> Result<(tempfile::TempDir, Arc<RemoteIO>)> {
    let directory = tempfile::tempdir()?;
    let io = RemoteIO::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("retirement_test", "io")?,
        directory.path().join("cache"),
        1,
        1024 * 1024,
        1024 * 1024,
    )?;
    Ok((directory, io))
}

fn wait_disabled(gate: &NativeIoGate) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !gate.disabled.load(std::sync::atomic::Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "retirement admission did not close"
        );
        thread::yield_now();
    }
}

fn callback_exit_is_required(host_file: bool) -> Result<()> {
    let (directory, io) = fixture()?;
    let path = if host_file {
        directory
            .path()
            .join("tursodb_temp_file")
            .to_str()
            .unwrap()
            .to_owned()
    } else {
        io.database_path()
    };
    let file = io.open_file(&path, OpenFlags::Create, false)?;
    let latch = Arc::new(CallbackLatch::default());
    let callback_latch = latch.clone();
    let (entered, callback_entered) = mpsc::channel();
    let completion = Completion::new_write(move |result| {
        assert!(result.is_ok());
        entered.send(()).unwrap();
        callback_latch.wait();
    });
    let (done, retired) = mpsc::channel();
    thread::scope(|scope| -> Result<()> {
        let writer = file.clone();
        let write =
            scope.spawn(move || writer.pwrite(0, Arc::new(Buffer::new_temporary(8)), completion));
        callback_entered.recv_timeout(Duration::from_secs(1))?;
        let retiring = io.clone();
        let retire = scope.spawn(move || {
            let result = retiring.retirement_token(Instant::now() + Duration::from_secs(1));
            done.send(result.is_ok()).unwrap();
        });
        wait_disabled(&io.native);
        assert!(file.size().is_err());
        assert!(io.open_file(&path, OpenFlags::Create, false).is_err());
        assert!(matches!(retired.try_recv(), Err(mpsc::TryRecvError::Empty)));
        latch.release();
        let _completion = write.join().unwrap()?;
        assert!(retired.recv_timeout(Duration::from_secs(1))?);
        retire.join().unwrap();
        Ok(())
    })?;
    assert!(file.size().is_err());
    assert!(io.step().is_err());
    Ok(())
}

#[test]
fn remote_file_retirement_waits_for_actual_callback_return() -> Result<()> {
    callback_exit_is_required(false)
}

#[test]
fn unix_host_temporary_file_retirement_waits_for_actual_callback_return() -> Result<()> {
    callback_exit_is_required(true)
}

#[test]
fn expired_drain_deadline_preserves_disabled_dispatch_without_resetting_budget() -> Result<()> {
    let (_directory, io) = fixture()?;
    let ticket = io.native.enter()?;
    let error = io.retirement_token(Instant::now()).err().unwrap();
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline)
    );
    assert!(io.native.enter().is_err());
    drop(ticket);
    let token = io.retirement_token(Instant::now())?;
    let expected: Arc<dyn IO> = io.clone();
    assert!(Arc::ptr_eq(&token.io, &expected));
    drop(token);
    assert!(io.native.enter().is_err());
    Ok(())
}

#[test]
fn completed_or_cancelled_token_does_not_replace_actual_call_exit() -> Result<()> {
    let (_directory, io) = fixture()?;
    let ticket = io.native.enter()?;
    let completion = Completion::new_write(|_| {});
    completion.abort();
    assert!(completion.finished());
    let error = io.retirement_token(Instant::now()).err().unwrap();
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline)
    );
    drop(ticket);
    let token = io.retirement_token(Instant::now())?;
    drop(token);
    Ok(())
}

#[test]
fn first_enabled_check_racing_retirement_does_not_wait_for_token_drop() -> Result<()> {
    let gate = Arc::new(NativeIoGate::default());
    let (checked, reached) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    let (returned, rejected) = mpsc::channel();
    thread::scope(|scope| -> Result<()> {
        let reader = gate.clone();
        let incoming = scope.spawn(move || {
            let mut first = true;
            let result = reader.enter_checked(|| {
                if first {
                    first = false;
                    checked.send(()).unwrap();
                    resumed.recv_timeout(Duration::from_secs(1)).unwrap();
                }
            });
            returned.send(result.is_err()).unwrap();
        });
        reached.recv_timeout(Duration::from_secs(1))?;
        gate.disable();
        let token = gate.calls.try_write().unwrap();
        resume.send(())?;
        let rejection = rejected.recv_timeout(Duration::from_millis(200));
        drop(token);
        incoming.join().unwrap();
        assert!(rejection?);
        Ok(())
    })
}
