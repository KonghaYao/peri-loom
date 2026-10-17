//! Lock-order regressions for the snapshots taken beside a publication.
//!
//! `RemoteStorage::publish` holds the storage lock across its remote writes. Any caller
//! that takes the storage lock while holding the WAL state lock makes WAL readers wait for
//! those uploads. The tests below park a segment PUT, then prove that resource, maintenance
//! and size snapshots do not serialize the WAL read behind it.

use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use turso_core::io::{Buffer, Completion, FileSyncType, OpenFlags, IO};

use super::{Namespace, RemoteIO};
use crate::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};

#[derive(Default)]
struct PublishGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl PublishGate {
    fn pause(&self) -> Result<()> {
        let mut state = self.state.lock().expect("gate mutex poisoned");
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).expect("gate mutex poisoned");
        }
        Ok(())
    }

    fn wait_entered(&self) {
        let state = self.state.lock().expect("gate mutex poisoned");
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(10), |state| !state.0)
            .expect("gate mutex poisoned");
        assert!(
            state.0 && !timeout.timed_out(),
            "publication did not reach the segment PUT"
        );
    }

    fn release(&self) {
        self.state.lock().expect("gate mutex poisoned").1 = true;
        self.changed.notify_all();
    }
}

struct SegmentGateStore {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<PublishGate>,
}

impl ObjectStore for SegmentGateStore {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.contains("segments/") {
            self.gate.pause()?;
        }
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn settle<T>(receiver: &mpsc::Receiver<Result<T, String>>, what: &str) -> Result<T> {
    receiver
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| anyhow!("{what} did not settle after the publication: {error}"))?
        .map_err(|error| anyhow!("{what} failed: {error}"))
}

#[test]
fn parked_publication_does_not_block_wal_reads_behind_usage_snapshots() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::open(objects.path())?);
    let gate = Arc::new(PublishGate::default());
    let store: Arc<dyn ObjectStore> = Arc::new(SegmentGateStore {
        inner: backing,
        gate: gate.clone(),
    });
    let io = RemoteIO::open(
        store,
        Namespace::new("io", "locking")?,
        cache.path(),
        0,
        8 * 1024 * 1024,
        64 * 1024 * 1024,
    )?;
    let main = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    // A partial page write stages one dirty segment, so the publication has a real segment
    // PUT the gate can park while the storage lock is held.
    let page = Arc::new(Buffer::new_temporary(4096));
    page.as_mut_slice().fill(9);
    assert!(main
        .pwrite(
            0,
            page,
            Completion::new_write(|result| assert_eq!(result.unwrap(), 4096))
        )?
        .finished());

    let parked = main.clone();
    let publisher = std::thread::spawn(move || -> Result<()> {
        drop(parked.sync(
            Completion::new_sync(|result| {
                result.expect("main sync");
            }),
            FileSyncType::Fsync,
        )?);
        Ok(())
    });
    gate.wait_entered();

    let (usage_sender, usage_receiver) = mpsc::channel();
    let usage_io = io.clone();
    let usage = std::thread::spawn(move || {
        let outcome = usage_io
            .resources()
            .map(|usage| usage.wal_bytes)
            .map_err(|error| format!("{error:?}"));
        let _ = usage_sender.send(outcome);
    });

    let (signals_sender, signals_receiver) = mpsc::channel();
    let signals_io = io.clone();
    let signals = std::thread::spawn(move || {
        let outcome = signals_io
            .maintenance_signals()
            .map(|signals| signals.pending_wal_bytes)
            .map_err(|error| format!("{error:?}"));
        let _ = signals_sender.send(outcome);
    });

    let (size_sender, size_receiver) = mpsc::channel();
    let size_file = main.clone();
    let size = std::thread::spawn(move || {
        let outcome = size_file.size().map_err(|error| format!("{error:?}"));
        let _ = size_sender.send(outcome);
    });

    // Let each snapshot reach the storage lock the publication holds. If a snapshot instead
    // held the state lock while waiting, the WAL read below cannot finish before release.
    std::thread::sleep(Duration::from_millis(250));

    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let buffer = Arc::new(Buffer::new_temporary(32));
        let outcome = wal
            .pread(0, Completion::new_read(buffer, |_| None))
            .map(|completion| completion.finished())
            .map_err(|error| format!("{error:?}"));
        let _ = sender.send(outcome);
    });
    let outcome = receiver.recv_timeout(Duration::from_secs(2));
    gate.release();
    let _ = reader.join();
    usage.join().expect("usage thread");
    signals.join().expect("signals thread");
    size.join().expect("size thread");

    match outcome {
        Ok(Ok(true)) => {}
        other => panic!("WAL read waited for the parked publication: {other:?}"),
    }
    settle(&usage_receiver, "resource snapshot")?;
    settle(&signals_receiver, "maintenance snapshot")?;
    settle(&size_receiver, "main size")?;
    publisher.join().expect("publisher thread")?;
    Ok(())
}
