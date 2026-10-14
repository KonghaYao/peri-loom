use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use turso_core::io::{Buffer, Completion, FileSyncType, OpenFlags, IO};

use crate::database::reclamation::CleanupFailure;
use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use crate::runtime::{DatabaseId, DatabaseProvider, WorkerLimits, WorkerRuntime};
use crate::session::{AdmissionError, UnsettledSqlOutcome};

#[test]
fn platform_file_callback_must_return_before_native_reclamation() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(LocalObjectStore::open(directory.path().join("objects"))?),
        Namespace::new("native_physical", "callback")?,
        directory.path().join("cache"),
        10,
    )?);
    let session = database.session()?;
    let hint = session.lane.hint();
    let engine = Arc::downgrade(database.engine.lock().unwrap().as_ref().unwrap());
    let connection = Arc::downgrade(session.lane.connection.lock().unwrap().as_ref().unwrap());
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("tursodb_temp_file");
    let file = database
        .io()
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)?;
    let (entered, callback_entered) = mpsc::channel();
    let (resume, callback_resume) = mpsc::channel();
    let callback_resume = Mutex::new(callback_resume);
    let completion = Completion::new_write(move |result| {
        assert!(result.is_ok());
        entered.send(()).unwrap();
        callback_resume
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
    });
    let reclaimed = Arc::new(AtomicUsize::new(0));
    let observed = reclaimed.clone();
    database.on_native_reclaimed(move || {
        assert!(engine.upgrade().is_none());
        assert!(connection.upgrade().is_none());
        observed.fetch_add(1, Ordering::SeqCst);
    })?;
    thread::scope(|scope| -> Result<()> {
        let writing =
            scope.spawn(|| file.pwrite(0, Arc::new(Buffer::new(vec![37; 32])), completion.clone()));
        callback_entered.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(std::fs::read(&path)?, vec![37; 32]);
        let deadline = Instant::now() + Duration::from_millis(30);
        let error = database.cleanup_deferred_until(deadline).unwrap_err();
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Deadline)
        );
        assert!(Instant::now() >= deadline);
        assert_eq!(reclaimed.load(Ordering::SeqCst), 0);
        assert!(database.engine.lock().unwrap().is_some());
        assert!(database.session().is_err());
        resume.send(())?;
        assert!(writing.join().unwrap()?.finished());
        database.cleanup_deferred_until(Instant::now() + Duration::from_secs(1))?;
        Ok(())
    })?;
    assert_eq!(reclaimed.load(Ordering::SeqCst), 1);
    assert_eq!(session.lane.hint(), hint);
    assert!(session
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    let callbacks = Arc::new(AtomicUsize::new(0));
    let observed = callbacks.clone();
    let rejected = Completion::new_write(move |result| {
        assert!(result.is_err());
        observed.fetch_add(1, Ordering::SeqCst);
    });
    assert!(file
        .pwrite(0, Arc::new(Buffer::new(vec![99; 32])), rejected.clone())
        .is_err());
    assert!(rejected.finished());
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert!(file
        .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)
        .is_err());
    assert_eq!(std::fs::read(&path)?, vec![37; 32]);
    assert!(database.io().step().is_err());
    database.cleanup_deferred_until(Instant::now() + Duration::from_secs(1))?;
    assert_eq!(reclaimed.load(Ordering::SeqCst), 1);
    Ok(())
}

struct LockedLocalStore {
    inner: LocalObjectStore,
    entered: Mutex<Option<mpsc::Sender<()>>>,
    lose_ack: AtomicBool,
    failed: AtomicBool,
    mutations: AtomicUsize,
}

impl ObjectStore for LockedLocalStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.ends_with("/wal/index") && self.failed.load(Ordering::Acquire) {
            bail!("local fixture receipt unavailable");
        }
        if let Some(entered) = self.entered.lock().unwrap().take() {
            entered.send(())?;
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.put(key, bytes, condition)?;
        if key.ends_with("/wal/index") && self.lose_ack.load(Ordering::Acquire) {
            self.failed.store(true, Ordering::Release);
            bail!("local fixture durable index acknowledgement lost");
        }
        Ok(result)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

struct LocalProvider {
    store: Arc<LockedLocalStore>,
    opens: AtomicUsize,
}

impl DatabaseProvider for LocalProvider {
    fn open_store(&self, _identifier: &DatabaseId, _epoch: u64) -> Result<Arc<dyn ObjectStore>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(self.store.clone())
    }
}

#[test]
fn locked_local_connection_io_keeps_cleanup_incomplete_and_next_epoch_quarantined() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("objects");
    let store = Arc::new(LockedLocalStore {
        inner: LocalObjectStore::open(&root)?,
        entered: Mutex::new(None),
        lose_ack: AtomicBool::new(false),
        failed: AtomicBool::new(false),
        mutations: AtomicUsize::new(0),
    });
    let provider = Arc::new(LocalProvider {
        store: store.clone(),
        opens: AtomicUsize::new(0),
    });
    let runtime = WorkerRuntime::new(
        provider.clone(),
        directory.path().join("cache"),
        WorkerLimits::default(),
    )?;
    let identifier = DatabaseId::new(Namespace::new("native_physical", "connection")?, "main")?;
    let handle = Arc::new(runtime.open(identifier.clone(), 10)?);
    handle.execute("CREATE TABLE ledger(value INTEGER)")?;
    let index_key = identifier.namespace.key("wal/index")?;
    let previous_index = store.inner.get(&index_key)?.unwrap();
    let (entered, dispatching) = mpsc::channel();
    thread::scope(|scope| -> Result<()> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join(".lock"))?;
        lock.lock()?;
        *store.entered.lock().unwrap() = Some(entered);
        store.lose_ack.store(true, Ordering::Release);
        let writer = handle.clone();
        let writing =
            scope.spawn(move || writer.execute("INSERT INTO ledger VALUES(41) RETURNING value"));
        dispatching.recv_timeout(Duration::from_secs(1))?;
        let deadline = Instant::now() + Duration::from_millis(30);
        let error = handle.request_close_until(deadline).unwrap_err();
        assert!(Instant::now() >= deadline);
        assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert_eq!(
            error.downcast_ref::<CleanupFailure>(),
            Some(&CleanupFailure::Deadline)
        );
        assert_eq!(
            runtime
                .open(identifier.clone(), 11)
                .err()
                .unwrap()
                .downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Quarantine)
        );
        assert_eq!(provider.opens.load(Ordering::SeqCst), 1);
        drop(lock);
        let error = writing.join().unwrap().unwrap_err();
        assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert!(error.downcast_ref::<turso_core::LimboError>().is_some());
        Ok(())
    })?;
    assert!(store.failed.load(Ordering::Acquire));
    assert_ne!(
        store.inner.get(&index_key)?.unwrap().etag,
        previous_index.etag
    );
    let mutations = store.mutations.load(Ordering::SeqCst);
    let status = runtime.status()?;
    assert_eq!(status.databases.len(), 1);
    assert!(status.databases[0].resources.is_none());
    assert!(handle
        .request_close_until(Instant::now() + Duration::from_secs(1))
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    drop(handle);
    assert_eq!(
        runtime
            .open(identifier, 11)
            .err()
            .unwrap()
            .downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Quarantine)
    );
    assert_eq!(provider.opens.load(Ordering::SeqCst), 1);
    assert_eq!(store.mutations.load(Ordering::SeqCst), mutations);
    Ok(())
}
