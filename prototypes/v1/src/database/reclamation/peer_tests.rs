use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use turso_core::LimboError;

use super::CleanupFailure;
use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use crate::session::{AdmissionError, UnsettledSqlOutcome};

#[derive(Default)]
pub(super) struct LostAckStore {
    inner: MemoryObjectStore,
    pub(super) armed: AtomicBool,
    failed: AtomicUsize,
    pub(super) mutations: AtomicUsize,
    pub(super) reads: AtomicUsize,
    receipt_available: AtomicBool,
}

impl ObjectStore for LostAckStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if key.ends_with("/wal/index")
            && self.failed.load(Ordering::Acquire) > 0
            && !self.receipt_available.load(Ordering::Acquire)
        {
            bail!("fixture mutation receipt readback unavailable");
        }
        self.inner.get(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key, expected)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.put(key, bytes, condition)?;
        if key.ends_with("/wal/index") && self.armed.load(Ordering::Acquire) {
            self.failed.fetch_add(1, Ordering::SeqCst);
            bail!("fixture backend mutation acknowledgement unavailable");
        }
        Ok(result)
    }
}

pub(super) struct Fixture {
    pub(super) database: Arc<RemoteDatabase>,
    pub(super) store: Arc<LostAckStore>,
    _directory: tempfile::TempDir,
}

impl Fixture {
    pub(super) fn open() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LostAckStore::default());
        let database = Arc::new(RemoteDatabase::open(
            store.clone(),
            Namespace::new("native_cleanup", "isolated")?,
            directory.path().join("cache"),
            10,
        )?);
        database
            .session()?
            .execute("CREATE TABLE ledger(value INTEGER)")?;
        Ok(Self {
            database,
            store,
            _directory: directory,
        })
    }
}

#[test]
fn actual_native_lost_ack_returns_original_unknown_and_reclaims_heavy_refs() -> Result<()> {
    let fixture = Fixture::open()?;
    let session = fixture.database.session()?;
    let hint = session.lane.hint();
    let engine = Arc::downgrade(fixture.database.engine.lock().unwrap().as_ref().unwrap());
    fixture.store.armed.store(true, Ordering::Release);
    let error = session
        .execute("INSERT INTO ledger VALUES(17) RETURNING value")
        .unwrap_err();
    assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
    assert!(error.downcast_ref::<LimboError>().is_some());
    assert_eq!(fixture.store.failed.load(Ordering::SeqCst), 1);
    assert!(engine.upgrade().is_none());
    assert_eq!(session.lane.hint(), hint);
    assert!(fixture.database.pending.lock().unwrap().is_empty());
    assert!(fixture.database.reclamation.state.lock().unwrap().collected);
    assert!(session
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    Ok(())
}

#[test]
fn actual_native_receipt_recovers_known_lost_ack_without_sql_replay() -> Result<()> {
    let fixture = Fixture::open()?;
    let session = fixture.database.session()?;
    let previous = session.lane.hint();
    fixture
        .store
        .receipt_available
        .store(true, Ordering::Release);
    fixture.store.armed.store(true, Ordering::Release);
    session.execute("INSERT INTO ledger VALUES(31) RETURNING value")?;
    assert_eq!(fixture.store.failed.load(Ordering::SeqCst), 1);
    assert!(fixture.database.admission().healthy().is_ok());
    assert!(fixture.database.pending.lock().unwrap().is_empty());
    assert!(session.lane.hint().sequence > previous.sequence);
    assert!(!session.lane.hint().owner);
    assert_eq!(
        session.query("SELECT value FROM ledger")?[0][0].to_string(),
        "31"
    );
    Ok(())
}

#[test]
fn leader_snapshot_before_late_pending_cannot_reclaim_held_peer_or_lose_statement() -> Result<()> {
    let fixture = Fixture::open()?;
    let peer = Arc::new(fixture.database.session()?);
    let hint = peer.lane.hint();
    let engine = Arc::downgrade(fixture.database.engine.lock().unwrap().as_ref().unwrap());
    let (reached, enqueued) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    fixture
        .database
        .reclamation
        .hooks
        .before_enqueue
        .install(move || {
            reached.send(()).unwrap();
            resumed.recv_timeout(Duration::from_secs(2)).unwrap();
        });
    let (conflict, conflicted) = mpsc::channel();
    fixture
        .database
        .reclamation
        .hooks
        .conflict
        .install(move || {
            conflict.send(()).unwrap();
        });
    fixture.store.armed.store(true, Ordering::Release);
    thread::scope(|scope| -> Result<()> {
        let writer = peer.clone();
        let failed =
            scope.spawn(move || writer.execute("INSERT INTO ledger VALUES(23) RETURNING value"));
        enqueued.recv_timeout(Duration::from_secs(1))?;
        assert!(fixture.database.pending.lock().unwrap().is_empty());
        let deadline = Instant::now() + Duration::from_millis(100);
        let cleanup = fixture.database.clone();
        let leader = scope.spawn(move || cleanup.cleanup_deferred_until(deadline));
        conflicted.recv_timeout(Duration::from_secs(1))?;
        assert!(engine.upgrade().is_some());
        let error = leader.join().unwrap().unwrap_err();
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Deadline)
        );
        assert_eq!(
            fixture.database.cleanup_failure(),
            Some(CleanupFailure::Deadline)
        );
        let mutations = fixture.store.mutations.load(Ordering::SeqCst);
        resume.send(())?;
        let error = failed.join().unwrap().unwrap_err();
        assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert!(error.downcast_ref::<LimboError>().is_some());
        assert!(engine.upgrade().is_none());
        assert!(fixture.database.pending.lock().unwrap().is_empty());
        assert!(fixture.database.reclamation.state.lock().unwrap().collected);
        assert_eq!(fixture.store.mutations.load(Ordering::SeqCst), mutations);
        Ok(())
    })?;
    assert_eq!(peer.lane.hint(), hint);
    assert!(peer
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    Ok(())
}

#[test]
fn close_absolute_deadline_does_not_restart_cleanup_budget_or_ack_unknown() -> Result<()> {
    let fixture = Fixture::open()?;
    let peer = Arc::new(fixture.database.session()?);
    let closing = fixture.database.session()?;
    let closing_hint = closing.lane.hint();
    let (reached, enqueued) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    fixture
        .database
        .reclamation
        .hooks
        .before_enqueue
        .install(move || {
            reached.send(()).unwrap();
            resumed.recv_timeout(Duration::from_secs(2)).unwrap();
        });
    fixture.store.armed.store(true, Ordering::Release);
    thread::scope(|scope| -> Result<()> {
        let writer = peer.clone();
        let failed =
            scope.spawn(move || writer.execute("INSERT INTO ledger VALUES(29) RETURNING value"));
        enqueued.recv_timeout(Duration::from_secs(1))?;
        let started = Instant::now();
        let error = closing
            .request_close_until(started + Duration::from_millis(30))
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert_eq!(
            error.downcast_ref::<CleanupFailure>(),
            Some(&CleanupFailure::Deadline)
        );
        assert_eq!(closing.lane.hint(), closing_hint);
        assert!(!fixture.database.reclamation.state.lock().unwrap().collected);
        resume.send(())?;
        assert!(failed
            .join()
            .unwrap()
            .unwrap_err()
            .downcast_ref::<UnsettledSqlOutcome>()
            .is_some());
        assert!(fixture.database.reclamation.state.lock().unwrap().collected);
        assert!(fixture.database.pending.lock().unwrap().is_empty());
        Ok(())
    })?;
    assert_eq!(closing.lane.hint(), closing_hint);
    assert!(closing
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    Ok(())
}
