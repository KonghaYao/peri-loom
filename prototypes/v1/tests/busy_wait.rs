use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use peri_loom_v1::{
    database::{RemoteDatabase, RemoteSession},
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
    session::{AdmissionError, Budget},
};
use turso_core::LimboError;

struct Observation {
    started: Instant,
    reached: mpsc::Sender<()>,
}

#[derive(Default)]
struct ProbeStore {
    inner: MemoryObjectStore,
    checks: AtomicUsize,
    physical: AtomicUsize,
    clock: AtomicU64,
    revoked: AtomicBool,
    armed: AtomicBool,
    observation: Mutex<Option<Observation>>,
}

impl ProbeStore {
    fn observe_wait(&self) -> mpsc::Receiver<()> {
        let (sender, receiver) = mpsc::channel();
        self.checks.store(0, Ordering::SeqCst);
        *self.observation.lock().unwrap() = Some(Observation {
            started: Instant::now(),
            reached: sender,
        });
        receiver
    }
}

impl ObjectStore for ProbeStore {
    fn validate_local(&self) -> Result<()> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        let mut observation = self.observation.lock().unwrap();
        if observation
            .as_ref()
            .is_some_and(|current| current.started.elapsed() >= Duration::from_millis(30))
        {
            let current = observation.take().unwrap();
            let _ = current.reached.send(());
        }
        drop(observation);
        if self.armed.load(Ordering::SeqCst) {
            ensure!(
                self.clock.load(Ordering::SeqCst) < 1000,
                "LeaseExpired test local clock"
            );
            ensure!(
                !self.revoked.load(Ordering::SeqCst),
                "Fenced test local revocation"
            );
        }
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.physical.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.physical.fetch_add(1, Ordering::SeqCst);
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.physical.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.physical.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }
}

struct Fixture {
    database: Arc<RemoteDatabase>,
    store: Arc<ProbeStore>,
    _directory: tempfile::TempDir,
}

impl Fixture {
    fn open() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(ProbeStore::default());
        let database = Arc::new(RemoteDatabase::open(
            store.clone(),
            Namespace::new("busy_wait", "isolated")?,
            directory.path(),
            0,
        )?);
        database
            .session()?
            .execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY);")?;
        Ok(Self {
            database,
            store,
            _directory: directory,
        })
    }
}

fn ledger(session: &RemoteSession) -> Result<Vec<String>> {
    Ok(session
        .query("SELECT id FROM ledger ORDER BY id;")?
        .iter()
        .map(|row| row[0].to_string())
        .collect())
}

#[test]
fn holder_commit_and_reader_progress_while_same_writer_call_waits() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = Arc::new(fixture.database.session()?);
    let reader = fixture.database.session()?;
    holder.execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(1);")?;
    let observed = fixture.store.observe_wait();
    let (sender, receiver) = mpsc::channel();
    let writer = contender.clone();
    thread::scope(|scope| -> Result<()> {
        scope.spawn(move || {
            sender
                .send(writer.execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(2); COMMIT;"))
                .unwrap();
        });
        observed
            .recv_timeout(Duration::from_millis(500))
            .context("writer did not wait for held lock")?;
        ensure!(
            receiver.try_recv().is_err(),
            "writer returned before lock release"
        );
        ensure!(
            ledger(&reader)?.is_empty(),
            "reader saw uncommitted holder row"
        );
        holder.execute("COMMIT;")?;
        receiver
            .recv_timeout(Duration::from_secs(2))
            .context("writer did not resume")??;
        Ok(())
    })?;
    ensure!(!contender.in_transaction()?, "resumed batch did not commit");
    ensure!(
        ledger(&reader)? == ["1", "2"],
        "single-call commit ledger mismatch"
    );
    ensure!(
        fixture.store.checks.load(Ordering::SeqCst) < 1000,
        "busy wait spun through local guards"
    );
    Ok(())
}

#[test]
fn default_writer_admission_deadline_exhausts_without_batch_side_effects() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = fixture.database.session()?;
    holder.execute("BEGIN IMMEDIATE;")?;
    fixture.store.checks.store(0, Ordering::SeqCst);
    let physical = fixture.store.physical.load(Ordering::SeqCst);
    let started = Instant::now();
    let error = contender
        .execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(7); COMMIT;")
        .unwrap_err();
    let elapsed = started.elapsed();
    ensure!(
        error.downcast_ref::<AdmissionError>() == Some(&AdmissionError::Deadline),
        "expected typed admission deadline, got {error:#}"
    );
    ensure!(
        elapsed >= Budget::default().max_query_duration && elapsed < Duration::from_secs(6),
        "writer queue did not share the original default query budget"
    );
    ensure!(
        fixture.store.physical.load(Ordering::SeqCst) == physical,
        "held-lock wait dispatched physical object I/O"
    );
    ensure!(
        fixture.store.checks.load(Ordering::SeqCst) < 4000,
        "writer admission used a spinning queue"
    );
    ensure!(
        !contender.in_transaction()?,
        "failed BEGIN retained transaction"
    );
    holder.execute("ROLLBACK;")?;
    ensure!(
        ledger(&contender)?.is_empty(),
        "expired queued batch wrote a prefix"
    );
    eprintln!(
        "terminal={:?} original_five_second_budget=true physical_io=0 single_batch_call=1 ledger_empty=true",
        error.downcast_ref::<AdmissionError>()
    );
    Ok(())
}

#[test]
fn short_write_query_admission_budget_stops_before_io() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = fixture.database.session()?;
    contender.query("SELECT id FROM ledger;")?;
    holder.execute("BEGIN IMMEDIATE;")?;
    let physical = fixture.store.physical.load(Ordering::SeqCst);
    let started = Instant::now();
    let error = contender
        .query_with_budget(
            "INSERT INTO ledger VALUES(8) RETURNING id;",
            &Budget {
                max_query_duration: Duration::from_millis(40),
                ..Budget::default()
            },
        )
        .unwrap_err();
    let elapsed = started.elapsed();
    ensure!(
        error.downcast_ref::<AdmissionError>() == Some(&AdmissionError::Deadline),
        "wrong short-budget terminal error: {error:#}"
    );
    ensure!(
        elapsed >= Duration::from_millis(40) && elapsed < Duration::from_millis(300),
        "query budget was reset by writer admission"
    );
    ensure!(
        fixture.store.physical.load(Ordering::SeqCst) == physical,
        "expired query dispatched physical object I/O"
    );
    ensure!(
        !contender.in_transaction()?,
        "expired implicit write retained transaction"
    );
    holder.execute("ROLLBACK;")?;
    ensure!(
        ledger(&contender)?.is_empty(),
        "expired write-query produced side effects"
    );
    eprintln!("query_budget_ms=40 admission_deadline=true physical_io=0 ledger_empty=true terminal={error:#}");
    Ok(())
}

#[test]
fn expired_ingress_budget_never_starts_a_write() -> Result<()> {
    let fixture = Fixture::open()?;
    let session = fixture.database.session()?;
    let physical = fixture.store.physical.load(Ordering::SeqCst);
    let error = session
        .execute_with_budget(
            "INSERT INTO ledger VALUES(9);",
            &Budget {
                max_query_duration: Duration::from_nanos(1),
                ..Budget::default()
            },
        )
        .unwrap_err();
    ensure!(
        error.downcast_ref::<AdmissionError>() == Some(&AdmissionError::Deadline),
        "expired budget was not enforced"
    );
    ensure!(
        fixture.store.physical.load(Ordering::SeqCst) == physical,
        "expired ingress wrote physical state"
    );
    ensure!(
        ledger(&session)?.is_empty(),
        "expired ingress changed ledger"
    );
    Ok(())
}

#[test]
fn local_clock_expiry_or_revocation_during_admission_wait_stops_before_io() -> Result<()> {
    for revoke in [false, true] {
        let fixture = Fixture::open()?;
        let holder = fixture.database.session()?;
        let contender = Arc::new(fixture.database.session()?);
        holder.execute("BEGIN IMMEDIATE;")?;
        fixture.store.armed.store(true, Ordering::SeqCst);
        let physical = fixture.store.physical.load(Ordering::SeqCst);
        let observed = fixture.store.observe_wait();
        let (sender, receiver) = mpsc::channel();
        let writer = contender.clone();
        thread::scope(|scope| -> Result<()> {
            scope.spawn(move || {
                sender
                    .send(writer.execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(10); COMMIT;"))
                    .unwrap();
            });
            observed
                .recv_timeout(Duration::from_millis(500))
                .context("local fence test did not reach wait")?;
            let invalidated = Instant::now();
            if revoke {
                fixture.store.revoked.store(true, Ordering::SeqCst);
            } else {
                fixture.store.clock.store(1000, Ordering::SeqCst);
            }
            let error = receiver
                .recv_timeout(Duration::from_millis(300))
                .context("sleep ignored local fence")?
                .unwrap_err();
            let expected = if revoke {
                "Fenced test local revocation"
            } else {
                "LeaseExpired test local clock"
            };
            ensure!(
                format!("{error:#}").contains(expected),
                "local fence cause was lost: {error:#}"
            );
            ensure!(
                invalidated.elapsed() < Duration::from_millis(300),
                "local fence waited for busy timeout"
            );
            ensure!(
                fixture.store.physical.load(Ordering::SeqCst) == physical,
                "locally fenced writer dispatched physical I/O"
            );
            eprintln!("mock_local_revoke={revoke} stopped_before_io=true terminal={error:#}");
            Ok(())
        })?;
        fixture.store.armed.store(false, Ordering::SeqCst);
        ensure!(
            !contender.in_transaction()?,
            "local fence failure retained transaction"
        );
        holder.execute("ROLLBACK;")?;
        ensure!(
            ledger(&contender)?.is_empty(),
            "locally fenced batch changed ledger"
        );
    }
    Ok(())
}

#[test]
fn committed_batch_prefix_survives_later_constraint_and_explicit_rollback() -> Result<()> {
    let fixture = Fixture::open()?;
    let session = fixture.database.session()?;
    let error = session
        .execute(
            "BEGIN IMMEDIATE; INSERT INTO ledger VALUES(1); COMMIT; \
         BEGIN IMMEDIATE; INSERT INTO ledger VALUES(2); INSERT INTO ledger VALUES(2); COMMIT;",
        )
        .unwrap_err();
    ensure!(
        matches!(
            error.downcast_ref::<LimboError>(),
            Some(LimboError::Constraint(..))
        ),
        "expected native constraint, got {error:#}"
    );
    ensure!(
        session.in_transaction()?,
        "statement ABORT unexpectedly ended explicit transaction"
    );
    session.execute("ROLLBACK;")?;
    ensure!(!session.in_transaction()?, "explicit rollback failed");
    let reader = fixture.database.session()?;
    ensure!(
        ledger(&reader)? == ["1"],
        "rollback erased committed prefix or retained failed suffix"
    );
    ensure!(
        reader.query("PRAGMA integrity_check;")?[0][0].to_string() == "ok",
        "prefix database integrity failed"
    );
    eprintln!("single_batch_call=1 committed_prefix_retained=true explicit_rollback_only_cleared_suffix=true terminal={error:#}");
    Ok(())
}
