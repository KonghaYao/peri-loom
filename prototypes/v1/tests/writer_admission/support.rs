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
    ownership::types::AccessRole,
    session::{AdmissionError, Budget},
};

struct CallGate {
    thread: &'static str,
    reached: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

#[derive(Default)]
pub struct ProbeStore {
    inner: MemoryObjectStore,
    physical: AtomicUsize,
    clock: AtomicU64,
    revoked: AtomicBool,
    readonly: AtomicBool,
    gate: Mutex<Option<CallGate>>,
}

impl ProbeStore {
    pub fn physical(&self) -> usize {
        self.physical.load(Ordering::SeqCst)
    }

    pub fn revoke(&self, revoked: bool) {
        self.revoked.store(revoked, Ordering::SeqCst);
    }

    pub fn expire(&self) {
        self.clock.store(1000, Ordering::SeqCst);
    }

    pub fn renew_mock(&self) {
        self.clock.store(0, Ordering::SeqCst);
    }

    pub fn block_statement_entry(
        &self,
        thread: &'static str,
    ) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (reached, observation) = mpsc::channel();
        let (resume, release) = mpsc::channel();
        *self.gate.lock().unwrap() = Some(CallGate {
            thread,
            reached,
            release,
        });
        (observation, resume)
    }

    fn check_gate(&self) -> Result<()> {
        let current = thread::current();
        let mut pending = self.gate.lock().unwrap();
        let matches = pending
            .as_ref()
            .is_some_and(|gate| current.name() == Some(gate.thread));
        let gate = if matches { pending.take() } else { None };
        drop(pending);
        if let Some(gate) = gate {
            gate.reached.send(()).context("call gate observer gone")?;
            gate.release
                .recv_timeout(Duration::from_secs(3))
                .context("call gate was not released")?;
        }
        Ok(())
    }
}

impl ObjectStore for ProbeStore {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn validate_session(&self) -> Result<()> {
        self.validate_local()
    }

    fn validate_transaction(&self, _: bool) -> Result<()> {
        self.check_gate()?;
        self.validate_local()
    }

    fn validate_local(&self) -> Result<()> {
        ensure!(
            self.clock.load(Ordering::SeqCst) < 1000,
            "writer admission mock local lease expired"
        );
        ensure!(
            !self.revoked.load(Ordering::SeqCst),
            "writer admission mock local ownership revoked"
        );
        Ok(())
    }

    fn access_role(&self) -> AccessRole {
        if self.readonly.load(Ordering::SeqCst) {
            AccessRole::ReadOnly
        } else {
            AccessRole::Writer
        }
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

pub struct Fixture {
    pub database: Arc<RemoteDatabase>,
    pub store: Arc<ProbeStore>,
    directory: tempfile::TempDir,
}

impl Fixture {
    pub fn open() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(ProbeStore::default());
        let database = Arc::new(RemoteDatabase::open(
            store.clone(),
            Namespace::new("writer_admission", "isolated")?,
            directory.path().join("writer-cache"),
            0,
        )?);
        database.execute(
            "CREATE TABLE ledger(id INTEGER PRIMARY KEY, delta INTEGER NOT NULL); \
             CREATE TABLE account(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL); \
             INSERT INTO account VALUES(1,1000);",
        )?;
        Ok(Self {
            database,
            store,
            directory,
        })
    }

    pub fn readonly(&self) -> Result<Arc<RemoteDatabase>> {
        self.database.checkpoint()?;
        self.store.readonly.store(true, Ordering::SeqCst);
        Ok(Arc::new(RemoteDatabase::open(
            self.store.clone(),
            Namespace::new("writer_admission", "isolated")?,
            self.directory.path().join("readonly-cache"),
            0,
        )?))
    }
}

pub fn batch(identifier: i64, delta: i64) -> String {
    format!(
        "BEGIN IMMEDIATE; INSERT INTO ledger VALUES({identifier},{delta}); \
         UPDATE account SET balance=balance+({delta}) WHERE id=1; COMMIT;"
    )
}

pub fn verify(session: &RemoteSession, expected: &[(i64, i64)]) -> Result<()> {
    let rows = session.query("SELECT id,delta FROM ledger ORDER BY id;")?;
    let actual = rows
        .iter()
        .map(|row| (row[0].to_string(), row[1].to_string()))
        .collect::<Vec<_>>();
    let expected_rows = expected
        .iter()
        .map(|(identifier, delta)| (identifier.to_string(), delta.to_string()))
        .collect::<Vec<_>>();
    ensure!(
        actual == expected_rows,
        "ledger effects differ from expected ACKs"
    );
    let balance = session.query("SELECT balance FROM account WHERE id=1;")?;
    ensure!(
        balance.len() == 1
            && balance[0].len() == 1
            && balance[0][0].to_string()
                == (1000 + expected.iter().map(|(_, delta)| delta).sum::<i64>()).to_string(),
        "balance does not match unique ledger effects"
    );
    let integrity = session.query("PRAGMA integrity_check;")?;
    ensure!(
        integrity.len() == 1 && integrity[0].len() == 1 && integrity[0][0].to_string() == "ok",
        "integrity check failed"
    );
    Ok(())
}

pub fn short_budget() -> Budget {
    Budget {
        max_query_duration: Duration::from_millis(100),
        ..Budget::default()
    }
}

pub fn expect_admission(error: &anyhow::Error, expected: AdmissionError) -> Result<()> {
    ensure!(
        error.downcast_ref::<AdmissionError>() == Some(&expected),
        "wrong typed admission failure: {error:#}"
    );
    Ok(())
}

pub fn wait_queue(database: &RemoteDatabase, expected: &[u64]) -> Result<()> {
    let started = Instant::now();
    loop {
        let status = database.writer_admission_status()?;
        ensure!(!status.quarantined, "fixture unexpectedly quarantined");
        if status.queued == expected {
            return Ok(());
        }
        ensure!(
            started.elapsed() < Duration::from_secs(1),
            "actual queued tickets differ: {:?}, expected {expected:?}",
            status.queued
        );
        thread::sleep(Duration::from_millis(1));
    }
}

pub fn expect_owner(database: &RemoteDatabase, owner: Option<u64>) -> Result<()> {
    let status = database.writer_admission_status()?;
    ensure!(!status.quarantined, "fixture unexpectedly quarantined");
    ensure!(
        status.owner == owner,
        "owner does not match native settled state"
    );
    Ok(())
}
