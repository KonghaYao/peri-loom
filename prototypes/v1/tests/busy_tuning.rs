use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use peri_loom_v1::{
    database::{RemoteDatabase, RemoteSession},
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
    session::Budget,
};
use turso_core::LimboError;

struct PrefixHook {
    statements: usize,
    reached: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}

struct WaitObservation {
    started: Instant,
    reached: mpsc::Sender<()>,
}

#[derive(Default)]
struct TestStore {
    inner: MemoryObjectStore,
    physical: AtomicUsize,
    prefix: Mutex<Option<PrefixHook>>,
    waiting: Mutex<Option<WaitObservation>>,
}

impl TestStore {
    fn observe_wait(&self) -> mpsc::Receiver<()> {
        let (sender, receiver) = mpsc::channel();
        *self.waiting.lock().unwrap() = Some(WaitObservation {
            started: Instant::now(),
            reached: sender,
        });
        receiver
    }

    fn pause_before_second_begin(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (reached_sender, reached_receiver) = mpsc::channel();
        let (resume_sender, resume_receiver) = mpsc::channel();
        *self.prefix.lock().unwrap() = Some(PrefixHook {
            statements: 0,
            reached: reached_sender,
            resume: resume_receiver,
        });
        (reached_receiver, resume_sender)
    }
}

impl ObjectStore for TestStore {
    fn validate_local(&self) -> Result<()> {
        let mut waiting = self.waiting.lock().unwrap();
        if waiting
            .as_ref()
            .is_some_and(|wait| wait.started.elapsed() >= Duration::from_millis(30))
        {
            let wait = waiting.take().unwrap();
            let _ = wait.reached.send(());
        }
        Ok(())
    }

    fn validate_transaction(&self, _: bool) -> Result<()> {
        let mut pending = self.prefix.lock().unwrap();
        let pause = if let Some(hook) = pending.as_mut() {
            hook.statements += 1;
            hook.statements == 4
        } else {
            false
        };
        let hook = if pause { pending.take() } else { None };
        drop(pending);
        if let Some(hook) = hook {
            hook.reached
                .send(())
                .context("prefix observation receiver gone")?;
            hook.resume
                .recv_timeout(Duration::from_secs(2))
                .context("prefix observation did not resume")?;
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
    store: Arc<TestStore>,
    _directory: tempfile::TempDir,
}

impl Fixture {
    fn open() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(TestStore::default());
        let database = Arc::new(RemoteDatabase::open(
            store.clone(),
            Namespace::new("busy_tuning", "isolated")?,
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
fn release_after_one_and_half_seconds_commits_same_call_with_reader_progress() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = Arc::new(fixture.database.session()?);
    let reader = fixture.database.session()?;
    holder.execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(1);")?;
    let started = Instant::now();
    let waiting = fixture.store.observe_wait();
    let (sender, receiver) = mpsc::channel();
    let writer = contender.clone();
    thread::scope(|scope| -> Result<()> {
        scope.spawn(move || {
            sender
                .send(writer.execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(2); COMMIT;"))
                .unwrap();
        });
        waiting
            .recv_timeout(Duration::from_millis(500))
            .context("contender never waited")?;
        ensure!(
            receiver.try_recv().is_err(),
            "contender ended before lock release"
        );
        ensure!(ledger(&reader)?.is_empty(), "reader saw uncommitted holder");
        thread::sleep(Duration::from_millis(1500).saturating_sub(started.elapsed()));
        let pending = receiver.try_recv();
        holder.execute("COMMIT;")?;
        ensure!(
            matches!(pending, Err(mpsc::TryRecvError::Empty)),
            "one-call contender exhausted before 1.5s holder release: {pending:?}"
        );
        receiver
            .recv_timeout(Duration::from_secs(3))
            .context("contender did not resume")??;
        Ok(())
    })?;
    ensure!(
        !contender.in_transaction()?,
        "ACK did not finish contender transaction"
    );
    ensure!(ledger(&reader)? == ["1", "2"], "same-call ledger mismatch");
    ensure!(
        reader.query("PRAGMA integrity_check;")?[0][0].to_string() == "ok",
        "integrity failed"
    );
    Ok(())
}

#[test]
fn interactive_stale_read_transaction_returns_busy_snapshot_without_reopen() -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = fixture.database.session()?;
    contender.execute("BEGIN;")?;
    ensure!(
        ledger(&contender)?.is_empty(),
        "initial snapshot was not empty"
    );
    holder.execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(10); COMMIT;")?;
    let physical = fixture.store.physical.load(Ordering::SeqCst);
    let started = Instant::now();
    let error = contender
        .execute("INSERT INTO ledger VALUES(11);")
        .unwrap_err();
    ensure!(
        matches!(
            error.downcast_ref::<LimboError>(),
            Some(LimboError::BusySnapshot)
        ),
        "expected native BusySnapshot, got {error:#}"
    );
    ensure!(
        started.elapsed() < Duration::from_millis(500),
        "interactive stale snapshot waited for busy handler"
    );
    ensure!(
        fixture.store.physical.load(Ordering::SeqCst) == physical,
        "failed promotion dispatched physical I/O"
    );
    ensure!(
        contender.in_transaction()?,
        "stale interactive transaction was reopened or aborted"
    );
    ensure!(
        ledger(&contender)?.is_empty(),
        "read snapshot changed without caller rollback"
    );
    contender.execute("ROLLBACK;")?;
    ensure!(
        ledger(&contender)? == ["10"],
        "explicit rollback did not expose committed state"
    );
    ensure!(
        ledger(&fixture.database.session()?)? == ["10"],
        "failed business write was replayed"
    );
    Ok(())
}

fn prefix_then_blocked_begin(
    budget: Budget,
    hook_delay: Duration,
    duration_error: bool,
) -> Result<()> {
    let fixture = Fixture::open()?;
    let holder = fixture.database.session()?;
    let contender = Arc::new(fixture.database.session()?);
    let (reached, resume) = fixture.store.pause_before_second_begin();
    let (sender, receiver) = mpsc::channel();
    let writer = contender.clone();
    let query_duration = budget.max_query_duration;
    let started = Instant::now();
    thread::scope(|scope| -> Result<()> {
        scope.spawn(move || {
            sender.send(writer.execute_with_budget(
                "BEGIN IMMEDIATE; INSERT INTO ledger VALUES(20); COMMIT; BEGIN IMMEDIATE; INSERT INTO ledger VALUES(21); COMMIT;",
                &budget,
            )).unwrap();
        });
        reached
            .recv_timeout(Duration::from_secs(1))
            .context("committed prefix hook did not arrive")?;
        holder.execute("BEGIN IMMEDIATE;")?;
        let physical = fixture.store.physical.load(Ordering::SeqCst);
        thread::sleep(hook_delay);
        resume.send(()).context("prefix hook receiver gone")?;
        let error = receiver
            .recv_timeout(Duration::from_secs(6))
            .context("blocked suffix did not terminate")?
            .unwrap_err();
        if duration_error {
            ensure!(
                format!("{error:#}").contains("query duration budget exceeded"),
                "cumulative batch budget cause lost: {error:#}"
            );
            ensure!(
                started.elapsed() >= query_duration
                    && started.elapsed() < query_duration + Duration::from_millis(150),
                "budget reset between statements or hook delay not accounted"
            );
        } else {
            ensure!(
                matches!(error.downcast_ref::<LimboError>(), Some(LimboError::Busy)),
                "expected typed Busy after committed prefix: {error:#}"
            );
        }
        ensure!(
            fixture.store.physical.load(Ordering::SeqCst) == physical,
            "failed suffix dispatched physical I/O"
        );
        holder.execute("ROLLBACK;")?;
        Ok(())
    })?;
    ensure!(
        !contender.in_transaction()?,
        "failed suffix BEGIN retained transaction"
    );
    ensure!(
        ledger(&contender)? == ["20"],
        "terminal error erased committed prefix or replayed suffix"
    );
    ensure!(
        ledger(&fixture.database.session()?)? == ["20"],
        "fresh reader prefix oracle failed"
    );
    Ok(())
}

#[test]
fn single_batch_committed_prefix_survives_later_typed_busy() -> Result<()> {
    prefix_then_blocked_begin(Budget::default(), Duration::ZERO, false)
}

#[test]
fn all_statements_share_original_query_budget_including_prefix_pause() -> Result<()> {
    prefix_then_blocked_begin(
        Budget {
            max_query_duration: Duration::from_millis(250),
            ..Budget::default()
        },
        Duration::from_millis(150),
        true,
    )
}

#[test]
fn default_five_second_batch_budget_cuts_off_suffix_before_four_second_backoff() -> Result<()> {
    ensure!(
        Budget::default().max_query_duration == Duration::from_secs(5),
        "default query budget changed"
    );
    prefix_then_blocked_begin(Budget::default(), Duration::from_millis(1500), true)
}
