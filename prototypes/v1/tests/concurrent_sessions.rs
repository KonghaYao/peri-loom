use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use peri_loom_v1::{
    database::{RemoteDatabase, RemoteSession},
    namespace::Namespace,
    object_store::{LocalObjectStore, MemoryObjectStore, ObjectStore},
    session::Budget,
};
use turso_core::{LimboError, Numeric, Value};

#[derive(Clone, Copy, Debug)]
enum Backend {
    Memory,
    Local,
}

struct Fixture {
    database: Arc<RemoteDatabase>,
    directory: tempfile::TempDir,
}

impl Fixture {
    fn open(backend: Backend) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store: Arc<dyn ObjectStore> = match backend {
            Backend::Memory => Arc::new(MemoryObjectStore::default()),
            Backend::Local => Arc::new(LocalObjectStore::open(directory.path().join("objects"))?),
        };
        let database = Arc::new(RemoteDatabase::open(
            store,
            Namespace::new("concurrent_test", "isolated")?,
            directory.path().join("cache"),
            0,
        )?);
        let session = database.session()?;
        session.execute_with_budget(
            "CREATE TABLE ledger(operation_id TEXT PRIMARY KEY,delta INTEGER NOT NULL); \
             CREATE TABLE account(id INTEGER PRIMARY KEY,balance INTEGER NOT NULL); \
             INSERT INTO account VALUES(1,1000);",
            &budget(),
        )?;
        Ok(Self {
            database,
            directory,
        })
    }
}

fn budget() -> Budget {
    Budget {
        max_vm_steps: 100_000,
        max_query_duration: Duration::from_secs(5),
        ..Budget::default()
    }
}

fn batch(operation: &str, delta: i64) -> String {
    format!(
        "BEGIN IMMEDIATE; INSERT INTO ledger VALUES('{operation}',{delta}); \
         UPDATE account SET balance=balance+({delta}) WHERE id=1; COMMIT;"
    )
}

fn assert_busy(backend: Backend, operation: &str, error: &anyhow::Error) -> Result<()> {
    let native = error.downcast_ref::<LimboError>();
    let chain = error.chain().map(ToString::to_string).collect::<Vec<_>>();
    eprintln!(
        "backend={backend:?} operation={operation} native_type={} native={native:?} chain={chain:?}",
        std::any::type_name::<LimboError>()
    );
    ensure!(
        matches!(native, Some(LimboError::Busy)),
        "expected native Busy, got {error:#}"
    );
    ensure!(
        chain
            .last()
            .is_some_and(|message| message == "Database is busy"),
        "native Busy source missing"
    );
    Ok(())
}

fn verify(session: &RemoteSession, expected: &BTreeMap<String, i64>) -> Result<()> {
    let rows = session.query_with_budget(
        "SELECT operation_id,delta FROM ledger ORDER BY operation_id;",
        &budget(),
    )?;
    let mut actual = BTreeMap::new();
    for row in rows {
        let [Value::Text(operation), Value::Numeric(Numeric::Integer(delta))] = row.as_slice()
        else {
            bail!("unexpected ledger row shape");
        };
        ensure!(
            actual
                .insert(operation.as_str().to_owned(), *delta)
                .is_none(),
            "duplicate ledger operation"
        );
    }
    ensure!(
        &actual == expected,
        "ledger side effects do not match terminal outcomes"
    );
    let balances =
        session.query_with_budget("SELECT balance FROM account WHERE id=1;", &budget())?;
    let [balance_row] = balances.as_slice() else {
        bail!("missing singleton account");
    };
    let [Value::Numeric(Numeric::Integer(balance))] = balance_row.as_slice() else {
        bail!("unexpected account row shape");
    };
    ensure!(
        *balance == 1000 + expected.values().sum::<i64>(),
        "account/ledger mismatch"
    );
    let integrity = session.query_with_budget("PRAGMA integrity_check;", &budget())?;
    ensure!(
        matches!(integrity.as_slice(), [row] if matches!(row.as_slice(), [Value::Text(value)] if value.as_str() == "ok")),
        "integrity check failed"
    );
    Ok(())
}

#[test]
fn held_immediate_transaction_returns_typed_busy_without_batch_side_effects() -> Result<()> {
    for backend in [Backend::Memory, Backend::Local] {
        let fixture = Fixture::open(backend)?;
        let holder = fixture.database.session()?;
        let contender = fixture.database.session()?;
        holder.execute_with_budget("BEGIN IMMEDIATE;", &budget())?;
        holder.execute_with_budget(
            "INSERT INTO ledger VALUES('holder_uncommitted',100);",
            &budget(),
        )?;
        ensure!(holder.in_transaction()?, "holder did not enter transaction");
        let failed_operation = "failed_b_while_locked";
        let error = contender
            .execute_with_budget(&batch(failed_operation, 7), &budget())
            .unwrap_err();
        assert_busy(backend, failed_operation, &error)?;
        ensure!(
            !contender.in_transaction()?,
            "failed BEGIN left an active contender transaction"
        );
        holder.execute_with_budget("ROLLBACK;", &budget())?;
        ensure!(
            !holder.in_transaction()?,
            "explicit rollback did not finish"
        );
        verify(&contender, &BTreeMap::new())?;
        let fresh_operation = "new_b_after_explicit_rollback";
        contender.execute_with_budget(&batch(fresh_operation, 11), &budget())?;
        let expected = BTreeMap::from([(fresh_operation.to_owned(), 11)]);
        verify(&contender, &expected)?;
        let fresh_reader = fixture.database.session()?;
        verify(&fresh_reader, &expected)?;
        eprintln!("backend={backend:?} failed_operation_absent=true holder_rollback_proven=true fresh_operation_calls=1 ledger_oracle=true private_fixture={}", fixture.directory.path().is_dir());
    }
    Ok(())
}

struct Outcome {
    operation: String,
    delta: i64,
    result: Result<()>,
}

fn four_once(fixture: &Fixture, prefix: &str) -> Result<Vec<Outcome>> {
    let sessions = (0..4)
        .map(|_| fixture.database.session().map(Arc::new))
        .collect::<Result<Vec<_>>>()?;
    let barrier = Arc::new(Barrier::new(5));
    let (sender, receiver) = mpsc::channel();
    let outcomes = thread::scope(|scope| -> Result<Vec<Outcome>> {
        for (index, session) in sessions.iter().cloned().enumerate() {
            let sender = sender.clone();
            let barrier = barrier.clone();
            let operation = format!("{prefix}_{index}");
            let delta = i64::try_from(index)? + 1;
            scope.spawn(move || {
                barrier.wait();
                let result = session.execute_with_budget(&batch(&operation, delta), &budget());
                sender
                    .send(Outcome {
                        operation,
                        delta,
                        result,
                    })
                    .unwrap();
            });
        }
        barrier.wait();
        (0..4)
            .map(|_| {
                receiver
                    .recv_timeout(Duration::from_secs(10))
                    .context("bounded writer completion")
            })
            .collect()
    })?;
    for session in sessions {
        ensure!(
            !session.in_transaction()?,
            "batch left an active transaction"
        );
    }
    Ok(outcomes)
}

#[test]
fn four_single_call_batches_expose_busy_limits_without_sql_replay() -> Result<()> {
    for backend in [Backend::Memory, Backend::Local] {
        let fixture = Fixture::open(backend)?;
        let holder = fixture.database.session()?;
        holder.execute_with_budget("BEGIN IMMEDIATE;", &budget())?;
        let blocked = four_once(&fixture, "blocked_once")?;
        ensure!(blocked.len() == 4, "missing blocked writer outcome");
        for outcome in blocked {
            let error = outcome.result.unwrap_err();
            assert_busy(backend, &outcome.operation, &error)?;
        }
        holder.execute_with_budget("ROLLBACK;", &budget())?;
        verify(&holder, &BTreeMap::new())?;
        let raced = four_once(&fixture, "fresh_race_once")?;
        ensure!(raced.len() == 4, "missing concurrent writer outcome");
        let mut expected = BTreeMap::new();
        let mut busy = 0;
        for outcome in raced {
            match outcome.result {
                Ok(()) => {
                    expected.insert(outcome.operation, outcome.delta);
                }
                Err(error) => {
                    assert_busy(backend, &outcome.operation, &error)?;
                    busy += 1;
                }
            }
        }
        ensure!(
            !expected.is_empty(),
            "no writer made progress without held transaction"
        );
        ensure!(
            expected.len() == 4 && busy == 0,
            "bounded short transactions did not serialize within busy timeout"
        );
        let fresh_reader = fixture.database.session()?;
        verify(&fresh_reader, &expected)?;
        eprintln!("backend={backend:?} held_lock_busy=4 race_ack={} race_busy={busy} calls_per_business_operation=1 failed_operations_absent=true ledger_oracle=true schedule_dependent_counts=true", expected.len());
    }
    Ok(())
}
