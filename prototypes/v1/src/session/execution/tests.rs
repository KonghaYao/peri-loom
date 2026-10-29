use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
use crate::session::{
    AdmissionError, Budget, GuardedIntentReason, GuardedStatementOutcome, RequestDeadline,
    SqlEvidence,
};
use anyhow::Result;

thread_local! {
    static AFTER_DONE: Cell<Option<Duration>> = const { Cell::new(None) };
}

pub(super) fn after_done() {
    if let Some(delay) = AFTER_DONE.take() {
        std::thread::sleep(delay);
    }
}

#[test]
fn committed_statement_late_budget_failure_has_terminal_evidence() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("evidence", "done")?,
        cache.path(),
        1,
    )?);
    let session = database.session()?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    let budget = Budget {
        max_query_duration: Duration::from_millis(100),
        ..Budget::default()
    };
    // 仅在引擎 Done 后注入等待，避免依赖宿主速度碰巧越过预算。
    AFTER_DONE.set(Some(Duration::from_millis(250)));
    let error = session
        .execute_with_budget("INSERT INTO items VALUES(1)", &budget)
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<SqlEvidence>(),
        Some(&SqlEvidence::Settled),
        "{error:#}"
    );
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline)
    );
    assert_eq!(
        database.session()?.query("SELECT value FROM items")?[0][0].to_string(),
        "1"
    );
    Ok(())
}

/// The guarded tests share the process-wide work counters, so they must not run
/// concurrently with each other while one of them measures a delta.
static GUARDED_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn guarded_test_lock() -> std::sync::MutexGuard<'static, ()> {
    GUARDED_TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn guarded_session(name: &str) -> Result<(tempfile::TempDir, crate::session::RemoteSession)> {
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("guarded", name)?,
        cache.path(),
        1,
    )?);
    let session = database.session()?;
    Ok((cache, session))
}

#[test]
fn guarded_readonly_statement_prepares_once_and_returns_rows() -> Result<()> {
    let _serial = guarded_test_lock();
    crate::work_metrics::set_enabled(true);
    let (_cache, session) = guarded_session("read_once")?;
    let before = crate::work_metrics::snapshot().expect("enabled snapshot");
    let outcome = session.guarded_readonly_statement("SELECT 7 AS answer", &[], &[], true)?;
    let after = crate::work_metrics::snapshot().expect("enabled snapshot");
    let GuardedStatementOutcome::Executed(result) = outcome else {
        panic!("a read-only statement asked for a durable intent");
    };
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].to_string(), "7");
    assert_eq!(
        after.execution.guarded_statement_calls - before.execution.guarded_statement_calls,
        1
    );
    assert_eq!(
        after.execution.guarded_statement_prepares - before.execution.guarded_statement_prepares,
        1,
        "the same statement must be prepared once"
    );
    assert_eq!(
        after.execution.guarded_statement_reprepares
            - before.execution.guarded_statement_reprepares,
        0,
        "the read-only path must not need a reprepare"
    );
    assert_eq!(
        after.execution.guarded_statement_requires_intent
            - before.execution.guarded_statement_requires_intent,
        0
    );
    Ok(())
}

#[test]
fn guarded_write_returns_requires_intent_before_executing() -> Result<()> {
    let _serial = guarded_test_lock();
    let (_cache, session) = guarded_session("write_refused")?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    let outcome =
        session.guarded_readonly_statement("INSERT INTO items VALUES (1)", &[], &[], true)?;
    assert!(matches!(
        outcome,
        GuardedStatementOutcome::RequiresIntent(GuardedIntentReason::EngineWritable)
    ));
    assert_eq!(
        session.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "0"
    );
    assert!(session.autocommit(&Budget::trusted_default())?);
    Ok(())
}

#[test]
fn guarded_trigger_side_effects_are_refused_before_the_insert() -> Result<()> {
    let _serial = guarded_test_lock();
    let (_cache, session) = guarded_session("trigger_refused")?;
    session.execute(
        "CREATE TABLE source(value INTEGER); \
         CREATE TABLE audit(value INTEGER); \
         CREATE TRIGGER copy AFTER INSERT ON source BEGIN \
             INSERT INTO audit VALUES (NEW.value); \
         END;",
    )?;
    let outcome =
        session.guarded_readonly_statement("INSERT INTO source VALUES (9)", &[], &[], true)?;
    assert!(matches!(
        outcome,
        GuardedStatementOutcome::RequiresIntent(GuardedIntentReason::EngineWritable)
    ));
    assert_eq!(
        session.query("SELECT count(*) FROM source")?[0][0].to_string(),
        "0"
    );
    assert_eq!(
        session.query("SELECT count(*) FROM audit")?[0][0].to_string(),
        "0"
    );
    Ok(())
}

#[test]
fn guarded_transaction_control_and_pragma_keep_the_original_path() -> Result<()> {
    let _serial = guarded_test_lock();
    let (_cache, session) = guarded_session("control_refused")?;
    for sql in [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT marker",
        "PRAGMA user_version = 7",
        "PRAGMA user_version",
        "ATTACH DATABASE ':memory:' AS attached",
        "-- leading comment\n /* block */ BEGIN",
    ] {
        let outcome = session.guarded_readonly_statement(sql, &[], &[], true)?;
        assert!(
            matches!(
                outcome,
                GuardedStatementOutcome::RequiresIntent(GuardedIntentReason::ConnectionControl)
            ),
            "{sql} took the guarded path"
        );
    }
    assert!(session.autocommit(&Budget::trusted_default())?);
    assert_eq!(session.query("PRAGMA user_version")?[0][0].to_string(), "0");
    Ok(())
}

#[test]
fn guarded_transaction_context_is_not_an_engine_write_verdict() -> Result<()> {
    let _serial = guarded_test_lock();
    let (_cache, session) = guarded_session("transaction_context")?;
    session.execute("BEGIN")?;
    let outcome = session.guarded_readonly_statement("SELECT 7", &[], &[], true)?;
    assert!(matches!(
        outcome,
        GuardedStatementOutcome::RequiresIntent(GuardedIntentReason::TransactionContext)
    ));
    assert!(!session.autocommit(&Budget::trusted_default())?);
    session.execute("ROLLBACK")?;
    let outcome = session.guarded_readonly_statement("SELECT 7", &[], &[], true)?;
    let GuardedStatementOutcome::Executed(result) = outcome else {
        panic!("transaction state contaminated a later read-only statement");
    };
    assert_eq!(result.rows[0][0].to_string(), "7");
    assert!(result.autocommit);
    Ok(())
}

/// A schema change between preparation and the first step reprepares the
/// statement. The reprepared program is a write, and the guarded step must
/// refuse it instead of running it, even though the first program was
/// read-only.
#[test]
fn guarded_step_refuses_program_turned_into_a_write_by_reprepare() -> Result<()> {
    let _serial = guarded_test_lock();
    let (_cache, session) = guarded_session("reprepare_refused")?;
    let database = session.lane.database()?;
    let control = session.lane.control.clone();
    let connection_guard = session
        .lane
        .connection
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let connection = connection_guard.as_ref().expect("lane connection");
    connection.execute(
        "CREATE SEQUENCE gate_sequence START 1; \
         CREATE VIEW gate_view AS SELECT 1 AS value;",
    )?;
    let statement = {
        let mut runner = connection.query_runner(b"SELECT value FROM gate_view");
        let statement = runner.next().expect("statement")?.expect("statement");
        assert!(runner.next().is_none());
        statement
    };
    assert!(statement.get_program().is_readonly());
    connection.execute(
        "DROP VIEW gate_view; \
         CREATE VIEW gate_view AS SELECT nextval('gate_sequence') AS value;",
    )?;
    let prior_changes = connection.total_changes();
    let budget = Budget::trusted_default();
    let deadline = RequestDeadline::new(&budget)?;
    let store = database.storage().store().clone();
    let mut guard = crate::session::budget::ExecutionBudget::install(
        connection,
        "SELECT value FROM gate_view",
        &budget,
        store,
        &deadline,
        control.clone(),
    )?;
    let context = super::ExecutionContext {
        database: &database,
        connection,
        control: &control,
        deadline: &deadline,
    };
    let (rows, statement) = super::run_guarded(&context, statement, &mut guard, true)?;
    assert!(
        rows.is_none(),
        "a program reprepared into a write must not run"
    );
    assert!(!connection.is_in_write_tx());
    assert_eq!(connection.total_changes(), prior_changes);
    drop(statement);
    Ok(())
}

#[test]
fn statement_select_reports_engine_counters_and_duration() -> Result<()> {
    let (_cache, session) = guarded_session("statement_select_stats")?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    session.execute("INSERT INTO items VALUES (1), (2), (3)")?;
    let result = session.statement("SELECT value FROM items", &[])?;
    assert_eq!(result.rows.len(), 3);
    assert!(
        result.rows_read >= result.rows.len() as u64,
        "a three-row scan must account for at least the returned rows: {}",
        result.rows_read
    );
    assert_eq!(result.rows_written, 0);
    assert!(result.query_duration_ms.is_finite());
    assert!(result.query_duration_ms >= 0.0);
    Ok(())
}

#[test]
fn statement_insert_reports_written_rows_and_duration() -> Result<()> {
    let (_cache, session) = guarded_session("statement_insert_stats")?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    let result = session.statement("INSERT INTO items VALUES (1)", &[])?;
    assert_eq!(result.affected_row_count, 1);
    assert_eq!(result.rows_written, 1);
    assert!(result.query_duration_ms.is_finite());
    assert!(result.query_duration_ms >= 0.0);
    Ok(())
}

#[test]
fn guarded_statement_reports_engine_counters_and_duration() -> Result<()> {
    let _serial = guarded_test_lock();
    let (_cache, session) = guarded_session("guarded_statement_stats")?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    session.execute("INSERT INTO items VALUES (1), (2), (3)")?;
    let outcome = session.guarded_readonly_statement("SELECT value FROM items", &[], &[], true)?;
    let GuardedStatementOutcome::Executed(result) = outcome else {
        panic!("a read-only statement asked for a durable intent");
    };
    assert_eq!(result.rows.len(), 3);
    assert!(
        result.rows_read >= result.rows.len() as u64,
        "a three-row scan must account for at least the returned rows: {}",
        result.rows_read
    );
    assert_eq!(result.rows_written, 0);
    assert!(result.query_duration_ms.is_finite());
    assert!(result.query_duration_ms >= 0.0);
    Ok(())
}
