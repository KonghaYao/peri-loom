use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Result};
use turso_core::{
    Connection, Database, MemoryIO, NoncommittingAbort, OpenOptions, SqliteDialect, Statement,
    StepResult,
};

fn connection() -> Result<(Arc<Database>, Arc<Connection>)> {
    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);
    let path = format!(
        "statement-safety-{}.db",
        NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
    );
    let database = Database::open(
        Arc::new(MemoryIO::new()),
        &path,
        OpenOptions::new(Arc::new(SqliteDialect)),
    )?;
    let connection = database.connect()?;
    connection.execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY);")?;
    Ok((database, connection))
}

fn next_row(statement: &mut Statement) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match statement.step()? {
            StepResult::Row => return Ok(()),
            StepResult::IO | StepResult::Yield => statement._io().step()?,
            StepResult::Sleep { duration } => std::thread::sleep(duration),
            terminal => bail!("expected row, received {terminal:?}"),
        }
    }
    bail!("row did not arrive within test deadline")
}

fn count(connection: &Arc<Connection>) -> Result<String> {
    let mut statement = connection.prepare("SELECT count(*) FROM ledger;")?;
    next_row(&mut statement)?;
    let value = statement
        .row()
        .unwrap()
        .get_values()
        .next()
        .unwrap()
        .to_string();
    Ok(value)
}

#[test]
fn noncommitting_cleanup_does_not_publish_returning_write() -> Result<()> {
    let (_database, connection) = connection()?;
    let mut statement = connection.prepare("INSERT INTO ledger VALUES(1) RETURNING id;")?;
    next_row(&mut statement)?;
    abort(&mut statement)?;
    drop(statement);
    ensure!(
        count(&connection)? == "0",
        "cleanup committed the interrupted write"
    );
    Ok(())
}

fn abort(statement: &mut Statement) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match statement.abort_noncommitting()? {
            NoncommittingAbort::StatementStopped => return Ok(()),
            NoncommittingAbort::PendingIo => statement._io().step()?,
            NoncommittingAbort::FinalizationAlreadyStarted => {
                bail!("simple RETURNING unexpectedly entered finalization")
            }
        }
    }
    bail!("noncommitting cleanup did not finish within test deadline")
}

#[test]
fn ordinary_reset_still_finishes_returning_write() -> Result<()> {
    let (_database, connection) = connection()?;
    let mut statement = connection.prepare("INSERT INTO ledger VALUES(1) RETURNING id;")?;
    next_row(&mut statement)?;
    statement.reset()?;
    drop(statement);
    ensure!(
        count(&connection)? == "1",
        "ordinary reset semantics changed"
    );
    Ok(())
}

#[test]
fn abort_keeps_committed_prefix_and_explicit_transaction_requires_rollback() -> Result<()> {
    let (_database, connection) = connection()?;
    connection.execute("INSERT INTO ledger VALUES(10); BEGIN; INSERT INTO ledger VALUES(20);")?;
    let mut statement = connection.prepare("INSERT INTO ledger VALUES(21) RETURNING id;")?;
    next_row(&mut statement)?;
    abort(&mut statement)?;
    drop(statement);
    ensure!(
        !connection.get_auto_commit(),
        "abort ended the explicit transaction"
    );
    ensure!(
        connection.is_in_write_tx(),
        "abort released the explicit writer"
    );
    connection.execute("ROLLBACK;")?;
    ensure!(
        count(&connection)? == "1",
        "abort lost prefix or committed current transaction"
    );
    Ok(())
}

#[test]
fn admission_pause_precedes_write_and_same_statement_can_resume() -> Result<()> {
    let (database, connection) = connection()?;
    let reader = database.connect()?;
    let mut statement = connection.prepare("INSERT INTO ledger VALUES(1);")?;
    let mut checks = 0;
    for _attempt in 0..4 {
        let result = statement.step_with_admission_check(|readonly| {
            checks += 1;
            readonly
        })?;
        ensure!(
            matches!(result, StepResult::Yield),
            "write did not pause: {result:?}"
        );
        ensure!(
            !connection.is_in_write_tx(),
            "paused statement acquired native writer"
        );
        ensure!(count(&reader)? == "0", "paused statement changed ledger");
    }
    ensure!(checks >= 4, "admission callback was skipped");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match statement.step_with_admission_check(|_| true)? {
            StepResult::Done => {
                drop(statement);
                ensure!(
                    count(&reader)? == "1",
                    "resume failed or duplicated operation"
                );
                return Ok(());
            }
            StepResult::IO | StepResult::Yield => statement._io().step()?,
            StepResult::Sleep { duration } => std::thread::sleep(duration),
            terminal => bail!("unexpected admitted terminal: {terminal:?}"),
        }
    }
    bail!("admitted statement did not complete")
}

#[test]
fn rejected_profile_can_stop_without_ending_existing_transaction() -> Result<()> {
    let (_database, connection) = connection()?;
    connection.execute("INSERT INTO ledger VALUES(10); BEGIN; INSERT INTO ledger VALUES(20);")?;
    let mut statement = connection.prepare("PRAGMA journal_mode = 'mvcc'")?;
    let mut callback_called = false;
    let result = statement.step_with_admission_check(|_| {
        callback_called = true;
        true
    });
    ensure!(result.is_err(), "unsupported profile was not rejected");
    ensure!(!callback_called, "unsupported program reached admission");
    ensure!(
        matches!(
            statement.abort_noncommitting()?,
            NoncommittingAbort::StatementStopped
        ),
        "unexecuted rejected statement did not stop"
    );
    drop(statement);
    ensure!(
        !connection.get_auto_commit() && connection.is_in_write_tx(),
        "rejected statement changed the existing transaction"
    );
    connection.execute("ROLLBACK;")?;
    ensure!(count(&connection)? == "1", "committed prefix changed");
    Ok(())
}

#[test]
fn nested_reuse_cannot_reset_root_returning_execution() -> Result<()> {
    let (_database, connection) = connection()?;
    let mut statement = connection.prepare("INSERT INTO ledger VALUES(1) RETURNING id")?;
    next_row(&mut statement)?;
    let before = format!("{:?}", statement.execution_state());
    let _ = statement.reset_for_subprogram_reuse();
    ensure!(
        format!("{:?}", statement.execution_state()) == before && statement.row().is_some(),
        "nested reuse reset a root statement and invalidated its cleanup proof"
    );
    abort(&mut statement)?;
    drop(statement);
    ensure!(count(&connection)? == "0", "root reuse committed a write");
    Ok(())
}
