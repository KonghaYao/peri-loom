use std::sync::Arc;

use anyhow::{Context, Result};
use turso_core::{Connection, LimboError, NoncommittingAbort, Statement, StepResult, Value};

use super::admission::{Permit, SessionControl, UnsettledSqlOutcome};
use super::budget::{Budget, ExecutionBudget, RequestDeadline};
use crate::database::RemoteDatabase;
use crate::metrics::{Metrics, Operation};

struct ExecutionContext<'call> {
    database: &'call RemoteDatabase,
    connection: &'call Arc<Connection>,
    control: &'call Arc<SessionControl>,
    deadline: &'call RequestDeadline,
}

pub(super) fn execute(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    sql: &str,
    budget: &Budget,
    deadline: &RequestDeadline,
) -> Result<()> {
    Metrics::global().observe(
        Operation::Execute,
        || {
            let context = ExecutionContext {
                database,
                connection,
                control,
                deadline,
            };
            let store = database.storage().store().clone();
            let mut guard = ExecutionBudget::install(
                connection,
                sql,
                budget,
                store,
                deadline,
                control.clone(),
            )?;
            let mut runner = connection.query_runner(sql.as_bytes());
            let mut steps = 0;
            loop {
                let prepared = {
                    let _operation = database.operation_until(deadline, false)?;
                    guard.check()?;
                    super::configure(connection);
                    runner.next()
                };
                let Some(prepared) = prepared else { break };
                if let Some(statement) = prepared.context("prepare remote SQL statement")? {
                    let (next_steps, _) = run(&context, statement, &mut guard, false, steps)?;
                    steps = next_steps;
                }
            }
            Ok(())
        },
        |_| 0,
    )
}

pub(super) fn query(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    sql: &str,
    budget: &Budget,
    deadline: &RequestDeadline,
) -> Result<Vec<Vec<Value>>> {
    Metrics::global().observe(
        Operation::Query,
        || {
            let context = ExecutionContext {
                database,
                connection,
                control,
                deadline,
            };
            let store = database.storage().store().clone();
            let mut guard = ExecutionBudget::install(
                connection,
                sql,
                budget,
                store,
                deadline,
                control.clone(),
            )?;
            let statement = {
                let _operation = database.operation_until(deadline, false)?;
                guard.check()?;
                super::configure(connection);
                connection.query(sql).context("prepare remote query")?
            };
            match statement {
                Some(statement) => {
                    run(&context, statement, &mut guard, true, 0).map(|(_, rows)| rows)
                }
                None => Ok(Vec::new()),
            }
        },
        |_| 0,
    )
}

fn writer(connection: &Connection) -> bool {
    connection.is_in_write_tx() || connection.get_pager().holds_write_lock()
}

fn run(
    context: &ExecutionContext<'_>,
    mut statement: Statement,
    guard: &mut ExecutionBudget<'_>,
    collect: bool,
    prior_steps: u64,
) -> Result<(u64, Vec<Vec<Value>>)> {
    let ExecutionContext {
        database,
        connection,
        control,
        deadline,
    } = *context;
    let mut permit: Option<Permit> = None;
    let mut terminal = false;
    let result = run_steps(
        context,
        &mut statement,
        guard,
        collect,
        prior_steps,
        &mut permit,
        &mut terminal,
    );
    match result {
        Ok(rows) => {
            let steps = prior_steps.saturating_add(statement.metrics().vm_steps);
            drop(statement);
            if let Some(permit) = permit {
                permit.settle(writer(connection))?;
            } else {
                database.admission().settle(control, writer(connection))?;
            }
            Ok((steps, rows))
        }
        Err(error) => {
            #[cfg(test)]
            database.before_native_abort();
            let stopped = if terminal {
                Ok(NoncommittingAbort::StatementStopped)
            } else {
                database
                    .operation_until(deadline, true)
                    .and_then(|_operation| Ok(statement.abort_noncommitting()?))
            };
            match stopped {
                Ok(NoncommittingAbort::StatementStopped) => {
                    drop(statement);
                    let settled = match permit {
                        Some(permit) => permit.settle(writer(connection)),
                        None => database.admission().settle(control, writer(connection)),
                    };
                    if settled.is_err() {
                        database.admission().quarantine();
                        return Err(error.context(UnsettledSqlOutcome));
                    }
                    Err(error)
                }
                Ok(
                    NoncommittingAbort::PendingIo | NoncommittingAbort::FinalizationAlreadyStarted,
                )
                | Err(_) => {
                    database.defer_native_retirement(statement);
                    drop(permit);
                    Err(error.context(UnsettledSqlOutcome))
                }
            }
        }
    }
}

fn run_steps(
    context: &ExecutionContext<'_>,
    statement: &mut Statement,
    guard: &mut ExecutionBudget<'_>,
    collect: bool,
    prior_steps: u64,
    permit: &mut Option<Permit>,
    terminal: &mut bool,
) -> Result<Vec<Vec<Value>>> {
    let ExecutionContext {
        database,
        connection,
        control,
        deadline,
    } = *context;
    let store = database.storage().store();
    store.validate_transaction(!connection.get_auto_commit())?;
    let mut rows = Vec::new();
    loop {
        guard.check()?;
        deadline.check()?;
        store.validate_local()?;
        let mut paused = false;
        let result = {
            let _operation = database.operation_until(deadline, false)?;
            statement.step_with_admission_check(|readonly| {
                if readonly || permit.is_some() {
                    true
                } else {
                    paused = true;
                    false
                }
            })?
        };
        if paused {
            let pager = connection.get_pager();
            let promote = pager.holds_read_lock() && !writer(connection);
            *permit = Some(database.admission().acquire(control, promote, || {
                guard.check()?;
                deadline.check()?;
                store.validate_local()
            })?);
            store.validate_transaction(!connection.get_auto_commit())?;
            continue;
        }
        match result {
            StepResult::Row => {
                let row = statement.row().context("native statement row missing")?;
                guard.account_row(row.get_values())?;
                if collect {
                    rows.push(row.get_values().cloned().collect());
                }
            }
            StepResult::Done => {
                *terminal = true;
                guard.account_steps(prior_steps.saturating_add(statement.metrics().vm_steps))?;
                return Ok(rows);
            }
            StepResult::Busy => return Err(LimboError::Busy.into()),
            StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
            StepResult::IO | StepResult::Yield => {
                let _operation = database.operation_until(deadline, false)?;
                guard.check()?;
                store.validate_local()?;
                statement._io().step()?;
                guard.check()?;
                store.validate_local()?;
            }
            StepResult::Sleep { duration } => super::drive::sleep(duration, store.as_ref(), guard)?,
        }
        guard.account_steps(prior_steps.saturating_add(statement.metrics().vm_steps))?;
    }
}
