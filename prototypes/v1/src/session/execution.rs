#[cfg(test)]
mod tests;

use std::num::NonZero;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use turso_core::{Connection, LimboError, NoncommittingAbort, Statement, StepResult, Value};

use super::admission::{Permit, SessionControl, UnsettledSqlOutcome};
use super::budget::{Budget, ExecutionBudget, RequestDeadline};
use super::{
    DescribeResult, GuardedIntentReason, GuardedStatementOutcome, StatementColumn, StatementResult,
    WriteCursorPage,
};
use crate::database::RemoteDatabase;
use crate::metrics::{Metrics, Operation};

struct ExecutionContext<'call> {
    database: &'call RemoteDatabase,
    connection: &'call Arc<Connection>,
    control: &'call Arc<SessionControl>,
    deadline: &'call RequestDeadline,
}

pub(super) fn describe(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    sql: &str,
    budget: &Budget,
    deadline: &RequestDeadline,
) -> Result<DescribeResult> {
    let store = database.storage().store().clone();
    let guard =
        ExecutionBudget::install(connection, sql, budget, store, deadline, control.clone())?;
    let _operation = database.operation_until(deadline, false)?;
    guard.check()?;
    super::configure(connection);
    let mut runner = connection.query_runner(sql.as_bytes());
    let statement = runner
        .next()
        .context("empty remote SQL statement")??
        .context("empty remote SQL statement")?;
    ensure!(
        runner.next().is_none(),
        "multiple SQL statements not supported"
    );
    let params = (1..=statement.parameters_count())
        .map(|index| statement.parameters().name(NonZero::new(index).unwrap()))
        .collect();
    let cols = (0..statement.num_columns())
        .map(|index| StatementColumn {
            name: statement.get_column_name(index).into_owned(),
            decltype: statement.get_column_decltype(index),
        })
        .collect();
    Ok(DescribeResult {
        params,
        cols,
        is_explain: !matches!(statement.get_query_mode(), turso_core::QueryMode::Normal),
        is_readonly: statement.get_program().is_readonly(),
    })
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

#[allow(clippy::too_many_arguments)]
pub(super) fn statement(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    sql: &str,
    args: &[Value],
    named_args: &[(String, Value)],
    // A caller that wants no rows still steps the statement to Done; only
    // materializing and returning them is skipped, so the write and its outcome
    // are unaffected.
    want_rows: bool,
    budget: &Budget,
    deadline: &RequestDeadline,
) -> Result<StatementResult> {
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
            let mut statement = {
                let _operation = database.operation_until(deadline, false)?;
                guard.check()?;
                super::configure(connection);
                let mut runner = connection.query_runner(sql.as_bytes());
                let statement = runner
                    .next()
                    .context("empty remote SQL statement")??
                    .context("empty remote SQL statement")?;
                ensure!(
                    runner.next().is_none(),
                    "multiple SQL statements not supported"
                );
                statement
            };
            bind_parameters(&mut statement, args, named_args)?;
            let cols = (0..statement.num_columns())
                .map(|index| StatementColumn {
                    name: statement.get_column_name(index).into_owned(),
                    decltype: statement.get_column_decltype(index),
                })
                .collect::<Vec<_>>();
            let prior_rowid_generation = connection.last_insert_rowid_generation();
            let prior_changes = connection.total_changes();
            let (_, rows) = run(&context, statement, &mut guard, want_rows, 0)?;
            let affected_row_count = if connection.total_changes() != prior_changes {
                connection.changes()
            } else {
                0
            };
            let last_insert_rowid = (connection.last_insert_rowid_generation()
                != prior_rowid_generation)
                .then(|| connection.last_insert_rowid());
            Ok(StatementResult {
                cols,
                rows,
                affected_row_count,
                last_insert_rowid,
                autocommit: connection.get_auto_commit(),
            })
        },
        |_| 0,
    )
}

/// Statements whose side effects the engine's read-only flag does not describe:
/// they control the transaction or change connection/database state without
/// writing a table row. They keep the describe+execute path instead of being
/// admitted by a classification that only answers "may this commit?".
const GUARDED_REFUSED_KEYWORDS: &[&str] = &[
    "BEGIN",
    "COMMIT",
    "END",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "PRAGMA",
    "ATTACH",
    "DETACH",
    "VACUUM",
    "ANALYZE",
    "REINDEX",
];

/// The first keyword of the statement, ignoring leading whitespace and comments.
/// Only ASCII is considered: anything else cannot name one of the refused
/// keywords, and the engine's verdict still governs whether it may run.
fn leading_keyword(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    loop {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index + 1 < bytes.len() && bytes[index] == b'-' && bytes[index + 1] == b'-' {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if index + 1 < bytes.len() && bytes[index] == b'/' && bytes[index + 1] == b'*' {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = index.saturating_add(2).min(bytes.len());
            continue;
        }
        break;
    }
    let start = index;
    while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_') {
        index += 1;
    }
    (start < index).then(|| sql[start..index].to_ascii_uppercase())
}

fn guarded_statement_fast_path(sql: &str) -> bool {
    leading_keyword(sql).is_none_or(|keyword| !GUARDED_REFUSED_KEYWORDS.contains(&keyword.as_str()))
}

/// Prepares, binds and steps one statement while requiring the engine to call
/// it read-only before every step, including the step after an automatic
/// reprepare. A statement the engine classifies as writing before it can run is
/// aborted and answered with `RequiresIntent`, so the caller persists its
/// intent and takes the committing path.
#[allow(clippy::too_many_arguments)]
pub(super) fn guarded_readonly_statement(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    sql: &str,
    args: &[Value],
    named_args: &[(String, Value)],
    want_rows: bool,
    budget: &Budget,
    deadline: &RequestDeadline,
) -> Result<GuardedStatementOutcome> {
    Metrics::global().observe(
        Operation::Query,
        || {
            crate::work_metrics::note_guarded_statement_call();
            // A statement inside an explicit transaction cannot be proven to
            // finish without committing that transaction, and a transaction
            // control statement has effects the read-only flag does not cover.
            if !connection.get_auto_commit() {
                crate::work_metrics::note_guarded_statement_outcome(0, true);
                return Ok(GuardedStatementOutcome::RequiresIntent(
                    GuardedIntentReason::TransactionContext,
                ));
            }
            if !guarded_statement_fast_path(sql) {
                crate::work_metrics::note_guarded_statement_outcome(0, true);
                return Ok(GuardedStatementOutcome::RequiresIntent(
                    GuardedIntentReason::ConnectionControl,
                ));
            }
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
            let mut statement = {
                let _operation = database.operation_until(deadline, false)?;
                guard.check()?;
                super::configure(connection);
                let mut runner = connection.query_runner(sql.as_bytes());
                let statement = runner
                    .next()
                    .context("empty remote SQL statement")??
                    .context("empty remote SQL statement")?;
                ensure!(
                    runner.next().is_none(),
                    "multiple SQL statements not supported"
                );
                statement
            };
            crate::work_metrics::note_guarded_statement_prepare();
            bind_parameters(&mut statement, args, named_args)?;
            let cols = (0..statement.num_columns())
                .map(|index| StatementColumn {
                    name: statement.get_column_name(index).into_owned(),
                    decltype: statement.get_column_decltype(index),
                })
                .collect::<Vec<_>>();
            let prior_rowid_generation = connection.last_insert_rowid_generation();
            let prior_changes = connection.total_changes();
            let (rows, statement) = run_guarded(&context, statement, &mut guard, want_rows)?;
            let reprepares = statement.metrics().reprepares;
            drop(statement);
            if database
                .admission()
                .settle(control, writer(connection))
                .is_err()
            {
                database.admission().quarantine();
                return Err(UnsettledSqlOutcome.into());
            }
            deadline.evidence.settle();
            let Some(rows) = rows else {
                crate::work_metrics::note_guarded_statement_outcome(reprepares, true);
                return Ok(GuardedStatementOutcome::RequiresIntent(
                    GuardedIntentReason::EngineWritable,
                ));
            };
            let affected_row_count = if connection.total_changes() != prior_changes {
                connection.changes()
            } else {
                0
            };
            let last_insert_rowid = (connection.last_insert_rowid_generation()
                != prior_rowid_generation)
                .then(|| connection.last_insert_rowid());
            crate::work_metrics::note_guarded_statement_outcome(reprepares, false);
            Ok(GuardedStatementOutcome::Executed(StatementResult {
                cols,
                rows,
                affected_row_count,
                last_insert_rowid,
                autocommit: connection.get_auto_commit(),
            }))
        },
        |_| 0,
    )
}

/// Runs the statement with the read-only admission enforced on every step. The
/// statement is returned so its engine counters can be read and so the caller
/// keeps ownership of the engine state until it settles the session.
fn run_guarded(
    context: &ExecutionContext<'_>,
    mut statement: Statement,
    guard: &mut ExecutionBudget<'_>,
    collect: bool,
) -> Result<(Option<Vec<Vec<Value>>>, Statement)> {
    let ExecutionContext {
        database,
        connection,
        control,
        deadline,
    } = *context;
    let store = database.storage().store();
    store.validate_transaction(!connection.get_auto_commit())?;
    let mut rows = Vec::new();
    let stepped = (|| -> Result<Option<Vec<Vec<Value>>>> {
        loop {
            guard.check()?;
            deadline.check()?;
            let mut refused = false;
            let result = {
                let _operation = database.operation_until(deadline, false)?;
                let step_clock = guard.enter_step()?;
                // The engine calls this again for every step and for the new
                // program after an automatic reprepare, so a schema change that
                // turns the statement into a write cannot slip through the
                // verdict taken at prepare time.
                let result = statement.step_with_admission_check(|readonly| {
                    if readonly {
                        deadline.evidence.enter();
                        true
                    } else {
                        refused = true;
                        false
                    }
                });
                step_clock.finish()?;
                result?
            };
            if refused {
                // Nothing the engine classified as writing ran; every program
                // that did run carried a read-only verdict for that step.
                guard.account_steps(statement.metrics().vm_steps)?;
                return Ok(None);
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
                    guard.account_steps(statement.metrics().vm_steps)?;
                    return Ok(Some(rows));
                }
                StepResult::Busy => return Err(LimboError::Busy.into()),
                StepResult::Interrupt => {
                    guard.check()?;
                    deadline.check()?;
                    store.validate_local()?;
                    return Err(LimboError::Interrupt.into());
                }
                StepResult::IO | StepResult::Yield => {
                    let _operation = database.operation_until(deadline, false)?;
                    guard.check()?;
                    store.validate_local()?;
                    statement._io().step()?;
                    guard.check()?;
                    store.validate_local()?;
                }
                StepResult::Sleep { duration } => {
                    super::drive::sleep(duration, store.as_ref(), guard)?
                }
            }
            guard.account_steps(statement.metrics().vm_steps)?;
        }
    })();
    match stepped {
        Ok(Some(rows)) => Ok((Some(rows), statement)),
        Ok(None) => {
            let stopped = database
                .operation_until(deadline, true)
                .and_then(|_operation| Ok(statement.abort_noncommitting()?));
            match stopped {
                Ok(NoncommittingAbort::StatementStopped) => Ok((None, statement)),
                Ok(
                    NoncommittingAbort::PendingIo | NoncommittingAbort::FinalizationAlreadyStarted,
                )
                | Err(_) => {
                    database.defer_native_retirement(statement);
                    Err(UnsettledSqlOutcome.into())
                }
            }
        }
        Err(error) => {
            #[cfg(test)]
            database.before_native_abort();
            let stopped = database
                .operation_until(deadline, true)
                .and_then(|_operation| Ok(statement.abort_noncommitting()?));
            match stopped {
                Ok(NoncommittingAbort::StatementStopped) => {
                    drop(statement);
                    if database
                        .admission()
                        .settle(control, writer(connection))
                        .is_err()
                    {
                        database.admission().quarantine();
                        return Err(error.context(UnsettledSqlOutcome));
                    }
                    deadline.evidence.settle();
                    Err(error)
                }
                Ok(
                    NoncommittingAbort::PendingIo | NoncommittingAbort::FinalizationAlreadyStarted,
                )
                | Err(_) => {
                    database.defer_native_retirement(statement);
                    Err(error.context(UnsettledSqlOutcome))
                }
            }
        }
    }
}

pub(super) struct ReadCursor {
    statement: Statement,
    budget: Budget,
    deadline: RequestDeadline,
}

pub(super) struct WriteCursor {
    statement: Statement,
    budget: Budget,
    deadline: RequestDeadline,
    permit: Option<Permit>,
    bytes: usize,
    prior_changes: i64,
    prior_rowid_generation: u64,
}

pub(super) fn open_write_cursor(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    sql: &str,
    args: &[Value],
    named_args: &[(String, Value)],
    budget: &Budget,
) -> Result<(WriteCursor, Vec<StatementColumn>)> {
    let deadline = RequestDeadline::new(budget)?;
    let store = database.storage().store().clone();
    let guard =
        ExecutionBudget::install(connection, sql, budget, store, &deadline, control.clone())?;
    let _operation = database.operation_until(&deadline, false)?;
    guard.check()?;
    super::configure(connection);
    let mut runner = connection.query_runner(sql.as_bytes());
    let mut statement = runner
        .next()
        .context("empty remote SQL statement")??
        .context("empty remote SQL statement")?;
    ensure!(
        runner.next().is_none(),
        "multiple SQL statements not supported"
    );
    // A write cursor without RETURNING columns is a valid statement: it emits no
    // rows, but its step end still carries affected_row_count and
    // last_insert_rowid. The Hrana conformance suite and the serverless clients
    // open cursor streams for such writes, so requiring columns here would refuse
    // a statement the protocol defines.
    ensure!(
        !statement.get_program().is_readonly(),
        "write cursor requires writable SQL"
    );
    bind_parameters(&mut statement, args, named_args)?;
    let cols = (0..statement.num_columns())
        .map(|index| StatementColumn {
            name: statement.get_column_name(index).into_owned(),
            decltype: statement.get_column_decltype(index),
        })
        .collect();
    Ok((
        WriteCursor {
            statement,
            budget: budget.clone(),
            deadline,
            permit: None,
            bytes: 0,
            prior_changes: connection.total_changes(),
            prior_rowid_generation: connection.last_insert_rowid_generation(),
        },
        cols,
    ))
}

pub(super) fn fetch_write_cursor(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    cursor: &mut WriteCursor,
    page_size: usize,
) -> Result<WriteCursorPage> {
    ensure!(
        (1..=256).contains(&page_size),
        "cursor page size exceeds limit"
    );
    let store = database.storage().store().clone();
    let mut guard = ExecutionBudget::install(
        connection,
        "",
        &cursor.budget,
        store,
        &cursor.deadline,
        control.clone(),
    )?;
    guard.resume_bytes(cursor.bytes)?;
    let context = ExecutionContext {
        database,
        connection,
        control,
        deadline: &cursor.deadline,
    };
    let mut terminal = false;
    let (rows, done) = run_steps(
        &context,
        &mut cursor.statement,
        &mut guard,
        true,
        0,
        &mut cursor.permit,
        &mut terminal,
        Some(page_size),
    )?;
    cursor.bytes = guard.accounted_bytes();
    let affected_row_count = done.then(|| {
        if connection.total_changes() != cursor.prior_changes {
            connection.changes()
        } else {
            0
        }
    });
    let last_insert_rowid = (done
        && connection.last_insert_rowid_generation() != cursor.prior_rowid_generation)
        .then(|| connection.last_insert_rowid());
    Ok(WriteCursorPage {
        rows,
        done,
        affected_row_count,
        last_insert_rowid,
    })
}

pub(super) fn settle_write_cursor(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    cursor: WriteCursor,
) -> Result<()> {
    let WriteCursor {
        statement, permit, ..
    } = cursor;
    drop(statement);
    match permit {
        Some(permit) => permit.settle(writer(connection)),
        None => database.admission().settle(control, writer(connection)),
    }
}

pub(super) fn cancel_write_cursor(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    mut cursor: WriteCursor,
) -> Result<()> {
    let deadline =
        RequestDeadline::until(std::time::Instant::now() + std::time::Duration::from_secs(20));
    let aborted = database
        .operation_until(&deadline, true)
        .and_then(|_operation| Ok(cursor.statement.abort_noncommitting()?));
    match aborted {
        Ok(NoncommittingAbort::StatementStopped) => {
            drop(cursor.statement);
            let settled = match cursor.permit {
                Some(permit) => permit.settle(writer(connection)),
                None => database.admission().settle(control, writer(connection)),
            };
            if settled.is_err() {
                database.admission().quarantine();
                return Err(UnsettledSqlOutcome.into());
            }
            Ok(())
        }
        _ => {
            database.defer_native_retirement(cursor.statement);
            drop(cursor.permit);
            database.admission().quarantine();
            Err(UnsettledSqlOutcome.into())
        }
    }
}

pub(super) fn open_read_cursor(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    sql: &str,
    args: &[Value],
    named_args: &[(String, Value)],
    budget: &Budget,
) -> Result<(ReadCursor, Vec<StatementColumn>)> {
    let deadline = RequestDeadline::new(budget)?;
    let store = database.storage().store().clone();
    let guard =
        ExecutionBudget::install(connection, sql, budget, store, &deadline, control.clone())?;
    let _operation = database.operation_until(&deadline, false)?;
    guard.check()?;
    super::configure(connection);
    let mut runner = connection.query_runner(sql.as_bytes());
    let mut statement = runner
        .next()
        .context("empty remote SQL statement")??
        .context("empty remote SQL statement")?;
    ensure!(
        runner.next().is_none(),
        "multiple SQL statements not supported"
    );
    ensure!(
        statement.get_program().is_readonly(),
        "read cursor requires read-only SQL"
    );
    bind_parameters(&mut statement, args, named_args)?;
    let cols = (0..statement.num_columns())
        .map(|index| StatementColumn {
            name: statement.get_column_name(index).into_owned(),
            decltype: statement.get_column_decltype(index),
        })
        .collect();
    Ok((
        ReadCursor {
            statement,
            budget: budget.clone(),
            deadline,
        },
        cols,
    ))
}

pub(super) fn fetch_read_cursor(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    cursor: &mut ReadCursor,
    page_size: usize,
) -> Result<(Vec<Vec<Value>>, bool)> {
    ensure!(
        (1..=256).contains(&page_size),
        "cursor page size exceeds limit"
    );
    let store = database.storage().store().clone();
    let mut guard = ExecutionBudget::install(
        connection,
        "",
        &cursor.budget,
        store,
        &cursor.deadline,
        control.clone(),
    )?;
    let context = ExecutionContext {
        database,
        connection,
        control,
        deadline: &cursor.deadline,
    };
    let mut permit = None;
    let mut terminal = false;
    let result = run_steps(
        &context,
        &mut cursor.statement,
        &mut guard,
        true,
        0,
        &mut permit,
        &mut terminal,
        Some(page_size),
    );
    if let Some(permit) = permit {
        drop(permit);
        database.admission().quarantine();
        return Err(UnsettledSqlOutcome.into());
    }
    result
}

pub(super) fn cancel_read_cursor(
    database: &RemoteDatabase,
    connection: &Arc<Connection>,
    control: &Arc<SessionControl>,
    mut cursor: ReadCursor,
) -> Result<()> {
    let deadline =
        RequestDeadline::until(std::time::Instant::now() + std::time::Duration::from_secs(20));
    let aborted = database
        .operation_until(&deadline, true)
        .and_then(|_operation| Ok(cursor.statement.abort_noncommitting()?));
    match aborted {
        Ok(NoncommittingAbort::StatementStopped) => {
            drop(cursor);
            database.admission().settle(control, writer(connection))?;
            Ok(())
        }
        _ => {
            database.defer_native_retirement(cursor.statement);
            Err(UnsettledSqlOutcome.into())
        }
    }
}

fn bind_parameters(
    statement: &mut Statement,
    args: &[Value],
    named_args: &[(String, Value)],
) -> Result<()> {
    let count = statement.parameters_count();
    ensure!(
        args.len().saturating_add(named_args.len()) == count,
        "SQL parameter count mismatch"
    );
    let mut bound = vec![false; count];
    for (name, value) in named_args {
        ensure!(!name.is_empty(), "empty SQL parameter name");
        let index = if [':', '@', '$', '?']
            .iter()
            .any(|prefix| name.starts_with(*prefix))
        {
            statement.parameter_index(name)
        } else {
            let mut found = None;
            for prefix in [':', '@', '$'] {
                if let Some(index) = statement.parameter_index(&format!("{prefix}{name}")) {
                    ensure!(found.is_none(), "ambiguous SQL parameter name");
                    found = Some(index);
                }
            }
            found
        }
        .context("unknown SQL parameter name")?;
        ensure!(!bound[index.get() - 1], "duplicate SQL parameter name");
        bound[index.get() - 1] = true;
        statement.bind_at(index, value.clone())?;
    }
    let mut positional = args.iter();
    for (slot, already_bound) in bound.into_iter().enumerate() {
        if already_bound {
            continue;
        }
        let index = NonZero::new(slot + 1).unwrap();
        if let Some(name) = statement.parameters().name(index) {
            ensure!(
                ![':', '@', '$']
                    .iter()
                    .any(|prefix| name.starts_with(*prefix)),
                "missing named SQL parameter"
            );
        }
        let value = positional.next().context("SQL parameter count mismatch")?;
        statement.bind_at(index, value.clone())?;
    }
    ensure!(positional.next().is_none(), "SQL parameter count mismatch");
    Ok(())
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
    let executed = crate::work_metrics::ExecuteTimer::begin();
    let result = run_steps(
        context,
        &mut statement,
        guard,
        collect,
        prior_steps,
        &mut permit,
        &mut terminal,
        None,
    );
    match result {
        Ok((rows, _)) => {
            let steps = prior_steps.saturating_add(statement.metrics().vm_steps);
            executed.finish(Some(steps));
            drop(statement);
            if let Some(permit) = permit {
                permit.settle(writer(connection))?;
            } else {
                database.admission().settle(control, writer(connection))?;
            }
            deadline.evidence.settle();
            Ok((steps, rows))
        }
        Err(error) => {
            // The engine step count of a failed execution is not reported: the abort path
            // can retire the statement, and a counter that reads state a retired statement
            // may already have released is worse than a missing number.
            executed.finish(None);
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
                    deadline.evidence.settle();
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

#[allow(clippy::too_many_arguments)]
fn run_steps(
    context: &ExecutionContext<'_>,
    statement: &mut Statement,
    guard: &mut ExecutionBudget<'_>,
    collect: bool,
    prior_steps: u64,
    permit: &mut Option<Permit>,
    terminal: &mut bool,
    page_size: Option<usize>,
) -> Result<(Vec<Vec<Value>>, bool)> {
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
        let mut paused = false;
        let result = {
            let _operation = database.operation_until(deadline, false)?;
            let step_clock = guard.enter_step()?;
            let result = statement.step_with_admission_check(|readonly| {
                if readonly || permit.is_some() {
                    deadline.evidence.enter();
                    true
                } else {
                    paused = true;
                    false
                }
            });
            step_clock.finish()?;
            result?
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
                    if page_size.is_some_and(|limit| rows.len() == limit) {
                        guard.account_steps(
                            prior_steps.saturating_add(statement.metrics().vm_steps),
                        )?;
                        return Ok((rows, false));
                    }
                }
            }
            StepResult::Done => {
                *terminal = true;
                #[cfg(test)]
                tests::after_done();
                guard.account_steps(prior_steps.saturating_add(statement.metrics().vm_steps))?;
                return Ok((rows, true));
            }
            StepResult::Busy => return Err(LimboError::Busy.into()),
            StepResult::Interrupt => {
                guard.check()?;
                deadline.check()?;
                store.validate_local()?;
                return Err(LimboError::Interrupt.into());
            }
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
