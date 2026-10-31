use std::time::Instant;

use anyhow::{bail, ensure, Context, Result};
use base64::Engine;
use serde_json::{json, Value};
use turso_core::Value as SqlValue;

use crate::metrics::{Metrics, Operation};
use crate::ownership::types::OwnershipCommand;
use crate::runtime::{result_reservation_bytes, BudgetLane};
use crate::session::{
    AdmissionError, Budget, GuardedIntentReason, GuardedStatementOutcome, SqlEvidence,
    StatementResult,
};

use super::diagnostics::Action;
use super::outcome::{execution_completed, execution_rejected};
use super::protocol::{
    BatchStatement, NamedStatementArg, SessionState, StatementArg, WorkerCommand,
};
use super::server::State;
use super::session_entry::SessionCall;
use super::sessions::{rows, value};

/// Memory charged for one request before its command is dispatched. Input is the SQL text
/// and bound arguments as they are already held by the parsed request; output is the
/// conservative upper bound of a row-bearing response. `Execute` and control commands with
/// no row payload still pay for their input.
#[derive(Clone, Copy)]
struct RequestMemory {
    lane: BudgetLane,
    bytes: u64,
}

impl RequestMemory {
    fn for_command(command: &WorkerCommand, control: bool) -> Self {
        let result = result_reservation_bytes(Budget::trusted_default().max_result_bytes);
        let (input, output) = match command {
            WorkerCommand::Execute { sql, .. } => (sql.len() as u64, 0),
            WorkerCommand::Query { sql, .. } => (sql.len() as u64, result),
            WorkerCommand::Describe { sql, .. } => (sql.len() as u64, 0),
            WorkerCommand::Statement {
                sql,
                args,
                named_args,
                want_rows,
                ..
            }
            | WorkerCommand::GuardedReadonlyStatement {
                sql,
                args,
                named_args,
                want_rows,
                ..
            } => (
                sql_bytes(sql, args, named_args),
                if *want_rows { result } else { 0 },
            ),
            // The block may return up to `max_result_bytes` of its own plus one
            // statement that crosses that boundary, so the reservation covers
            // both rather than treating the block as a single result.
            WorkerCommand::GuardedReadonlyBatch {
                statements,
                max_result_bytes,
                ..
            } => {
                let input = statements.iter().fold(0u64, |total, statement| {
                    total.saturating_add(sql_bytes(
                        &statement.sql,
                        &statement.args,
                        &statement.named_args,
                    ))
                });
                (
                    input,
                    result_reservation_bytes(
                        max_result_bytes.saturating_add(Budget::trusted_default().max_result_bytes),
                    ),
                )
            }
            WorkerCommand::OpenReadCursor {
                sql,
                args,
                named_args,
                ..
            }
            | WorkerCommand::OpenWriteCursor {
                sql,
                args,
                named_args,
                ..
            } => (sql_bytes(sql, args, named_args), 0),
            WorkerCommand::OpenReadCursorFirst {
                sql,
                args,
                named_args,
                ..
            }
            | WorkerCommand::OpenWriteCursorFirst {
                sql,
                args,
                named_args,
                ..
            } => (sql_bytes(sql, args, named_args), result),
            WorkerCommand::FetchReadCursor { .. } | WorkerCommand::FetchWriteCursor { .. } => {
                (0, result)
            }
            // Control responses carry per-database resources and metrics. Charging the same
            // result bound is what makes the headroom real rather than a label.
            WorkerCommand::Ownership { .. }
            | WorkerCommand::GetMetrics
            | WorkerCommand::GetCapacityTrace { .. } => (0, result),
            _ => (0, 0),
        };
        let lane = if control {
            BudgetLane::Control
        } else if matches!(
            command,
            WorkerCommand::CloseSession { .. } | WorkerCommand::CloseOwnedSessions { .. }
        ) {
            BudgetLane::Cleanup
        } else {
            BudgetLane::Sql
        };
        Self {
            lane,
            bytes: input.saturating_add(output),
        }
    }
}

fn sql_bytes(sql: &str, args: &[StatementArg], named_args: &[NamedStatementArg]) -> u64 {
    let mut bytes = sql.len() as u64;
    for arg in args {
        bytes = bytes.saturating_add(arg_bytes(arg));
    }
    for arg in named_args {
        bytes = bytes
            .saturating_add(arg.name.len() as u64)
            .saturating_add(arg_bytes(&arg.value));
    }
    bytes
}

fn arg_bytes(arg: &StatementArg) -> u64 {
    match arg {
        StatementArg::Null => 0,
        StatementArg::Integer { value } => value.len() as u64,
        StatementArg::Float { .. } => std::mem::size_of::<f64>() as u64,
        StatementArg::Text { value } => value.len() as u64,
        StatementArg::Blob { base64 } => base64.len() as u64,
    }
}

impl State {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn dispatch(
        &self,
        command: WorkerCommand,
        control: bool,
        request_id: &str,
        version: u32,
        call: Option<&SessionCall>,
        terminal: &mut Option<SessionState>,
        deadline: Instant,
    ) -> Result<Value> {
        let _diagnostic_request = crate::slow_diagnostics::enter(request_id);
        // Charge before the match that can execute SQL. The token lives until `dispatch`
        // returns, so every result and error path returns the bytes; a refusal carries
        // `SqlEvidence::NotExecuted` from the runtime and no statement has run.
        let request_memory = RequestMemory::for_command(&command, control);
        let request_bytes = usize::try_from(request_memory.bytes).unwrap_or(usize::MAX);
        let _memory_reservation = self
            .runtime
            .reserve_inflight(request_memory.lane, request_bytes)
            .context("worker request memory admission")?;
        match command {
            WorkerCommand::GetCapacityTrace {
                stream_id,
                after_sequence,
                max_events,
            } => {
                ensure!(control, "control authorization required");
                self.runtime
                    .capacity_trace_page(&stream_id, after_sequence, max_events)
            }
            WorkerCommand::Capabilities => {
                // A capability answer is a fact about this worker binary, not
                // about a session or an execution lane, so it needs no session and
                // no durable evidence. Only v1 asks: v2 callers carry a writer
                // state the answer does not describe.
                ensure!(!control, "SQL authorization required");
                ensure!(
                    version == 1,
                    "readonly batch capabilities require protocol v1"
                );
                Ok(json!({"readonly_batch_protocol": 1}))
            }
            WorkerCommand::WriterProfileStatus => {
                ensure!(
                    !control && version == 2 && self.admission.enabled(),
                    "writer profile is disabled"
                );
                let sql_total = self.limits.max_inflight - self.limits.control_reserve;
                Ok(
                    json!({"protocol": 2, "writer_profile": true, "max_inflight": self.limits.max_inflight,
                    "control_reserve": self.limits.control_reserve, "sql_total": sql_total,
                    "general_sql": sql_total - 2, "owner_sql": 1, "cleanup_sql": 1}),
                )
            }
            WorkerCommand::GetMetrics => {
                ensure!(control, "control authorization required");
                let usage = self.admission.snapshot()?;
                let sql_inflight = usage.general_sql + usage.owner_sql + usage.cleanup_sql;
                // One sample per response: the ownership state includes per-database
                // resource probes, so taking it twice would report two instants of
                // the same worker. The memory ledger travels beside `resources`
                // because the controller decodes the worker report strictly.
                let (status, memory) = self.runtime.status_and_memory()?;
                let ownership_failures = status.failures;
                Ok(json!({
                    "metrics": Metrics::global().snapshot(),
                    "capacity_trace": self.runtime.capacity_trace_snapshot(),
                    "resources": status.resources,
                    "memory": memory,
                    "ownership_failures": ownership_failures,
                    "ownership_failures_count": ownership_failures.len(),
                    "lifecycle": self.sessions.lifecycle_metrics()?,
                    "lifecycle_protocol": 1,
                    "shutdown_protocol": 1,
                    "http_inflight": self.inflight.in_use(),
                    "sql_inflight": sql_inflight,
                    "rpc_lanes": usage,
                    "writer_profile": self.admission.enabled(),
                    "sessions": self.sessions.len()?,
                    "retirement": self.runtime.retirement_health(),
                }))
            }
            WorkerCommand::Ownership { action } => {
                ensure!(control, "control authorization required");
                let revoked = match &action {
                    OwnershipCommand::RevokeDatabase { database, .. } => Some(database.clone()),
                    _ => None,
                };
                let result = self.runtime.command(action);
                let cleanup = revoked
                    .map(|identifier| self.sessions.clear_database(&identifier))
                    .transpose();
                let result = result?;
                cleanup?;
                Ok(result)
            }
            WorkerCommand::ShutdownWithin { remaining_ms } => {
                ensure!(control, "control authorization required");
                ensure!(
                    (1..=20_000).contains(&remaining_ms),
                    "invalid shutdown remaining budget"
                );
                self.shutdown_budget
                    .start_with(std::time::Duration::from_millis(remaining_ms));
                self.stop();
                Ok(json!({"stopping": true, "clean": false}))
            }
            WorkerCommand::Shutdown => {
                ensure!(control, "control authorization required");
                self.stop();
                Ok(json!({"stopping": true}))
            }
            WorkerCommand::CloseOwnedSessions {
                binding,
                owner_incarnation,
                open_request_id,
            } => {
                ensure!(control, "control authorization required");
                self.sessions
                    .close_owned(&binding, &owner_incarnation, &open_request_id, deadline)
            }
            _ if control => bail!("SQL authorization required"),
            WorkerCommand::OpenOwnedSession {
                database,
                owner_incarnation,
            } => {
                crate::ownership::types::validate_label(&owner_incarnation)?;
                let (mut result, snapshot) = self.sessions.open(
                    &database,
                    version,
                    deadline,
                    Some(owner_incarnation),
                    request_id,
                )?;
                result["binding"] = serde_json::to_value(
                    self.sessions.binding(
                        result["session"]
                            .as_str()
                            .context("missing session identity")?,
                    )?,
                )?;
                *terminal = snapshot;
                Ok(result)
            }
            WorkerCommand::OpenSession { database } => {
                let (result, snapshot) = self
                    .sessions
                    .open(&database, version, deadline, None, request_id)?;
                *terminal = snapshot;
                Ok(result)
            }
            WorkerCommand::CloseSession { session } => {
                *terminal = self.sessions.close(&session, version, deadline)?;
                Ok(json!({"closed": true}))
            }
            WorkerCommand::Execute { sql, .. } => {
                ensure!(
                    sql.len() <= Budget::trusted_default().max_sql_bytes,
                    "SQL byte budget exceeded"
                );
                let call = call.context("worker session RPC guard absent")?;
                let result =
                    crate::slow_diagnostics::native("execute", || call.handle().execute(&sql));
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Execute, request_id, error);
                }
                result.map(|_| json!({"executed": true}))
            }
            WorkerCommand::Query { sql, .. } => {
                ensure!(
                    sql.len() <= Budget::trusted_default().max_sql_bytes,
                    "SQL byte budget exceeded"
                );
                let call = call.context("worker session RPC guard absent")?;
                let result = crate::slow_diagnostics::native("query", || call.handle().query(&sql))
                    .map(rows);
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Query, request_id, error);
                }
                result
            }
            WorkerCommand::Describe { sql, .. } => {
                ensure!(
                    sql.len() <= Budget::trusted_default().max_sql_bytes,
                    "SQL byte budget exceeded"
                );
                let call = call.context("worker session RPC guard absent")?;
                let result = call.handle().describe(&sql).map(|result| json!({
                    "params": result.params.into_iter().map(|name| json!({"name": name})).collect::<Vec<_>>(),
                    "cols": result.cols.into_iter().map(|column| json!({"name": column.name, "decltype": column.decltype})).collect::<Vec<_>>(),
                    "is_explain": result.is_explain,
                    "is_readonly": result.is_readonly,
                }));
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Query, request_id, error);
                }
                result
            }
            WorkerCommand::SessionAutocommit { .. } => {
                let call = call.context("worker session RPC guard absent")?;
                let result = call
                    .handle()
                    .autocommit()
                    .map(|autocommit| json!({"autocommit": autocommit}));
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Query, request_id, error);
                }
                result
            }
            WorkerCommand::Statement {
                sql,
                args,
                named_args,
                want_rows,
                ..
            } => {
                ensure!(
                    sql.len() <= Budget::trusted_default().max_sql_bytes,
                    "SQL byte budget exceeded"
                );
                let args = args
                    .into_iter()
                    .map(native_arg)
                    .collect::<Result<Vec<_>>>()?;
                let named_args = named_args
                    .into_iter()
                    .map(|arg| Ok((arg.name, native_arg(arg.value)?)))
                    .collect::<Result<Vec<_>>>()?;
                let call = call.context("worker session RPC guard absent")?;
                let result = crate::slow_diagnostics::native("statement", || {
                    call.handle()
                        .statement_with_named_args(&sql, &args, &named_args, want_rows)
                })
                .map(encoded_statement_result);
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Query, request_id, error);
                }
                result
            }
            WorkerCommand::GuardedReadonlyStatement {
                sql,
                args,
                named_args,
                want_rows,
                ..
            } => {
                ensure!(
                    sql.len() <= Budget::trusted_default().max_sql_bytes,
                    "SQL byte budget exceeded"
                );
                let args = args
                    .into_iter()
                    .map(native_arg)
                    .collect::<Result<Vec<_>>>()?;
                let named_args = named_args
                    .into_iter()
                    .map(|arg| Ok((arg.name, native_arg(arg.value)?)))
                    .collect::<Result<Vec<_>>>()?;
                let call = call.context("worker session RPC guard absent")?;
                let result = crate::slow_diagnostics::native("guarded_readonly_statement", || {
                    call.handle()
                        .guarded_readonly_statement(&sql, &args, &named_args, want_rows)
                })
                .map(|outcome| match outcome {
                    crate::session::GuardedStatementOutcome::Executed(result) => {
                        encoded_statement_result(result)
                    }
                    crate::session::GuardedStatementOutcome::RequiresIntent(reason) => {
                        json!({"requires_intent": true, "reason": guarded_intent_reason(reason)})
                    }
                });
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Query, request_id, error);
                }
                result
            }
            WorkerCommand::GuardedReadonlyBatch {
                statements,
                max_result_bytes,
                ..
            } => {
                // A block the caller cannot describe is refused before any
                // statement runs, so an invalid block is never partially executed.
                let prepared = prepare_guarded_batch(statements)?;
                if Instant::now() >= deadline {
                    return Err(anyhow::Error::new(AdmissionError::Deadline)
                        .context(SqlEvidence::NotExecuted));
                }
                let call = call.context("worker session RPC guard absent")?;
                let items = crate::slow_diagnostics::native("guarded_readonly_batch", || {
                    run_guarded_batch(&prepared, max_result_bytes, deadline, |statement| {
                        call.handle().guarded_readonly_statement(
                            &statement.sql,
                            &statement.args,
                            &statement.named_args,
                            statement.want_rows,
                        )
                    })
                })?;
                Ok(json!({"items": items.items, "next": items.next}))
            }
            WorkerCommand::OpenReadCursor {
                sql,
                args,
                named_args,
                ..
            } => {
                let args = args
                    .into_iter()
                    .map(native_arg)
                    .collect::<Result<Vec<_>>>()?;
                let named_args = named_args
                    .into_iter()
                    .map(|arg| Ok((arg.name, native_arg(arg.value)?)))
                    .collect::<Result<Vec<_>>>()?;
                let call = call.context("worker session RPC guard absent")?;
                let result = call.handle().open_read_cursor(&sql, &args, &named_args).map(|cols| {
                    json!({"cols": cols.into_iter().map(|column| json!({"name": column.name, "decltype": column.decltype})).collect::<Vec<_>>()})
                });
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                result
            }
            WorkerCommand::OpenReadCursorFirst {
                sql,
                args,
                named_args,
                max_rows,
                ..
            } => dispatch_cursor_first(
                call, sql, args, named_args, max_rows, false, version, terminal,
            ),
            WorkerCommand::OpenWriteCursorFirst {
                sql,
                args,
                named_args,
                max_rows,
                ..
            } => dispatch_cursor_first(
                call, sql, args, named_args, max_rows, true, version, terminal,
            ),
            WorkerCommand::FetchReadCursor { max_rows, .. } => {
                let call = call.context("worker session RPC guard absent")?;
                let result = crate::slow_diagnostics::native("cursor_fetch_read", || call.handle().fetch_read_cursor(max_rows)).map(|(rows, done)| {
                    json!({"rows": rows.iter().map(|row| row.iter().map(value).collect::<Vec<_>>()).collect::<Vec<_>>(), "done": done})
                });
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                result
            }
            WorkerCommand::CloseReadCursor { .. } => {
                let call = call.context("worker session RPC guard absent")?;
                let result = call
                    .handle()
                    .close_read_cursor()
                    .map(|_| json!({"closed": true}));
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                result
            }
            WorkerCommand::OpenWriteCursor {
                sql,
                args,
                named_args,
                ..
            } => {
                let args = args
                    .into_iter()
                    .map(native_arg)
                    .collect::<Result<Vec<_>>>()?;
                let named_args = named_args
                    .into_iter()
                    .map(|arg| Ok((arg.name, native_arg(arg.value)?)))
                    .collect::<Result<Vec<_>>>()?;
                let call = call.context("worker session RPC guard absent")?;
                let result = call.handle().open_write_cursor(&sql, &args, &named_args).map(|cols| {
                    json!({"cols": cols.into_iter().map(|column| json!({"name": column.name, "decltype": column.decltype})).collect::<Vec<_>>()})
                });
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                result
            }
            WorkerCommand::FetchWriteCursor { max_rows, .. } => {
                let call = call.context("worker session RPC guard absent")?;
                let result = crate::slow_diagnostics::native("cursor_fetch_write", || {
                    call.handle().fetch_write_cursor(max_rows)
                })
                .map(|page| {
                    let rows = page
                        .rows
                        .iter()
                        .map(|row| row.iter().map(value).collect::<Vec<_>>())
                        .collect::<Vec<_>>();
                    if page.done {
                        json!({"rows": rows, "done": true,
                            "affected_row_count": page.affected_row_count,
                            "last_insert_rowid": page.last_insert_rowid})
                    } else {
                        json!({"rows": rows, "done": false,
                            "affected_row_count": null,
                            "last_insert_rowid": null})
                    }
                });
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                result
            }
            WorkerCommand::CloseWriteCursor { .. } => {
                let call = call.context("worker session RPC guard absent")?;
                let result = call
                    .handle()
                    .close_write_cursor()
                    .map(|abort_proven| json!({"closed": true, "abort_proven": abort_proven}));
                if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
                    *terminal = call.terminal_state().unwrap_or(None);
                }
                result
            }
            WorkerCommand::Checkpoint { .. } => {
                let result = Metrics::global().observe(
                    Operation::Checkpoint,
                    || {
                        call.context("worker session RPC guard absent")?
                            .handle()
                            .checkpoint()
                    },
                    |_| 0,
                );
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Checkpoint, request_id, error);
                }
                result.map(|_| json!({"checkpointed": true}))
            }
            WorkerCommand::Compact { .. } => {
                let result = call
                    .context("worker session RPC guard absent")
                    .and_then(|call| call.handle().compact_wal());
                if let Err(error) = &result {
                    self.diagnostics.emit(Action::Compact, request_id, error);
                }
                result.map(|_| json!({"compacted": true}))
            }
            WorkerCommand::Diagnostics { .. } => call
                .context("worker session RPC guard absent")?
                .handle()
                .diagnostics(),
        }
    }
}

/// One wire shape for statement results, shared by the plain and the guarded
/// path so the two can never drift when a field is added.
fn encoded_statement_result(result: StatementResult) -> Value {
    json!({
        "cols": result.cols.into_iter().map(|column| json!({"name": column.name, "decltype": column.decltype})).collect::<Vec<_>>(),
        "rows": result.rows.into_iter().map(|row| row.iter().map(value).collect::<Vec<_>>()).collect::<Vec<_>>(),
        "affected_row_count": result.affected_row_count,
        "last_insert_rowid": result.last_insert_rowid,
        "autocommit": result.autocommit,
        "rows_read": result.rows_read,
        "rows_written": result.rows_written,
        "query_duration_ms": result.query_duration_ms,
    })
}

/// The largest block one batch command carries. It matches the platform's
/// pipeline audit batch, so a block never spans more statements than one durable
/// batch and a single call cannot hide an unbounded amount of work.
const MAX_GUARDED_BATCH_STATEMENTS: usize = 8;

#[derive(Debug)]
struct PreparedBatchStatement {
    sql: String,
    args: Vec<SqlValue>,
    named_args: Vec<(String, SqlValue)>,
    want_rows: bool,
}

struct GuardedBatchItems {
    items: Vec<Value>,
    next: usize,
}

/// Validates the whole block before any statement runs: an invalid argument or an
/// oversized statement is `NotExecuted` for the entire command, so a malformed
/// block can never be half executed.
fn prepare_guarded_batch(statements: Vec<BatchStatement>) -> Result<Vec<PreparedBatchStatement>> {
    let prepared = (|| -> Result<Vec<PreparedBatchStatement>> {
        ensure!(!statements.is_empty(), "empty guarded readonly batch");
        ensure!(
            statements.len() <= MAX_GUARDED_BATCH_STATEMENTS,
            "guarded readonly batch limit exceeded"
        );
        let budget = Budget::trusted_default();
        let mut prepared = Vec::with_capacity(statements.len());
        for statement in statements {
            ensure!(
                statement.sql.len() <= budget.max_sql_bytes,
                "SQL byte budget exceeded"
            );
            let args = statement
                .args
                .into_iter()
                .map(native_arg)
                .collect::<Result<Vec<_>>>()?;
            let named_args = statement
                .named_args
                .into_iter()
                .map(|arg| Ok((arg.name, native_arg(arg.value)?)))
                .collect::<Result<Vec<_>>>()?;
            prepared.push(PreparedBatchStatement {
                sql: statement.sql,
                args,
                named_args,
                want_rows: statement.want_rows,
            });
        }
        Ok(prepared)
    })();
    prepared.map_err(|error| error.context(SqlEvidence::NotExecuted))
}

/// Runs one guarded statement per entry and stops at the first entry that is not
/// a settled read. Each statement keeps its own budget, evidence and verdict; the
/// block only removes round trips. `next` counts the entries already decided, so
/// a caller resumes at `next` and never replays an entry the worker ran.
fn run_guarded_batch<F>(
    statements: &[PreparedBatchStatement],
    max_result_bytes: usize,
    deadline: Instant,
    mut execute: F,
) -> Result<GuardedBatchItems>
where
    F: FnMut(&PreparedBatchStatement) -> Result<GuardedStatementOutcome>,
{
    let mut items = Vec::with_capacity(statements.len());
    let mut bytes = 0u64;
    let mut next = 0usize;
    for (index, statement) in statements.iter().enumerate() {
        // Checked at entry boundaries: a deadline that expires mid-statement is
        // answered by that statement's own budget and evidence, never by a
        // half-decided entry.
        if Instant::now() >= deadline {
            break;
        }
        next = index + 1;
        match execute(statement) {
            Ok(GuardedStatementOutcome::Executed(result)) => {
                let item = json!({"index": index, "status": "ok", "result": encoded_statement_result(result)});
                bytes = bytes.saturating_add(go_html_escaped_json_len(&item));
                items.push(item);
                // The entry that crosses the budget is still returned: it ran and
                // settled, so hiding it would leave the caller without its result.
                if bytes > max_result_bytes as u64 {
                    break;
                }
            }
            Ok(GuardedStatementOutcome::RequiresIntent(reason)) => {
                items.push(json!({"index": index, "status": "requires_intent", "reason": guarded_intent_reason(reason)}));
                break;
            }
            Err(error) => {
                let mut item =
                    json!({"index": index, "status": "error", "error": format!("{error:#}")});
                if execution_completed(&error) {
                    item["execution_completed"] = json!(true);
                } else if execution_rejected(&error) {
                    item["execution_completed"] = json!(false);
                }
                items.push(item);
                break;
            }
        }
    }
    Ok(GuardedBatchItems { items, next })
}

/// The JSON byte count the platform's `encoding/json` would produce for this
/// item. HTML escaping turns `<`, `>` and `&` into six bytes and U+2028/U+2029
/// into three more than their UTF-8 form; serde emits all of them raw, so the
/// scan adds the difference. Counting an upper bound keeps a block under the
/// caller's pipeline budget without encoding the response twice.
fn go_html_escaped_json_len(item: &Value) -> u64 {
    let encoded = serde_json::to_vec(item).expect("guarded batch item serializes");
    let mut total = 0u64;
    let mut index = 0usize;
    while index < encoded.len() {
        let byte = encoded[index];
        if byte == b'<' || byte == b'>' || byte == b'&' {
            total = total.saturating_add(6);
            index += 1;
        } else if byte == 0xE2
            && index + 2 < encoded.len()
            && encoded[index + 1] == 0x80
            && matches!(encoded[index + 2], 0xA8 | 0xA9)
        {
            total = total.saturating_add(6);
            index += 3;
        } else {
            total = total.saturating_add(1);
            index += 1;
        }
    }
    total
}

fn guarded_intent_reason(reason: GuardedIntentReason) -> &'static str {
    match reason {
        GuardedIntentReason::EngineWritable => "engine_writable",
        GuardedIntentReason::TransactionContext => "transaction_context",
        GuardedIntentReason::ConnectionControl => "connection_control",
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_cursor_first(
    call: Option<&SessionCall>,
    sql: String,
    args: Vec<StatementArg>,
    named_args: Vec<NamedStatementArg>,
    max_rows: usize,
    writing: bool,
    version: u32,
    terminal: &mut Option<SessionState>,
) -> Result<Value> {
    if !(1..=256).contains(&max_rows) {
        return Err(anyhow::anyhow!("cursor page size exceeds limit")
            .context(crate::session::SqlEvidence::NotExecuted));
    }
    let args = args
        .into_iter()
        .map(native_arg)
        .collect::<Result<Vec<_>>>()?;
    let named_args = named_args
        .into_iter()
        .map(|arg| Ok((arg.name, native_arg(arg.value)?)))
        .collect::<Result<Vec<_>>>()?;
    let call = call.context("worker session RPC guard absent")?;
    let result = call.handle().open_cursor_first(&sql, &args, &named_args, max_rows, writing)
        .map(|first| {
            let mut result = json!({
                "cols": first.cols.into_iter().map(|column| json!({"name": column.name, "decltype": column.decltype})).collect::<Vec<_>>(),
                "rows": first.page.rows.iter().map(|row| row.iter().map(value).collect::<Vec<_>>()).collect::<Vec<_>>(),
                "done": first.page.done,
                "autocommit": first.autocommit,
            });
            if writing {
                result["affected_row_count"] = json!(first.page.affected_row_count);
                result["last_insert_rowid"] = json!(first.page.last_insert_rowid);
            }
            result
        });
    if version == 2 && result.as_ref().err().is_none_or(execution_completed) {
        *terminal = call.terminal_state().unwrap_or(None);
    }
    result
}

fn native_arg(arg: StatementArg) -> Result<SqlValue> {
    match arg {
        StatementArg::Null => Ok(SqlValue::Null),
        StatementArg::Integer { value } => Ok(SqlValue::from_i64(value.parse()?)),
        StatementArg::Float { value } => {
            ensure!(value.is_finite(), "non-finite SQL float");
            Ok(SqlValue::from_f64(value))
        }
        StatementArg::Text { value } => Ok(SqlValue::build_text(value)),
        StatementArg::Blob { base64 } => {
            let bytes = base64::engine::general_purpose::STANDARD.decode(base64)?;
            Ok(SqlValue::from_slice(&bytes)?)
        }
    }
}

#[cfg(test)]
mod memory_tests {
    use super::{BudgetLane, RequestMemory};
    use crate::session::Budget;
    use crate::worker_rpc::protocol::WorkerCommand;

    fn session() -> String {
        "a".repeat(64)
    }

    #[test]
    fn request_memory_classifies_the_lanes_and_reserves_results() {
        let metrics = RequestMemory::for_command(&WorkerCommand::GetMetrics, true);
        assert_eq!(metrics.lane, BudgetLane::Control);
        assert!(metrics.bytes >= Budget::trusted_default().max_result_bytes as u64);

        let close =
            RequestMemory::for_command(&WorkerCommand::CloseSession { session: session() }, false);
        assert_eq!(close.lane, BudgetLane::Cleanup);
        assert_eq!(close.bytes, 0);

        let execute = RequestMemory::for_command(
            &WorkerCommand::Execute {
                session: session(),
                sql: "SELECT 1".into(),
            },
            false,
        );
        assert_eq!(execute.lane, BudgetLane::Sql);
        assert_eq!(execute.bytes, "SELECT 1".len() as u64);

        let query = RequestMemory::for_command(
            &WorkerCommand::Query {
                session: session(),
                sql: "SELECT 1".into(),
            },
            false,
        );
        assert_eq!(query.lane, BudgetLane::Sql);
        assert!(query.bytes > execute.bytes);

        let batch = RequestMemory::for_command(
            &WorkerCommand::GuardedReadonlyBatch {
                session: session(),
                statements: vec![
                    super::BatchStatement {
                        sql: "SELECT 1".into(),
                        args: vec![],
                        named_args: vec![],
                        want_rows: true,
                    },
                    super::BatchStatement {
                        sql: "SELECT 2".into(),
                        args: vec![],
                        named_args: vec![],
                        want_rows: true,
                    },
                ],
                max_result_bytes: 1024,
            },
            false,
        );
        assert_eq!(batch.lane, BudgetLane::Sql);
        // Input is the aggregate SQL text; output covers the block budget plus
        // one statement that crosses it.
        assert_eq!(
            batch.bytes,
            16 + crate::runtime::result_reservation_bytes(
                1024 + Budget::trusted_default().max_result_bytes
            )
        );
    }
}

#[cfg(test)]
mod batch_tests {
    use std::time::{Duration, Instant};

    use anyhow::{anyhow, Result};
    use serde_json::json;
    use turso_core::Value;

    use super::{
        go_html_escaped_json_len, prepare_guarded_batch, run_guarded_batch, BatchStatement,
        GuardedIntentReason, GuardedStatementOutcome, StatementResult,
    };
    use crate::session::{SqlEvidence, UnsettledSqlOutcome};

    fn statement(sql: &str) -> BatchStatement {
        BatchStatement {
            sql: sql.into(),
            args: vec![],
            named_args: vec![],
            want_rows: true,
        }
    }

    fn result(value: i64) -> StatementResult {
        StatementResult {
            cols: vec![],
            rows: vec![vec![Value::from_i64(value)]],
            affected_row_count: 0,
            last_insert_rowid: None,
            autocommit: true,
            rows_read: 1,
            rows_written: 0,
            query_duration_ms: 0.5,
        }
    }

    fn prepared(count: usize) -> Vec<super::PreparedBatchStatement> {
        prepare_guarded_batch(
            (0..count)
                .map(|index| statement(&format!("SELECT {index}")))
                .collect(),
        )
        .expect("valid batch")
    }

    #[test]
    fn an_invalid_block_is_rejected_before_any_statement_runs() -> Result<()> {
        for block in [
            vec![],
            (0..9)
                .map(|index| statement(&format!("SELECT {index}")))
                .collect(),
            vec![BatchStatement {
                sql: "SELECT ?".into(),
                args: vec![super::StatementArg::Blob {
                    base64: "not base64".into(),
                }],
                named_args: vec![],
                want_rows: true,
            }],
        ] {
            let error = prepare_guarded_batch(block).expect_err("invalid block admitted");
            assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_none());
            assert_eq!(
                error.downcast_ref::<SqlEvidence>(),
                Some(&SqlEvidence::NotExecuted)
            );
        }
        Ok(())
    }

    #[test]
    fn every_settled_statement_is_reported_in_order() -> Result<()> {
        let prepared = prepared(3);
        let items = run_guarded_batch(
            &prepared,
            usize::MAX,
            Instant::now() + Duration::from_secs(5),
            |statement| {
                let value: i64 = statement.sql.trim_start_matches("SELECT ").parse()?;
                Ok(GuardedStatementOutcome::Executed(result(value)))
            },
        )?;
        assert_eq!(items.next, 3);
        assert_eq!(items.items.len(), 3);
        for (index, item) in items.items.iter().enumerate() {
            assert_eq!(item["index"], index);
            assert_eq!(item["status"], "ok");
            assert_eq!(
                item["result"]["rows"],
                json!([[index as i64]]),
                "item {index}"
            );
            for field in [
                "cols",
                "rows",
                "affected_row_count",
                "last_insert_rowid",
                "autocommit",
                "rows_read",
                "rows_written",
                "query_duration_ms",
            ] {
                assert!(item["result"].get(field).is_some(), "{field} missing");
            }
        }
        Ok(())
    }

    #[test]
    fn a_refused_write_stops_the_block_after_the_reads() -> Result<()> {
        let prepared = prepared(3);
        let items = run_guarded_batch(
            &prepared,
            usize::MAX,
            Instant::now() + Duration::from_secs(5),
            |statement| {
                if statement.sql == "SELECT 1" {
                    return Ok(GuardedStatementOutcome::RequiresIntent(
                        GuardedIntentReason::EngineWritable,
                    ));
                }
                Ok(GuardedStatementOutcome::Executed(result(0)))
            },
        )?;
        assert_eq!(items.next, 2);
        assert_eq!(items.items[0]["status"], "ok");
        assert_eq!(
            items.items[1],
            json!({"index": 1, "status": "requires_intent", "reason": "engine_writable"})
        );
        assert_eq!(items.items.len(), 2);
        Ok(())
    }

    #[test]
    fn a_settled_failure_stops_and_keeps_the_completion_proof() -> Result<()> {
        let prepared = prepared(3);
        let items = run_guarded_batch(
            &prepared,
            usize::MAX,
            Instant::now() + Duration::from_secs(5),
            |statement| {
                if statement.sql == "SELECT 1" {
                    return Err(anyhow!("private syntax failure").context(SqlEvidence::Settled));
                }
                Ok(GuardedStatementOutcome::Executed(result(0)))
            },
        )?;
        assert_eq!(items.next, 2);
        assert_eq!(items.items[0]["status"], "ok");
        assert_eq!(items.items[1]["index"], 1);
        assert_eq!(items.items[1]["status"], "error");
        assert_eq!(items.items[1]["execution_completed"], true);
        assert!(
            items.items[1]["error"]
                .as_str()
                .is_some_and(|text| text.ends_with("private syntax failure")),
            "{}",
            items.items[1]
        );
        assert_eq!(items.items.len(), 2);
        Ok(())
    }

    #[test]
    fn an_unsettled_failure_carries_no_completion_proof() -> Result<()> {
        let prepared = prepared(1);
        let items = run_guarded_batch(
            &prepared,
            usize::MAX,
            Instant::now() + Duration::from_secs(5),
            |_| Err(anyhow!("lost reply").context(UnsettledSqlOutcome)),
        )?;
        assert_eq!(items.next, 1);
        assert_eq!(items.items[0]["status"], "error");
        assert!(items.items[0].get("execution_completed").is_none());
        Ok(())
    }

    #[test]
    fn the_statement_that_crosses_the_result_budget_is_returned_and_stops_the_block() -> Result<()>
    {
        let prepared = prepared(4);
        let single = json!({"index": 0, "status": "ok", "result": super::encoded_statement_result(result(0))});
        let budget = go_html_escaped_json_len(&single) as usize;
        let items = run_guarded_batch(
            &prepared,
            budget,
            Instant::now() + Duration::from_secs(5),
            |_| Ok(GuardedStatementOutcome::Executed(result(0))),
        )?;
        // One entry fits exactly; the second crosses and is still reported.
        assert_eq!(items.next, 2);
        assert_eq!(items.items.len(), 2);
        Ok(())
    }

    #[test]
    fn the_deadline_is_checked_at_entry_boundaries() -> Result<()> {
        let prepared = prepared(4);
        let mut calls = 0;
        let items = run_guarded_batch(
            &prepared,
            usize::MAX,
            Instant::now() + Duration::from_millis(30),
            |_| {
                calls += 1;
                std::thread::sleep(Duration::from_millis(40));
                Ok(GuardedStatementOutcome::Executed(result(0)))
            },
        )?;
        assert_eq!(calls, 1);
        assert_eq!(items.next, 1);
        assert_eq!(items.items.len(), 1);
        assert_eq!(items.items[0]["status"], "ok");
        Ok(())
    }

    #[test]
    fn the_result_estimate_applies_the_platform_html_escapes() {
        let plain = json!({"text": "abc"});
        let escaped = json!({"text": "<>&"});
        let escaped_bytes = serde_json::to_vec(&escaped).expect("json").len() as u64;
        assert_eq!(
            go_html_escaped_json_len(&plain),
            plain.to_string().len() as u64
        );
        // Go's encoding/json turns each of the three ASCII characters into six
        // bytes, so the estimate is five more per character.
        assert_eq!(go_html_escaped_json_len(&escaped), escaped_bytes + 3 * 5);
        // serde emits U+2028/U+2029 raw; Go escapes each to six bytes, three
        // more than the three-byte UTF-8 form.
        let line = json!({"text": "\u{2028}\u{2029}"});
        let line_bytes = serde_json::to_vec(&line).expect("json").len() as u64;
        assert_eq!(go_html_escaped_json_len(&line), line_bytes + 2 * 3);
    }
}
