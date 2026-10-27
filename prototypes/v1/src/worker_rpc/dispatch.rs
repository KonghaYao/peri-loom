use std::time::Instant;

use anyhow::{bail, ensure, Context, Result};
use base64::Engine;
use serde_json::{json, Value};
use turso_core::Value as SqlValue;

use crate::metrics::{Metrics, Operation};
use crate::ownership::types::OwnershipCommand;
use crate::runtime::{result_reservation_bytes, BudgetLane};
use crate::session::Budget;

use super::diagnostics::Action;
use super::outcome::execution_completed;
use super::protocol::{NamedStatementArg, SessionState, StatementArg, WorkerCommand};
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
                let result = crate::slow_diagnostics::native("statement", || call.handle().statement_with_named_args(&sql, &args, &named_args, want_rows)).map(|result| {
                    json!({
                        "cols": result.cols.into_iter().map(|column| json!({"name": column.name, "decltype": column.decltype})).collect::<Vec<_>>(),
                        "rows": result.rows.into_iter().map(|row| row.iter().map(value).collect::<Vec<_>>()).collect::<Vec<_>>(),
                        "affected_row_count": result.affected_row_count,
                        "last_insert_rowid": result.last_insert_rowid,
                        "autocommit": result.autocommit,
                    })
                });
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
                    crate::session::GuardedStatementOutcome::Executed(result) => json!({
                        "cols": result.cols.into_iter().map(|column| json!({"name": column.name, "decltype": column.decltype})).collect::<Vec<_>>(),
                        "rows": result.rows.into_iter().map(|row| row.iter().map(value).collect::<Vec<_>>()).collect::<Vec<_>>(),
                        "affected_row_count": result.affected_row_count,
                        "last_insert_rowid": result.last_insert_rowid,
                        "autocommit": result.autocommit,
                    }),
                    crate::session::GuardedStatementOutcome::RequiresIntent(reason) => {
                        let reason = match reason {
                            crate::session::GuardedIntentReason::EngineWritable => "engine_writable",
                            crate::session::GuardedIntentReason::TransactionContext => "transaction_context",
                            crate::session::GuardedIntentReason::ConnectionControl => "connection_control",
                        };
                        json!({"requires_intent": true, "reason": reason})
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
    }
}
