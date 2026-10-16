use std::time::Instant;

use anyhow::{bail, ensure, Context, Result};
use base64::Engine;
use serde_json::{json, Value};
use turso_core::Value as SqlValue;

use crate::metrics::{Metrics, Operation};
use crate::ownership::types::OwnershipCommand;
use crate::session::Budget;

use super::diagnostics::Action;
use super::outcome::execution_completed;
use super::protocol::{SessionState, StatementArg, WorkerCommand};
use super::server::State;
use super::session_entry::SessionCall;
use super::sessions::{rows, value};

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
                // the same worker.
                let status = self.runtime.status()?;
                let ownership_failures = status.failures;
                Ok(json!({
                    "metrics": Metrics::global().snapshot(),
                    "capacity_trace": self.runtime.capacity_trace_snapshot(),
                    "resources": status.resources,
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
                let result = call.handle().execute(&sql);
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
                let result = call.handle().query(&sql).map(rows);
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
                let result = call.handle().statement_with_named_args(&sql, &args, &named_args, want_rows).map(|result| {
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
            WorkerCommand::FetchReadCursor { max_rows, .. } => {
                let call = call.context("worker session RPC guard absent")?;
                let result = call.handle().fetch_read_cursor(max_rows).map(|(rows, done)| {
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
                let result = call.handle().fetch_write_cursor(max_rows).map(|page| {
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
                    .map(|_| json!({"closed": true}));
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
