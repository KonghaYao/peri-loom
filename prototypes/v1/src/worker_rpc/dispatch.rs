use std::sync::atomic::Ordering;

use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};

use crate::metrics::{Metrics, Operation};
use crate::ownership::types::OwnershipCommand;
use crate::session::Budget;

use super::diagnostics::Action;
use super::outcome::execution_completed;
use super::protocol::{SessionState, WorkerCommand};
use super::server::State;
use super::session_entry::SessionCall;
use super::sessions::rows;

impl State {
    pub(super) fn dispatch(
        &self,
        command: WorkerCommand,
        control: bool,
        request_id: &str,
        version: u32,
        call: Option<&SessionCall>,
        terminal: &mut Option<SessionState>,
    ) -> Result<Value> {
        match command {
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
                Ok(json!({
                    "metrics": Metrics::global().snapshot(),
                    "resources": self.runtime.status()?.resources,
                    "http_inflight": self.inflight.load(Ordering::SeqCst),
                    "sql_inflight": sql_inflight,
                    "rpc_lanes": usage,
                    "writer_profile": self.admission.enabled(),
                    "sessions": self.sessions.len()?,
                }))
            }
            WorkerCommand::Ownership { action } => {
                ensure!(control, "control authorization required");
                let revoked = match &action {
                    OwnershipCommand::RevokeDatabase { database, .. } => Some(database.clone()),
                    _ => None,
                };
                let result = self.runtime.command(action)?;
                if let Some(identifier) = revoked {
                    self.sessions.clear_database(&identifier)?;
                }
                Ok(result)
            }
            WorkerCommand::Shutdown => {
                ensure!(control, "control authorization required");
                self.stopped.store(true, Ordering::SeqCst);
                Ok(json!({"stopping": true}))
            }
            _ if control => bail!("SQL authorization required"),
            WorkerCommand::OpenSession { database } => {
                let (result, snapshot) = self.sessions.open(&database, version)?;
                *terminal = snapshot;
                Ok(result)
            }
            WorkerCommand::CloseSession { session } => {
                *terminal = self.sessions.close(&session, version)?;
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
