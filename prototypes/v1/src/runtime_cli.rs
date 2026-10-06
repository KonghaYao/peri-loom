use std::collections::BTreeMap;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

use crate::object_store::ObjectStore;
use crate::provider::LocalAuthorityProvider;
use crate::runtime::{DatabaseHandle, DatabaseId, WorkerLimits, WorkerRuntime};

const MAX_REQUEST_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    id: JsonValue,
    op: String,
    database: Option<DatabaseId>,
    epoch: Option<u64>,
    session: Option<String>,
    sql: Option<String>,
    idle_ms: Option<u64>,
}

struct WorkerProtocol {
    runtime: Arc<WorkerRuntime>,
    sessions: BTreeMap<String, DatabaseHandle>,
    next_session: u64,
}

impl WorkerProtocol {
    fn handle(&mut self, request: Request) -> Result<JsonValue> {
        ensure!(
            request.id.is_string() || request.id.is_u64(),
            "id must be a string or unsigned integer"
        );
        let result = match request.op.as_str() {
            "open" => {
                ensure!(
                    request.session.is_none() && request.sql.is_none() && request.idle_ms.is_none(),
                    "unexpected open arguments"
                );
                let identifier = request.database.context("database is required")?;
                let epoch = request.epoch.context("epoch is required")?;
                let next = self
                    .next_session
                    .checked_add(1)
                    .context("session ID overflow")?;
                let handle = self.runtime.open(identifier, epoch)?;
                let session = format!("s{next}");
                self.next_session = next;
                self.sessions.insert(session.clone(), handle);
                return Ok(json!({"session": session}));
            }
            "exec" | "query" | "checkpoint" | "compact" => {
                ensure!(
                    request.database.is_none()
                        && request.epoch.is_none()
                        && request.idle_ms.is_none(),
                    "unexpected session arguments"
                );
                let session = request.session.context("session is required")?;
                let handle = self.sessions.get(&session).context("unknown session")?;
                match request.op.as_str() {
                    "exec" => {
                        handle.execute(request.sql.as_deref().context("sql is required")?)?;
                        json!({"ok": true})
                    }
                    "query" => {
                        let rows =
                            handle.query(request.sql.as_deref().context("sql is required")?)?;
                        json!(rows
                            .iter()
                            .map(|row| row.iter().map(sql_value).collect::<Vec<_>>())
                            .collect::<Vec<_>>())
                    }
                    "checkpoint" => {
                        ensure!(request.sql.is_none(), "checkpoint takes no sql");
                        handle.checkpoint()?;
                        json!({"ok": true})
                    }
                    _ => {
                        ensure!(request.sql.is_none(), "compact takes no sql");
                        handle.compact_wal()?;
                        json!({"ok": true})
                    }
                }
            }
            "close" => {
                ensure!(
                    request.database.is_none()
                        && request.epoch.is_none()
                        && request.sql.is_none()
                        && request.idle_ms.is_none(),
                    "unexpected close arguments"
                );
                let session = request.session.context("session is required")?;
                drop(self.sessions.remove(&session).context("unknown session")?);
                json!({"ok": true})
            }
            "unload" | "drain" => {
                ensure!(
                    request.session.is_none()
                        && request.epoch.is_none()
                        && request.sql.is_none()
                        && request.idle_ms.is_none(),
                    "unexpected lifecycle arguments"
                );
                let identifier = request.database.context("database is required")?;
                identifier.validate()?;
                let changed = if request.op == "unload" {
                    self.runtime.close(&identifier)?
                } else {
                    self.runtime.drain(&identifier)?
                };
                json!({"changed": changed})
            }
            "status" => {
                ensure!(
                    request.database.is_none()
                        && request.session.is_none()
                        && request.epoch.is_none()
                        && request.sql.is_none()
                        && request.idle_ms.is_none(),
                    "status takes no arguments"
                );
                serde_json::to_value(self.runtime.status()?)?
            }
            "evict" => {
                ensure!(
                    request.database.is_none()
                        && request.session.is_none()
                        && request.epoch.is_none()
                        && request.sql.is_none(),
                    "unexpected evict arguments"
                );
                let idle_ms = request.idle_ms.context("idle_ms is required")?;
                json!({"evicted": self.runtime.evict_idle(Duration::from_millis(idle_ms))?})
            }
            _ => bail!("unknown worker operation"),
        };
        Ok(json!({"result": result}))
    }
}

pub fn run(store: Arc<dyn ObjectStore>, cache: impl AsRef<Path>) -> Result<()> {
    let runtime = WorkerRuntime::new(
        Arc::new(LocalAuthorityProvider::new(store)),
        cache,
        WorkerLimits::default(),
    )?;
    let mut protocol = WorkerProtocol {
        runtime,
        sessions: BTreeMap::new(),
        next_session: 0,
    };
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    loop {
        let mut line = Vec::new();
        let count = input
            .by_ref()
            .take(MAX_REQUEST_BYTES + 1)
            .read_until(b'\n', &mut line)?;
        if count == 0 {
            break;
        }
        ensure!(
            line.len() as u64 <= MAX_REQUEST_BYTES,
            "worker request too large"
        );
        let response = match serde_json::from_slice::<JsonValue>(&line) {
            Ok(value) => {
                let identifier = value.get("id").cloned().unwrap_or(JsonValue::Null);
                let result = serde_json::from_value::<Request>(value)
                    .map_err(anyhow::Error::from)
                    .and_then(|request| protocol.handle(request));
                match result {
                    Ok(mut response) => {
                        response["id"] = identifier;
                        response["ok"] = json!(true);
                        response
                    }
                    Err(error) => {
                        json!({"id": identifier, "ok": false, "error": format!("{error:#}")})
                    }
                }
            }
            Err(error) => json!({"id": null, "ok": false, "error": error.to_string()}),
        };
        serde_json::to_writer(&mut output, &response)?;
        writeln!(output)?;
        output.flush()?;
    }
    drop(protocol.sessions);
    Ok(())
}

fn sql_value(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Numeric(Numeric::Integer(integer)) => json!(integer),
        Value::Numeric(Numeric::Float(float)) => json!(f64::from(*float)),
        Value::Text(text) => json!(text.as_str()),
        Value::Blob(bytes) => json!(bytes.as_slice()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;

    fn request(value: JsonValue) -> Request {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn invalid_arguments_do_not_open_database() -> Result<()> {
        let cache = tempfile::tempdir()?;
        let runtime = WorkerRuntime::new(
            Arc::new(LocalAuthorityProvider::new(Arc::new(
                MemoryObjectStore::default(),
            ))),
            cache.path(),
            WorkerLimits::default(),
        )?;
        let mut protocol = WorkerProtocol {
            runtime,
            sessions: BTreeMap::new(),
            next_session: 0,
        };
        let identifier = json!({"namespace":{"tenant":"tenant","database":"db"},"branch":"main"});
        for value in [
            json!({"id":"bad","op":"open","database":identifier,"epoch":0,"sql":"SELECT 1"}),
            json!({"id":"bad","op":"open","database":identifier}),
            json!({"id":null,"op":"open","database":identifier,"epoch":0}),
            json!({"id":"bad","op":"unknown","database":identifier}),
        ] {
            assert!(protocol.handle(request(value)).is_err());
        }
        assert_eq!(protocol.runtime.status()?.loaded_databases, 0);
        assert_eq!(protocol.next_session, 0);
        assert!(protocol.sessions.is_empty());
        Ok(())
    }

    #[test]
    fn closing_a_protocol_session_aborts_its_transaction() -> Result<()> {
        let cache = tempfile::tempdir()?;
        let runtime = WorkerRuntime::new(
            Arc::new(LocalAuthorityProvider::new(Arc::new(
                MemoryObjectStore::default(),
            ))),
            cache.path(),
            WorkerLimits::default(),
        )?;
        let mut protocol = WorkerProtocol {
            runtime,
            sessions: BTreeMap::new(),
            next_session: 0,
        };
        let identifier = json!({"namespace":{"tenant":"tenant","database":"db"},"branch":"main"});
        let first = protocol.handle(request(
            json!({"id":"one","op":"open","database":identifier,"epoch":0}),
        ))?;
        let session = first["session"].clone();
        protocol.handle(request(json!({"id":"create","op":"exec","session":session,"sql":"CREATE TABLE items(id INTEGER PRIMARY KEY)"})))?;
        protocol.handle(request(json!({"id":"write","op":"exec","session":session,"sql":"BEGIN; INSERT INTO items VALUES(1)"})))?;
        protocol.handle(request(
            json!({"id":"close","op":"close","session":session}),
        ))?;
        let reopened = protocol.handle(request(
            json!({"id":"two","op":"open","database":identifier,"epoch":0}),
        ))?;
        let result = protocol.handle(request(json!({"id":"query","op":"query","session":reopened["session"],"sql":"SELECT count(*) FROM items"})))?;
        assert_eq!(result["result"], json!([[0]]));
        Ok(())
    }
}
