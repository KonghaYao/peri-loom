use std::collections::BTreeMap;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

use crate::metrics::Metrics;
use crate::object_store::metrics::MetricsObjectStore;
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
    finish(run_protocol(store, cache))
}

fn run_protocol(store: Arc<dyn ObjectStore>, cache: impl AsRef<Path>) -> Result<()> {
    let runtime = WorkerRuntime::new(
        Arc::new(LocalAuthorityProvider::new(instrumented_store(store))),
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

/// Counters are recorded at work points that already accept the `PERI_WORK_METRICS` switch;
/// wrapping here only adds the object-store get/put accounting the exit snapshot exports. The
/// wrapper is installed only when both switches are on, so enabling the counters alone keeps
/// the store stack identical to before.
fn instrumented_store(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    if !crate::work_metrics::enabled() || !crate::work_metrics::exit_snapshot_configured() {
        return store;
    }
    Arc::new(MetricsObjectStore::new(store, Metrics::global()).with_backend("worker"))
}

/// Diagnostics never rewrite the close outcome: a snapshot failure is reported on stderr (the
/// process's existing error channel) and the protocol result is returned unchanged.
fn finish(result: Result<()>) -> Result<()> {
    if let Err(error) = crate::work_metrics::write_exit_snapshot_if_configured() {
        eprintln!("peri-loom-v1: metrics exit snapshot not written: {error:#}");
    }
    result
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

    #[test]
    fn a_worker_write_advances_the_counters_the_exit_snapshot_exports() -> Result<()> {
        crate::work_metrics::set_enabled(true);
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
        let opened = protocol.handle(request(
            json!({"id":"open","op":"open","database":identifier,"epoch":0}),
        ))?;
        let session = opened["session"].clone();
        protocol.handle(request(json!({"id":"create","op":"exec","session":session,"sql":"CREATE TABLE items(id INTEGER PRIMARY KEY)"})))?;
        let before = crate::work_metrics::snapshot().expect("enabled snapshot");
        protocol.handle(request(
            json!({"id":"write","op":"exec","session":session,"sql":"INSERT INTO items VALUES(1)"}),
        ))?;
        let after = crate::work_metrics::snapshot().expect("enabled snapshot");
        assert!(
            after.wal_workload.append_calls - before.wal_workload.append_calls >= 1,
            "the write path did not reach the WAL append counter"
        );
        assert!(
            after.metadata.root_publish_attempts - before.metadata.root_publish_attempts >= 1,
            "the write path did not reach the root CAS counter"
        );
        Ok(())
    }

    #[test]
    fn instrumented_store_counts_object_operations_while_diagnostics_are_on() -> Result<()> {
        crate::work_metrics::set_enabled(true);
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("metrics.json");
        let _env = SnapshotPathEnv::set(Some(&path));
        let store = instrumented_store(Arc::new(MemoryObjectStore::default()));
        let before = Metrics::global().snapshot().put.total;
        store.put(
            "metrics/probe",
            b"value",
            crate::object_store::PutCondition::Any,
        )?;
        let after = Metrics::global().snapshot().put.total;
        assert!(after > before, "the wrapper did not count the put");
        Ok(())
    }

    #[test]
    fn counters_without_an_exit_snapshot_keep_the_store_unwrapped() {
        crate::work_metrics::set_enabled(true);
        let _env = SnapshotPathEnv::set(None);
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let instrumented = instrumented_store(store.clone());
        assert!(
            Arc::ptr_eq(&store, &instrumented),
            "the counters alone must not add a store wrapper"
        );
    }

    struct SnapshotPathEnv {
        previous: Option<std::ffi::OsString>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl SnapshotPathEnv {
        fn set(path: Option<&Path>) -> Self {
            let lock = crate::work_metrics::SNAPSHOT_ENV_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let previous = std::env::var_os(crate::work_metrics::SNAPSHOT_PATH_ENV);
            match path {
                Some(path) => std::env::set_var(crate::work_metrics::SNAPSHOT_PATH_ENV, path),
                None => std::env::remove_var(crate::work_metrics::SNAPSHOT_PATH_ENV),
            }
            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for SnapshotPathEnv {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => {
                    std::env::set_var(crate::work_metrics::SNAPSHOT_PATH_ENV, value);
                }
                None => std::env::remove_var(crate::work_metrics::SNAPSHOT_PATH_ENV),
            }
        }
    }

    #[test]
    fn a_failed_exit_snapshot_does_not_rewrite_the_close_outcome() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("missing").join("metrics.json");
        let _env = SnapshotPathEnv::set(Some(&path));

        assert!(finish(Ok(())).is_ok(), "a diagnostic failure changed Ok");
        assert!(
            finish(Err(anyhow::anyhow!("close failed"))).is_err(),
            "a diagnostic failure hid the close error"
        );
        assert!(!path.exists());
    }
}
