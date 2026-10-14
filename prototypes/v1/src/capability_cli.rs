use crate::authority::Authority;
use crate::capabilities::Capabilities;
use crate::database::RemoteDatabase;
use crate::gc::GarbageCollector;
use crate::namespace::Namespace;
use crate::object_store::ObjectStore;
use anyhow::{bail, ensure, Result};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use turso_core::{Numeric, Value as SqlValue};

pub fn handles(command: &str) -> bool {
    matches!(
        command,
        "refs"
            | "branch"
            | "snapshot"
            | "clone"
            | "restore"
            | "pitr"
            | "pitr-time"
            | "drop-branch"
            | "drop-snapshot"
            | "retain-after"
            | "gc-inspect"
            | "gc"
            | "branch-exec"
            | "branch-query"
            | "branch-checkpoint"
    )
}

pub fn run(
    store: Arc<dyn ObjectStore>,
    namespace: Namespace,
    cache: &Path,
    arguments: &[String],
) -> Result<Value> {
    ensure!(!arguments.is_empty(), "capability command required");
    let command = arguments[0].as_str();
    ensure!(handles(command), "unknown capability command");
    let count = match command {
        "refs" | "gc-inspect" => 1,
        "gc" | "drop-branch" | "drop-snapshot" => 2,
        "branch-checkpoint" => 2,
        _ => 3,
    };
    ensure!(
        arguments.len() == count,
        "wrong argument count for {command}"
    );
    let authority = Authority::open(store, namespace.clone())?;
    let capabilities = Capabilities::new(authority.clone());
    match command {
        "refs" => Ok(serde_json::to_value(capabilities.list()?)?),
        "branch" => {
            capabilities.branch(&arguments[1], &arguments[2])?;
            Ok(json!({"ok": true}))
        }
        "snapshot" => Ok(serde_json::to_value(
            capabilities.snapshot(&arguments[1], &arguments[2])?,
        )?),
        "clone" => {
            capabilities.clone(&arguments[1], &arguments[2])?;
            Ok(json!({"ok": true}))
        }
        "restore" => {
            capabilities.restore(&arguments[1], &arguments[2])?;
            Ok(json!({"ok": true, "reopen_required": true}))
        }
        "pitr" | "pitr-time" => {
            let selector = arguments[2].parse()?;
            let point = if command == "pitr" {
                capabilities.pitr(&arguments[1], selector)?
            } else {
                capabilities.resolve_time(&arguments[1], selector)?
            };
            capabilities.restore_point(&arguments[1], &point)?;
            Ok(json!({"ok": true, "point": point, "reopen_required": true}))
        }
        "drop-branch" => {
            capabilities.delete_branch(&arguments[1])?;
            Ok(json!({"ok": true}))
        }
        "drop-snapshot" => {
            capabilities.delete_snapshot(&arguments[1])?;
            Ok(json!({"ok": true}))
        }
        "retain-after" => {
            capabilities.retain_after(&arguments[1], arguments[2].parse()?)?;
            Ok(json!({"ok": true}))
        }
        "gc-inspect" => Ok(serde_json::to_value(
            GarbageCollector::new(authority, Duration::from_secs(60)).inspect()?,
        )?),
        "gc" => Ok(serde_json::to_value(
            GarbageCollector::new(authority, Duration::from_secs(arguments[1].parse()?))
                .collect()?,
        )?),
        "branch-exec" | "branch-query" | "branch-checkpoint" => {
            let database =
                RemoteDatabase::open(authority.branch_store(&arguments[1])?, namespace, cache, 0)?;
            match command {
                "branch-exec" => {
                    database.execute(&arguments[2])?;
                    Ok(json!({"ok": true}))
                }
                "branch-checkpoint" => {
                    database.checkpoint()?;
                    Ok(json!({"manifest": database.storage().manifest()}))
                }
                _ => Ok(Value::Array(
                    database
                        .query(&arguments[2])?
                        .iter()
                        .map(|row| Value::Array(row.iter().map(sql_value).collect()))
                        .collect(),
                )),
            }
        }
        _ => bail!("unknown capability command"),
    }
}

fn sql_value(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Numeric(Numeric::Integer(integer)) => json!(integer),
        SqlValue::Numeric(Numeric::Float(float)) => json!(f64::from(*float)),
        SqlValue::Text(text) => json!(text.as_str()),
        SqlValue::Blob(bytes) => json!(bytes.as_slice()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;

    #[test]
    fn invalid_arguments_do_not_initialize_storage() -> Result<()> {
        for arguments in [vec![], vec!["snapshot"], vec!["unknown", "main", "copy"]] {
            let store = Arc::new(MemoryObjectStore::default());
            let namespace = Namespace::new("tenant", "cli")?;
            let arguments = arguments
                .iter()
                .map(|argument| argument.to_string())
                .collect::<Vec<_>>();
            assert!(run(store.clone(), namespace, Path::new("unused"), &arguments).is_err());
            assert!(store.list("")?.is_empty());
        }
        Ok(())
    }
}
