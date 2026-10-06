use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

use crate::ownership::{OwnedHandle, OwnershipRuntime};
use crate::runtime::DatabaseId;

pub(super) struct Sessions {
    runtime: Arc<OwnershipRuntime>,
    sessions: Mutex<BTreeMap<String, (DatabaseId, Arc<OwnedHandle>)>>,
}

impl Sessions {
    pub fn new(runtime: Arc<OwnershipRuntime>) -> Self {
        Self {
            runtime,
            sessions: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn open(&self, database: &DatabaseId) -> Result<JsonValue> {
        let handle = Arc::new(self.runtime.open_session(database)?);
        let mut random = [0u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let identifier = random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let readonly = handle.is_readonly()?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
        let std::collections::btree_map::Entry::Vacant(entry) = sessions.entry(identifier.clone())
        else {
            anyhow::bail!("session identity collision");
        };
        entry.insert((database.clone(), handle));
        Ok(json!({"session": identifier, "readonly": readonly}))
    }

    pub fn handle(&self, identifier: &str) -> Result<Arc<OwnedHandle>> {
        self.sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
            .get(identifier)
            .map(|(_, handle)| handle.clone())
            .context("unknown worker session")
    }

    pub fn close(&self, identifier: &str) -> Result<()> {
        let handle = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
            .remove(identifier)
            .context("unknown worker session")?;
        drop(handle);
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        let handles = std::mem::take(
            &mut *self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?,
        );
        drop(handles);
        Ok(())
    }

    pub fn clear_database(&self, database: &DatabaseId) -> Result<()> {
        let removed = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
            let identifiers = sessions
                .iter()
                .filter(|(_, (identifier, _))| identifier == database)
                .map(|(session, _)| session.clone())
                .collect::<Vec<_>>();
            identifiers
                .into_iter()
                .filter_map(|identifier| sessions.remove(&identifier))
                .collect::<Vec<_>>()
        };
        drop(removed);
        Ok(())
    }
}

pub(super) fn rows(rows: Vec<Vec<Value>>) -> JsonValue {
    JsonValue::Array(
        rows.into_iter()
            .map(|row| JsonValue::Array(row.iter().map(value).collect()))
            .collect(),
    )
}

fn value(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Numeric(Numeric::Integer(integer)) => json!(integer),
        Value::Numeric(Numeric::Float(float)) => json!(f64::from(*float)),
        Value::Text(text) => json!(text.as_str()),
        Value::Blob(bytes) => json!(bytes.as_slice()),
    }
}
