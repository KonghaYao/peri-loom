use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{ensure, Context, Result};
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

use crate::ownership::OwnershipRuntime;
use crate::runtime::DatabaseId;
use crate::session::Budget;

use super::protocol::SessionState;
use super::session_entry::{Entry, SessionCall};

#[derive(Default)]
struct Registry {
    sessions: BTreeMap<String, Arc<Entry>>,
    opening: usize,
}

pub(super) struct Sessions {
    runtime: Arc<OwnershipRuntime>,
    registry: Mutex<Registry>,
    image_refresh: Mutex<()>,
    max_sessions: usize,
}

struct Opening<'sessions>(&'sessions Sessions);

impl Drop for Opening<'_> {
    fn drop(&mut self) {
        let mut registry = self
            .0
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(registry.opening > 0, "worker opening accounting underflow");
        registry.opening -= 1;
    }
}

impl Sessions {
    pub fn new(runtime: Arc<OwnershipRuntime>, max_sessions: usize) -> Self {
        Self {
            runtime,
            registry: Mutex::new(Registry::default()),
            image_refresh: Mutex::new(()),
            max_sessions,
        }
    }

    pub fn open(
        &self,
        database: &DatabaseId,
        version: u32,
    ) -> Result<(JsonValue, Option<SessionState>)> {
        database.validate()?;
        {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
            ensure!(
                registry.sessions.len() + registry.opening < self.max_sessions,
                "worker session capacity exceeded"
            );
            registry.opening += 1;
        }
        let _opening = Opening(self);
        let _refresh = self
            .image_refresh
            .lock()
            .map_err(|_| anyhow::anyhow!("image refresh mutex poisoned"))?;
        self.refresh_initial_image(database)?;
        let handle = Arc::new(self.runtime.open_session(database)?);
        let readonly = handle.is_readonly()?;
        let mut random = [0u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let identifier = random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let entry = Entry::new(database.clone(), handle)?;
        let terminal = if version == 2 {
            Some(entry.opened_state(&identifier)?)
        } else {
            None
        };
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
        let std::collections::btree_map::Entry::Vacant(slot) =
            registry.sessions.entry(identifier.clone())
        else {
            anyhow::bail!("session identity collision");
        };
        slot.insert(entry);
        Ok((
            json!({"session": identifier, "readonly": readonly}),
            terminal,
        ))
    }

    fn entry(&self, identifier: &str) -> Result<Arc<Entry>> {
        ensure!(
            identifier.len() == 64
                && identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "invalid worker session identity"
        );
        self.registry
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
            .sessions
            .get(identifier)
            .cloned()
            .context("unknown worker session")
    }

    pub fn begin(&self, identifier: &str) -> Result<SessionCall> {
        let _refresh = self
            .image_refresh
            .lock()
            .map_err(|_| anyhow::anyhow!("image refresh mutex poisoned"))?;
        let entry = self.entry(identifier)?;
        self.refresh_initial_image(&entry.database)?;
        self.entry(identifier)?.begin(identifier)
    }

    fn refresh_initial_image(&self, database: &DatabaseId) -> Result<()> {
        if self.runtime.initial_image_replaced(database)? {
            let previous = {
                let registry = self
                    .registry
                    .lock()
                    .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
                registry
                    .sessions
                    .iter()
                    .filter(|(_, entry)| entry.database == *database)
                    .map(|(identifier, entry)| {
                        Ok((identifier.clone(), entry.rebindable_sequence()?))
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            self.clear_database(database)?;
            self.runtime.unload_replaced_image(database)?;
            for (identifier, sequence) in previous {
                let handle = Arc::new(self.runtime.open_session(database)?);
                let entry = Entry::new(database.clone(), handle)?;
                entry.preserve_sequence(sequence)?;
                self.registry
                    .lock()
                    .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
                    .sessions
                    .insert(identifier, entry);
            }
        }
        Ok(())
    }

    pub fn known(&self, identifier: &str) -> bool {
        self.entry(identifier)
            .and_then(|entry| entry.available())
            .unwrap_or(false)
    }

    pub fn close(&self, identifier: &str, version: u32) -> Result<Option<SessionState>> {
        let _refresh = self
            .image_refresh
            .lock()
            .map_err(|_| anyhow::anyhow!("image refresh mutex poisoned"))?;
        let deadline = Instant::now() + Budget::trusted_default().max_query_duration;
        let entry = self.entry(identifier)?;
        let terminal = entry.close(identifier, deadline, version)?;
        self.remove(identifier, &entry)?;
        Ok(terminal)
    }

    fn remove(&self, identifier: &str, entry: &Arc<Entry>) -> Result<()> {
        let removed = {
            let mut registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
            ensure!(
                registry
                    .sessions
                    .get(identifier)
                    .is_some_and(|current| Arc::ptr_eq(current, entry)),
                "worker session identity changed"
            );
            registry.sessions.remove(identifier)
        };
        drop(removed);
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        self.clear_matching(|_| true)
    }

    pub fn clear_database(&self, database: &DatabaseId) -> Result<()> {
        self.clear_matching(|entry| entry.database == *database)
    }

    fn clear_matching(&self, selected: impl Fn(&Entry) -> bool) -> Result<()> {
        let entries = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
            registry
                .sessions
                .iter()
                .filter(|(_, entry)| selected(entry))
                .map(|(identifier, entry)| (identifier.clone(), entry.clone()))
                .collect::<Vec<_>>()
        };
        for (identifier, entry) in entries {
            let deadline = Instant::now() + Budget::trusted_default().max_query_duration;
            entry.close(&identifier, deadline, 1)?;
            self.remove(&identifier, &entry)?;
        }
        Ok(())
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
            .sessions
            .len())
    }
}

pub(super) fn rows(rows: Vec<Vec<Value>>) -> JsonValue {
    JsonValue::Array(
        rows.into_iter()
            .map(|row| JsonValue::Array(row.iter().map(value).collect()))
            .collect(),
    )
}

pub(super) fn value(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Numeric(Numeric::Integer(integer)) => json!(integer),
        Value::Numeric(Numeric::Float(float)) => json!(f64::from(*float)),
        Value::Text(text) => json!(text.as_str()),
        Value::Blob(bytes) => json!(bytes.as_slice()),
    }
}
