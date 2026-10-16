use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

use crate::ownership::OwnershipRuntime;
use crate::runtime::DatabaseId;
use crate::session::{AdmissionError, Budget};

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
    image_refresh: RefreshLocks,
    max_sessions: usize,
}

#[derive(Default)]
struct RefreshLocks(Mutex<BTreeMap<DatabaseId, Weak<Mutex<()>>>>);

impl RefreshLocks {
    fn for_database(&self, database: &DatabaseId) -> Result<Arc<Mutex<()>>> {
        let mut locks = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("image refresh registry poisoned"))?;
        if let Some(lock) = locks.get(database).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        // An active caller owns the Arc, so a concurrent caller cannot get a new lock.
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(Mutex::new(()));
        locks.insert(database.clone(), Arc::downgrade(&lock));
        Ok(lock)
    }

    fn lock_until(lock: &Mutex<()>, deadline: Instant) -> Result<MutexGuard<'_, ()>> {
        loop {
            ensure!(Instant::now() < deadline, AdmissionError::Deadline);
            match lock.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(TryLockError::Poisoned(_)) => {
                    anyhow::bail!("image refresh mutex poisoned")
                }
                Err(TryLockError::WouldBlock) => {
                    std::thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(2)),
                    );
                }
            }
        }
    }
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
            image_refresh: RefreshLocks::default(),
            max_sessions,
        }
    }

    pub fn open(
        &self,
        database: &DatabaseId,
        version: u32,
        deadline: Instant,
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
        let refresh = self.image_refresh.for_database(database)?;
        let _refresh = RefreshLocks::lock_until(&refresh, deadline)?;
        self.refresh_initial_image(database)?;
        ensure!(Instant::now() < deadline, AdmissionError::Deadline);
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

    pub fn begin(&self, identifier: &str, deadline: Instant) -> Result<SessionCall> {
        let database = self.entry(identifier)?.database.clone();
        let refresh = self.image_refresh.for_database(&database)?;
        let _refresh = RefreshLocks::lock_until(&refresh, deadline)?;
        self.refresh_initial_image(&database)?;
        ensure!(Instant::now() < deadline, AdmissionError::Deadline);
        self.entry(identifier)?.begin(identifier)
    }

    fn refresh_initial_image(&self, database: &DatabaseId) -> Result<()> {
        if self.runtime.initial_image_replaced(database)? {
            let entries = {
                let registry = self
                    .registry
                    .lock()
                    .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
                registry
                    .sessions
                    .iter()
                    .filter(|(_, entry)| entry.database == *database)
                    .map(|(identifier, entry)| (identifier.clone(), entry.clone()))
                    .collect::<Vec<_>>()
            };
            let previous = entries
                .into_iter()
                .map(|(identifier, entry)| Ok((identifier, entry.rebindable_sequence()?)))
                .collect::<Result<Vec<_>>>()?;
            self.clear_matching(|entry| entry.database == *database)?;
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

    pub fn close(
        &self,
        identifier: &str,
        version: u32,
        deadline: Instant,
    ) -> Result<Option<SessionState>> {
        let database = self.entry(identifier)?.database.clone();
        let refresh = self.image_refresh.for_database(&database)?;
        let _refresh = RefreshLocks::lock_until(&refresh, deadline)?;
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
        let refresh = self.image_refresh.for_database(database)?;
        let _refresh = refresh
            .lock()
            .map_err(|_| anyhow::anyhow!("image refresh mutex poisoned"))?;
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

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::RefreshLocks;
    use crate::namespace::Namespace;
    use crate::runtime::DatabaseId;
    use crate::session::AdmissionError;
    use std::sync::{mpsc, Arc};
    use std::time::{Duration, Instant};

    #[test]
    fn image_refresh_blocks_only_the_same_database_and_reclaims_idle_locks() -> anyhow::Result<()> {
        let locks = RefreshLocks::default();
        let a = DatabaseId::new(Namespace::new("tenant", "a")?, "main")?;
        let b = DatabaseId::new(Namespace::new("tenant", "b")?, "main")?;
        let a_lock = locks.for_database(&a)?;
        let a_second = locks.for_database(&a)?;
        assert!(Arc::ptr_eq(&a_lock, &a_second));
        let held = a_lock.lock().unwrap();
        assert!(a_second.try_lock().is_err());
        let b_lock = locks.for_database(&b)?;
        let _b_held = b_lock.try_lock().expect("unrelated database must proceed");
        drop(held);
        drop(a_second);
        drop(a_lock);
        let replacement = locks.for_database(&a)?;
        assert_eq!(locks.0.lock().unwrap().len(), 2);
        assert!(replacement.try_lock().is_ok());
        Ok(())
    }

    #[test]
    fn slow_database_refresh_does_not_block_another_database_or_wait_past_deadline(
    ) -> anyhow::Result<()> {
        let locks = RefreshLocks::default();
        let a = DatabaseId::new(Namespace::new("tenant", "slow")?, "main")?;
        let b = DatabaseId::new(Namespace::new("tenant", "healthy")?, "main")?;
        let a_lock = locks.for_database(&a)?;
        let b_lock = locks.for_database(&b)?;
        let (ready, started) = mpsc::channel();
        let slow = std::thread::spawn(move || {
            let _guard = a_lock.lock().unwrap();
            ready.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });
        started.recv()?;
        let first = Instant::now();
        let _healthy = RefreshLocks::lock_until(&b_lock, first + Duration::from_millis(250))?;
        let waiting_a = locks.for_database(&a)?;
        let failure =
            RefreshLocks::lock_until(&waiting_a, Instant::now() + Duration::from_millis(30))
                .unwrap_err();
        assert!(failure.downcast_ref::<AdmissionError>() == Some(&AdmissionError::Deadline));
        assert!(first.elapsed() < Duration::from_millis(250));
        slow.join().expect("slow refresh thread panicked");
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

pub(super) fn value(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Numeric(Numeric::Integer(integer)) => json!(integer),
        Value::Numeric(Numeric::Float(float)) => json!(f64::from(*float)),
        Value::Text(text) => json!(text.as_str()),
        Value::Blob(bytes) => json!(bytes.as_slice()),
    }
}
