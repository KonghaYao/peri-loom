use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use serde_json::{json, Value as JsonValue};
use turso_core::{Numeric, Value};

use crate::ownership::OwnershipRuntime;
use crate::runtime::DatabaseId;
use crate::session::{AdmissionError, Budget, RequestDeadline};

use super::protocol::SessionState;
use super::session_entry::{Entry, SessionCall};

fn duration_env(name: &str, default: u64) -> Result<Duration> {
    let value = match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("invalid {name}"))?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    ensure!(
        value > 0 && value <= 86_400_000,
        "invalid lifecycle duration: {name}"
    );
    Ok(Duration::from_millis(value))
}

#[derive(Default)]
struct Registry {
    sessions: BTreeMap<String, Arc<Entry>>,
    closed: BTreeMap<String, (Instant, Arc<Entry>)>,
    opening: usize,
}

pub(super) struct Sessions {
    runtime: Arc<OwnershipRuntime>,
    registry: Mutex<Registry>,
    image_refresh: RefreshLocks,
    max_sessions: usize,
    idle: Duration,
    max_age: Duration,
    database_idle: Duration,
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
    pub fn new(runtime: Arc<OwnershipRuntime>, max_sessions: usize) -> Result<Self> {
        Ok(Self {
            runtime,
            registry: Mutex::new(Registry::default()),
            image_refresh: RefreshLocks::default(),
            max_sessions,
            idle: duration_env("PERI_WORKER_SESSION_IDLE_MS", 900_000)?,
            max_age: duration_env("PERI_WORKER_SESSION_MAX_AGE_MS", 3_600_000)?,
            // A failed open keeps its instance resident (`WorkerRuntime::release` only frees
            // the session slot of a non-draining entry); what drops the instance is the sweep
            // that reads this window, and that sweep is what made a cold database unopenable
            // when it ran a second after the last session closed. The window therefore covers
            // the *floor* of the load budget (`PERI_WORKER_LOAD_MS`, never below 60 s), so a
            // database that just loaded is still resident for the retry that follows it. That
            // is a floor, not a relation between the two settings: an operator who raises the
            // budget past this window has to raise this window too, or the retry pays a second
            // cold load.
            database_idle: duration_env("PERI_WORKER_DB_IDLE_UNLOAD_MS", 60_000)?,
        })
    }

    pub fn open(
        &self,
        database: &DatabaseId,
        version: u32,
        deadline: Instant,
        owner_incarnation: Option<String>,
        request_id: &str,
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
        // The cold load is allowed to outrun the RPC deadline: `WorkerRuntime::open`
        // anchors the session budget after the load returns. The probes below are part
        // of the same request, so without a new anchor they inherit the RPC deadline the
        // load has already spent, and a load that succeeded would still fail the open on
        // a deadline the design explicitly let it exceed.
        let _session_phase =
            RequestDeadline::scope(Instant::now() + Budget::trusted_default().wall_duration());
        let readonly = handle.is_readonly()?;
        let mut random = [0u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let identifier = random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let entry = Entry::new(
            database.clone(),
            handle,
            owner_incarnation,
            request_id.to_owned(),
        )?;
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
                .map(|(identifier, entry)| {
                    Ok((
                        identifier,
                        entry.rebindable_sequence()?,
                        entry.owner_incarnation.clone(),
                        entry.open_request_id.clone(),
                        entry,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            self.clear_matching(|entry| entry.database == *database)?;
            self.runtime.unload_replaced_image(database)?;
            for (identifier, sequence, owner, request_id, previous) in previous {
                let handle = Arc::new(self.runtime.open_session(database)?);
                let entry = Entry::new(database.clone(), handle, owner, request_id)?;
                entry.preserve_sequence(sequence)?;
                entry.preserve_lifetime(&previous)?;
                let mut registry = self
                    .registry
                    .lock()
                    .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
                registry.closed.remove(&identifier);
                registry.sessions.insert(identifier, entry);
            }
        }
        Ok(())
    }

    pub fn binding(&self, identifier: &str) -> Result<crate::ownership::types::LeaseBinding> {
        Ok(self.entry(identifier)?.binding.clone())
    }

    pub fn known(&self, identifier: &str) -> bool {
        self.registry
            .lock()
            .map(|registry| {
                registry.sessions.contains_key(identifier)
                    || registry.closed.contains_key(identifier)
            })
            .unwrap_or(false)
    }

    pub fn close(
        &self,
        identifier: &str,
        version: u32,
        deadline: Instant,
    ) -> Result<Option<SessionState>> {
        if let Some(entry) = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
            .closed
            .get(identifier)
            .map(|(_, entry)| entry.clone())
        {
            return entry.close(identifier, deadline, version);
        }
        let entry = self.entry(identifier)?;
        let refresh = self.image_refresh.for_database(&entry.database)?;
        let _refresh = RefreshLocks::lock_until(&refresh, deadline)?;
        let entry = self.entry(identifier).or_else(|_| {
            self.registry
                .lock()
                .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
                .closed
                .get(identifier)
                .map(|(_, entry)| entry.clone())
                .context("unknown worker session")
        })?;
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
            if registry
                .closed
                .get(identifier)
                .is_some_and(|(_, old)| Arc::ptr_eq(old, entry))
            {
                return Ok(());
            }
            ensure!(
                registry
                    .sessions
                    .get(identifier)
                    .is_some_and(|current| Arc::ptr_eq(current, entry)),
                "worker session identity changed"
            );
            registry
                .closed
                .retain(|_, (at, _)| at.elapsed() < Duration::from_secs(300));
            while registry.closed.len() >= self.max_sessions {
                let oldest = registry
                    .closed
                    .iter()
                    .min_by_key(|(_, (at, _))| *at)
                    .map(|(id, _)| id.clone())
                    .unwrap();
                registry.closed.remove(&oldest);
            }
            registry
                .closed
                .insert(identifier.to_owned(), (Instant::now(), entry.clone()));
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
        let mut failure = None;
        let mut failed = 0;
        // Start every cleanup before waiting, so one blocked native lane cannot
        // consume the deadlines of otherwise independent databases.
        for (identifier, entry) in &entries {
            if let Err(error) = entry.start_close(identifier) {
                failed += 1;
                failure.get_or_insert(error);
            }
        }
        let deadline = Instant::now() + Budget::trusted_default().wall_duration();
        for (identifier, entry) in entries {
            let error = entry
                .close(&identifier, deadline, 1)
                .err()
                .or_else(|| self.remove(&identifier, &entry).err());
            // A failure is recorded but never stops the sweep: the remaining
            // databases still need their own verdict.
            if let Some(error) = error {
                failed += 1;
                failure.get_or_insert(error);
            }
        }
        match failure {
            // The count is part of the verdict, so an operator sees how many
            // cleanups are unproven rather than only the first error.
            Some(error) => Err(error.context(format!("{failed} session cleanups failed"))),
            None => Ok(()),
        }
    }

    pub fn sweep(&self, revoked: &[DatabaseId]) -> Result<()> {
        let entries: Vec<_> = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?
            .sessions
            .iter()
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect();
        for (id, entry) in entries {
            let refresh = self.image_refresh.for_database(&entry.database)?;
            let _refresh = match refresh.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::WouldBlock) => continue,
                Err(TryLockError::Poisoned(_)) => anyhow::bail!("image refresh mutex poisoned"),
            };
            let (expired, closing, closed, _, _, _) = entry.lifecycle(self.idle, self.max_age);
            if closed {
                self.remove(&id, &entry)?;
            } else if !closing && revoked.contains(&entry.database) {
                entry.start_close(&id)?;
            } else if !closing && expired {
                entry.expire(&id, self.idle, self.max_age)?;
            }
        }
        self.runtime.evict_idle(self.database_idle)?;
        Ok(())
    }

    pub fn close_owned(
        &self,
        binding: &crate::ownership::types::LeaseBinding,
        owner: &str,
        request: &str,
        deadline: Instant,
    ) -> Result<JsonValue> {
        crate::ownership::types::validate_label(owner)?;
        crate::ownership::types::validate_label(request)?;
        let refresh = self.image_refresh.for_database(&binding.database)?;
        let _refresh = RefreshLocks::lock_until(&refresh, deadline)?;
        let registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
        let entries: Vec<_> = registry
            .sessions
            .iter()
            .chain(registry.closed.iter().map(|(id, (_, entry))| (id, entry)))
            .filter(|(_, entry)| {
                entry.owner_incarnation.as_deref() == Some(owner)
                    && entry.open_request_id == request
                    && entry.binding == *binding
            })
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect();
        drop(registry);
        let mut pending = 0;
        let mut quarantined = 0;
        for (id, entry) in &entries {
            entry.start_close(id)?;
            let (_, _, closed, failed, _, _) = entry.lifecycle(self.idle, self.max_age);
            if failed {
                quarantined += 1;
            } else if !closed {
                pending += 1;
            }
        }
        // Absence is not proof of retirement: the open may still be in flight.
        Ok(
            json!({"matched": entries.len(), "pending": pending, "quarantined": quarantined,
            "completed": !entries.is_empty() && pending == 0 && quarantined == 0}),
        )
    }

    pub fn lifecycle_metrics(&self) -> Result<JsonValue> {
        let registry = self
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("worker session registry poisoned"))?;
        let (mut pending, mut orphan, mut quarantined) = (0, 0, 0);
        let (mut pending_age, mut orphan_age, mut quarantine_age) = (0, 0, 0);
        for entry in registry.sessions.values() {
            let (expired, closing, closed, failed, age, close_age) =
                entry.lifecycle(self.idle, self.max_age);
            if closing && !closed && !failed {
                pending += 1;
                pending_age = pending_age.max(close_age);
            }
            if expired && !closed {
                orphan += 1;
                orphan_age = orphan_age.max(age);
            }
            if failed {
                quarantined += 1;
                quarantine_age = quarantine_age.max(close_age);
            }
        }
        Ok(
            json!({"pending_close": {"count": pending, "oldest_age_ms": pending_age},
            "orphan": {"count": orphan, "oldest_age_ms": orphan_age},
            "quarantined": {"count": quarantined, "oldest_age_ms": quarantine_age},
            "session_idle_ms": self.idle.as_millis(), "session_max_age_ms": self.max_age.as_millis(),
            "database_idle_unload_ms": self.database_idle.as_millis()}),
        )
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
    use crate::worker_rpc::session_entry::StubPhase;
    use crate::worker_rpc::sessions::{Entry, Sessions};
    use std::sync::{mpsc, Arc};
    use std::time::{Duration, Instant};

    #[test]
    fn one_failed_cleanup_still_clears_healthy_sessions_and_reports_the_ledger(
    ) -> anyhow::Result<()> {
        let healthy = "a".repeat(64);
        let failing = "b".repeat(64);
        let (sessions, _cache) = sessions_with(vec![
            (
                healthy.clone(),
                Entry::stub(
                    database("healthy")?,
                    None,
                    "open-healthy",
                    StubPhase::Closed,
                ),
            ),
            (
                failing.clone(),
                Entry::stub(
                    database("failing")?,
                    None,
                    "open-failing",
                    StubPhase::Failed("unsettled outcome".into()),
                ),
            ),
        ])?;
        let error = sessions.clear().unwrap_err();
        assert!(
            format!("{error:#}").contains("1 session cleanups failed"),
            "{error:#}"
        );
        let registry = sessions.registry.lock().unwrap();
        // The healthy database is released and the unproven one keeps its slot, so
        // capacity is never reclaimed on an unproven close.
        assert!(!registry.sessions.contains_key(&healthy));
        assert!(registry.sessions.contains_key(&failing));
        assert!(!registry.closed.contains_key(&failing));
        Ok(())
    }

    #[test]
    fn owner_scoped_close_refuses_to_claim_completion_without_a_proven_match() -> anyhow::Result<()>
    {
        let identifier = "c".repeat(64);
        let entry = Entry::stub(
            database("owned")?,
            Some("gateway-1"),
            "open-1",
            StubPhase::Closed,
        );
        let binding = entry.binding.clone();
        let (sessions, _cache) = sessions_with(vec![(identifier, entry)])?;
        let deadline = Instant::now() + Duration::from_secs(5);
        for (label, candidate, owner, request) in [
            (
                "unknown open identity",
                binding.clone(),
                "gateway-1",
                "open-other",
            ),
            (
                "wrong owner incarnation",
                binding.clone(),
                "gateway-2",
                "open-1",
            ),
            (
                "stale binding",
                stale_binding(&binding)?,
                "gateway-1",
                "open-1",
            ),
        ] {
            let reply = sessions.close_owned(&candidate, owner, request, deadline)?;
            assert_eq!(reply["matched"], 0, "{label}");
            assert_eq!(reply["completed"], false, "{label}");
        }
        let reply = sessions.close_owned(&binding, "gateway-1", "open-1", deadline)?;
        assert_eq!(reply["matched"], 1);
        assert_eq!(reply["quarantined"], 0);
        assert_eq!(reply["completed"], true);
        Ok(())
    }

    #[test]
    fn owner_scoped_close_never_completes_on_an_unproven_session() -> anyhow::Result<()> {
        let entry = Entry::stub(
            database("quarantined")?,
            Some("gateway-1"),
            "open-2",
            StubPhase::Failed("unsettled outcome".into()),
        );
        let binding = entry.binding.clone();
        let (sessions, _cache) = sessions_with(vec![("d".repeat(64), entry)])?;
        let reply = sessions.close_owned(
            &binding,
            "gateway-1",
            "open-2",
            Instant::now() + Duration::from_secs(5),
        )?;
        assert_eq!(reply["matched"], 1);
        assert_eq!(reply["quarantined"], 1);
        assert_eq!(reply["completed"], false);
        Ok(())
    }

    fn database(name: &str) -> anyhow::Result<DatabaseId> {
        DatabaseId::new(Namespace::new("tenant", name)?, "main")
    }

    fn stale_binding(
        binding: &crate::ownership::types::LeaseBinding,
    ) -> anyhow::Result<crate::ownership::types::LeaseBinding> {
        let mut stale = binding.clone();
        stale.renewal_sequence += 1;
        Ok(stale)
    }

    fn sessions_with(
        entries: Vec<(String, Arc<Entry>)>,
    ) -> anyhow::Result<(Sessions, tempfile::TempDir)> {
        use crate::authority_rpc::{AuthorityClient, RemoteAuthorityProvider};
        use crate::ownership::types::LeaseClock;
        use crate::ownership::OwnershipRuntime;
        use crate::runtime::WorkerLimits;
        struct Clock;
        impl LeaseClock for Clock {
            fn now_ms(&self) -> anyhow::Result<u64> {
                Ok(0)
            }
        }
        let cache = tempfile::tempdir()?;
        let provider = RemoteAuthorityProvider::new(
            AuthorityClient::new("http://127.0.0.1:9/", "0".repeat(64))?,
            "worker".into(),
        )?;
        let runtime = OwnershipRuntime::new(
            "worker".into(),
            provider,
            cache.path(),
            WorkerLimits::default(),
            Arc::new(Clock),
        )?;
        let sessions = Sessions::new(runtime, 8)?;
        for (identifier, entry) in entries {
            sessions
                .registry
                .lock()
                .unwrap()
                .sessions
                .insert(identifier, entry);
        }
        Ok((sessions, cache))
    }

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
    fn an_idle_database_is_kept_longer_than_a_cold_load_may_take() -> anyhow::Result<()> {
        let (sessions, _cache) = sessions_with(Vec::new())?;
        // Unloading on a shorter timer makes the next request pay a cold load, and the load
        // budget (`PERI_WORKER_LOAD_MS`, never below 60 s) is the shortest that load can be.
        // This pins the floor the default window covers, not a relation between the settings:
        // the budget can be raised past this window by configuration.
        assert!(
            sessions.database_idle >= Duration::from_secs(60),
            "{}",
            sessions.database_idle.as_secs()
        );
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
