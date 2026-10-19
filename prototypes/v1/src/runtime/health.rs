use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::{registry::Entry, WorkerRuntime};

/// How long a shutdown sample may wait for a native or control-plane lock to clear.
/// The caller supplies the absolute deadline so the wait stays inside the process-wide
/// shutdown budget; this is only the retry cadence.
const SAMPLE_RETRY_INTERVAL: Duration = Duration::from_millis(10);

impl WorkerRuntime {
    pub fn retirement_health(&self) -> Value {
        let started = Instant::now();
        let Ok(registry) = self.registry.try_lock() else {
            return unavailable("runtime.registry", "state_locked", started);
        };
        let retiring = registry
            .entries
            .values()
            .filter(|entry| matches!(entry, Entry::Retiring(_)))
            .count();
        let quarantine_tombstones = registry.quarantined.len();
        let mut quarantined = registry
            .quarantined
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        drop(registry);
        let Ok(observers) = self.native_instances.try_lock() else {
            return unavailable("runtime.native_instances", "state_locked", started);
        };
        let databases = observers
            .iter()
            .filter_map(|(identifier, database)| {
                database
                    .upgrade()
                    .map(|database| (identifier.clone(), database))
            })
            .collect::<Vec<_>>();
        drop(observers);
        let mut samples = Vec::new();
        let mut unavailable_databases = Vec::new();
        let mut quarantined_live = 0;
        let mut pending_statements = 0;
        let mut pending_io = 0;
        let mut retained_engines = 0;
        let mut retained_connections = 0;
        for (identifier, database) in databases {
            let sample = database.retirement_health();
            if sample["available"] != true {
                // Keep the database identity next to the lock name: an unavailable sample
                // without either cannot be attributed during shutdown.
                unavailable_databases.push(json!({
                    "database": identifier.namespace.prefix(),
                    "reason": sample["reason"],
                    "lock": sample["lock"],
                    "lock_held": sample["lock_held"],
                    "state": sample["state"],
                    "elapsed_us": sample["elapsed_us"],
                }));
                continue;
            }
            quarantined_live += usize::from(sample["quarantined"] == true);
            if sample["quarantined"] == true {
                quarantined.insert(identifier);
            }
            pending_statements += sample["pending_retained_statements"].as_u64().unwrap_or(0);
            pending_io += sample["io"]["pending_native_operations"]
                .as_u64()
                .unwrap_or(0);
            retained_engines += sample["retained_engines"].as_u64().unwrap_or(0);
            retained_connections += sample["retained_connections"].as_u64().unwrap_or(0);
            samples.push(sample);
        }
        if !unavailable_databases.is_empty() {
            return json!({
                "available": false,
                "reason": "native_sample_unavailable",
                "lock": "database",
                "lock_held": true,
                "unavailable": unavailable_databases,
                "elapsed_us": started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                "quarantined_databases": null,
                "retiring_databases": null,
                "pending_statements": null,
                "pending_io": null,
                "retained_engines": null,
                "retained_connections": null,
            });
        }
        json!({"available": true, "quarantined_databases": quarantined.len(), "retiring_databases": retiring, "pending_statements": pending_statements, "pending_io": pending_io, "retained_engines": retained_engines, "retained_connections": retained_connections, "quarantine_tombstones": quarantine_tombstones, "quarantined_live_databases": quarantined_live, "native_instances": samples.len(), "instances": samples, "sampling": "non_atomic_read_only; http_inflight_and_control_lane_include_this_metrics_rpc; pending_io_counts_native_dispatch_including_synchronous_callbacks_not_remote_network_requests; retained_counts_are_quarantined_native_refs; not_completion_proof", "sampled_after_us": started.elapsed().as_micros().min(u64::MAX as u128) as u64})
    }

    /// Samples retirement until it is clean or `deadline` passes. A non-blocking sample can
    /// lose a transient race with a native lock; a shutdown sample must not turn that race
    /// into an unclean verdict before the holder had its bounded chance to finish.
    ///
    /// The last sample is returned either way. An unavailable sample, a non-zero counter or
    /// an expired deadline all remain unproven; this method never manufactures clean proof.
    pub fn retirement_health_until(&self, deadline: Instant) -> Value {
        let started = Instant::now();
        loop {
            let mut sample = self.retirement_health();
            let clean = sample["available"] == true && retirement_counts_clear(&sample);
            let now = Instant::now();
            if clean || now >= deadline {
                if let Value::Object(fields) = &mut sample {
                    fields.insert(
                        "waited_ms".to_owned(),
                        json!(started.elapsed().as_millis().min(u64::MAX as u128) as u64),
                    );
                }
                return sample;
            }
            std::thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(SAMPLE_RETRY_INTERVAL),
            );
        }
    }
}

fn retirement_counts_clear(sample: &Value) -> bool {
    [
        "quarantined_databases",
        "retiring_databases",
        "pending_statements",
        "pending_io",
        "retained_engines",
        "retained_connections",
    ]
    .iter()
    .all(|key| sample[*key].as_u64() == Some(0))
}

fn unavailable(lock: &'static str, state: &'static str, started: Instant) -> Value {
    json!({"available": false, "reason": "runtime_lock_unavailable", "lock": lock, "lock_held": true, "state": state, "elapsed_us": started.elapsed().as_micros().min(u64::MAX as u128) as u64, "quarantined_databases": null, "retiring_databases": null, "pending_statements": null, "pending_io": null, "retained_engines": null, "retained_connections": null})
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::Result;

    use super::*;
    use crate::namespace::Namespace;
    use crate::object_store::{LocalObjectStore, ObjectStore};
    use crate::runtime::{DatabaseId, DatabaseProvider, WorkerLimits};

    struct LocalProvider(Arc<LocalObjectStore>);

    impl DatabaseProvider for LocalProvider {
        fn open_store(&self, _: &DatabaseId, _: u64) -> Result<Arc<dyn ObjectStore>> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn retirement_health_reports_null_not_zero_when_registry_is_unavailable() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let provider = Arc::new(LocalProvider(Arc::new(LocalObjectStore::open(
            directory.path().join("objects"),
        )?)));
        let runtime = WorkerRuntime::new(
            provider,
            directory.path().join("cache"),
            WorkerLimits::default(),
        )?;
        let sample = runtime.retirement_health();
        assert_eq!(sample["available"], true);
        for name in [
            "quarantined_databases",
            "retiring_databases",
            "pending_statements",
            "pending_io",
        ] {
            assert_eq!(sample[name], 0);
        }
        let registry = runtime.registry.lock().unwrap();
        let sample = runtime.retirement_health();
        assert_eq!(sample["available"], false);
        assert_eq!(sample["lock"], "runtime.registry");
        assert_eq!(sample["reason"], "runtime_lock_unavailable");
        for name in [
            "quarantined_databases",
            "retiring_databases",
            "pending_statements",
            "pending_io",
            "retained_engines",
            "retained_connections",
        ] {
            assert!(sample[name].is_null());
        }
        drop(registry);
        Ok(())
    }

    fn runtime_with_loaded_database() -> Result<(Arc<WorkerRuntime>, tempfile::TempDir)> {
        let directory = tempfile::tempdir()?;
        let provider = Arc::new(LocalProvider(Arc::new(LocalObjectStore::open(
            directory.path().join("objects"),
        )?)));
        let runtime = WorkerRuntime::new(
            provider,
            directory.path().join("cache"),
            WorkerLimits::default(),
        )?;
        let identifier = DatabaseId::new(Namespace::new("runtime", "health")?, "main")?;
        let handle = runtime.open(identifier, 1)?;
        drop(handle);
        Ok((runtime, directory))
    }

    #[test]
    fn bounded_sample_waits_for_a_transient_control_plane_lock() -> Result<()> {
        let (runtime, _directory) = runtime_with_loaded_database()?;
        assert_eq!(runtime.retirement_health()["available"], true);
        let registry = runtime.registry.lock().unwrap();
        assert_eq!(runtime.retirement_health()["lock"], "runtime.registry");
        let (started, sampler_started) = std::sync::mpsc::channel();
        let sampler = {
            let runtime = runtime.clone();
            std::thread::spawn(move || {
                started.send(()).unwrap();
                runtime.retirement_health_until(Instant::now() + Duration::from_secs(2))
            })
        };
        sampler_started.recv().unwrap();
        // Keep the lock held long enough for the sampler's first attempt to lose the
        // try_lock race; the retry loop is what has to recover from it.
        std::thread::sleep(Duration::from_millis(60));
        drop(registry);
        let sample = sampler.join().unwrap();
        assert_eq!(sample["available"], true);
        assert!(sample["waited_ms"].as_u64().unwrap() >= 40, "{sample}");
        Ok(())
    }

    #[test]
    fn bounded_sample_returns_the_held_lock_when_its_deadline_expires() -> Result<()> {
        let (runtime, _directory) = runtime_with_loaded_database()?;
        let registry = runtime.registry.lock().unwrap();
        let sample = runtime.retirement_health_until(Instant::now() + Duration::from_millis(50));
        assert_eq!(sample["available"], false);
        assert_eq!(sample["reason"], "runtime_lock_unavailable");
        assert_eq!(sample["lock"], "runtime.registry");
        assert_eq!(sample["lock_held"], true);
        assert!(sample["waited_ms"].as_u64().unwrap() >= 40, "{sample}");
        drop(registry);
        Ok(())
    }
}
