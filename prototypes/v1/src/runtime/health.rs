use serde_json::{json, Value};

use super::{registry::Entry, WorkerRuntime};

impl WorkerRuntime {
    pub fn retirement_health(&self) -> Value {
        let Ok(registry) = self.registry.try_lock() else {
            return unavailable("registry_unavailable");
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
            return unavailable("native_observers_unavailable");
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
        let mut quarantined_live = 0;
        let mut pending_statements = 0;
        let mut pending_io = 0;
        let mut retained_engines = 0;
        let mut retained_connections = 0;
        for (identifier, database) in databases {
            let sample = database.retirement_health();
            if sample["available"] != true {
                return unavailable("native_sample_unavailable");
            }
            quarantined_live += usize::from(sample["quarantined"] == true);
            if sample["quarantined"] == true {
                quarantined.insert(identifier);
            }
            pending_statements += sample["pending_retained_statements"].as_u64().unwrap();
            pending_io += sample["io"]["pending_native_operations"].as_u64().unwrap();
            retained_engines += sample["retained_engines"].as_u64().unwrap();
            retained_connections += sample["retained_connections"].as_u64().unwrap();
            samples.push(sample);
        }
        json!({"available": true, "quarantined_databases": quarantined.len(), "retiring_databases": retiring, "pending_statements": pending_statements, "pending_io": pending_io, "retained_engines": retained_engines, "retained_connections": retained_connections, "quarantine_tombstones": quarantine_tombstones, "quarantined_live_databases": quarantined_live, "native_instances": samples.len(), "instances": samples, "sampling": "non_atomic_read_only; http_inflight_and_control_lane_include_this_metrics_rpc; pending_io_counts_native_dispatch_including_synchronous_callbacks_not_remote_network_requests; retained_counts_are_quarantined_native_refs; not_completion_proof"})
    }
}

fn unavailable(reason: &str) -> Value {
    json!({"available": false, "reason": reason, "quarantined_databases": null, "retiring_databases": null, "pending_statements": null, "pending_io": null, "retained_engines": null, "retained_connections": null})
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;

    use super::*;
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
}
