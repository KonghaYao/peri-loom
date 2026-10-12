use super::{registry::Entry, WorkerRuntime};
use anyhow::{ensure, Result};
use serde_json::{json, Value};

impl WorkerRuntime {
    pub fn capacity_trace_snapshot(&self) -> Value {
        if !crate::capacity_trace::enabled() {
            return crate::capacity_trace::snapshot();
        }
        let mappings = match self.capacity_runtime_mappings() {
            Ok(mappings) => mappings,
            Err(_) => {
                return json!({"enabled": true, "status": "mapping_lock_unavailable", "gap": true})
            }
        };
        let mut snapshot = crate::capacity_trace::snapshot();
        if let Some(databases) = snapshot["databases"].as_array_mut() {
            for database in databases {
                if mappings.iter().any(|mapping| {
                    mapping["queueInstance"].is_string()
                        && mapping["queueInstance"] == database["queueInstance"]
                }) && !bind_runtime_mapping(database, &mappings)
                    && matches!(
                        database["status"].as_str(),
                        Some("available" | "unmapped" | "gap")
                    )
                {
                    database["status"] = json!("mapping_unavailable");
                    database["gap"] = json!(true);
                }
            }
        }
        let incomplete = snapshot["exportTruncated"] == true
            || snapshot["evictedSources"].as_u64().unwrap_or(0) != 0
            || snapshot["registrationLost"].as_u64().unwrap_or(0) != 0
            || mappings
                .iter()
                .any(|mapping| mapping["status"] != "available")
            || snapshot["databases"].as_array().is_none_or(|databases| {
                databases
                    .iter()
                    .any(|database| database["status"] != "available")
            })
            || snapshot["namespaces"].as_array().is_none_or(|namespaces| {
                namespaces
                    .iter()
                    .any(|namespace| namespace["status"] != "available")
            });
        snapshot["gap"] = json!(incomplete);
        if snapshot["exportTruncated"] != true {
            snapshot["status"] = json!(if incomplete {
                "incomplete"
            } else if instances_empty(&mappings, &snapshot) {
                "missing"
            } else {
                "available"
            });
        }
        snapshot["runtimeMappings"] = json!(mappings);
        crate::capacity_trace::bound_export(snapshot)
    }

    pub fn capacity_trace_page(
        &self,
        stream_id: &str,
        after_sequence: u64,
        max_events: usize,
    ) -> Result<Value> {
        let mut page = crate::capacity_trace::page(stream_id, after_sequence, max_events)?;
        if page["enabled"] == true
            && page["stream"]["scope"] == "actualWriterAdmission"
            && matches!(page["status"].as_str(), Some("available" | "unmapped"))
        {
            let mappings = self.capacity_runtime_mappings()?;
            ensure!(
                bind_runtime_mapping(&mut page["stream"], &mappings),
                "capacity trace live mapping unavailable"
            );
            if page["status"] == "unmapped" && page["stream"]["status"] == "available" {
                page["status"] = json!("available");
            }
        }
        ensure!(
            serde_json::to_vec(&page)?.len() <= 64 * 1024,
            "capacity trace page budget exceeded"
        );
        Ok(page)
    }

    fn capacity_runtime_mappings(&self) -> Result<Vec<Value>> {
        let instances = {
            let registry = self
                .registry
                .try_lock()
                .map_err(|_| anyhow::anyhow!("capacity trace mapping lock unavailable"))?;
            registry
                .entries
                .iter()
                .filter_map(|(identifier, entry)| {
                    if let Entry::Loaded(loaded) = entry {
                        if !loaded.draining {
                            return Some((
                                identifier.clone(),
                                loaded.epoch,
                                loaded.database.clone(),
                            ));
                        }
                    }
                    None
                })
                .collect::<Vec<_>>()
        };
        let mappings = instances.iter().map(|(identifier, epoch, instance)| {
            match instance.database() {
                Ok(database) => json!({"databaseId": identifier, "epoch": epoch,
                    "queueInstance": database.admission().trace_identity(), "status": "available"}),
                Err(_) => json!({"databaseId": identifier, "epoch": epoch, "status": "mapping_unavailable"}),
            }
        }).collect::<Vec<_>>();
        let registry = self
            .registry
            .try_lock()
            .map_err(|_| anyhow::anyhow!("capacity trace mapping lock unavailable"))?;
        ensure!(instances.iter().all(|(identifier, epoch, instance)| {
            matches!(registry.entries.get(identifier), Some(Entry::Loaded(loaded))
                if !loaded.draining && loaded.epoch == *epoch && std::sync::Arc::ptr_eq(&loaded.database, instance))
        }), "capacity trace live mapping changed");
        Ok(mappings)
    }
}

fn bind_runtime_mapping(stream: &mut Value, mappings: &[Value]) -> bool {
    let mut matches = mappings.iter().filter(|mapping| {
        mapping["queueInstance"].is_string() && mapping["queueInstance"] == stream["queueInstance"]
    });
    let Some(mapping) = matches.next() else {
        return false;
    };
    if matches.next().is_some()
        || mapping["status"] != "available"
        || !mapping["databaseId"].is_object()
        || mapping["epoch"].as_u64().is_none()
    {
        return false;
    }
    if stream["scope"] != "actualWriterAdmission"
        || (stream["status"] == "available"
            && (stream["databaseId"] != mapping["databaseId"]
                || stream["epoch"] != mapping["epoch"]))
        || (!stream["databaseId"].is_null() && stream["databaseId"] != mapping["databaseId"])
        || (!stream["epoch"].is_null() && stream["epoch"] != mapping["epoch"])
    {
        return false;
    }
    if stream["status"] == "unmapped" {
        if stream["lost"] != 0
            || stream["dropped"] != 0
            || stream["gap"] != false
            || stream["prefixGap"] != false
        {
            return false;
        }
        stream["status"] = json!("available");
    }
    stream["databaseId"] = mapping["databaseId"].clone();
    stream["epoch"] = mapping["epoch"].clone();
    true
}

fn instances_empty(mappings: &[Value], snapshot: &Value) -> bool {
    mappings.is_empty()
        && snapshot["namespaces"].as_array().is_none_or(Vec::is_empty)
        && snapshot["databases"].as_array().is_none_or(Vec::is_empty)
}

#[cfg(test)]
mod tests {
    use super::bind_runtime_mapping;
    use serde_json::json;

    #[test]
    fn live_mapping_requires_exact_queue_database_epoch_and_clean_unmapped_state() {
        let mapping = json!({"status": "available", "queueInstance": "live-queue", "databaseId": {"database": "owned"}, "epoch": 2});
        let stream = json!({"status": "unmapped", "queueInstance": "live-queue", "scope": "actualWriterAdmission",
            "databaseId": null, "epoch": null, "lost": 0, "dropped": 0, "gap": false, "prefixGap": false});
        let mut valid = stream.clone();
        assert!(bind_runtime_mapping(
            &mut valid,
            std::slice::from_ref(&mapping)
        ));
        assert_eq!(valid["status"], "available");
        assert_eq!(valid["databaseId"], mapping["databaseId"]);
        assert_eq!(valid["epoch"], mapping["epoch"]);
        assert!(bind_runtime_mapping(
            &mut valid,
            std::slice::from_ref(&mapping)
        ));
        for (field, value) in [
            ("databaseId", json!(null)),
            ("epoch", json!(null)),
            ("databaseId", json!({"database": "foreign"})),
            ("epoch", json!(3)),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            let original = invalid.clone();
            assert!(!bind_runtime_mapping(
                &mut invalid,
                std::slice::from_ref(&mapping)
            ));
            assert_eq!(invalid, original);
        }
        for (field, value) in [
            ("queueInstance", json!("retired-queue")),
            ("databaseId", json!({"database": "foreign"})),
            ("epoch", json!(3)),
            ("scope", json!("physical_inner")),
            ("lost", json!(1)),
            ("dropped", json!(1)),
            ("gap", json!(true)),
            ("prefixGap", json!(true)),
        ] {
            let mut invalid = stream.clone();
            invalid[field] = value;
            let original = invalid.clone();
            assert!(!bind_runtime_mapping(
                &mut invalid,
                std::slice::from_ref(&mapping)
            ));
            assert_eq!(invalid, original);
        }
        assert!(!bind_runtime_mapping(&mut stream.clone(), &[]));
        let mut conflicting = mapping.clone();
        conflicting["databaseId"] = json!({"database": "foreign"});
        assert!(!bind_runtime_mapping(
            &mut stream.clone(),
            &[mapping.clone(), conflicting]
        ));
        let mut conflicting_epoch = mapping.clone();
        conflicting_epoch["epoch"] = json!(3);
        assert!(!bind_runtime_mapping(
            &mut stream.clone(),
            &[mapping.clone(), conflicting_epoch]
        ));
        assert!(!bind_runtime_mapping(
            &mut stream.clone(),
            &[mapping.clone(), mapping.clone()]
        ));
        let mut unavailable = mapping.clone();
        unavailable["status"] = json!("mapping_unavailable");
        assert!(!bind_runtime_mapping(&mut stream.clone(), &[unavailable]));
        for status in [
            "gap",
            "sequence_gap",
            "lock_unavailable",
            "export_truncated",
        ] {
            let mut failed = stream.clone();
            failed["status"] = json!(status);
            failed["lost"] = json!(1);
            failed["gap"] = json!(true);
            assert!(bind_runtime_mapping(
                &mut failed,
                std::slice::from_ref(&mapping)
            ));
            assert_eq!(failed["status"], status);
            assert_eq!(failed["lost"], 1);
            assert_eq!(failed["gap"], true);
        }
    }
}
