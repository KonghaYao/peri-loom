use super::catalog;
use anyhow::{ensure, Context, Result};
use peri_loom_v1::manifest::SEGMENT_SIZE;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::provider::LocalAuthorityProvider;
use peri_loom_v1::runtime::{DatabaseId, WorkerLimits, WorkerRuntime};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use turso_core::{Numeric, Value};

struct CountingStore {
    inner: LocalObjectStore,
    reads: Mutex<BTreeMap<String, (u64, u64)>>,
}

impl ObjectStore for CountingStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let object = self.inner.get(key)?;
        if let Some(object) = &object {
            let mut reads = self.reads.lock().unwrap();
            let counts = reads.entry(key.to_owned()).or_default();
            counts.0 += 1;
            counts.1 += object.bytes.len() as u64;
        }
        Ok(object)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn count(reads: &BTreeMap<String, (u64, u64)>, keys: &BTreeSet<String>) -> (u64, u64) {
    keys.iter()
        .filter_map(|key| reads.get(key))
        .fold((0, 0), |total, counts| {
            (total.0 + counts.0, total.1 + counts.1)
        })
}

pub fn assert_lazy_current_stream(store: &Path, cache: &Path, name: &str) -> Result<()> {
    ensure!(!cache.exists(), "counting probe cache is not empty");
    let control = catalog::authority(store, name)?;
    let root = control.snapshot()?.root;
    let namespace = control.namespace().clone();
    let mut current_hashes = BTreeSet::new();
    if let Some(wal) = &root.branches["main"].wal {
        let index: serde_json::Value = serde_json::from_slice(&control.read_blob(wal)?)?;
        for entry in index["entries"]
            .as_array()
            .context("current WAL entries missing")?
        {
            current_hashes.insert(
                entry["hash"]
                    .as_str()
                    .context("WAL chunk hash missing")?
                    .to_owned(),
            );
        }
    }
    let mut segments = BTreeSet::new();
    let mut current_wal = BTreeSet::new();
    let mut archived_wal = BTreeSet::new();
    for (logical, entry) in root.objects.entries()? {
        let physical = namespace.key(&format!("authority/blobs/{}", entry.blob))?;
        if logical.starts_with("segments/") {
            segments.insert(physical);
        } else if let Some(hash) = logical.strip_prefix("wal/chunks/") {
            if current_hashes.contains(hash) {
                current_wal.insert(physical);
            } else {
                archived_wal.insert(physical);
            }
        }
    }
    ensure!(
        !archived_wal.is_empty(),
        "fixture lacks archived WAL chunks"
    );
    let counted = Arc::new(CountingStore {
        inner: LocalObjectStore::open(store)?,
        reads: Mutex::new(BTreeMap::new()),
    });
    let limits = WorkerLimits {
        max_open_databases: 1,
        max_total_sessions: 1,
        max_sessions_per_database: 1,
        segment_ram_bytes: SEGMENT_SIZE,
        segment_disk_bytes: (SEGMENT_SIZE * 2) as u64,
    };
    let runtime = WorkerRuntime::new(
        Arc::new(LocalAuthorityProvider::new(counted.clone())),
        cache,
        limits.clone(),
    )?;
    let identifier = DatabaseId::new(namespace, "main")?;
    let session = runtime.open(identifier.clone(), 0)?;
    let opening = session.resources()?;
    let opening_gets = count(&counted.reads.lock().unwrap(), &segments).0;
    ensure!(
        opening_gets > 0 && opening_gets <= 4,
        "cold open segment GET count: {opening_gets}"
    );
    for record in [999, 250] {
        let rows = session.query(&format!(
            "SELECT id,length(payload) FROM records WHERE id={record}"
        ))?;
        assert_eq!(rows[0][0], Value::Numeric(Numeric::Integer(record)));
        assert_eq!(rows[0][1], Value::Numeric(Numeric::Integer(2048)));
    }
    let usage = session.resources()?;
    ensure!(
        usage.segment_ram_bytes <= limits.segment_ram_bytes
            && usage.segment_disk_bytes <= limits.segment_disk_bytes,
        "configured cache caps not enforced: {usage:?}"
    );
    ensure!(
        usage.wal_bytes <= 64 * 1024,
        "current WAL image is not bounded"
    );
    let reads = counted.reads.lock().unwrap();
    let segment_reads = count(&reads, &segments);
    let archive_reads = count(&reads, &archived_wal);
    let current_reads = count(&reads, &current_wal);
    ensure!(
        segment_reads.0 <= 8,
        "point queries fetched too many segment bodies: {segment_reads:?}"
    );
    ensure!(
        archive_reads.0 == 0,
        "cold open downloaded archived WAL bodies: {archive_reads:?}"
    );
    ensure!(
        current_reads.1 <= 64 * 1024,
        "current replay transferred too much WAL payload: {current_reads:?}"
    );
    eprintln!("P3 counting probe: cold_loads={} cold_segment_gets={opening_gets} point_segment_gets={} segment_payload_bytes={} archived_wal_gets={} current_wal_payload_bytes={} ram_cache_bytes={} disk_cache_bytes={}", opening.segment_loads, segment_reads.0, segment_reads.1, archive_reads.0, current_reads.1, usage.segment_ram_bytes, usage.segment_disk_bytes);
    drop(reads);
    drop(session);
    ensure!(runtime.close(&identifier)?, "counting probe did not unload");
    assert_eq!(runtime.status()?.loaded_databases, 0);
    catalog::assert_cache_only(cache)?;
    Ok(())
}
