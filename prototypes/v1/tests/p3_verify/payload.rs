use super::{identifier, memory, runtime};
use anyhow::Result;
use peri_loom_v1::{manifest::SEGMENT_SIZE, runtime::WorkerLimits};
use std::{fs, path::Path, time::Duration};

fn payload_bytes(path: &Path) -> Result<u64> {
    let mut bytes = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            bytes += payload_bytes(&entry.path())?;
        } else if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "seg")
        {
            bytes += entry.metadata()?.len();
        }
    }
    Ok(bytes)
}

#[test]
fn tiny_cache_caps_measure_payload_and_drop_writer_releases_capacity() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let store = memory();
    let limits = WorkerLimits {
        max_total_sessions: 2,
        max_sessions_per_database: 2,
        segment_ram_bytes: SEGMENT_SIZE,
        segment_disk_bytes: SEGMENT_SIZE as u64,
        ..WorkerLimits::default()
    };
    let runtime = runtime(store.clone(), cache.path(), limits)?;
    let id = identifier("payload", "db");
    let writer = runtime.open(id.clone(), 0)?;
    let reader = runtime.open(id.clone(), 0)?;
    writer.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, payload BLOB); BEGIN")?;
    for number in 0..96 {
        writer.execute(&format!(
            "INSERT INTO records VALUES({number}, randomblob(8192))"
        ))?;
    }
    writer.execute("COMMIT")?;
    writer.checkpoint()?;
    writer.compact_wal()?;
    reader.query("SELECT sum(length(payload)) FROM records")?;
    let resources = reader.resources()?;
    assert!(resources.segment_ram_bytes <= SEGMENT_SIZE);
    assert!(resources.segment_disk_bytes <= SEGMENT_SIZE as u64);
    assert_eq!(resources.segment_disk_bytes, payload_bytes(cache.path())?);
    assert_eq!(resources.dirty_segment_bytes, 0);
    assert_eq!(resources.pending_wal_bytes, 0);
    assert!(resources.wal_bytes <= 64 * 1024);
    writer.execute("BEGIN IMMEDIATE; INSERT INTO records VALUES(999, randomblob(16384))")?;
    assert_eq!(
        reader.query("SELECT count(*) FROM records")?[0][0].to_string(),
        "96"
    );
    assert!(!reader.in_transaction()?);
    assert!(runtime.evict_idle(Duration::ZERO)?.is_empty());
    assert!(runtime.open(id.clone(), 0).is_err());
    assert!(reader.compact_wal().is_err());
    drop(writer);
    let replacement = runtime.open(id.clone(), 0)?;
    replacement.execute("INSERT INTO records VALUES(1000, X'01')")?;
    assert_eq!(
        reader.query("SELECT count(*) FROM records WHERE id=999")?[0][0].to_string(),
        "0"
    );
    drop(reader);
    drop(replacement);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?, vec![id.clone()]);
    assert!(runtime.status()?.databases.is_empty());
    fs::remove_dir_all(cache.path())?;
    let cold = runtime.open(id, 0)?;
    assert_eq!(
        cold.query("SELECT count(*) FROM records")?[0][0].to_string(),
        "97"
    );
    Ok(())
}

#[test]
fn tenant_and_delimiter_names_do_not_alias_after_full_cache_disposal() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let runtime = runtime(memory(), cache.path(), WorkerLimits::default())?;
    let identifiers = [
        identifier("a-b", "c"),
        identifier("a", "b-c"),
        identifier("other", "c"),
    ];
    for (index, id) in identifiers.iter().enumerate() {
        let handle = runtime.open(id.clone(), 0)?;
        handle.execute(&format!(
            "CREATE TABLE marker(value INTEGER); INSERT INTO marker VALUES({index})"
        ))?;
    }
    assert_eq!(runtime.evict_idle(Duration::ZERO)?.len(), 3);
    fs::remove_dir_all(cache.path())?;
    for (index, id) in identifiers.iter().enumerate() {
        let handle = runtime.open(id.clone(), 0)?;
        assert_eq!(
            handle.query("SELECT value FROM marker")?[0][0].to_string(),
            index.to_string()
        );
    }
    for invalid in ["../other", "a/b", "", ".", "é", "a\\b"] {
        let mut forged = identifiers[0].clone();
        forged.namespace.tenant = invalid.to_owned();
        assert!(runtime.open(forged.clone(), 0).is_err());
        assert!(runtime.close(&forged).is_err());
        assert!(runtime.drain(&forged).is_err());
        assert!(forged.cache_key().is_err());
    }
    Ok(())
}
