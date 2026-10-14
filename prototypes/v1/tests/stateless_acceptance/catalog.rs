use anyhow::{ensure, Context, Result};
use peri_loom_v1::authority::Authority;
use peri_loom_v1::manifest::{Manifest, SEGMENT_SIZE};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub fn database(name: &str, branch: &str) -> Value {
    json!({"namespace":{"tenant":"acceptance", "database":name}, "branch":branch})
}

pub fn authority(store: &Path, name: &str) -> Result<Arc<Authority>> {
    Authority::open(
        Arc::new(LocalObjectStore::open(store)?),
        Namespace::new("acceptance", name)?,
    )
}

pub fn manifest(authority: &Authority, branch: &str) -> Result<Manifest> {
    let snapshot = authority.snapshot()?;
    let head = snapshot.root.branches[branch]
        .head
        .as_ref()
        .context("branch HEAD missing")?;
    let head: Value = serde_json::from_slice(&authority.read_blob(head)?)?;
    let identifier = head["manifest"]
        .as_str()
        .context("HEAD manifest ID missing")?;
    let entry = snapshot
        .root
        .objects
        .get(&format!("manifests/{identifier}"))
        .context("manifest logical entry missing")?;
    Ok(serde_json::from_slice(&authority.read_blob(entry)?)?)
}

pub fn assert_authority_layout(store: &Path, name: &str) -> Result<()> {
    let store = LocalObjectStore::open(store)?;
    let namespace = Namespace::new("acceptance", name)?;
    ensure!(
        store.get(&namespace.key("authority/root")?)?.is_some(),
        "worker did not publish through P2 authority"
    );
    ensure!(
        store.get(&namespace.key("HEAD")?)?.is_none(),
        "worker used raw P1 HEAD layout"
    );
    ensure!(
        store.get(&namespace.key("wal/index")?)?.is_none(),
        "worker used raw P1 WAL index"
    );
    Ok(())
}

pub fn assert_cache_only(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if metadata.is_dir() {
            assert_cache_only(&entry.path())?;
            continue;
        }
        ensure!(
            metadata.is_file(),
            "unexpected non-file cache entry: {:?}",
            entry.path()
        );
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(hash) = name.strip_suffix(".seg") {
            ensure!(
                hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "invalid segment cache filename: {name}"
            );
            ensure!(
                entry.metadata()?.len() == SEGMENT_SIZE as u64,
                "invalid cached segment size"
            );
        } else {
            ensure!(
                name.contains("lock"),
                "non-cache DB/WAL file: {:?}",
                entry.path()
            );
        }
    }
    Ok(())
}

pub fn resources(status: &Value, database: &Value) -> Result<Value> {
    let entry = status["databases"]
        .as_array()
        .context("registry entries missing")?
        .iter()
        .find(|entry| entry["identifier"] == *database)
        .context("database status missing")?;
    ensure!(
        entry["resources"].is_object(),
        "resource accounting missing: {entry}"
    );
    Ok(entry["resources"].clone())
}

pub fn metric(resources: &Value, key: &str) -> Result<u64> {
    resources[key]
        .as_u64()
        .with_context(|| format!("resource metric missing: {key}: {resources}"))
}
