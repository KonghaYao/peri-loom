//! Upgrade and downgrade checks against the actual previous release binary.
//!
//! The version whitelist lives in the old binary, so a copied predicate would prove
//! nothing. `PERI_V112_BINARY` must point at a release build of v1.1.12 (commit d73e64f);
//! the test writes a real SQL store with it, upgrades that store with the current binary,
//! verifies the oracle across a restart, then proves the old binary now fails closed.

#[allow(dead_code)]
#[path = "stateless_acceptance/process.rs"]
mod process;

use anyhow::{Context, Result};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

const TENANT: &str = "local";
const DATABASE: &str = "upgrade-v112";

fn old_binary() -> Option<PathBuf> {
    std::env::var_os("PERI_V112_BINARY").map(PathBuf::from)
}

fn database() -> Value {
    json!({"namespace": {"tenant": TENANT, "database": DATABASE}, "branch": "main"})
}

fn root_version(store: &Path) -> Result<u32> {
    let store = LocalObjectStore::open(store)?;
    let namespace = Namespace::new(TENANT, DATABASE)?;
    let object = store
        .get(&namespace.key("authority/root")?)?
        .context("authority root missing")?;
    let envelope: serde_json::Value = serde_json::from_slice(&object.bytes)?;
    envelope["root"]["version"]
        .as_u64()
        .map(|version| version as u32)
        .context("authority root version missing")
}

#[test]
#[ignore = "requires PERI_V112_BINARY=<v1.1.12 release binary>"]
fn v112_store_upgrades_once_and_the_old_binary_rejects_v6() -> Result<()> {
    let Some(old) = old_binary() else {
        eprintln!("PERI_V112_BINARY not set; skipping previous-release upgrade check");
        return Ok(());
    };
    assert!(old.is_file(), "PERI_V112_BINARY is not a file: {old:?}");

    let store = tempfile::tempdir()?;
    let old_cache = tempfile::tempdir()?;
    let new_cache = tempfile::tempdir()?;
    let restart_cache = tempfile::tempdir()?;

    {
        let mut worker = process::Worker::spawn_binary(&old, store.path(), old_cache.path())?;
        let session = worker.open(&database())?;
        worker.exec(
            &session,
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
        )?;
        worker.exec(&session, "INSERT INTO items VALUES(0, 'v112')")?;
        worker.exec(&session, "INSERT INTO items VALUES(1, 'v112')")?;
        worker.close(&session)?;
        worker.finish()?;
    }
    assert_eq!(
        root_version(store.path())?,
        1,
        "the previous release must leave a version-1 head"
    );

    {
        let mut worker = process::Worker::spawn(store.path(), new_cache.path())?;
        let session = worker.open(&database())?;
        assert_eq!(
            worker.query(&session, "SELECT id,value FROM items ORDER BY id")?,
            json!([[0, "v112"], [1, "v112"]])
        );
        worker.exec(&session, "INSERT INTO items VALUES(2, 'v6')")?;
        worker.close(&session)?;
        worker.finish()?;
    }
    assert_eq!(
        root_version(store.path())?,
        6,
        "the current binary must migrate the local head to v6"
    );

    {
        let mut worker = process::Worker::spawn(store.path(), restart_cache.path())?;
        let session = worker.open(&database())?;
        assert_eq!(
            worker.query(&session, "SELECT id,value FROM items ORDER BY id")?,
            json!([[0, "v112"], [1, "v112"], [2, "v6"]]),
            "the upgraded store must recover without a cache"
        );
        worker.close(&session)?;
        worker.finish()?;
    }

    {
        let mut worker = process::Worker::spawn_binary(&old, store.path(), old_cache.path())?;
        let response = worker.rejected(json!({"op":"open", "database":database(), "epoch":0}))?;
        let message = response["error"].as_str().unwrap_or_default();
        assert!(
            message.contains("unsupported authority root version"),
            "the previous release must reject v6 at decode, got: {message}"
        );
    }
    assert_eq!(
        root_version(store.path())?,
        6,
        "a rejected old open must not rewrite the head"
    );
    Ok(())
}
