//! Process-level concurrent local open.
//!
//! The unit test races two threads inside one process. This one starts two real worker
//! processes against the same `LocalObjectStore` directory and releases both `open`
//! requests before reading either answer, so the CAS and file-lock path is exercised across
//! processes rather than only across threads.

#[allow(dead_code)]
#[path = "stateless_acceptance/process.rs"]
mod process;

use anyhow::{ensure, Context, Result};
use peri_loom_v1::authority::Authority;
use peri_loom_v1::manifest::{digest, Manifest};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::LocalObjectStore;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

const ROUNDS: usize = 3;
const SEED_PUBLICATIONS: usize = 512;
const LOCAL_ROOT_VERSION: u32 = 6;

/// Publish a valid empty HEAD through the authority so the later worker opens do not also
/// race `RemoteStorage::open`'s fresh-HEAD initialization. That initialization has its own
/// pre-existing stale-view behaviour (a failed `Absent` put is followed by a cached miss),
/// which is outside this test's migration/CAS scope.
fn seed_branch_head(authority: &Arc<Authority>) -> Result<()> {
    let manifest = Manifest::default();
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let manifest_entry = authority.upload(&manifest_bytes)?;
    let manifest_id = digest(&manifest_bytes);
    authority.transact(None, |root| {
        root.objects
            .insert(format!("manifests/{manifest_id}"), manifest_entry.clone())?;
        Ok(())
    })?;
    let head_bytes = serde_json::to_vec(&json!({"manifest": manifest_id}))?;
    let head_entry = authority.upload(&head_bytes)?;
    authority.transact(None, |root| {
        root.branches.get_mut("main").unwrap().head = Some(head_entry.clone());
        Ok(())
    })?;
    Ok(())
}

fn seed_v1_store(directory: &Path, namespace: &Namespace) -> Result<u64> {
    let store = Arc::new(LocalObjectStore::open(directory)?);
    let authority = Authority::open(store, namespace.clone())?;
    seed_branch_head(&authority)?;
    for _ in 0..SEED_PUBLICATIONS {
        authority.transact(None, |_| Ok(()))?;
    }
    let revision = authority.snapshot()?.root.revision;
    Ok(revision)
}

fn database(namespace: &Namespace) -> serde_json::Value {
    json!({"namespace": namespace, "branch": "main"})
}

#[test]
fn two_worker_processes_open_one_v1_store_and_observe_one_v6_head() -> Result<()> {
    for round in 0..ROUNDS {
        let store = tempfile::tempdir()?;
        let first_cache = tempfile::tempdir()?;
        let second_cache = tempfile::tempdir()?;
        let namespace = Namespace::new("local", format!("process-race-{round}"))?;
        let base_revision = seed_v1_store(store.path(), &namespace)?;

        let mut first = process::Worker::spawn(store.path(), first_cache.path())?;
        let mut second = process::Worker::spawn(store.path(), second_cache.path())?;
        let request = json!({"op":"open", "database":database(&namespace), "epoch":0});
        let first_id = first.send(request.clone())?;
        let second_id = second.send(request)?;
        let first_response = first.receive(&first_id)?;
        let second_response = second.receive(&second_id)?;
        ensure!(
            first_response["ok"] == true && second_response["ok"] == true,
            "concurrent open failed: first={first_response} second={second_response}\
             \nfirst stderr:\n{}\nsecond stderr:\n{}",
            first.diagnostics(),
            second.diagnostics()
        );
        let first_session = first_response["session"]
            .as_str()
            .context("first session missing")?
            .to_owned();
        let second_session = second_response["session"]
            .as_str()
            .context("second session missing")?
            .to_owned();
        first.close(&first_session)?;
        second.close(&second_session)?;
        first.finish()?;
        second.finish()?;

        let store = Arc::new(LocalObjectStore::open(store.path())?);
        let reopened = Authority::open(store, namespace)?;
        let root = reopened.snapshot()?.root;
        assert_eq!(root.version, LOCAL_ROOT_VERSION);
        assert!(
            root.revision > base_revision,
            "round {round}: the version-1 store was not migrated"
        );
        assert!(root.receipts.len() <= 128);
    }
    Ok(())
}
