//! Process-level empty-store open race.
//!
//! `RemoteStorage::open` initializes HEAD with a conditional put. Two workers opening the
//! same brand-new namespace can both observe HEAD absent; the loser's put fails and its
//! branch session must resolve the winner's pointer instead of repeating its cached
//! "absent". This starts two real worker processes and releases both opens before reading
//! either answer.

#[allow(dead_code)]
#[path = "stateless_acceptance/process.rs"]
mod process;

use anyhow::{ensure, Result};
use peri_loom_v1::authority::Authority;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::LocalObjectStore;
use serde_json::json;
use std::sync::Arc;

const ROUNDS: usize = 8;

#[test]
fn two_worker_processes_open_one_empty_store() -> Result<()> {
    for round in 0..ROUNDS {
        let store = tempfile::tempdir()?;
        let first_cache = tempfile::tempdir()?;
        let second_cache = tempfile::tempdir()?;
        let namespace = Namespace::new("local", format!("empty-open-race-{round}"))?;
        let mut first = process::Worker::spawn(store.path(), first_cache.path())?;
        let mut second = process::Worker::spawn(store.path(), second_cache.path())?;
        let request = json!({
            "op": "open",
            "database": {"namespace": namespace, "branch": "main"},
            "epoch": 0,
        });
        let first_id = first.send(request.clone())?;
        let second_id = second.send(request)?;
        let first_response = first.receive(&first_id)?;
        let second_response = second.receive(&second_id)?;
        ensure!(
            first_response["ok"] == true && second_response["ok"] == true,
            "round {round}: concurrent empty open failed: first={first_response} \
             second={second_response}\nfirst stderr:\n{}\nsecond stderr:\n{}",
            first.diagnostics(),
            second.diagnostics()
        );
        first.finish()?;
        second.finish()?;

        let store = Arc::new(LocalObjectStore::open(store.path())?);
        let reopened = Authority::open(store, namespace)?;
        ensure!(
            reopened.snapshot()?.root.branches["main"].head.is_some(),
            "round {round}: the concurrent open left no branch HEAD"
        );
    }
    Ok(())
}
