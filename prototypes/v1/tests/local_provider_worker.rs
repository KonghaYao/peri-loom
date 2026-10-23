#[allow(dead_code)]
#[path = "stateless_acceptance/process.rs"]
mod process;

use anyhow::Result;
use peri_loom_v1::{authority::Authority, namespace::Namespace, object_store::LocalObjectStore};
use serde_json::json;
use std::sync::Arc;

#[test]
fn local_worker_non_roll_commit_uses_one_root_and_recovers_without_cache() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let namespace = Namespace::new("local", "worker-fast-path")?;
    let database = json!({"namespace":namespace, "branch":"main"});
    let mut worker = process::Worker::spawn(directory.path(), cache.path())?;
    let session = worker.open(&database)?;
    worker.exec(
        &session,
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
    )?;
    let authority = Authority::open(
        Arc::new(LocalObjectStore::open(directory.path())?),
        namespace,
    )?;
    for index in 0..3 {
        let before = authority.snapshot()?.root.revision;
        worker.exec(
            &session,
            &format!("INSERT INTO items VALUES({index}, 'durable')"),
        )?;
        let after = authority.snapshot()?.root.revision;
        assert_eq!(
            after - before,
            1,
            "real worker fell back to chunk/index publications"
        );
    }
    assert_eq!(
        worker.query(&session, "SELECT count(*) FROM items")?,
        json!([[3]])
    );
    worker.close(&session)?;
    worker.finish()?;
    drop(worker);
    // Discard every cache byte: the next process must recover only from authority objects.
    cache.close()?;
    let empty_cache = tempfile::tempdir()?;
    let mut reopened = process::Worker::spawn(directory.path(), empty_cache.path())?;
    let session = reopened.open(&database)?;
    assert_eq!(
        reopened.query(&session, "SELECT count(*) FROM items")?,
        json!([[3]])
    );
    reopened.exec(&session, "INSERT INTO items VALUES(3, 'after-reopen')")?;
    assert_eq!(
        reopened.query(&session, "SELECT count(*) FROM items")?,
        json!([[4]])
    );
    reopened.close(&session)?;
    reopened.finish()?;
    Ok(())
}
