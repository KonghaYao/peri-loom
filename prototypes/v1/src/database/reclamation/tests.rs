use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tempfile::TempDir;
use turso_core::IO;

use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
use crate::session::{AdmissionError, RequestDeadline, UnsettledSqlOutcome};

fn fixture() -> Result<(TempDir, Arc<RemoteDatabase>)> {
    let directory = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("native_retirement", "isolated")?,
        directory.path().join("cache"),
        10,
    )?);
    Ok((directory, database))
}

#[test]
fn inactive_collection_releases_native_arcs_without_relabeling_unknown() -> Result<()> {
    let (_directory, database) = fixture()?;
    let first = database.session()?;
    let second = database.session()?;
    let first_hint = first.lane.hint();
    let second_hint = second.lane.hint();
    let native_engine = Arc::downgrade(database.engine.lock().unwrap().as_ref().unwrap());
    let first_connection = Arc::downgrade(first.lane.connection.lock().unwrap().as_ref().unwrap());
    let second_connection =
        Arc::downgrade(second.lane.connection.lock().unwrap().as_ref().unwrap());
    database.admission().quarantine();
    let deadline = RequestDeadline::until(Instant::now() + Duration::from_secs(1));
    let guard = database.retirement_guard(&deadline)?;
    database.collect_inactive_native(&guard)?;
    assert!(native_engine.upgrade().is_none());
    assert!(first_connection.upgrade().is_none());
    assert!(second_connection.upgrade().is_none());
    assert_eq!(first.lane.hint(), first_hint);
    assert_eq!(second.lane.hint(), second_hint);
    assert_eq!(
        first
            .query("SELECT 1")
            .unwrap_err()
            .downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Quarantine)
    );
    assert!(first
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    assert!(second
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    assert!(database.session().is_err());
    assert!(database.io().step().is_err());
    drop(guard);
    assert!(database.io().step().is_err());
    Ok(())
}

#[test]
fn collected_session_shells_do_not_keep_directory_lock_alive() -> Result<()> {
    let (directory, database) = fixture()?;
    let session = database.session()?;
    let weak_database = Arc::downgrade(&database);
    database.admission().quarantine();
    let deadline = RequestDeadline::until(Instant::now() + Duration::from_secs(1));
    let guard = database.retirement_guard(&deadline)?;
    database.collect_inactive_native(&guard)?;
    drop(guard);
    drop(database);
    assert!(weak_database.upgrade().is_none());
    assert!(session
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    let cache = directory.path().join("cache");
    let replacement = RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("native_retirement", "isolated")?,
        cache,
        11,
    )?;
    drop(replacement);
    Ok(())
}
