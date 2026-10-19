use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use turso_core::io::{Buffer, Completion, OpenFlags, IO};

use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::LocalObjectStore;
use crate::session::{Budget, RequestDeadline};

#[test]
fn real_native_callback_and_retained_refs_are_visible_without_blocking() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = RemoteDatabase::open(
        Arc::new(LocalObjectStore::open(directory.path().join("objects"))?),
        Namespace::new("health", "callback")?,
        directory.path().join("cache"),
        10,
    )?;
    let temporary = directory.path().join("tursodb_temp_file");
    let file = database
        .io()
        .open_file(temporary.to_str().unwrap(), OpenFlags::Create, false)?;
    let (entered, observed) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    let resumed = Mutex::new(resumed);
    let completion = Completion::new_write(move |result| {
        assert!(result.is_ok());
        entered.send(()).unwrap();
        resumed
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
    });
    thread::scope(|threads| -> Result<()> {
        let writing =
            threads.spawn(|| file.pwrite(0, Arc::new(Buffer::new(vec![31; 32])), completion));
        observed.recv_timeout(Duration::from_secs(1))?;
        let sample = database.retirement_health();
        assert_eq!(sample["available"], true);
        assert_eq!(sample["io"]["pending_native_operations"], 1);
        database.admission().quarantine();
        let sample = database.retirement_health();
        assert_eq!(sample["quarantined"], true);
        assert_eq!(sample["retained_engines"], 1);
        assert_eq!(sample["retained_connections"], 1);
        let pending = database.pending.lock().unwrap();
        let sample = database.retirement_health();
        assert_eq!(sample["available"], false);
        assert_eq!(sample["reason"], "native_lock_unavailable");
        assert_eq!(sample["lock"], "pending");
        assert_eq!(sample["lock_held"], true);
        assert_eq!(sample["database"], "health/callback/");
        drop(pending);
        resume.send(())?;
        assert!(writing.join().unwrap()?.finished());
        assert_eq!(
            database.retirement_health()["io"]["pending_native_operations"],
            0
        );
        Ok(())
    })?;
    database.cleanup_deferred_until(std::time::Instant::now() + Duration::from_secs(1))?;
    let sample = database.retirement_health();
    assert_eq!(sample["retained_engines"], 0);
    assert_eq!(sample["retained_connections"], 0);
    Ok(())
}

/// This is the lock a checkpoint or compaction holds through
/// `maintenance_connection` for the whole unit. The sampler must name it, because an
/// unavailable sample at shutdown cannot otherwise be told apart from a registry race.
#[test]
fn maintenance_connection_is_reported_as_lane_connection() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = RemoteDatabase::open(
        Arc::new(LocalObjectStore::open(directory.path().join("objects"))?),
        Namespace::new("health", "maintenance")?,
        directory.path().join("cache"),
        10,
    )?;
    let deadline = RequestDeadline::new(&Budget::trusted_default())?;
    let connection = database.lane.maintenance_connection(&deadline, &database)?;
    let sample = database.retirement_health();
    assert_eq!(sample["available"], false);
    assert_eq!(sample["lock"], "lane.connection");
    assert_eq!(sample["lock_held"], true);
    assert_eq!(sample["database"], "health/maintenance/");
    drop(connection);
    assert_eq!(database.retirement_health()["available"], true);
    Ok(())
}
