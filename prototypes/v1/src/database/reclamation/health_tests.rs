use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use turso_core::io::{Buffer, Completion, OpenFlags, IO};

use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::LocalObjectStore;

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
        assert_eq!(database.retirement_health()["available"], false);
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
