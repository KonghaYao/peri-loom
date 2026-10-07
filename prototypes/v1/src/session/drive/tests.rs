use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
use crate::session::{AdmissionError, Budget, RequestDeadline};

use super::{sleep, ExecutionBudget};

#[test]
fn sleep_respects_requested_duration_with_live_local_guard_and_unchanged_hint() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(MemoryObjectStore::default());
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        Namespace::new("sleep_guard", "positive")?,
        directory.path(),
        1,
    )?);
    let session = database.session()?;
    let previous = session.lane.hint();
    let connection = session.lane.connection.lock().unwrap();
    let budget = Budget::default();
    let deadline = RequestDeadline::new(&budget)?;
    let guard = ExecutionBudget::install(
        connection.as_ref().unwrap(),
        "SELECT 1",
        &budget,
        store.clone(),
        &deadline,
        session.lane.control.clone(),
    )?;
    let requested = Duration::from_millis(20);
    let started = Instant::now();
    sleep(requested, store.as_ref(), &guard)?;
    assert!(started.elapsed() >= requested);
    guard.check()?;
    deadline.check()?;
    assert_eq!(session.lane.hint(), previous);
    Ok(())
}

#[test]
fn sleep_preserves_duration_deadline_context_without_relabeling_vm_caps() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(MemoryObjectStore::default());
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        Namespace::new("sleep_guard", "deadline")?,
        directory.path(),
        1,
    )?);
    let session = database.session()?;
    let connection = session.lane.connection.lock().unwrap();
    let budget = Budget {
        max_query_duration: Duration::from_millis(20),
        ..Budget::default()
    };
    let deadline = RequestDeadline::new(&budget)?;
    let guard = ExecutionBudget::install(
        connection.as_ref().unwrap(),
        "SELECT 1",
        &budget,
        store.clone(),
        &deadline,
        session.lane.control.clone(),
    )?;
    let cap = guard.account_steps(u64::MAX).unwrap_err();
    assert!(cap.downcast_ref::<AdmissionError>().is_none());
    assert_eq!(cap.to_string(), "VM step budget exceeded");
    let error = sleep(Duration::from_secs(1), store.as_ref(), &guard).unwrap_err();
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline)
    );
    assert_eq!(
        error.root_cause().to_string(),
        "query duration budget exceeded"
    );
    Ok(())
}

#[test]
fn sleep_preserves_local_closing_context_without_native_busy_or_io_dispatch() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(MemoryObjectStore::default());
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        Namespace::new("sleep_guard", "closing")?,
        directory.path(),
        1,
    )?);
    let session = database.session()?;
    let connection = session.lane.connection.lock().unwrap();
    let budget = Budget::default();
    let deadline = RequestDeadline::new(&budget)?;
    let guard = ExecutionBudget::install(
        connection.as_ref().unwrap(),
        "SELECT 1",
        &budget,
        store.clone(),
        &deadline,
        session.lane.control.clone(),
    )?;
    session.lane.control.closing.store(true, Ordering::Release);
    let error = sleep(Duration::from_secs(1), store.as_ref(), &guard).unwrap_err();
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Closing)
    );
    assert!(error.downcast_ref::<turso_core::LimboError>().is_none());
    Ok(())
}
