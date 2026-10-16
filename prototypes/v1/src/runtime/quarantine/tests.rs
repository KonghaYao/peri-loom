use anyhow::Result;

use super::{AdmissionError, DatabaseId, Entry, Registry};
use crate::namespace::Namespace;

fn identifier() -> DatabaseId {
    DatabaseId::new(
        Namespace::new("retirement_test", "isolated").unwrap(),
        "main",
    )
    .unwrap()
}

#[test]
fn quarantined_database_keeps_visible_capacity_while_healthy_idle_database_retires() -> Result<()> {
    use crate::object_store::MemoryObjectStore;
    use crate::provider::LocalAuthorityProvider;
    use crate::runtime::{WorkerLimits, WorkerRuntime};
    use std::sync::Arc;
    use std::time::Duration;
    let directory = tempfile::tempdir()?;
    let runtime = WorkerRuntime::new(
        Arc::new(LocalAuthorityProvider::new(Arc::new(
            MemoryObjectStore::default(),
        ))),
        directory.path(),
        WorkerLimits::default(),
    )?;
    let bad = identifier();
    let healthy = DatabaseId::new(Namespace::new("retirement_test", "healthy")?, "main")?;
    let bad_handle = runtime.open(bad.clone(), 1)?;
    let healthy_handle = runtime.open(healthy.clone(), 1)?;
    healthy_handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(7)")?;
    {
        let registry = runtime.registry.lock().unwrap();
        let Entry::Loaded(loaded) = registry.entries.get(&bad).unwrap() else {
            panic!("missing instance")
        };
        loaded.database.database()?.admission().quarantine();
    }
    drop(healthy_handle);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?, vec![healthy.clone()]);
    let status = runtime.status()?;
    assert_eq!(status.quarantined_count, 1);
    // The quarantined database still holds a native entry, so it is resident for
    // reporting too; its slot is separately visible through the quarantine ledger.
    assert_eq!(status.resident_count, 1);
    assert!(status.oldest_resident_age_ms < u64::MAX);
    assert!(runtime.open(bad.clone(), 2).is_err());
    let reopened = runtime.open(healthy, 1)?;
    assert_eq!(
        reopened.query("SELECT value FROM data")?[0][0].to_string(),
        "7"
    );
    drop(reopened);
    drop(bad_handle);
    assert_eq!(runtime.status()?.quarantined_count, 1);
    Ok(())
}

#[test]
fn unknown_tombstone_survives_zero_sessions_and_retiring_completion() -> Result<()> {
    let identifier = identifier();
    let mut registry = Registry::default();
    registry
        .entries
        .insert(identifier.clone(), Entry::Retiring(10));
    registry.quarantined.insert(identifier.clone(), 10);
    registry.finish_retiring(&identifier);
    assert_eq!(registry.sessions, 0);
    assert!(matches!(
        registry.entries.get(&identifier),
        Some(Entry::Retiring(10))
    ));
    assert_eq!(
        registry
            .check_available(&identifier)
            .unwrap_err()
            .downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Quarantine)
    );
    assert_eq!(registry.quarantined.get(&identifier), Some(&10));
    Ok(())
}

#[test]
fn removing_native_entry_does_not_remove_unknown_tombstone() {
    let identifier = identifier();
    let mut registry = Registry::default();
    registry.quarantined.insert(identifier.clone(), 10);
    assert!(registry.entries.remove(&identifier).is_none());
    assert_eq!(
        registry
            .check_available(&identifier)
            .unwrap_err()
            .downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Quarantine)
    );
}

#[test]
fn clean_retirement_can_remove_entry_without_poisoning_other_database() -> Result<()> {
    let identifier = identifier();
    let mut registry = Registry::default();
    registry
        .entries
        .insert(identifier.clone(), Entry::Retiring(10));
    registry.finish_retiring(&identifier);
    assert!(!registry.entries.contains_key(&identifier));
    registry.check_available(&identifier)?;
    let other = DatabaseId::new(identifier.namespace.clone(), "other")?;
    registry.quarantined.insert(identifier.clone(), 10);
    registry.check_available(&other)?;
    Ok(())
}

#[test]
fn offline_tombstones_consume_capacity_but_not_the_reported_residency() -> Result<()> {
    use std::time::{Duration, Instant};
    let first = identifier();
    let mut registry = Registry::default();
    registry.quarantined.insert(first.clone(), 10);
    registry
        .quarantined_since
        .insert(first.clone(), Instant::now() - Duration::from_secs(30));
    // A sample can outlive its entry, so the reported age must be filtered by the
    // entry map rather than read from the timestamp table directly.
    registry
        .resident_since
        .insert(first.clone(), Instant::now() - Duration::from_secs(30));
    assert_eq!(registry.capacity_resident_count(), 1);
    assert_eq!(registry.live_resident_count(), 0);
    assert_eq!(registry.oldest_live_resident_age_ms(), 0);
    assert!(registry.oldest_quarantined_age_ms() >= 30_000);
    Ok(())
}

#[test]
fn tombstones_without_heavy_entries_still_consume_database_capacity() -> Result<()> {
    let first = identifier();
    let second = DatabaseId::new(first.namespace.clone(), "other")?;
    let mut registry = Registry::default();
    registry.quarantined.insert(first.clone(), 10);
    assert_eq!(registry.capacity_resident_count(), 1);
    registry.check_capacity(2)?;
    registry.quarantined.insert(second, 11);
    assert_eq!(registry.capacity_resident_count(), 2);
    assert!(registry.check_capacity(2).is_err());
    assert!(registry.check_available(&first).is_err());
    assert_eq!(registry.quarantined.get(&first), Some(&10));
    Ok(())
}

#[test]
fn capacity_counts_quarantine_and_native_entry_as_one_resident_identity() -> Result<()> {
    let first = identifier();
    let second = DatabaseId::new(first.namespace.clone(), "other")?;
    let mut registry = Registry::default();
    registry.quarantined.insert(first.clone(), 10);
    registry.entries.insert(first.clone(), Entry::Retiring(10));
    assert_eq!(registry.capacity_resident_count(), 1);
    registry.check_capacity(2)?;
    registry.entries.insert(second, Entry::Retiring(11));
    assert_eq!(registry.capacity_resident_count(), 2);
    assert!(registry.check_capacity(2).is_err());
    registry.finish_retiring(&first);
    assert_eq!(registry.capacity_resident_count(), 2);
    assert!(registry.check_capacity(2).is_err());
    Ok(())
}

#[test]
fn actual_reclaimed_native_instance_remains_counted_and_cannot_reopen() -> Result<()> {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::object_store::MemoryObjectStore;
    use crate::provider::LocalAuthorityProvider;
    use crate::runtime::{WorkerLimits, WorkerRuntime};
    use crate::session::RequestDeadline;

    let directory = tempfile::tempdir()?;
    let runtime = WorkerRuntime::new(
        Arc::new(LocalAuthorityProvider::new(Arc::new(
            MemoryObjectStore::default(),
        ))),
        directory.path().join("cache"),
        WorkerLimits {
            max_open_databases: 1,
            ..WorkerLimits::default()
        },
    )?;
    let first = identifier();
    let second = DatabaseId::new(Namespace::new("retirement_test", "other")?, "main")?;
    let handle = runtime.open(first.clone(), 10)?;
    let database = {
        let registry = runtime.registry.lock().unwrap();
        let Entry::Loaded(loaded) = registry.entries.get(&first).unwrap() else {
            panic!("expected native instance")
        };
        loaded.database.database()?
    };
    let weak_database = Arc::downgrade(&database);
    database.admission().quarantine();
    let deadline = RequestDeadline::until(Instant::now() + Duration::from_secs(1));
    let guard = database.retirement_guard(&deadline)?;
    database.collect_inactive_native(&guard)?;
    drop(guard);
    drop(database);
    assert!(weak_database.upgrade().is_none());
    let status = runtime.status()?;
    assert_eq!(status.loaded_databases, 1);
    assert_eq!(
        status.databases[0].lifecycle,
        crate::runtime::DatabaseLifecycle::Draining
    );
    assert!(status.databases[0].resources.is_none());
    assert!(handle.query("SELECT 1").is_err());
    assert!(runtime.open(first.clone(), 11).is_err());
    assert!(runtime
        .open(second, 11)
        .err()
        .unwrap()
        .to_string()
        .contains("worker database capacity exceeded"));
    assert!(runtime.drain(&first).is_err());
    drop(handle);
    let registry = runtime.registry.lock().unwrap();
    assert_eq!(registry.sessions, 0);
    assert_eq!(registry.capacity_resident_count(), 1);
    assert_eq!(registry.quarantined.get(&first), Some(&10));
    Ok(())
}
