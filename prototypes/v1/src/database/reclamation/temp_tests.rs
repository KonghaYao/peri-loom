use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use turso_core::{LimboError, RetirementErrors, TempStore};

use super::peer_tests::Fixture;
use super::CleanupFailure;
use crate::session::{RequestDeadline, UnsettledSqlOutcome};

#[test]
fn fresh_factory_sets_memory_once_without_per_statement_temp_override() -> Result<()> {
    let fixture = Fixture::open()?;
    let default = fixture.database.lane.connection.lock().unwrap();
    assert_eq!(
        default.as_ref().unwrap().get_temp_store(),
        TempStore::Memory
    );
    drop(default);
    let session = fixture.database.session()?;
    let connection = session.lane.connection.lock().unwrap();
    let connection = connection.as_ref().unwrap();
    assert_eq!(connection.get_temp_store(), TempStore::Memory);
    connection.set_temp_store(TempStore::File);
    crate::session::configure(connection);
    assert_eq!(connection.get_temp_store(), TempStore::File);
    Ok(())
}

#[test]
fn explicit_memory_begin_failed_main_commit_verifies_both_pagers_before_reclamation() -> Result<()>
{
    let fixture = Fixture::open()?;
    let session = fixture.database.session()?;
    session.execute("INSERT INTO ledger VALUES(10)")?;
    assert_eq!(
        session.query("SELECT value FROM ledger")?[0][0].to_string(),
        "10"
    );
    session.execute("BEGIN IMMEDIATE; INSERT INTO ledger VALUES(20)")?;
    assert!(session.in_transaction()?);
    let hint = session.lane.hint();
    assert!(hint.owner);
    let engine = Arc::downgrade(fixture.database.engine.lock().unwrap().as_ref().unwrap());
    let connection = session
        .lane
        .connection
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    assert_eq!(connection.get_temp_store(), TempStore::Memory);
    assert!(connection.get_pager().holds_write_lock());
    fixture.store.armed.store(true, Ordering::Release);
    let error = session.execute("COMMIT").unwrap_err();
    assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
    assert!(error.downcast_ref::<LimboError>().is_some());
    assert_eq!(session.lane.hint(), hint);
    assert_eq!(fixture.database.cleanup_failure(), None);
    assert!(fixture.database.pending.lock().unwrap().is_empty());
    assert!(fixture.database.reclamation.state.lock().unwrap().collected);
    let mutations = fixture.store.mutations.load(Ordering::SeqCst);
    let reads = fixture.store.reads.load(Ordering::SeqCst);
    let deadline = RequestDeadline::until(Instant::now() + Duration::from_secs(1));
    let guard = fixture.database.retirement_guard(&deadline)?;
    connection.verify_noncommitting_retired(&guard)?;
    assert!(connection.is_closed());
    assert_eq!(fixture.store.mutations.load(Ordering::SeqCst), mutations);
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), reads);
    assert!(engine.upgrade().is_some());
    drop(connection);
    assert!(engine.upgrade().is_none());
    drop(guard);
    assert!(session
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    assert!(fixture.database.session().is_err());
    Ok(())
}

#[test]
fn recorded_retirement_error_is_fixed_metadata_not_a_cleanup_or_health_proof() -> Result<()> {
    let fixture = Fixture::open()?;
    fixture
        .database
        .record_retirement_errors(&RetirementErrors {
            abort_error: Some(LimboError::InternalError("fixture sensitive source".into())),
            io_error: None,
            cleanup_error: None,
        });
    assert_eq!(
        fixture.database.cleanup_failure(),
        Some(CleanupFailure::RetirementError)
    );
    assert!(!fixture.database.reclamation.state.lock().unwrap().collected);
    assert!(fixture.database.engine.lock().unwrap().is_some());
    assert!(!fixture
        .database
        .cleanup_failure()
        .unwrap()
        .to_string()
        .contains("sensitive"));
    Ok(())
}
