use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use super::{ExecutionEvidence, SqlEvidence};
use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
use crate::session::{AdmissionError, Budget, UnsettledSqlOutcome};

#[test]
fn absence_of_execution_and_settlement_are_independent_evidence() {
    let evidence = ExecutionEvidence::default();
    let rejected = evidence.failure(anyhow::anyhow!("duration"));
    assert_eq!(
        rejected.downcast_ref::<SqlEvidence>(),
        Some(&SqlEvidence::NotExecuted)
    );
    evidence.enter();
    let unknown = evidence.failure(anyhow::anyhow!("duration"));
    assert!(unknown.is::<UnsettledSqlOutcome>());
    evidence.settle();
    let completed = evidence.failure(anyhow::anyhow!("duration"));
    assert_eq!(
        completed.downcast_ref::<SqlEvidence>(),
        Some(&SqlEvidence::Settled)
    );
    let unknown = evidence.failure(anyhow::Error::new(UnsettledSqlOutcome));
    assert!(unknown.is::<UnsettledSqlOutcome>());
    assert!(unknown.downcast_ref::<SqlEvidence>().is_none());
}

#[test]
fn connected_session_prepare_rejection_never_claims_execution() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("evidence", "prepare")?,
        cache.path(),
        1,
    )?);
    let session = database.session()?;
    let error = session.execute("INSERT INTO absent VALUES(1)").unwrap_err();
    assert_eq!(
        error.downcast_ref::<SqlEvidence>(),
        Some(&SqlEvidence::NotExecuted)
    );
    assert_eq!(session.query("SELECT 1")?[0][0].to_string(), "1");
    Ok(())
}

#[test]
fn committed_prefix_duration_failure_is_settled_not_rejected() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("evidence", "prefix")?,
        cache.path(),
        1,
    )?);
    let session = database.session()?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    let budget = Budget {
        max_query_duration: Duration::from_millis(100),
        max_vm_steps: u64::MAX,
        ..Budget::default()
    };
    let error = session.execute_with_budget("INSERT INTO items VALUES(1); WITH RECURSIVE forever(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM forever) SELECT sum(x) FROM forever", &budget).unwrap_err();
    assert_eq!(
        error.downcast_ref::<SqlEvidence>(),
        Some(&SqlEvidence::Settled),
        "{error:#}"
    );
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline)
    );
    assert_eq!(
        database.session()?.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    Ok(())
}

#[test]
fn queue_full_is_rejected_without_vm_execution() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("evidence", "queue")?,
        cache.path(),
        1,
    )?);
    let owner = database.session()?;
    owner.execute("CREATE TABLE items(value INTEGER); BEGIN IMMEDIATE")?;
    let controls = (0..8)
        .map(|_| database.admission().register())
        .collect::<Result<Vec<_>>>()?;
    std::thread::scope(|threads| -> Result<()> {
        let waiters = controls
            .iter()
            .map(|control| {
                threads.spawn(|| {
                    database
                        .admission()
                        .acquire(control, false, || Ok(()))
                        .map(|permit| permit.settle(false))
                })
            })
            .collect::<Vec<_>>();
        let deadline = Instant::now() + Duration::from_secs(5);
        while database.writer_admission_status()?.queued.len() != 8 {
            assert!(Instant::now() < deadline, "writer queue did not fill");
            std::thread::sleep(Duration::from_millis(1));
        }
        let session = database.session()?;
        let error = session.execute("INSERT INTO items VALUES(1)").unwrap_err();
        for control in &controls {
            database.admission().close_requested(control);
        }
        for waiter in waiters {
            assert!(waiter.join().unwrap().is_err());
        }
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::QueueFull)
        );
        assert_eq!(
            error.downcast_ref::<SqlEvidence>(),
            Some(&SqlEvidence::NotExecuted),
            "{error:#}"
        );
        Ok(())
    })?;
    owner.execute("ROLLBACK")?;
    assert_eq!(
        database.session()?.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "0"
    );
    Ok(())
}
