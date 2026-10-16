use super::*;
use crate::authority_rpc::AuthorityClient;
use crate::ownership::types::LeaseBinding;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct Clock(AtomicU64);
impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        let now = self.0.load(Ordering::SeqCst);
        ensure!(now != u64::MAX, "clock unavailable");
        Ok(now)
    }
}

#[test]
fn status_isolates_clock_failure_and_never_detaches_healthy_siblings() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let token = format!(
        "{:064x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let provider = RemoteAuthorityProvider::new(
        AuthorityClient::new("http://127.0.0.1:9/", token)?,
        "worker".into(),
    )?;
    let runtime = OwnershipRuntime::new(
        "worker".into(),
        provider,
        cache.path(),
        WorkerLimits::default(),
        Arc::new(Clock::default()),
    )?;
    for (name, now) in [("bad", u64::MAX), ("healthy", 100)] {
        let database = DatabaseId::new(crate::namespace::Namespace::new("tenant", name)?, "main")?;
        let binding = LeaseBinding {
            database: database.clone(),
            root_incarnation: "root".into(),
            authority_term: 1,
            branch_incarnation: 1,
            epoch: 1,
            worker_id: "worker".into(),
            lease_id: name.into(),
            grant_id: name.into(),
            renewal_sequence: 0,
            role: AccessRole::Writer,
        };
        runtime.contexts.lock().unwrap().insert(
            database,
            Arc::new(LeaseContext {
                clock: Arc::new(Clock(AtomicU64::new(now))),
                state: Mutex::new(LeaseState {
                    binding,
                    mode: OwnershipMode::Draining,
                    role: AccessRole::Writer,
                    deadline_ms: 50,
                    generation: 1,
                    handles: 0,
                    opening: false,
                    renewing: false,
                    detaching: false,
                    last_now: 0,
                }),
            }),
        );
    }
    for _ in 0..2 {
        let status = runtime.status()?;
        assert_eq!(status.failures.len(), 1);
        assert_eq!(status.failures[0].database.namespace.database, "bad");
        assert_eq!(status.databases.len(), 1);
        assert_eq!(status.databases[0].mode, OwnershipMode::Revoked);
        assert_eq!(status.databases[0].database.namespace.database, "healthy");
    }
    // No installed remote lease is needed for sampling or local retirement.
    runtime.sweep_revocations()?;
    assert_eq!(runtime.status()?.failures.len(), 1);
    Ok(())
}

#[test]
fn detach_failures_are_ledgered_once_and_cleared_once_the_database_recovers() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let provider = RemoteAuthorityProvider::new(
        AuthorityClient::new("http://127.0.0.1:9/", "0".repeat(64))?,
        "worker".into(),
    )?;
    let runtime = OwnershipRuntime::new(
        "worker".into(),
        provider,
        cache.path(),
        WorkerLimits::default(),
        Arc::new(Clock::default()),
    )?;
    let database = DatabaseId::new(
        crate::namespace::Namespace::new("tenant", "detach")?,
        "main",
    )?;
    let failure = crate::ownership::types::OwnershipFailure {
        database: database.clone(),
        error: "first failure".into(),
    };
    let recovered = runtime
        .detach_failures
        .lock()
        .map_err(|_| anyhow::anyhow!("detach failure ledger poisoned"))?
        .len();
    assert_eq!(recovered, 0);
    runtime.note_detach_failures(std::slice::from_ref(&failure))?;
    // Repeated sweeps keep the first message instead of reprinting ten times a
    // second, and the ledger is what status() reports.
    runtime.note_detach_failures(&[crate::ownership::types::OwnershipFailure {
        database: database.clone(),
        error: "later failure".into(),
    }])?;
    let status = runtime.status()?;
    assert_eq!(status.failures.len(), 1);
    assert_eq!(status.failures[0].database, database);
    assert_eq!(status.failures[0].error, "first failure");
    // A successful detach clears the database from the ledger.
    runtime.note_detach_failures(&[])?;
    assert!(runtime.status()?.failures.is_empty());
    Ok(())
}
