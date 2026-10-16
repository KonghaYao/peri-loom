use super::*;
use crate::{object_store::MemoryObjectStore, ownership::types::LeaseClock};
use std::time::{Duration, Instant};

struct Clock;
impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1000)
    }
}

#[test]
fn namespace_init_unclaimed_cell_is_not_quiescent() {
    // `service()` publishes the cell before `acquire` claims it, so this state is
    // reachable while an initialization is about to dispatch its first write. The
    // strict answer is pinned here because it is the fail-closed one.
    let cell = NamespaceCell::default();
    assert!(!cell.quiescent());
}

#[test]
fn namespace_init_failure_disposition_matrix() {
    let read = anyhow!("injected transport").context(ReadFailure);
    let invalid = anyhow!("invalid data");
    for phase in [
        InitPhase::PreflightFencing,
        InitPhase::RootRead,
        InitPhase::RootCreateCas,
        InitPhase::DeepVerify,
        InitPhase::BootRead,
        InitPhase::BootPublish,
        InitPhase::ClockAnchor,
        InitPhase::ServiceBuild,
    ] {
        for disposition in [
            Disposition::None,
            Disposition::Rejected,
            Disposition::Committed,
            Disposition::Unknown,
        ] {
            let expected = match disposition {
                Disposition::None | Disposition::Rejected => FailureKind::Retryable,
                Disposition::Committed => FailureKind::FailedDefinite,
                Disposition::Unknown => FailureKind::BlockedUnknown,
            };
            assert_eq!(classify(disposition, &read), expected, "{phase:?}");
            assert_eq!(
                classify(disposition, &invalid),
                if disposition == Disposition::Unknown {
                    FailureKind::BlockedUnknown
                } else {
                    FailureKind::FailedDefinite
                },
                "{phase:?}"
            );
        }
    }
}

#[test]
fn namespace_init_waiters_receive_original_failure_not_a_new_generation() -> Result<()> {
    let namespace = Namespace::new("init", "waiters")?;
    let store = Arc::new(MemoryObjectStore::default());
    let owner = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
    let attempt = Arc::new(Attempt::default());
    let cell = Arc::new(NamespaceCell {
        state: Mutex::new(CellState::Initializing(attempt.clone())),
    });
    let mut threads = Vec::new();
    for _ in 0..8 {
        let cell = cell.clone();
        let owner = owner.clone();
        let namespace = namespace.clone();
        threads.push(std::thread::spawn(move || {
            cell.acquire(&owner, &namespace).err().unwrap().to_string()
        }));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while Arc::strong_count(&attempt) < 10 {
        anyhow::ensure!(Instant::now() < deadline, "waiters did not join attempt");
        std::thread::yield_now();
    }
    assert!(!cell.quiescent());
    let failure = Arc::new(Failure {
        phase: InitPhase::RootRead,
        disposition: Disposition::None,
        kind: FailureKind::Retryable,
        error: anyhow!("original read failure").context(ReadFailure),
        _backend: NamespaceBackend::scoped(store, namespace.clone()),
    });
    *cell.state.lock().unwrap() = CellState::Retryable(failure.clone());
    *attempt.outcome.lock().unwrap() = Some(Err(failure));
    // A later caller may finish a successful generation before old waiters wake.
    cell.acquire(&owner, &namespace)?;
    attempt.changed.notify_all();
    for thread in threads {
        assert!(thread.join().unwrap().contains("original read failure"));
    }
    assert!(cell.quiescent());
    Ok(())
}
