use super::*;
use crate::namespace::Namespace;

fn database(name: &str) -> DatabaseId {
    DatabaseId::new(Namespace::new("test", name).unwrap(), "main").unwrap()
}

fn stub(db: &str, owner: Option<&str>, request: &str, phase: StubPhase) -> Arc<Entry> {
    Entry::stub(database(db), owner, request, phase)
}

#[test]
fn close_wait_timeout_joins_same_job_and_caches_terminal() -> Result<()> {
    let entry = stub("retry", None, "open-1", StubPhase::ClosingWithoutResult);
    assert!(entry.close("session", Instant::now(), 2).is_err());
    let since = entry.state.lock().unwrap().closing_since;
    let receipt = SessionState {
        session: "session".into(),
        binding: entry.binding.clone(),
        owner: false,
        sequence: 1,
    };
    entry.state.lock().unwrap().close_result = Some(Ok(receipt));
    entry.changed.notify_all();
    for _ in 0..3 {
        let terminal = entry
            .close("session", Instant::now() + Duration::from_secs(1), 2)?
            .unwrap();
        assert_eq!(terminal.sequence, 1);
        assert_eq!(entry.state.lock().unwrap().closing_since, since);
    }
    Ok(())
}

#[test]
fn unproven_cleanup_never_restarts_or_claims_release() -> Result<()> {
    let entry = stub(
        "quarantined",
        None,
        "open-2",
        StubPhase::Failed("unsettled outcome".into()),
    );
    for _ in 0..3 {
        let error = entry
            .close("session", Instant::now() + Duration::from_secs(1), 2)
            .unwrap_err();
        assert!(error.to_string().contains("quarantined"));
        let (_, closing, closed, failed, _, _) = entry.lifecycle(Duration::ZERO, Duration::ZERO);
        assert!(closing && failed && !closed);
    }
    Ok(())
}

#[test]
fn real_cleanup_failure_keeps_the_handle_and_the_quarantine_verdict() -> Result<()> {
    // No injected result: the cleanup thread runs `finish_close`, which fails
    // because there is no handle left to close. That is the production shape for
    // a session whose native identity was lost, and the entry must keep reporting
    // unproven instead of releasing its slot.
    let entry = stub(
        "lost",
        Some("gateway-1"),
        "open-3",
        StubPhase::OpenWithoutHandle,
    );
    let error = entry
        .close("session", Instant::now() + Duration::from_secs(5), 2)
        .unwrap_err();
    assert!(error.to_string().contains("quarantined"), "{error:#}");
    let (_, closing, closed, failed, _, _) = entry.lifecycle(Duration::ZERO, Duration::ZERO);
    assert!(closing && failed && !closed);
    assert!(entry.state.lock().unwrap().handle.is_none());
    Ok(())
}

#[test]
fn active_call_is_not_idle_but_absolute_expiry_remains_enforced() -> Result<()> {
    let entry = stub("expiry", None, "open-4", StubPhase::Closed);
    entry.state.lock().unwrap().active = true;
    assert!(!entry.lifecycle(Duration::ZERO, Duration::from_secs(60)).0);
    assert!(entry.lifecycle(Duration::ZERO, Duration::ZERO).0);
    Ok(())
}
