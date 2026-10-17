use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};

use super::{AdmissionError, UnsettledSqlOutcome, WriterAdmission};

/// Waits carry a deadline. These waiters live in a `thread::scope`, whose unwind joins them, so
/// a waiter that nothing ever releases would turn a failing assertion into a hang.
fn bounded_check(deadline: Instant) -> impl Fn() -> Result<()> {
    move || {
        ensure!(Instant::now() < deadline, AdmissionError::Deadline);
        Ok(())
    }
}

fn wait_queue(admission: &WriterAdmission, expected: &[u64]) {
    let started = Instant::now();
    loop {
        if admission.status().unwrap().queued == expected {
            return;
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn native_trace_observes_admission_release_and_cancel_without_changing_fifo() -> Result<()> {
    let trace = crate::capacity_trace::test_trace();
    let admission = Arc::new(WriterAdmission {
        trace: Some(trace.clone()),
        ..WriterAdmission::default()
    });
    let holder = admission.register()?;
    let waiter = admission.register()?;
    admission.acquire(&holder, false, || Ok(()))?.settle(true)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    thread::scope(|scope| -> Result<()> {
        let admission_clone = admission.clone();
        let waiter_clone = waiter.clone();
        let waiting = scope.spawn(move || {
            admission_clone
                .acquire(&waiter_clone, false, bounded_check(deadline))
                .err()
                .unwrap()
        });
        wait_queue(&admission, &[waiter.id]);
        admission.close_requested(&waiter);
        assert!(waiting.join().unwrap().is::<AdmissionError>());
        Ok(())
    })?;
    admission
        .acquire(&holder, false, || Ok(()))?
        .settle(false)?;
    let snapshot = trace.test_events();
    let events = snapshot["events"].as_array().unwrap();
    let kinds = events
        .iter()
        .map(|event| event["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            "enqueue",
            "admit",
            "enqueue",
            "cancel",
            "owner_reuse",
            "release"
        ]
    );
    assert_eq!(events[2]["holderControlId"], events[0]["controlId"]);
    assert!(admission.status()?.owner.is_none());
    assert!(admission.status()?.queued.is_empty());
    Ok(())
}

#[test]
fn coordinator_model_fifo_cancel_head_and_atomic_hint() -> Result<()> {
    let admission = Arc::new(WriterAdmission::default());
    let holder = admission.register()?;
    let first = admission.register()?;
    let second = admission.register()?;
    admission.acquire(&holder, false, || Ok(()))?.settle(true)?;
    assert!(holder.hint().owner);
    assert_eq!(holder.hint().sequence, 1);
    let (sender, receiver) = mpsc::channel();
    let deadline = Instant::now() + Duration::from_secs(10);
    thread::scope(|scope| -> Result<()> {
        let cloned = admission.clone();
        let first_control = first.clone();
        let first_sender = sender.clone();
        scope.spawn(move || {
            let result = cloned.acquire(&first_control, false, bounded_check(deadline));
            first_sender
                .send(result.map(|permit| {
                    permit.settle(false).unwrap();
                    first_control.id
                }))
                .unwrap();
        });
        wait_queue(&admission, &[first.id]);
        let cloned = admission.clone();
        let second_control = second.clone();
        scope.spawn(move || {
            let result = cloned.acquire(&second_control, false, bounded_check(deadline));
            sender
                .send(result.map(|permit| {
                    permit.settle(false).unwrap();
                    second_control.id
                }))
                .unwrap();
        });
        wait_queue(&admission, &[first.id, second.id]);
        admission.close_requested(&first);
        let error = receiver.recv_timeout(Duration::from_secs(1))?.unwrap_err();
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Closing)
        );
        wait_queue(&admission, &[second.id]);
        admission
            .acquire(&holder, false, || Ok(()))?
            .settle(false)?;
        assert!(!holder.hint().owner);
        assert_eq!(holder.hint().sequence, 2);
        assert_eq!(receiver.recv_timeout(Duration::from_secs(1))??, second.id);
        Ok(())
    })?;
    assert!(admission.status()?.queued.is_empty());
    Ok(())
}

#[test]
fn dropping_unsettled_permit_quarantines_without_transferring_owner() -> Result<()> {
    let admission = Arc::new(WriterAdmission::default());
    let holder = admission.register()?;
    let permit = admission.acquire(&holder, false, || Ok(()))?;
    assert_eq!(holder.hint().sequence, 0);
    drop(permit);
    let status = admission.status()?;
    assert!(status.quarantined);
    assert_eq!(status.owner, Some(holder.id));
    assert_eq!(
        admission
            .healthy()
            .unwrap_err()
            .downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Quarantine)
    );
    Ok(())
}

#[test]
fn coordinator_model_queue_cap_and_promotion_are_independent_typed_errors() -> Result<()> {
    let admission = Arc::new(WriterAdmission::default());
    let holder = admission.register()?;
    admission.acquire(&holder, false, || Ok(()))?.settle(true)?;
    let waiters = (0..8)
        .map(|_| admission.register())
        .collect::<Result<Vec<_>>>()?;
    let overflow = admission.register()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    thread::scope(|scope| -> Result<()> {
        let mut expected = Vec::new();
        let mut threads = Vec::new();
        for control in &waiters {
            let cloned = admission.clone();
            let waiter = control.clone();
            threads.push(scope.spawn(move || {
                cloned
                    .acquire(&waiter, false, bounded_check(deadline))
                    .err()
                    .unwrap()
            }));
            expected.push(control.id);
            wait_queue(&admission, &expected);
        }
        let error = admission
            .acquire(&overflow, false, || Ok(()))
            .err()
            .unwrap();
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::QueueFull)
        );
        let error = admission.acquire(&overflow, true, || Ok(())).err().unwrap();
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Conflict)
        );
        for control in &waiters {
            admission.close_requested(control);
        }
        for waiter in threads {
            assert_eq!(
                waiter.join().unwrap().downcast_ref::<AdmissionError>(),
                Some(&AdmissionError::Closing)
            );
        }
        Ok(())
    })?;
    assert!(admission.status()?.queued.is_empty());
    assert_eq!(holder.hint().sequence, 1);
    assert!(holder.hint().owner);
    admission
        .acquire(&holder, false, || Ok(()))?
        .settle(false)?;
    Ok(())
}

#[test]
fn unsettled_context_preserves_original_error_and_is_not_database_quarantine() {
    let error = anyhow::Error::new(AdmissionError::Deadline)
        .context(UnsettledSqlOutcome)
        .context("outer context");
    assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline)
    );
    let rejected = anyhow::Error::new(AdmissionError::Quarantine);
    assert!(rejected.downcast_ref::<UnsettledSqlOutcome>().is_none());
    assert_eq!(
        rejected.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Quarantine)
    );
}
