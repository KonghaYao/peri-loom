use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use turso_core::LimboError;

use super::peer_tests::Fixture;
use super::CleanupFailure;
use crate::session::UnsettledSqlOutcome;

#[test]
fn last_known_stopped_peer_reclaims_after_original_cleanup_safety_deadline() -> Result<()> {
    let fixture = Fixture::open()?;
    let reader = Arc::new(fixture.database.session()?);
    let writer = Arc::new(fixture.database.session()?);
    let writer_hint = writer.lane.hint();
    let engine = Arc::downgrade(fixture.database.engine.lock().unwrap().as_ref().unwrap());
    let (reached, ready) = mpsc::channel();
    let (resume, release) = mpsc::channel();
    fixture
        .database
        .reclamation
        .hooks
        .before_abort
        .install(move || {
            reached.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(35)).unwrap();
        });
    thread::scope(|scope| -> Result<()> {
        let reader_session = reader.clone();
        let reader_call =
            scope.spawn(move || reader_session.query("SELECT abs(-9223372036854775808)"));
        ready.recv_timeout(Duration::from_secs(1))?;
        fixture.store.armed.store(true, Ordering::Release);
        let writer_session = writer.clone();
        let started = Instant::now();
        let writer_call = scope
            .spawn(move || writer_session.execute("INSERT INTO ledger VALUES(53) RETURNING value"));
        let writer_error = writer_call.join().unwrap().unwrap_err();
        assert!(started.elapsed() >= Duration::from_secs(20));
        assert!(writer_error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert!(writer_error.downcast_ref::<LimboError>().is_some());
        assert_eq!(
            writer_error.downcast_ref::<CleanupFailure>(),
            Some(&CleanupFailure::Deadline)
        );
        assert_eq!(fixture.database.pending.lock().unwrap().len(), 1);
        assert!(!fixture.database.reclamation.state.lock().unwrap().collected);
        assert!(engine.upgrade().is_some());
        let mutations = fixture.store.mutations.load(Ordering::SeqCst);
        let reads = fixture.store.reads.load(Ordering::SeqCst);
        resume.send(())?;
        let reader_error = reader_call.join().unwrap().unwrap_err();
        assert!(matches!(
            reader_error.downcast_ref::<LimboError>(),
            Some(LimboError::IntegerOverflow)
        ));
        assert!(reader_error.downcast_ref::<UnsettledSqlOutcome>().is_none());
        assert!(fixture.database.pending.lock().unwrap().is_empty());
        assert!(fixture.database.reclamation.state.lock().unwrap().collected);
        assert!(engine.upgrade().is_none());
        assert_eq!(fixture.store.mutations.load(Ordering::SeqCst), mutations);
        assert_eq!(fixture.store.reads.load(Ordering::SeqCst), reads);
        assert_eq!(writer.lane.hint(), writer_hint);
        assert!(writer
            .close()
            .unwrap_err()
            .downcast_ref::<UnsettledSqlOutcome>()
            .is_some());
        Ok(())
    })
}
