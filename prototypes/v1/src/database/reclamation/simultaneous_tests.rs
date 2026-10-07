use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use turso_core::LimboError;

use super::peer_tests::Fixture;
use crate::session::{Budget, UnsettledSqlOutcome};

#[test]
fn two_native_failed_peers_register_before_recollection_and_release_heavy_refs() -> Result<()> {
    let fixture = Fixture::open()?;
    let reader = Arc::new(fixture.database.session()?);
    let writer = Arc::new(fixture.database.session()?);
    let reader_hint = reader.lane.hint();
    let writer_hint = writer.lane.hint();
    let engine = Arc::downgrade(fixture.database.engine.lock().unwrap().as_ref().unwrap());
    let reader_connection =
        Arc::downgrade(reader.lane.connection.lock().unwrap().as_ref().unwrap());
    let writer_connection =
        Arc::downgrade(writer.lane.connection.lock().unwrap().as_ref().unwrap());
    let (reader_reached, reader_ready) = mpsc::channel();
    let (reader_resume, reader_release) = mpsc::channel();
    fixture
        .database
        .reclamation
        .hooks
        .before_abort
        .install(move || {
            reader_reached.send(()).unwrap();
            reader_release.recv_timeout(Duration::from_secs(2)).unwrap();
        });
    let (writer_reached, writer_ready) = mpsc::channel();
    let (writer_resume, writer_release) = mpsc::channel();
    fixture
        .database
        .reclamation
        .hooks
        .before_enqueue
        .install(move || {
            writer_reached.send(()).unwrap();
            writer_release.recv_timeout(Duration::from_secs(2)).unwrap();
        });
    let (leader_reached, leader_ready) = mpsc::channel();
    let (leader_resume, leader_release) = mpsc::channel();
    fixture
        .database
        .reclamation
        .hooks
        .conflict
        .install(move || {
            leader_reached.send(()).unwrap();
            leader_release.recv_timeout(Duration::from_secs(2)).unwrap();
        });
    thread::scope(|scope| -> Result<()> {
        let reader_session = reader.clone();
        let reader_call = scope.spawn(move || {
            reader_session.query_with_budget(
                "SELECT abs(-9223372036854775808)",
                &Budget {
                    max_query_duration: Duration::from_millis(100),
                    ..Budget::default()
                },
            )
        });
        reader_ready.recv_timeout(Duration::from_secs(1))?;
        fixture.store.armed.store(true, Ordering::Release);
        let writer_session = writer.clone();
        let writer_call = scope
            .spawn(move || writer_session.execute("INSERT INTO ledger VALUES(47) RETURNING value"));
        writer_ready.recv_timeout(Duration::from_secs(1))?;
        let cleanup = fixture.database.clone();
        let leader = scope
            .spawn(move || cleanup.cleanup_deferred_until(Instant::now() + Duration::from_secs(2)));
        leader_ready.recv_timeout(Duration::from_secs(1))?;
        thread::sleep(Duration::from_millis(120));
        let (first_registered, first_ready) = mpsc::channel();
        fixture
            .database
            .reclamation
            .hooks
            .after_enqueue
            .install(move || {
                first_registered.send(()).unwrap();
            });
        reader_resume.send(())?;
        first_ready.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(fixture.database.pending.lock().unwrap().len(), 1);
        assert!(engine.upgrade().is_some());
        let (second_registered, second_ready) = mpsc::channel();
        fixture
            .database
            .reclamation
            .hooks
            .after_enqueue
            .install(move || {
                second_registered.send(()).unwrap();
            });
        writer_resume.send(())?;
        second_ready.recv_timeout(Duration::from_secs(1))?;
        assert_eq!(fixture.database.pending.lock().unwrap().len(), 2);
        assert_eq!(fixture.database.lanes.lock().unwrap().len(), 3);
        assert!(fixture.database.session().is_err());
        let mutations = fixture.store.mutations.load(Ordering::SeqCst);
        leader_resume.send(())?;
        leader.join().unwrap()?;
        let reader_error = reader_call.join().unwrap().unwrap_err();
        assert!(matches!(
            reader_error.downcast_ref::<LimboError>(),
            Some(LimboError::IntegerOverflow)
        ));
        assert!(reader_error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        let writer_error = writer_call.join().unwrap().unwrap_err();
        assert!(writer_error.downcast_ref::<LimboError>().is_some());
        assert!(writer_error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert!(fixture.database.pending.lock().unwrap().is_empty());
        assert!(engine.upgrade().is_none());
        assert!(reader_connection.upgrade().is_none());
        assert!(writer_connection.upgrade().is_none());
        assert_eq!(fixture.store.mutations.load(Ordering::SeqCst), mutations);
        Ok(())
    })?;
    assert_eq!(reader.lane.hint(), reader_hint);
    assert_eq!(writer.lane.hint(), writer_hint);
    assert!(reader
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    assert!(writer
        .close()
        .unwrap_err()
        .downcast_ref::<UnsettledSqlOutcome>()
        .is_some());
    Ok(())
}
