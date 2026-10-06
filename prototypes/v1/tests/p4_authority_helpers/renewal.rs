use super::support::*;
use anyhow::Result;
use peri_loom_v1::{
    authority_rpc::protocol::MaintenanceOperation, database::RemoteDatabase, ownership::types::*,
};
use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

#[test]
fn concurrent_same_lease_renewals_wait_without_poisoning_native_commits() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let mut binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        store,
        identifier("main").namespace,
        cache.path(),
        10,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    for sequence in 1..=8 {
        let intent = RenewIntent {
            binding: binding.clone(),
            sequence,
            ttl_ms: 10000,
        };
        let request = RenewRequest {
            request_id: format!("renew-{sequence}"),
            nonce: authority
                .challenge(LeaseIntent::Renew(intent.clone()))?
                .nonce,
            intent,
        };
        let gate = Arc::new(Gate::default());
        let paused = gate.clone();
        *raw.before_put.lock().unwrap() = Some(Hook {
            needle: "authority/root".into(),
            skip: 0,
            action: Box::new(move || paused.enter()),
        });
        let service = authority.clone();
        let renewal = thread::spawn(move || service.renew(request));
        gate.entered();
        assert!(!authority.quiescent());
        let engine = database.clone();
        let (started, entered) = mpsc::channel();
        let (sent, received) = mpsc::channel();
        let commit = thread::spawn(move || {
            started.send(()).unwrap();
            sent.send(engine.execute(&format!(
                "BEGIN; INSERT INTO items VALUES({sequence}); COMMIT"
            )))
            .unwrap();
        });
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(received.recv_timeout(Duration::from_millis(10)).is_err());
        gate.release();
        binding = renewal.join().unwrap()?.binding;
        received.recv_timeout(Duration::from_secs(10)).unwrap()?;
        commit.join().unwrap();
        assert_eq!(
            root(&raw)?.snapshot()?.root.ownership.unwrap().branches["main"].renewal_sequence,
            sequence
        );
    }
    database.execute("INSERT INTO items VALUES(99)")?;
    assert_eq!(database.query("SELECT COUNT(*) FROM items")?.len(), 1);
    Ok(())
}

#[test]
fn service_pin_cleanup_and_snapshot_do_not_poison_active_native_io() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        store,
        identifier("main").namespace,
        cache.path(),
        10,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    database.checkpoint()?;
    for cleanup in [false, true] {
        let pin = authority.open_session(&binding, AccessRole::ReadOnly)?;
        let gate = Arc::new(Gate::default());
        let paused = gate.clone();
        *raw.before_put.lock().unwrap() = Some(Hook {
            needle: "authority/root".into(),
            skip: 0,
            action: Box::new(move || paused.enter()),
        });
        let service = authority.clone();
        let transition = thread::spawn(move || -> Result<()> {
            if cleanup {
                drop(pin);
            } else {
                service.maintenance(
                    &identifier("main"),
                    "snapshot",
                    MaintenanceOperation::Snapshot {
                        name: "safe".into(),
                    },
                )?;
                drop(pin);
            }
            Ok(())
        });
        gate.entered();
        let engine = database.clone();
        let (sent, received) = mpsc::channel();
        let commit = thread::spawn(move || {
            sent.send(engine.execute(if cleanup {
                "INSERT INTO items VALUES(2)"
            } else {
                "INSERT INTO items VALUES(1)"
            }))
            .unwrap()
        });
        assert!(received.recv_timeout(Duration::from_millis(10)).is_err());
        gate.release();
        transition.join().unwrap()?;
        received.recv_timeout(Duration::from_secs(10)).unwrap()?;
        commit.join().unwrap();
    }
    database.execute("INSERT INTO items VALUES(3)")?;
    assert_eq!(
        database.query("SELECT COUNT(*) FROM items")?[0][0].to_string(),
        "3"
    );
    Ok(())
}
