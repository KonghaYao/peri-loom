use super::{
    grant, grant_request, identifier,
    support::{Clock, Gate, ProbeStore},
};
use anyhow::Result;
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    manifest::SEGMENT_SIZE,
    object_store::{ObjectStore, PutCondition},
    ownership::types::{AccessRole, RevokeRequest},
};
use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

#[test]
fn takeover_waits_for_valid_dispatched_put_then_expired_old_publication_fails() -> Result<()> {
    let clock = Clock::new(1000);
    let store = ProbeStore::new(clock.clone());
    let authority = OwnershipAuthority::new(store.clone(), clock.clone());
    let id = identifier("pause");
    let lease = grant(&authority, id.clone(), 10, 100)?;
    let scoped = authority.open_session(&lease.binding, AccessRole::Writer)?;
    let takeover = grant_request(&authority, id.clone(), 11, 10, 1000, "takeover-11")?;
    let bytes = vec![9; SEGMENT_SIZE];
    let key = id
        .namespace
        .key(&format!("segments/{}", blake3::hash(&bytes).to_hex()))?;
    let gate = Arc::new(Gate::default());
    store.pause(
        format!("{}authority/blobs/", id.namespace.prefix()),
        gate.clone(),
    );
    let old = {
        let scoped = scoped.clone();
        let key = key.clone();
        thread::spawn(move || scoped.put(&key, &bytes, PutCondition::Absent))
    };
    gate.entered()?;
    clock.set(1100);
    let (sender, receiver) = mpsc::channel();
    let transition = {
        let authority = authority.clone();
        thread::spawn(move || {
            sender.send(authority.grant(takeover)).unwrap();
        })
    };
    let premature = receiver.recv_timeout(Duration::from_millis(80));
    let neighbor = grant(&authority, identifier("other-namespace"), 10, 1000);
    gate.release();
    assert!(
        matches!(premature, Err(mpsc::RecvTimeoutError::Timeout)),
        "takeover completed while physical PUT remained unresolved"
    );
    assert!(neighbor.is_ok(), "unrelated namespace blocked");
    assert!(
        old.join().unwrap().is_err(),
        "expired old request published its logical segment"
    );
    let next = receiver.recv_timeout(Duration::from_secs(2))??;
    transition.join().unwrap();
    assert_eq!(next.binding.epoch, 11);
    let dispatches = store.put_count();
    assert!(scoped
        .put(&key, &vec![9; SEGMENT_SIZE], PutCondition::Absent)
        .is_err());
    assert!(scoped.validate_session().is_err());
    assert_eq!(store.put_count(), dispatches);
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let root = base.snapshot()?.root;
    assert!(!root
        .objects
        .contains_key(key.strip_prefix(&id.namespace.prefix()).unwrap()));
    assert!(root.uploads.values().any(|entry| store
        .get(
            &id.namespace
                .key(&format!("authority/blobs/{}", entry.blob))
                .unwrap()
        )
        .unwrap()
        .is_some()));
    assert_eq!(root.ownership.unwrap().branches["main"].epoch, 11);
    Ok(())
}

#[test]
fn socket_close_and_absent_readback_never_complete_a_takeover_barrier() -> Result<()> {
    let clock = Clock::new(1000);
    let store = ProbeStore::new(clock.clone());
    let authority = OwnershipAuthority::new(store.clone(), clock);
    let id = identifier("unknown");
    let lease = grant(&authority, id.clone(), 10, 1000)?;
    let scoped = authority.open_session(&lease.binding, AccessRole::Writer)?;
    let takeover = grant_request(&authority, id.clone(), 11, 10, 1000, "must-not-complete")?;
    let bytes = vec![3; SEGMENT_SIZE];
    let key = id
        .namespace
        .key(&format!("segments/{}", blake3::hash(&bytes).to_hex()))?;
    let gate = Arc::new(Gate::default());
    let terminal = store.unknown(
        format!("{}authority/blobs/", id.namespace.prefix()),
        gate.clone(),
    );
    let failed = scoped.put(&key, &bytes, PutCondition::Absent);
    gate.entered()?;
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let before = base.snapshot()?.root;
    let outstanding = before
        .uploads
        .values()
        .find(|entry| entry.hash == blake3::hash(&bytes).to_hex().as_str())
        .unwrap();
    let physical = id
        .namespace
        .key(&format!("authority/blobs/{}", outstanding.blob))?;
    assert!(store.get(&physical)?.is_none());
    let dispatches = store.put_count();
    let takeover_result = authority.grant(takeover);
    let revoke_result = authority.revoke(RevokeRequest {
        request_id: "unknown-revoke".into(),
        database: id.clone(),
        expected_epoch: 10,
    });
    gate.release();
    terminal.recv_timeout(Duration::from_secs(2))??;
    assert!(failed.is_err());
    assert!(takeover_result.is_err());
    assert!(revoke_result.is_err());
    assert_eq!(store.put_count(), dispatches);
    assert!(
        store.get(&physical)?.is_some(),
        "fixture did not actually deliver the delayed physical PUT"
    );
    let after = base.snapshot()?.root;
    assert_eq!(after, before);
    assert_eq!(after.ownership.unwrap().branches["main"].epoch, 10);
    assert!(
        authority.state(&id).is_err(),
        "terminal completion silently reset uncertain namespace"
    );
    Ok(())
}

#[test]
fn scoped_store_rejects_raw_roots_other_namespaces_and_native_readonly_is_real() -> Result<()> {
    let clock = Clock::new(1000);
    let store = ProbeStore::new(clock.clone());
    let authority = OwnershipAuthority::new(store.clone(), clock);
    let id = identifier("scope");
    let lease = grant(&authority, id.clone(), 10, 1000)?;
    let scoped = authority.open_session(&lease.binding, AccessRole::Writer)?;
    let dispatches = store.put_count();
    for key in [
        id.namespace.key("authority/root")?,
        "another/database/HEAD".into(),
        id.namespace.key("HEAD")?,
    ] {
        assert!(scoped.put(&key, b"forged", PutCondition::Any).is_err());
        assert!(scoped.delete(&key, None).is_err());
    }
    assert_eq!(store.put_count(), dispatches);
    let writer_cache = tempfile::tempdir()?;
    let writer = peri_loom_v1::database::RemoteDatabase::open(
        scoped,
        id.namespace.clone(),
        writer_cache.path(),
        10,
    )?;
    writer.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(7)")?;
    writer.compact_wal()?;
    drop(writer);
    let readonly = authority.open_session(&lease.binding, AccessRole::ReadOnly)?;
    assert_eq!(
        readonly.access_role(),
        AccessRole::ReadOnly,
        "scoped ReadOnly store advertised Writer to native open"
    );
    let readonly_cache = tempfile::tempdir()?;
    let native = Arc::new(peri_loom_v1::database::RemoteDatabase::open(
        readonly,
        id.namespace,
        readonly_cache.path(),
        10,
    )?);
    assert!(native.is_readonly()?);
    let session = native.session()?;
    assert!(session.is_readonly()?);
    assert_eq!(
        session.query("SELECT id FROM items")?[0][0].to_string(),
        "7"
    );
    let dispatches = store.put_count();
    for sql in [
        "/* disguised write */ INSERT INTO items VALUES(8) RETURNING id",
        "DELETE FROM items RETURNING id",
        "UPDATE items SET id=9 RETURNING id",
        "PRAGMA user_version=19",
        "ATTACH ':memory:' AS escape",
    ] {
        assert!(
            session.query(sql).is_err(),
            "native readonly accepted query write: {sql}"
        );
    }
    assert!(session
        .execute("PRAGMA query_only=OFF; INSERT INTO items VALUES(10)")
        .is_err());
    assert_eq!(
        session.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    assert_eq!(store.put_count(), dispatches);
    Ok(())
}
