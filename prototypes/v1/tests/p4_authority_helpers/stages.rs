use super::support::*;
use anyhow::Result;
use peri_loom_v1::{
    manifest::Manifest,
    object_store::{ObjectStore, PutCondition},
    ownership::types::*,
};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
};

#[test]
fn takeover_fences_old_wal_head_upload_and_delete_before_raw_dispatch() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let old = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&old, AccessRole::Writer)?;
    initialize(store.clone())?;
    let head = identifier("main").namespace.key("HEAD")?;
    let etag = store.get(&head)?.unwrap().etag;
    grant(&authority, 11, 10)?;
    let count = raw.mutations.lock().unwrap().len();
    assert!(store.validate_session().is_err());
    assert!(store
        .put(&segment(b"stale"), b"stale", PutCondition::Absent)
        .is_err());
    assert!(store.put(&head, b"{}", PutCondition::Match(etag)).is_err());
    assert!(store
        .put(
            &identifier("main").namespace.key("wal/index")?,
            b"{}",
            PutCondition::Absent
        )
        .is_err());
    assert!(store.delete(&head, None).is_err());
    assert!(store
        .put(
            &identifier("main").namespace.key("authority/root")?,
            b"forged",
            PutCondition::Any
        )
        .is_err());
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    assert_eq!(authority.state(&identifier("main"))?.epoch, 11);
    Ok(())
}

#[test]
fn physical_put_ticket_blocks_takeover_until_terminal_response() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    let request = grant_request(&authority, "main", 11, 10, "takeover", 10000)?;
    let gate = Arc::new(Gate::default());
    let paused = gate.clone();
    *raw.before_put.lock().unwrap() = Some(Hook {
        needle: "authority/blobs/".into(),
        skip: 0,
        action: Box::new(move || paused.enter()),
    });
    let old_store = store.clone();
    let upload = thread::spawn(move || {
        old_store.put(&segment(b"in flight"), b"in flight", PutCondition::Absent)
    });
    gate.entered();
    assert!(!authority.quiescent());
    let (sent, received) = mpsc::channel();
    let other = authority.clone();
    let competing = request.clone();
    let takeover = thread::spawn(move || sent.send(other.grant(request)).unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while store.validate_session().is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "transition did not close admission"
        );
        thread::yield_now();
    }
    let count = raw.mutations.lock().unwrap().len();
    assert!(!authority.quiescent());
    for _attempt in 0..100 {
        assert!(store
            .put(&segment(b"queued"), b"queued", PutCondition::Absent)
            .is_err());
        assert!(store
            .get(&identifier("main").namespace.key("HEAD")?)
            .is_err());
        assert!(store.list(&identifier("main").namespace.prefix()).is_err());
        assert!(authority
            .open_session(&binding, AccessRole::Writer)
            .is_err());
        assert!(authority
            .validate_lease(&binding, AccessRole::Writer)
            .is_err());
        assert!(authority.grant(competing.clone()).is_err());
    }
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    assert!(received.recv_timeout(Duration::from_millis(50)).is_err());
    gate.release();
    upload.join().unwrap()?;
    received.recv_timeout(Duration::from_secs(10)).unwrap()?;
    takeover.join().unwrap();
    let count = raw.mutations.lock().unwrap().len();
    assert!(store
        .put(&segment(b"too late"), b"too late", PutCondition::Absent)
        .is_err());
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    Ok(())
}

#[test]
fn final_root_guard_rechecks_expiry_after_uncached_candidate_read() -> Result<()> {
    for after_upload in [false, true] {
        let (raw, clock, authority) = fixture()?;
        let binding = authority
            .grant(grant_request(&authority, "main", 10, 0, "grant", 1000)?)?
            .binding;
        let store = authority.open_session(&binding, AccessRole::Writer)?;
        let hash = initialize(store.clone())?;
        let snapshot = root(&raw)?.snapshot()?;
        let blob = snapshot.root.objects[&format!("manifests/{hash}")]
            .blob
            .clone();
        let root_key = identifier("main").namespace.key("authority/root")?;
        let expected = Arc::new(std::sync::Mutex::new(snapshot.clone()));
        let expected_bytes = Arc::new(std::sync::Mutex::new(
            raw.memory.get(&root_key)?.unwrap().bytes,
        ));
        let expired = Arc::new(AtomicBool::new(false));
        let observed = expired.clone();
        let expiry = Hook {
            needle: if after_upload {
                "authority/root".into()
            } else {
                blob
            },
            skip: 0,
            action: Box::new(move || {
                clock.set(1100);
                observed.store(true, Ordering::SeqCst);
            }),
        };
        if after_upload {
            let source = raw.clone();
            let captured = expected.clone();
            let captured_bytes = expected_bytes.clone();
            *raw.before_put.lock().unwrap() = Some(Hook {
                needle: "authority/blobs/".into(),
                skip: 0,
                action: Box::new(move || {
                    let reserved = root(&source).unwrap().snapshot().unwrap();
                    let key = reserved.root.namespace.key("authority/root").unwrap();
                    *captured.lock().unwrap() = reserved;
                    *captured_bytes.lock().unwrap() =
                        source.memory.get(&key).unwrap().unwrap().bytes;
                    *source.before_get.lock().unwrap() = Some(expiry);
                }),
            });
        } else {
            *raw.before_get.lock().unwrap() = Some(expiry);
        }
        let count = raw.mutations.lock().unwrap().len();
        let error = store
            .put(&segment(b"expiry"), b"expiry", PutCondition::Absent)
            .unwrap_err();
        assert!(expired.load(Ordering::SeqCst));
        assert!(format!("{error:#}").contains("LeaseExpired"));
        let mutations = raw.mutations.lock().unwrap().clone();
        let observed = root(&raw)?.snapshot()?;
        let expected = expected.lock().unwrap().clone();
        let expected_bytes = expected_bytes.lock().unwrap().clone();
        assert_eq!(observed.root, expected.root);
        assert_eq!(observed.etag, expected.etag);
        assert_eq!(raw.memory.get(&root_key)?.unwrap().bytes, expected_bytes);
        assert_eq!(mutations.len(), count + if after_upload { 2 } else { 0 });
        let staged = &mutations[count..];
        assert_eq!(
            staged
                .iter()
                .filter(|operation| operation.ends_with("/authority/root"))
                .count(),
            usize::from(after_upload)
        );
        assert_eq!(observed.root.objects, snapshot.root.objects);
        assert_eq!(observed.root.branches, snapshot.root.branches);
        if after_upload {
            assert_eq!(observed.root.revision, snapshot.root.revision + 1);
            assert_eq!(observed.root.uploads.len(), snapshot.root.uploads.len() + 1);
            let entry = observed
                .root
                .uploads
                .values()
                .find(|entry| entry.hash == blake3::hash(b"expiry").to_hex().as_str())
                .unwrap();
            let authority = root(&raw)?;
            assert_eq!(authority.read_blob(entry)?, b"expiry");
            assert!(authority.reachable(&observed.root)?.contains(&entry.blob));
        } else {
            assert_eq!(observed.root.revision, snapshot.root.revision);
        }
        eprintln!("expiry_stage after_upload={after_upload} staged_mutations={} final_root_publications=0 root_bytes_unchanged=true", staged.len());
    }
    Ok(())
}

#[test]
fn already_dispatched_put_may_finish_after_deadline_but_cannot_publish() -> Result<()> {
    let (raw, clock, authority) = fixture()?;
    let binding = authority
        .grant(grant_request(&authority, "main", 10, 0, "grant", 1000)?)?
        .binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    *raw.before_put.lock().unwrap() = Some(Hook {
        needle: "authority/blobs/".into(),
        skip: 0,
        action: Box::new(move || clock.set(1100)),
    });
    assert!(store
        .put(
            &segment(b"late complete"),
            b"late complete",
            PutCondition::Absent
        )
        .is_err());
    let snapshot = root(&raw)?.snapshot()?;
    assert!(snapshot.root.objects.is_empty());
    assert_eq!(snapshot.root.uploads.len(), 1);
    assert!(raw
        .memory
        .get(&identifier("main").namespace.key(&format!(
            "authority/blobs/{}",
            snapshot.root.uploads.keys().next().unwrap()
        ))?)?
        .is_some());
    assert!(authority.quiescent());
    Ok(())
}

#[test]
fn uncertain_physical_put_blocks_takeover_even_after_one_absence_readback() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    let request = grant_request(&authority, "main", 11, 10, "takeover", 10000)?;
    *raw.failure.lock().unwrap() = Some(("authority/blobs/".into(), false));
    assert!(store
        .put(
            &segment(b"delayed bytes"),
            b"delayed bytes",
            PutCondition::Absent
        )
        .is_err());
    assert!(!authority.quiescent());
    assert!(authority.grant(request).is_err());
    let (key, bytes) = raw.late.lock().unwrap().take().unwrap();
    raw.memory.put(&key, &bytes, PutCondition::Absent)?;
    assert!(authority
        .revoke(RevokeRequest {
            request_id: "revoke".into(),
            database: identifier("main"),
            expected_epoch: 10
        })
        .is_err());
    let snapshot = root(&raw)?.snapshot()?;
    assert_eq!(snapshot.root.ownership.unwrap().branches["main"].epoch, 10);
    assert_eq!(snapshot.root.uploads.len(), 1);
    Ok(())
}

#[test]
fn ambiguous_grant_ack_never_reports_new_live_lease() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let request = grant_request(&authority, "main", 10, 0, "grant", 10000)?;
    *raw.failure.lock().unwrap() = Some(("authority/root".into(), true));
    assert!(authority.grant(request.clone()).is_err());
    assert!(!authority.quiescent());
    assert!(authority.grant(request).is_err());
    let snapshot = root(&raw)?.snapshot()?;
    assert!(snapshot
        .root
        .ownership
        .unwrap()
        .commands
        .contains_key("grant"));
    Ok(())
}

#[test]
fn cas_retry_cannot_publish_after_ownership_changed() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    let base = root(&raw)?;
    *raw.before_put.lock().unwrap() = Some(Hook {
        needle: "authority/root".into(),
        skip: 0,
        action: Box::new(move || {
            base.transact(None, |root| {
                let record = root
                    .ownership
                    .as_mut()
                    .unwrap()
                    .branches
                    .get_mut("main")
                    .unwrap();
                record.epoch = 11;
                record.worker_id = "newworker".into();
                record.lease_id = "newlease".into();
                record.grant_id = "newgrant".into();
                Ok(())
            })
            .unwrap();
        }),
    });
    assert!(store
        .put(
            &segment(b"retry stale"),
            b"retry stale",
            PutCondition::Absent
        )
        .is_err());
    let snapshot = root(&raw)?.snapshot()?;
    assert!(snapshot.root.objects.is_empty());
    assert!(snapshot.root.uploads.is_empty());
    assert_eq!(snapshot.root.ownership.unwrap().branches["main"].epoch, 11);
    Ok(())
}

#[test]
fn readonly_session_has_native_role_and_only_authority_pin_cleanup() -> Result<()> {
    let (raw, clock, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let writer = authority.open_session(&binding, AccessRole::Writer)?;
    initialize(writer.clone())?;
    let reader = authority.open_session(&binding, AccessRole::ReadOnly)?;
    assert_eq!(reader.access_role(), AccessRole::ReadOnly);
    assert!(reader
        .get(&identifier("main").namespace.key("HEAD")?)?
        .is_some());
    let count = raw.mutations.lock().unwrap().len();
    assert!(reader
        .put(&segment(b"readonly"), b"readonly", PutCondition::Absent)
        .is_err());
    assert!(reader
        .delete(&identifier("main").namespace.key("HEAD")?, None)
        .is_err());
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    let pins = root(&raw)?.snapshot()?.root.pins.len();
    clock.set(10100);
    drop(reader);
    assert_eq!(root(&raw)?.snapshot()?.root.pins.len(), pins - 1);
    drop(writer);
    assert!(root(&raw)?.snapshot()?.root.pins.is_empty());
    Ok(())
}

#[test]
fn wal_epoch_and_head_generation_checked_before_upload() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    let hash = initialize(store.clone())?;
    let count = raw.mutations.lock().unwrap().len();
    let index = serde_json::to_vec(
        &json!({"epoch":11,"last_lsn":0,"replay_baseline_lsn":0,"entries":[],"receipts":{},"commit":null}),
    )?;
    assert!(store
        .put(
            &identifier("main").namespace.key("wal/index")?,
            &index,
            PutCondition::Absent
        )
        .is_err());
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    let manifest = Manifest {
        generation: 7,
        parent: Some(hash),
        ..Manifest::default()
    };
    let bytes = serde_json::to_vec(&manifest)?;
    let key = identifier("main")
        .namespace
        .key(&format!("manifests/{}", blake3::hash(&bytes).to_hex()))?;
    store.put(&key, &bytes, PutCondition::Absent)?;
    let head = identifier("main").namespace.key("HEAD")?;
    let etag = store.get(&head)?.unwrap().etag;
    let count = raw.mutations.lock().unwrap().len();
    assert!(store
        .put(
            &head,
            &serde_json::to_vec(&json!({"manifest":blake3::hash(&bytes).to_hex().to_string()}))?,
            PutCondition::Match(etag)
        )
        .is_err());
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    Ok(())
}

#[test]
fn forged_worker_namespace_never_initializes_a_catalog() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let mut binding = grant(&authority, 10, 0)?.binding;
    binding.database.namespace = peri_loom_v1::namespace::Namespace::new("other", "private")?;
    let count = raw.mutations.lock().unwrap().len();
    assert!(authority
        .open_session(&binding, AccessRole::Writer)
        .is_err());
    assert!(authority
        .validate_lease(&binding, AccessRole::ReadOnly)
        .is_err());
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    assert!(raw.memory.list("other/private/")?.is_empty());
    Ok(())
}

#[test]
fn readonly_grant_cannot_escalate_or_forge_bound_role() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let mut intent = GrantIntent {
        database: identifier("main"),
        worker_id: "reader".into(),
        epoch: 10,
        expected_epoch: 0,
        lease_id: "reader-lease".into(),
        ttl_ms: 10000,
        role: AccessRole::ReadOnly,
    };
    let request = GrantRequest {
        request_id: "reader-grant".into(),
        nonce: authority
            .challenge(LeaseIntent::Grant(intent.clone()))?
            .nonce,
        intent: intent.clone(),
    };
    let binding = authority.grant(request)?.binding;
    assert_eq!(
        authority.state(&identifier("main"))?.mode,
        OwnershipMode::ReadOnly
    );
    let count = raw.mutations.lock().unwrap().len();
    assert!(authority
        .open_session(&binding, AccessRole::Writer)
        .is_err());
    assert!(authority
        .validate_lease(&binding, AccessRole::Writer)
        .is_err());
    let mut forged = binding.clone();
    forged.role = AccessRole::Writer;
    assert!(authority.open_session(&forged, AccessRole::Writer).is_err());
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    let reader = authority.open_session(&binding, AccessRole::ReadOnly)?;
    assert!(reader
        .put(&segment(b"escalated"), b"escalated", PutCondition::Absent)
        .is_err());
    intent.epoch = 11;
    intent.expected_epoch = 10;
    intent.role = AccessRole::Writer;
    intent.worker_id = "writer".into();
    let writer = authority
        .grant(GrantRequest {
            request_id: "writer-grant".into(),
            nonce: authority
                .challenge(LeaseIntent::Grant(intent.clone()))?
                .nonce,
            intent,
        })?
        .binding;
    authority.open_session(&writer, AccessRole::ReadOnly)?;
    Ok(())
}
