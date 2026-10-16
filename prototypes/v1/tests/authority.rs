#[path = "../src/authority/test_support.rs"]
mod support;

use anyhow::{ensure, Result};
use peri_loom_v1::{
    authority::{Authority, BranchRef, Tombstone},
    gc::GarbageCollector,
    object_store::{LocalObjectStore, ObjectStore, PreconditionFailed, PutCondition},
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread,
    time::Duration,
};
use support::{fixture, namespace, publish_head};

#[test]
fn strict_revision_conflicts_are_typed_and_do_not_invoke_mutation() -> Result<()> {
    let (_, authority) = fixture()?;
    let revision = authority.snapshot()?.root.revision;
    authority.transact(None, |root| {
        root.branches.insert("other".into(), BranchRef::default());
        Ok(())
    })?;
    let calls = AtomicUsize::new(0);
    let error = authority
        .transact(Some(revision), |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap_err();
    assert!(error.is::<PreconditionFailed>());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn root_cas_retry_reruns_mutation_on_latest_root() -> Result<()> {
    let (store, authority) = fixture()?;
    let other = Authority::open(store.clone(), namespace())?;
    *store.before_root.lock().unwrap() = Some(Box::new(move || {
        other
            .transact(None, |root| {
                root.branches
                    .insert("competitor".into(), BranchRef::default());
                Ok(())
            })
            .unwrap();
    }));
    let calls = AtomicUsize::new(0);
    let snapshot = authority.transact(None, |root| {
        calls.fetch_add(1, Ordering::SeqCst);
        root.branches.insert("mine".into(), BranchRef::default());
        Ok(())
    })?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        snapshot.root.branches.contains_key("competitor")
            && snapshot.root.branches.contains_key("mine")
    );
    Ok(())
}

#[test]
fn root_lost_ack_receipt_survives_later_root_cas() -> Result<()> {
    let (store, authority) = fixture()?;
    let other = Authority::open(store.clone(), namespace())?;
    store.root_fault.store(1, Ordering::SeqCst);
    *store.after_root.lock().unwrap() = Some(Box::new(move || {
        other
            .transact(None, |root| {
                root.branches.insert("later".into(), BranchRef::default());
                Ok(())
            })
            .unwrap();
    }));
    let calls = AtomicUsize::new(0);
    let snapshot = authority.transact(None, |root| {
        calls.fetch_add(1, Ordering::SeqCst);
        root.branches.insert("mine".into(), BranchRef::default());
        Ok(())
    })?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot.root.revision, 3);
    assert_eq!(snapshot.root.receipts.len(), 2);
    assert!(
        snapshot.root.branches.contains_key("later") && snapshot.root.branches.contains_key("mine")
    );
    Ok(())
}

#[test]
fn unacknowledged_root_failure_does_not_replace_or_reset_state() -> Result<()> {
    let (store, authority) = fixture()?;
    let original = authority.snapshot()?;
    store.root_fault.store(2, Ordering::SeqCst);
    assert!(authority
        .transact(None, |root| {
            root.branches.insert("missing".into(), BranchRef::default());
            Ok(())
        })
        .is_err());
    let observed = authority.snapshot()?;
    assert_eq!(observed.root.revision, original.root.revision);
    assert_eq!(observed.root.incarnation, original.root.incarnation);
    assert!(!observed.root.branches.contains_key("missing"));
    assert!(authority
        .transact(None, |root| {
            root.incarnation = "0".repeat(64);
            Ok(())
        })
        .is_err());
    store
        .inner
        .delete(&namespace().key("authority/root")?, None)?;
    assert!(authority.snapshot().is_err());
    assert!(authority.transact(None, |_| Ok(())).is_err());
    Ok(())
}

#[test]
fn physical_intent_precedes_put_and_lost_ack_is_read_back() -> Result<()> {
    let (store, authority) = fixture()?;
    store.blob_fault.store(1, Ordering::SeqCst);
    let entry = authority.upload(b"confirmed")?;
    assert_eq!(authority.read_blob(&entry)?, b"confirmed");
    let snapshot = authority.snapshot()?;
    assert_eq!(snapshot.root.uploads.get(&entry.blob)?, Some(entry.clone()));
    assert!(authority.reachable(&snapshot.root)?.contains(&entry.blob));
    authority.transact(None, |root| {
        root.objects.insert("orphans/value".into(), entry.clone())?;
        Ok(())
    })?;
    assert!(!authority.snapshot()?.root.uploads.contains_key(&entry.blob));
    store.blob_fault.store(2, Ordering::SeqCst);
    assert!(authority.upload(b"uncertain").is_err());
    let reopened = Authority::open(store.clone(), namespace())?;
    assert_eq!(reopened.snapshot()?.root.uploads.len(), 1);
    let collector = GarbageCollector::new(reopened, Duration::ZERO);
    collector.collect()?;
    assert_eq!(authority.snapshot()?.root.uploads.len(), 1);
    Ok(())
}

#[test]
fn wrong_condition_and_stale_session_do_not_dispatch_physical_upload() -> Result<()> {
    let (_, authority) = fixture()?;
    let branch = authority.branch_store("main")?;
    let before = authority.snapshot()?.root.blobs.len();
    assert!(branch
        .put(
            &namespace().key("HEAD")?,
            b"bad",
            PutCondition::Match("missing".into())
        )
        .unwrap_err()
        .is::<PreconditionFailed>());
    assert_eq!(authority.snapshot()?.root.blobs.len(), before);
    authority.transact(None, |root| {
        let fresh = root.revision + 1;
        let branch = root.branches.get_mut("main").unwrap();
        branch.incarnation = fresh;
        branch.timeline = fresh;
        Ok(())
    })?;
    assert!(branch.validate_session().is_err());
    assert!(branch
        .put(&namespace().key("value")?, b"bad", PutCondition::Any)
        .is_err());
    assert!(branch.delete(&namespace().key("HEAD")?, None).is_err());
    assert_eq!(authority.snapshot()?.root.blobs.len(), before);
    Ok(())
}

#[test]
fn old_pinned_view_survives_writes_until_final_adapter_arc_drop() -> Result<()> {
    let (store, authority) = fixture()?;
    let old = authority.branch_store("main")?;
    let (old_segment, _) = publish_head(old.as_ref(), 17, None)?;
    let old_head = old.get(&namespace().key("HEAD")?)?.unwrap();
    let retained_native_handle = old.clone();
    let newer = authority.branch_store("main")?;
    publish_head(newer.as_ref(), 18, None)?;
    assert_eq!(
        old.get(&namespace().key("HEAD")?)?.unwrap().etag,
        old_head.etag
    );
    let reopened = Authority::open(store, namespace())?;
    assert_eq!(reopened.snapshot()?.root.pins.len(), 2);
    let collector = GarbageCollector::new(reopened.clone(), Duration::ZERO);
    assert_eq!(collector.mark()?, 0);
    drop(old);
    drop(newer);
    assert_eq!(reopened.snapshot()?.root.pins.len(), 1);
    assert_eq!(
        retained_native_handle
            .get(&namespace().key(&format!("segments/{old_segment}"))?)?
            .unwrap()
            .bytes,
        vec![17; peri_loom_v1::manifest::SEGMENT_SIZE]
    );
    drop(retained_native_handle);
    assert!(reopened.snapshot()?.root.pins.is_empty());
    assert!(collector.collect()?.deleted >= 3);
    Ok(())
}

#[test]
fn durable_pins_and_uncertain_intents_survive_disk_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(directory.path())?);
    let authority = Authority::open(store.clone(), namespace())?;
    let branch = authority.branch_store("main")?;
    publish_head(branch.as_ref(), 19, None)?;
    let orphan = authority.upload(b"unresolved-publication")?;
    std::mem::forget(branch);
    drop(authority);
    drop(store);
    let reopened = Authority::open(
        Arc::new(LocalObjectStore::open(directory.path())?),
        namespace(),
    )?;
    let snapshot = reopened.snapshot()?;
    assert_eq!(snapshot.root.pins.len(), 1);
    assert!(snapshot.root.uploads.contains_key(&orphan.blob));
    assert_eq!(GarbageCollector::new(reopened, Duration::ZERO).mark()?, 0);
    Ok(())
}

#[test]
fn repeated_content_has_unique_instances_and_late_delete_cannot_hit_new_upload() -> Result<()> {
    let (store, authority) = fixture()?;
    let old = authority.upload(b"same-content")?;
    authority.transact(None, |root| {
        root.objects.insert("orphans/old".into(), old.clone())?;
        Ok(())
    })?;
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert_eq!(collector.mark()?, 1);
    let new = authority.upload(b"same-content")?;
    assert_eq!(old.hash, new.hash);
    assert_ne!(old.blob, new.blob);
    authority.delete_tombstone(&old.blob)?;
    assert_eq!(authority.read_blob(&new)?, b"same-content");
    let deleted_revision = authority.snapshot()?.root.revision;
    let old_key = namespace().key(&format!("authority/blobs/{}", old.blob))?;
    store
        .inner
        .put(&old_key, b"same-content", PutCondition::Absent)?;
    authority.delete_tombstone(&old.blob)?;
    assert!(store.inner.get(&old_key)?.is_none());
    assert_eq!(authority.snapshot()?.root.revision, deleted_revision);
    assert_eq!(authority.read_blob(&new)?, b"same-content");
    assert!(authority
        .transact(None, |root| {
            root.blobs.insert(old.blob.clone(), old.clone())?;
            root.uploads.insert(old.blob.clone(), old.clone())?;
            Ok(())
        })
        .is_err());
    Ok(())
}

#[test]
fn deletion_rejects_arbitrary_ids_reachable_objects_and_fake_acknowledgments() -> Result<()> {
    let (_, authority) = fixture()?;
    let entry = authority.upload(b"intent")?;
    assert!(authority.delete_tombstone(&entry.blob).is_err());
    assert!(authority.delete_tombstone(&"f".repeat(64)).is_err());
    assert!(authority
        .transact(None, |root| {
            root.tombstones.insert(
                entry.blob.clone(),
                Tombstone {
                    entry: entry.clone(),
                    marked_ms: 0,
                    deleted: false,
                },
            )?;
            Ok(())
        })
        .is_err());
    authority.transact(None, |root| {
        root.objects.insert("orphans/value".into(), entry.clone())?;
        Ok(())
    })?;
    GarbageCollector::new(authority.clone(), Duration::ZERO).mark()?;
    assert!(authority
        .transact(None, |root| {
            root.tombstones.update(&entry.blob, |tombstone| {
                tombstone.deleted = true;
                Ok(())
            })?;
            root.blobs.remove(&entry.blob)?;
            Ok(())
        })
        .is_err());
    authority.delete_tombstone(&entry.blob)?;
    assert!(authority
        .transact(None, |root| {
            root.tombstones.remove(&entry.blob)?;
            Ok(())
        })
        .is_err());
    Ok(())
}

#[test]
fn metadata_validation_rejects_missing_dependencies_without_segment_body_reads() -> Result<()> {
    let (store, authority) = fixture()?;
    let branch = authority.branch_store("main")?;
    let (segment, _) = publish_head(branch.as_ref(), 20, Some("f".repeat(64)))?;
    store.segment_gets.store(0, Ordering::SeqCst);
    let snapshot = authority.snapshot()?;
    let reachable = authority.reachable(&snapshot.root)?;
    assert!(reachable.contains(&snapshot.root.objects[&format!("segments/{segment}")].blob));
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    assert!(authority
        .transact(None, |root| {
            root.objects.remove(&format!("segments/{segment}"))?;
            Ok(())
        })
        .is_err());
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    let bad = authority.upload(&serde_json::to_vec(
        &serde_json::json!({"manifest":"0".repeat(64)}),
    )?)?;
    assert!(authority
        .transact(None, |root| {
            root.branches.get_mut("main").unwrap().head = Some(bad.clone());
            Ok(())
        })
        .is_err());
    assert_eq!(store.segment_gets.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn root_envelope_corruption_fails_closed_without_reinitialization() -> Result<()> {
    let (store, authority) = fixture()?;
    let key = namespace().key("authority/root")?;
    let object = store.inner.get(&key)?.unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&object.bytes)?;
    value["root"]["revision"] = serde_json::json!(999);
    store
        .inner
        .put(&key, &serde_json::to_vec(&value)?, PutCondition::Any)?;
    assert!(authority.snapshot().is_err());
    assert!(Authority::open(store.clone(), namespace()).is_err());
    assert_eq!(
        store.inner.get(&key)?.unwrap().bytes,
        serde_json::to_vec(&value)?
    );
    Ok(())
}

#[test]
fn concurrent_root_operations_preserve_all_updates() -> Result<()> {
    let (store, authority) = fixture()?;
    let barrier = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let authority = Authority::open(store.clone(), namespace()).unwrap();
            let barrier = barrier.clone();
            thread::spawn(move || -> Result<()> {
                barrier.wait();
                authority.transact(None, |root| {
                    root.branches
                        .insert(format!("worker-{worker}"), BranchRef::default());
                    Ok(())
                })?;
                Ok(())
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap()?;
    }
    ensure!(
        authority.snapshot()?.root.branches.len() == 9,
        "lost root update"
    );
    Ok(())
}
