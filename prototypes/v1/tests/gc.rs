use anyhow::Result;
use peri_loom_v1::{
    authority::Authority,
    capabilities::Capabilities,
    database::RemoteDatabase,
    gc::GarbageCollector,
    manifest::{digest, SEGMENT_SIZE},
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition},
};
use std::{sync::Arc, time::Duration};

fn setup() -> Result<(Arc<MemoryObjectStore>, Arc<Authority>, Namespace)> {
    let raw = Arc::new(MemoryObjectStore::default());
    let namespace = Namespace::new("tenant", "gc")?;
    let authority = Authority::open(raw.clone(), namespace.clone())?;
    Ok((raw, authority, namespace))
}

#[test]
fn orphan_segments_are_tombstoned_and_physically_deleted() -> Result<()> {
    let (raw, authority, namespace) = setup()?;
    let session = authority.branch_store("main")?;
    let bytes = vec![37; SEGMENT_SIZE];
    let key = namespace.key(&format!("segments/{}", digest(&bytes)))?;
    session.put(&key, &bytes, PutCondition::Absent)?;
    assert_eq!(session.get(&key)?.unwrap().bytes, bytes);
    drop(session);
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    let physical_before = raw.list("")?.len();
    let result = collector.collect()?;
    assert!(result.marked > 0);
    assert_eq!(result.deleted, result.marked);
    assert!(raw.list("")?.len() < physical_before);
    let root = authority.snapshot()?.root;
    assert!(!root
        .objects
        .contains_key(&format!("segments/{}", digest(&bytes))));
    assert!(root.tombstones.values().all(|entry| entry.deleted));
    assert_eq!(collector.collect()?.deleted, 0);
    Ok(())
}

#[test]
fn reader_pin_protects_lazy_reads_until_last_session_handle_drops() -> Result<()> {
    let (_, authority, namespace) = setup()?;
    let session = authority.branch_store("main")?;
    let bytes = vec![91; SEGMENT_SIZE];
    let key = namespace.key(&format!("segments/{}", digest(&bytes)))?;
    session.put(&key, &bytes, PutCondition::Absent)?;
    let retained = session.clone();
    drop(session);
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert_eq!(collector.collect()?.marked, 0);
    assert_eq!(retained.get(&key)?.unwrap().bytes, bytes);
    drop(retained);
    assert!(collector.collect()?.marked > 0);
    Ok(())
}

#[test]
fn delayed_sweep_does_not_delete_new_instance_of_same_content() -> Result<()> {
    let (_, authority, namespace) = setup()?;
    let bytes = vec![19; SEGMENT_SIZE];
    let relative = format!("segments/{}", digest(&bytes));
    let key = namespace.key(&relative)?;
    let first = authority.branch_store("main")?;
    first.put(&key, &bytes, PutCondition::Absent)?;
    let original = authority.snapshot()?.root.objects[&relative].blob.clone();
    drop(first);
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert!(collector.mark()? > 0);
    let second = authority.branch_store("main")?;
    second.put(&key, &bytes, PutCondition::Absent)?;
    let replacement = authority.snapshot()?.root.objects[&relative].blob.clone();
    assert_ne!(original, replacement);
    assert!(collector.sweep()? > 0);
    assert_eq!(second.get(&key)?.unwrap().bytes, bytes);
    assert!(authority.snapshot()?.root.blobs.contains_key(&replacement));
    Ok(())
}

#[test]
fn completed_tombstone_reaps_late_arrival_without_deleting_replacement() -> Result<()> {
    let (raw, authority, namespace) = setup()?;
    let bytes = vec![82; SEGMENT_SIZE];
    let relative = format!("segments/{}", digest(&bytes));
    let key = namespace.key(&relative)?;
    let first = authority.branch_store("main")?;
    first.put(&key, &bytes, PutCondition::Absent)?;
    let original = authority.snapshot()?.root.objects[&relative].blob.clone();
    let old_physical = namespace.key(&format!("authority/blobs/{original}"))?;
    drop(first);
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert!(collector.collect()?.deleted > 0);
    assert!(raw.get(&old_physical)?.is_none());
    raw.put(&old_physical, &bytes, PutCondition::Absent)?;
    let replacement = authority.branch_store("main")?;
    replacement.put(&key, &bytes, PutCondition::Absent)?;
    let fresh = authority.snapshot()?.root.objects[&relative].blob.clone();
    assert_ne!(original, fresh);
    collector.collect()?;
    assert!(raw.get(&old_physical)?.is_none());
    assert_eq!(replacement.get(&key)?.unwrap().bytes, bytes);
    assert!(authority.snapshot()?.root.blobs.contains_key(&fresh));
    Ok(())
}

#[test]
fn safety_window_survives_authority_restart() -> Result<()> {
    let (raw, authority, namespace) = setup()?;
    let bytes = vec![24; SEGMENT_SIZE];
    let session = authority.branch_store("main")?;
    session.put(
        &namespace.key(&format!("segments/{}", digest(&bytes)))?,
        &bytes,
        PutCondition::Absent,
    )?;
    drop(session);
    let collector = GarbageCollector::new(authority, Duration::from_secs(3600));
    assert!(collector.mark()? > 0);
    assert_eq!(collector.sweep()?, 0);
    drop(collector);
    let restarted = Authority::open(raw, namespace)?;
    let collector = GarbageCollector::new(restarted, Duration::from_secs(3600));
    assert_eq!(collector.sweep()?, 0);
    assert!(collector.inspect()?.pending_deletions > 0);
    Ok(())
}

#[test]
fn snapshot_and_retained_history_survive_ref_deletion_and_gc() -> Result<()> {
    let (_, authority, namespace) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = RemoteDatabase::open(
        authority.branch_store("main")?,
        namespace.clone(),
        cache.path(),
        0,
    )?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1, 'pinned')")?;
    database.checkpoint()?;
    drop(database);
    let capabilities = Capabilities::new(authority.clone());
    capabilities.snapshot("main", "keep")?;
    capabilities.branch("main", "discard")?;
    capabilities.delete_branch("discard")?;
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    collector.collect()?;
    capabilities.clone("keep", "restored")?;
    let fresh = tempfile::tempdir()?;
    let recovered = RemoteDatabase::open(
        authority.branch_store("restored")?,
        namespace,
        fresh.path(),
        0,
    )?;
    let rows = recovered.query("SELECT value FROM items WHERE id=1")?;
    assert_eq!(rows.len(), 1);
    assert!(format!("{:?}", rows[0]).contains("pinned"));
    assert_eq!(recovered.query("PRAGMA integrity_check")?.len(), 1);
    Ok(())
}
