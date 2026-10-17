use anyhow::{bail, Context, Result};
use peri_loom_v1::{
    authority::{Authority, BranchStore},
    capabilities::Capabilities,
    database::RemoteDatabase,
    gc::GarbageCollector,
    manifest::{digest, Manifest, SEGMENT_SIZE},
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use turso_core::Value;

type RootHook = Box<dyn FnOnce() -> Result<()> + Send>;

#[derive(Default)]
struct FaultStore {
    inner: MemoryObjectStore,
    root_hook: Mutex<Option<RootHook>>,
    lose_root_ack: AtomicBool,
    fail_blob_put: AtomicBool,
    segment_gets: AtomicUsize,
    segment_puts: AtomicUsize,
    deletes: AtomicUsize,
}

impl ObjectStore for FaultStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let object = self.inner.get(key)?;
        if object
            .as_ref()
            .is_some_and(|object| object.bytes.len() == SEGMENT_SIZE)
        {
            self.segment_gets.fetch_add(1, Ordering::SeqCst);
        }
        Ok(object)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let root_cas =
            key.ends_with("/authority/root") && matches!(condition, PutCondition::Match(_));
        if root_cas {
            let hook = self.root_hook.lock().unwrap().take();
            if let Some(hook) = hook {
                hook()?;
            }
        }
        if key.contains("/authority/blobs/") && self.fail_blob_put.swap(false, Ordering::SeqCst) {
            bail!("injected physical upload failure");
        }
        if bytes.len() == SEGMENT_SIZE {
            self.segment_puts.fetch_add(1, Ordering::SeqCst);
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if root_cas && self.lose_root_ack.swap(false, Ordering::SeqCst) {
            bail!("injected lost root CAS acknowledgment");
        }
        Ok(etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn setup() -> Result<(Arc<FaultStore>, Arc<Authority>, Capabilities)> {
    let raw = Arc::new(FaultStore::default());
    let authority = Authority::open(raw.clone(), Namespace::new("p2-verifier", "database")?)?;
    let capabilities = Capabilities::new(authority.clone());
    Ok((raw, authority, capabilities))
}

fn open(authority: &Arc<Authority>, branch: &str, cache: &Path) -> Result<RemoteDatabase> {
    RemoteDatabase::open(
        authority.branch_store(branch)?,
        authority.namespace().clone(),
        cache,
        0,
    )
}

fn assert_ids(database: &RemoteDatabase, expected: &[i64]) -> Result<()> {
    assert_eq!(
        database.query("SELECT id FROM items ORDER BY id")?,
        expected
            .iter()
            .map(|value| vec![Value::from_i64(*value)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        database.query("PRAGMA integrity_check")?,
        vec![vec![Value::Text("ok".into())]]
    );
    Ok(())
}

fn orphan(authority: &Arc<Authority>, byte: u8) -> Result<(String, String)> {
    let bytes = vec![byte; SEGMENT_SIZE];
    let relative = format!("segments/{}", digest(&bytes));
    let session = authority.branch_store("main")?;
    session.put(
        &authority.namespace().key(&relative)?,
        &bytes,
        PutCondition::Absent,
    )?;
    let blob = authority.snapshot()?.root.objects[&relative].blob.clone();
    drop(session);
    Ok((relative, blob))
}

#[test]
fn metadata_fork_snapshot_clone_share_exact_physical_instances() -> Result<()> {
    let (raw, authority, capabilities) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(2)")?;
    let before = authority.snapshot()?.root;
    let gets = raw.segment_gets.load(Ordering::SeqCst);
    let puts = raw.segment_puts.load(Ordering::SeqCst);
    assert!(puts > 0);
    let point = capabilities.snapshot("main", "saved")?;
    capabilities.branch("main", "fork")?;
    capabilities.clone("saved", "copy")?;
    assert!(capabilities.snapshot("main", "saved").is_err());
    let after = authority.snapshot()?.root;
    assert_eq!(before.objects, after.objects);
    assert_eq!(before.blobs, after.blobs);
    assert_eq!(before.branches["main"].head, after.branches["fork"].head);
    assert_eq!(point.wal, after.branches["copy"].wal);
    assert_eq!(raw.segment_gets.load(Ordering::SeqCst), gets);
    assert_eq!(raw.segment_puts.load(Ordering::SeqCst), puts);
    let copy_cache = tempfile::tempdir()?;
    assert_ids(&open(&authority, "copy", copy_cache.path())?, &[1, 2])?;
    Ok(())
}

#[test]
fn native_history_ignores_reads_rollback_and_uncommitted_execute() -> Result<()> {
    let (_, authority, capabilities) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    let before = authority.snapshot()?.root.branches["main"].history.clone();
    assert!(!before.is_empty());
    database.query("SELECT count(*) FROM items")?;
    database.execute("BEGIN; INSERT INTO items VALUES(1)")?;
    assert_eq!(authority.snapshot()?.root.branches["main"].history, before);
    database.execute("ROLLBACK")?;
    assert_eq!(authority.snapshot()?.root.branches["main"].history, before);
    database.execute("BEGIN; INSERT INTO items VALUES(2); INSERT INTO items VALUES(3); COMMIT")?;
    let after = authority.snapshot()?.root.branches["main"].history.clone();
    assert_eq!(after.len(), before.len() + 1);
    assert!(after.last().unwrap().sequence > before.last().unwrap().sequence);
    assert_eq!(
        capabilities.pitr("main", after.last().unwrap().sequence)?,
        *after.last().unwrap()
    );
    Ok(())
}

#[test]
fn restore_reopen_keeps_snapshot_identity_but_advances_commit_high_water() -> Result<()> {
    let (_, authority, capabilities) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(2)")?;
    let saved = capabilities.snapshot("main", "saved")?;
    database.execute("INSERT INTO items VALUES(3)")?;
    let high_water = authority.snapshot()?.root.branches["main"].clone();
    let stale = authority.branch_store("main")?;
    capabilities.restore("main", "saved")?;
    assert!(stale.validate_session().is_err());
    assert!(database.query("SELECT id FROM items").is_err());
    let restored = authority.snapshot()?.root.branches["main"].clone();
    assert!(restored.incarnation > high_water.incarnation);
    assert!(restored.timeline > high_water.timeline);
    assert_eq!(restored.sequence, high_water.sequence);
    assert_eq!(restored.last_commit_ms, high_water.last_commit_ms);
    assert_eq!(restored.current_sequence, saved.sequence);
    assert_eq!(restored.current_commit_ms, saved.timestamp_ms);
    let repeated = capabilities.snapshot("main", "restored")?;
    assert_eq!(repeated.sequence, saved.sequence);
    assert_eq!(repeated.timestamp_ms, saved.timestamp_ms);
    assert_eq!(repeated.head, saved.head);
    assert_eq!(repeated.wal, saved.wal);
    drop(database);
    let fresh = tempfile::tempdir()?;
    let reopened = open(&authority, "main", fresh.path())?;
    assert_ids(&reopened, &[1, 2])?;
    reopened.execute("INSERT INTO items VALUES(4)")?;
    let current = authority.snapshot()?.root;
    assert!(current.branches["main"].current_sequence > high_water.sequence);
    assert_eq!(current.snapshots["saved"], saved);
    Ok(())
}

#[test]
fn pitr_snapshot_wal_tail_survives_reset_retention_gc_and_restart() -> Result<()> {
    let (raw, authority, capabilities) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(2)")?;
    let saved = capabilities.snapshot("main", "tail")?;
    let exact = capabilities.pitr("main", saved.sequence)?;
    assert_eq!(exact.wal, saved.wal);
    database.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    database.execute("INSERT INTO items VALUES(3)")?;
    drop(database);
    capabilities.retain_after("main", saved.sequence + 1)?;
    assert!(capabilities.pitr("main", saved.sequence).is_err());
    capabilities.delete_branch("main")?;
    GarbageCollector::new(authority.clone(), Duration::ZERO).collect()?;
    let namespace = authority.namespace().clone();
    drop(capabilities);
    drop(authority);
    let restarted = Authority::open(raw, namespace)?;
    Capabilities::new(restarted.clone()).clone("tail", "recovered")?;
    let fresh = tempfile::tempdir()?;
    assert_ids(&open(&restarted, "recovered", fresh.path())?, &[1, 2])?;
    Ok(())
}

#[test]
fn pitr_time_selects_latest_commit_during_idle_time_after_last_commit() -> Result<()> {
    let (_, authority, capabilities) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let history = authority.snapshot()?.root.branches["main"].history.clone();
    let latest = history.last().context("native commit history missing")?;
    let requested_ms = latest
        .timestamp_ms
        .checked_add(1)
        .context("commit time overflow")?;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let now_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        if now_ms >= requested_ms {
            break;
        }
        if Instant::now() >= deadline {
            bail!("wall clock did not reach idle request time; no PITR verdict");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(capabilities.resolve_time("main", requested_ms)?, *latest);
    assert!(capabilities
        .resolve_time("main", history[0].timestamp_ms - 1)
        .is_err());
    for point in &history {
        let expected = history
            .iter()
            .filter(|candidate| candidate.timestamp_ms <= point.timestamp_ms)
            .max_by_key(|candidate| {
                (
                    candidate.timestamp_ms,
                    candidate.sequence,
                    candidate.timeline,
                )
            })
            .unwrap();
        assert_eq!(
            capabilities.resolve_time("main", point.timestamp_ms)?,
            *expected
        );
    }
    Ok(())
}

#[test]
fn lost_root_ack_is_one_publication_and_survives_restart() -> Result<()> {
    let (raw, authority, capabilities) = setup()?;
    let before = authority.snapshot()?.root;
    raw.lose_root_ack.store(true, Ordering::SeqCst);
    capabilities.branch("main", "fork")?;
    let after = authority.snapshot()?.root;
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.receipts.len(), before.receipts.len() + 1);
    assert_eq!(after.branches.len(), 2);
    let restarted = Authority::open(raw, authority.namespace().clone())?;
    let recovered = restarted.snapshot()?.root;
    assert_eq!(recovered.incarnation, before.incarnation);
    assert_eq!(recovered.revision, after.revision);
    assert_eq!(recovered.receipts, after.receipts);
    assert!(Capabilities::new(restarted).branch("main", "fork").is_err());
    Ok(())
}

#[test]
fn gc_recomputes_after_concurrent_new_read_pin_and_rejects_resurrection() -> Result<()> {
    let (raw, authority, _) = setup()?;
    let (relative, blob) = orphan(&authority, 61)?;
    let entry = authority.snapshot()?.root.objects[&relative].clone();
    let concurrent = authority.clone();
    let pinned_key = authority.namespace().key(&relative)?;
    let reader_slot: Arc<Mutex<Option<Arc<BranchStore>>>> = Arc::new(Mutex::new(None));
    let slot = reader_slot.clone();
    *raw.root_hook.lock().unwrap() = Some(Box::new(move || {
        let reader = concurrent.branch_store("main")?;
        reader.get(&pinned_key)?;
        *slot.lock().unwrap() = Some(reader);
        Ok(())
    }));
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert_eq!(collector.collect()?.marked, 0);
    assert!(!authority.snapshot()?.root.tombstones.contains_key(&blob));
    assert_eq!(authority.read_blob(&entry)?, vec![61; SEGMENT_SIZE]);
    drop(reader_slot.lock().unwrap().take());
    assert!(collector.mark()? > 0);
    assert!(authority
        .transact(None, |root| {
            root.objects.insert(relative.clone(), entry.clone())?;
            Ok(())
        })
        .is_err());
    assert!(collector.sweep()? > 0);
    assert!(authority.read_blob(&entry).is_err());
    Ok(())
}

#[test]
fn gc_delays_exact_instance_deletion_and_never_touches_foreign_namespace() -> Result<()> {
    let (raw, authority, _) = setup()?;
    let foreign = Namespace::new("other-tenant", "database")?.key("authority/blobs/foreign")?;
    raw.inner
        .put(&foreign, b"foreign sentinel", PutCondition::Absent)?;
    let (relative, original) = orphan(&authority, 74)?;
    let delayed = GarbageCollector::new(authority.clone(), Duration::from_secs(3600));
    assert!(delayed.mark()? > 0);
    assert_eq!(delayed.sweep()?, 0);
    let restarted = Authority::open(raw.clone(), authority.namespace().clone())?;
    assert_eq!(
        GarbageCollector::new(restarted.clone(), Duration::from_secs(3600)).sweep()?,
        0
    );
    let session = restarted.branch_store("main")?;
    session.put(
        &restarted.namespace().key(&relative)?,
        &vec![74; SEGMENT_SIZE],
        PutCondition::Absent,
    )?;
    let replacement = restarted.snapshot()?.root.objects[&relative].blob.clone();
    assert_ne!(replacement, original);
    let immediate = GarbageCollector::new(restarted.clone(), Duration::ZERO);
    assert!(immediate.sweep()? > 0);
    assert_eq!(immediate.sweep()?, 0);
    assert_eq!(
        session
            .get(&restarted.namespace().key(&relative)?)?
            .unwrap()
            .bytes,
        vec![74; SEGMENT_SIZE]
    );
    assert_eq!(raw.inner.get(&foreign)?.unwrap().bytes, b"foreign sentinel");
    assert!(restarted.snapshot()?.root.blobs.contains_key(&replacement));
    Ok(())
}

#[test]
fn unresolved_upload_intent_cannot_be_released_tombstoned_or_deleted() -> Result<()> {
    let (raw, authority, _) = setup()?;
    raw.fail_blob_put.store(true, Ordering::SeqCst);
    assert!(authority
        .upload(b"upload with unresolved physical result")
        .is_err());
    let root = authority.snapshot()?.root;
    assert_eq!(root.uploads.len(), 1);
    let (blob, entry) = root.uploads.iter().next().unwrap();
    assert!(authority
        .transact(None, |root| {
            root.uploads.remove(blob)?;
            Ok(())
        })
        .is_err());
    assert!(authority
        .transact(None, |root| {
            root.tombstones.insert(
                blob.clone(),
                peri_loom_v1::authority::Tombstone {
                    entry: entry.clone(),
                    marked_ms: 0,
                    deleted: false,
                },
            )?;
            Ok(())
        })
        .is_err());
    assert!(authority.delete_tombstone(blob).is_err());
    let restarted = Authority::open(raw.clone(), authority.namespace().clone())?;
    assert_eq!(
        GarbageCollector::new(restarted.clone(), Duration::ZERO)
            .collect()?
            .marked,
        0
    );
    assert_eq!(restarted.snapshot()?.root.uploads.len(), 1);
    assert_eq!(raw.deletes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn corrupted_root_and_metadata_blob_fail_closed_before_deletion() -> Result<()> {
    let (raw, authority, capabilities) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    database.checkpoint()?;
    drop(database);
    capabilities.snapshot("main", "saved")?;
    let root_key = authority.namespace().key("authority/root")?;
    let original = raw.inner.get(&root_key)?.unwrap().bytes;
    raw.inner
        .put(&root_key, b"corrupt root", PutCondition::Any)?;
    assert!(authority.snapshot().is_err());
    assert!(Authority::open(raw.clone(), authority.namespace().clone()).is_err());
    assert!(GarbageCollector::new(authority.clone(), Duration::ZERO)
        .collect()
        .is_err());
    assert_eq!(raw.deletes.load(Ordering::SeqCst), 0);
    raw.inner.put(&root_key, &original, PutCondition::Any)?;
    let root = authority.snapshot()?.root;
    let head = root.branches["main"]
        .head
        .as_ref()
        .context("HEAD missing")?;
    let key = authority
        .namespace()
        .key(&format!("authority/blobs/{}", head.blob))?;
    raw.inner
        .put(&key, b"corrupt head body", PutCondition::Any)?;
    assert!(authority.read_blob(head).is_err());
    assert!(Authority::open(raw.clone(), authority.namespace().clone()).is_err());
    assert!(capabilities.clone("saved", "unsafe").is_err());
    assert!(GarbageCollector::new(authority, Duration::ZERO)
        .collect()
        .is_err());
    assert_eq!(raw.deletes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn deleted_recreated_branch_rejects_old_incarnation_and_root_counter_rewind() -> Result<()> {
    let (_, authority, capabilities) = setup()?;
    capabilities.branch("main", "temporary")?;
    let stale = authority.branch_store("temporary")?;
    let before = authority.snapshot()?.root;
    capabilities.delete_branch("temporary")?;
    capabilities.branch("main", "temporary")?;
    assert!(stale.validate_session().is_err());
    let after = authority.snapshot()?.root;
    assert!(after.branches["temporary"].incarnation > before.branches["temporary"].incarnation);
    assert!(authority
        .transact(None, |root| {
            root.revision = 1;
            Ok(())
        })
        .is_err());
    assert!(authority
        .transact(None, |root| {
            root.incarnation = digest(b"reset identity");
            Ok(())
        })
        .is_err());
    assert_eq!(authority.snapshot()?.root.revision, after.revision);
    Ok(())
}

#[test]
fn last_reader_handle_protects_lazy_segment_and_corruption_is_not_hidden() -> Result<()> {
    let (raw, authority, _) = setup()?;
    let (relative, blob) = orphan(&authority, 83)?;
    let session = authority.branch_store("main")?;
    let retained = session.clone();
    drop(session);
    let key = authority.namespace().key(&relative)?;
    assert_eq!(retained.list(&key)?, vec![key.clone()]);
    assert!(retained
        .pinned_entries()?
        .iter()
        .any(|entry| entry.blob == blob));
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert_eq!(collector.collect()?.marked, 0);
    assert_eq!(retained.get(&key)?.unwrap().bytes, vec![83; SEGMENT_SIZE]);
    let physical = authority
        .namespace()
        .key(&format!("authority/blobs/{blob}"))?;
    raw.inner
        .put(&physical, &vec![84; SEGMENT_SIZE], PutCondition::Any)?;
    assert!(retained.get(&key).is_err());
    raw.inner
        .put(&physical, &vec![83; SEGMENT_SIZE], PutCondition::Any)?;
    assert_eq!(collector.collect()?.marked, 0);
    drop(retained);
    assert!(collector.collect()?.marked > 0);
    assert!(raw.inner.get(&physical)?.is_none());
    Ok(())
}

#[test]
fn undiscovered_orphan_is_not_pinned_by_unrelated_reader() -> Result<()> {
    let (raw, authority, _) = setup()?;
    let (relative, blob) = orphan(&authority, 85)?;
    let reader = authority.branch_store("main")?;
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert_eq!(collector.collect()?.marked, 1);
    assert!(reader
        .get(&authority.namespace().key(&relative)?)?
        .is_none());
    assert!(raw
        .inner
        .get(
            &authority
                .namespace()
                .key(&format!("authority/blobs/{blob}"))?
        )?
        .is_none());
    Ok(())
}

#[test]
fn retained_history_pins_dependencies_but_manifest_parent_is_not_retention() -> Result<()> {
    let (_, authority, capabilities) = setup()?;
    let cache = tempfile::tempdir()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(2)")?;
    let before = authority.snapshot()?.root.branches["main"]
        .history
        .last()
        .cloned()
        .unwrap();
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(3)")?;
    database.checkpoint()?;
    drop(database);
    let latest = authority.snapshot()?.root.branches["main"]
        .history
        .last()
        .unwrap()
        .sequence;
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    collector.collect()?;
    let reachable = authority.reachable(&authority.snapshot()?.root)?;
    assert!(reachable.contains(&before.head.as_ref().unwrap().blob));
    assert!(reachable.contains(&before.wal.as_ref().unwrap().blob));
    capabilities.branch("main", "replay")?;
    capabilities.restore_point("replay", &before)?;
    let fresh = tempfile::tempdir()?;
    let replayed = open(&authority, "replay", fresh.path())?;
    assert_ids(&replayed, &[1, 2])?;
    drop(replayed);
    capabilities.delete_branch("replay")?;
    capabilities.retain_after("main", latest)?;
    collector.collect()?;
    let after = authority.snapshot()?.root;
    for entry in before.head.iter().chain(before.wal.iter()) {
        assert!(after
            .tombstones
            .get(&entry.blob)?
            .is_some_and(|tombstone| tombstone.deleted));
    }
    assert!(capabilities.pitr("main", before.sequence).is_err());
    let fresh = tempfile::tempdir()?;
    assert_ids(&open(&authority, "main", fresh.path())?, &[1, 2, 3])?;
    Ok(())
}

#[test]
fn gc_mark_cas_conflict_recomputes_new_head_transitive_dependencies() -> Result<()> {
    let (raw, authority, capabilities) = setup()?;
    let publisher = authority.branch_store("main")?;
    let (relative, segment_blob) = orphan(&authority, 97)?;
    let manifest = Manifest {
        generation: 1,
        logical_db_size: SEGMENT_SIZE as u64,
        segments: std::collections::BTreeMap::from([(
            0,
            relative.strip_prefix("segments/").unwrap().to_owned(),
        )]),
        ..Manifest::default()
    };
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let manifest_hash = digest(&manifest_bytes);
    let writer = authority.branch_store("main")?;
    writer.put(
        &authority
            .namespace()
            .key(&format!("manifests/{manifest_hash}"))?,
        &manifest_bytes,
        PutCondition::Absent,
    )?;
    drop(writer);
    let before = authority.snapshot()?.root;
    assert!(!authority.reachable(&before)?.contains(&segment_blob));
    let head_key = authority.namespace().key("HEAD")?;
    let head_bytes = serde_json::to_vec(&serde_json::json!({"manifest": manifest_hash}))?;
    *raw.root_hook.lock().unwrap() = Some(Box::new(move || {
        publisher.put(&head_key, &head_bytes, PutCondition::Absent)?;
        Ok(())
    }));
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert_eq!(collector.collect()?.marked, 0);
    let after = authority.snapshot()?.root;
    assert!(after.branches["main"].head.is_some());
    assert!(!after.tombstones.contains_key(&segment_blob));
    assert!(authority.reachable(&after)?.contains(&segment_blob));
    assert_eq!(raw.deletes.load(Ordering::SeqCst), 0);
    capabilities.snapshot("main", "concurrent")?;
    capabilities.clone("concurrent", "reader")?;
    let reader = authority.branch_store("reader")?;
    assert_eq!(
        reader
            .get(&authority.namespace().key(&relative)?)?
            .unwrap()
            .bytes,
        vec![97; SEGMENT_SIZE]
    );
    Ok(())
}
