use anyhow::{bail, Result};
use peri_loom_v1::authority::{Authority, RootSnapshot};
use peri_loom_v1::capabilities::Capabilities;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::manifest::SEGMENT_SIZE;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use turso_core::{Numeric, Value};

type PutHook = Box<dyn FnOnce() -> Result<()> + Send>;

#[derive(Default)]
struct ObservedStore {
    inner: MemoryObjectStore,
    segment_gets: AtomicU64,
    segment_puts: AtomicU64,
    hook: Mutex<Option<PutHook>>,
}

impl ObjectStore for ObservedStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let object = self.inner.get(key)?;
        if object
            .as_ref()
            .is_some_and(|object| object.bytes.len() == SEGMENT_SIZE)
        {
            self.segment_gets.fetch_add(1, Ordering::Relaxed);
        }
        Ok(object)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let hook = self.hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook()?;
        }
        if bytes.len() == SEGMENT_SIZE {
            self.segment_puts.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn setup() -> Result<(Arc<ObservedStore>, Arc<Authority>, Capabilities)> {
    let store = Arc::new(ObservedStore::default());
    let authority = Authority::open(store.clone(), Namespace::new("tenant", "database")?)?;
    let capabilities = Capabilities::new(authority.clone());
    Ok((store, authority, capabilities))
}

fn open(authority: &Arc<Authority>, branch: &str, cache: &Path) -> Result<RemoteDatabase> {
    RemoteDatabase::open(
        authority.branch_store(branch)?,
        authority.namespace().clone(),
        cache,
        0,
    )
}

fn count(database: &RemoteDatabase) -> Result<i64> {
    let rows = database.query("SELECT count(*) FROM items")?;
    match &rows[0][0] {
        Value::Numeric(Numeric::Integer(count)) => Ok(*count),
        value => bail!("unexpected count: {value:?}"),
    }
}

fn identity(snapshot: &RootSnapshot, branch: &str) -> (u64, u64, u64, u64) {
    let reference = &snapshot.root.branches[branch];
    (
        reference.incarnation,
        reference.timeline,
        reference.sequence,
        reference.last_commit_ms,
    )
}

#[test]
fn metadata_operations_do_not_get_or_put_segment_bodies() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (store, authority, capabilities) = setup()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1, 'base')")?;
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(2, 'tail')")?;
    let before = authority.snapshot()?;
    let gets = store.segment_gets.load(Ordering::Relaxed);
    let puts = store.segment_puts.load(Ordering::Relaxed);
    assert!(puts > 0);
    capabilities.branch("main", "fork")?;
    let point = capabilities.snapshot("main", "saved")?;
    capabilities.clone("saved", "copy")?;
    assert_eq!(
        capabilities.pitr("main", point.sequence)?.sequence,
        point.sequence
    );
    assert_eq!(
        capabilities
            .resolve_time("main", point.timestamp_ms)?
            .sequence,
        point.sequence
    );
    capabilities.restore("fork", "saved")?;
    capabilities.restore_point("copy", &point)?;
    capabilities.retain_after("copy", point.sequence + 1)?;
    assert_eq!(capabilities.list()?.branches.len(), 3);
    assert_eq!(capabilities.graph()?.objects, before.root.objects);
    capabilities.delete_branch("fork")?;
    capabilities.delete_branch("copy")?;
    capabilities.delete_snapshot("saved")?;
    let after = authority.snapshot()?;
    assert_eq!(after.root.objects, before.root.objects);
    assert_eq!(after.root.pins, before.root.pins);
    assert_eq!(
        serde_json::to_value(after.root.tombstones)?,
        serde_json::to_value(before.root.tombstones)?
    );
    assert_eq!(store.segment_gets.load(Ordering::Relaxed), gets);
    assert_eq!(store.segment_puts.load(Ordering::Relaxed), puts);
    Ok(())
}

#[test]
fn sql_fork_is_cow_and_clone_cold_recovers_snapshot_wal_tail() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let main = open(&authority, "main", &cache.path().join("main"))?;
    main.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    let payload = "x".repeat(4096);
    let mut sql = String::from("BEGIN;");
    for index in 0..100 {
        sql.push_str(&format!("INSERT INTO items VALUES({index}, '{payload}');"));
    }
    sql.push_str("COMMIT;");
    main.execute(&sql)?;
    main.checkpoint()?;
    let base = main.storage().manifest();
    assert!(base.segments.len() > 1);
    let forked = capabilities.branch("main", "feature")?;
    assert_ne!(
        forked.root.branches["feature"].timeline,
        forked.root.branches["main"].timeline
    );
    let feature = open(&authority, "feature", &cache.path().join("feature"))?;
    feature.execute("UPDATE items SET value='feature' WHERE id=0")?;
    feature.checkpoint()?;
    let next = feature.storage().manifest();
    assert!(base
        .segments
        .iter()
        .any(|(index, hash)| next.segments.get(index) == Some(hash)));
    assert_ne!(next.segments, base.segments);
    assert_eq!(
        main.query("SELECT length(value) FROM items WHERE id=0")?[0][0],
        Value::Numeric(Numeric::Integer(4096))
    );
    main.execute("INSERT INTO items VALUES(100, 'uncheckpointed')")?;
    let point = capabilities.snapshot("main", "tail")?;
    let cloned = capabilities.clone("tail", "copy")?;
    assert_eq!(cloned.root.branches["copy"].head, point.head);
    assert_eq!(cloned.root.branches["copy"].wal, point.wal);
    let clone = open(&authority, "copy", &cache.path().join("cold-copy"))?;
    assert_eq!(count(&clone)?, 101);
    clone.execute("INSERT INTO items VALUES(101, 'only-copy')")?;
    assert_eq!(count(&clone)?, 102);
    assert_eq!(count(&main)?, 101);
    assert_eq!(count(&feature)?, 100);
    Ok(())
}

#[test]
fn restore_fences_old_sql_session_and_never_rewinds_root_or_sequence() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let database = open(&authority, "main", &cache.path().join("old"))?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let saved = capabilities.snapshot("main", "saved")?;
    database.execute("INSERT INTO items VALUES(2)")?;
    assert_eq!(count(&database)?, 2);
    let before = authority.snapshot()?;
    let restored = capabilities.restore("main", "saved")?;
    let old = identity(&before, "main");
    let new = identity(&restored, "main");
    assert!(restored.root.revision > before.root.revision);
    assert!(new.0 > old.0 && new.1 > old.1);
    assert_eq!((new.2, new.3), (old.2, old.3));
    assert_eq!(restored.root.branches["main"].head, saved.head);
    assert!(database.query("SELECT count(*) FROM items").is_err());
    assert!(database.execute("INSERT INTO items VALUES(3)").is_err());
    let fresh = open(&authority, "main", &cache.path().join("fresh"))?;
    assert_eq!(count(&fresh)?, 1);
    fresh.execute("INSERT INTO items VALUES(4)")?;
    assert!(authority.snapshot()?.root.branches["main"].sequence > old.2);
    Ok(())
}

#[test]
fn immutable_snapshots_and_invalid_reference_names_are_rejected() -> Result<()> {
    let (_, authority, capabilities) = setup()?;
    let saved = capabilities.snapshot("main", "saved")?;
    let revision = authority.snapshot()?.root.revision;
    assert!(capabilities.snapshot("main", "saved").is_err());
    assert_eq!(authority.snapshot()?.root.snapshots["saved"], saved);
    for name in ["", "../escape", "a/b", "a.b", "a b", "分支", "a\0b"] {
        assert!(capabilities.branch("main", name).is_err());
        assert!(capabilities.snapshot("main", name).is_err());
        assert!(capabilities.clone("saved", name).is_err());
        assert!(capabilities.delete_branch(name).is_err());
        assert!(capabilities.delete_snapshot(name).is_err());
    }
    assert!(capabilities.branch("main", &"x".repeat(129)).is_err());
    assert!(capabilities.clone("missing", "new").is_err());
    assert!(capabilities.branch("main", "main").is_err());
    assert_eq!(authority.snapshot()?.root.revision, revision);
    Ok(())
}

#[test]
fn supplied_points_must_be_authentic_and_remain_reachable() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let saved = capabilities.snapshot("main", "saved")?;
    let mut forged = saved.clone();
    forged.id.push_str("forged");
    assert!(capabilities.restore_point("main", &forged).is_err());
    forged = saved.clone();
    forged.wal = None;
    assert!(capabilities.restore_point("main", &forged).is_err());
    capabilities.retain_after("main", saved.sequence + 1)?;
    let before = authority.snapshot()?;
    capabilities.restore_point("main", &saved)?;
    assert_eq!(authority.snapshot()?.root.snapshots["saved"], saved);
    assert_eq!(authority.snapshot()?.root.objects, before.root.objects);
    capabilities.delete_snapshot("saved")?;
    let revision = authority.snapshot()?.root.revision;
    assert!(capabilities.restore_point("main", &saved).is_err());
    assert_eq!(authority.snapshot()?.root.revision, revision);
    assert!(capabilities.pitr("main", saved.sequence).is_err());
    Ok(())
}

#[test]
fn pitr_restores_exact_commits_before_and_after_checkpoint() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let database = open(&authority, "main", &cache.path().join("writer"))?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let before_checkpoint = authority.snapshot()?.root.branches["main"].sequence;
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(2)")?;
    let after_checkpoint = authority.snapshot()?.root.branches["main"].sequence;
    database.checkpoint()?;
    database.execute("INSERT INTO items VALUES(3)")?;
    for (sequence, expected, name) in [
        (before_checkpoint, 1, "before"),
        (after_checkpoint, 2, "after"),
    ] {
        let point = capabilities.pitr("main", sequence)?;
        capabilities.branch("main", name)?;
        capabilities.restore_point(name, &point)?;
        let recovered = open(&authority, name, &cache.path().join(name))?;
        assert_eq!(count(&recovered)?, expected);
    }
    assert_eq!(count(&database)?, 3);
    let current = authority.snapshot()?.root.branches["main"].sequence;
    assert!(capabilities.pitr("main", current + 1).is_err());
    capabilities.retain_after("main", after_checkpoint)?;
    assert!(capabilities.pitr("main", before_checkpoint).is_err());
    assert_eq!(
        capabilities.pitr("main", after_checkpoint)?.sequence,
        after_checkpoint
    );
    Ok(())
}

#[test]
fn time_resolution_bounds_and_same_time_sequence_are_exact() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1); INSERT INTO items VALUES(2)")?;
    authority.transact(None, |root| {
        let history = &mut root.branches.get_mut("main").unwrap().history;
        assert!(history.len() >= 3);
        for (index, point) in history.iter_mut().enumerate() {
            point.timestamp_ms = if index == 0 { 100 } else { 200 };
        }
        Ok(())
    })?;
    let history = authority.snapshot()?.root.branches["main"].history.clone();
    assert!(capabilities.resolve_time("main", 99).is_err());
    assert!(capabilities.resolve_time("main", u64::MAX).is_err());
    assert_eq!(
        capabilities.resolve_time("main", 201)?,
        *history.last().unwrap()
    );
    assert_eq!(
        capabilities.resolve_time("main", 150)?.sequence,
        history[0].sequence
    );
    assert_eq!(
        capabilities.resolve_time("main", 200)?.sequence,
        history.last().unwrap().sequence
    );
    capabilities.retain_after("main", history[1].sequence)?;
    assert!(capabilities.resolve_time("main", 150).is_err());
    assert_eq!(capabilities.pitr("main", history[1].sequence)?, history[1]);
    Ok(())
}

#[test]
fn cas_retry_revalidates_deleted_source_and_immutable_snapshot_absence() -> Result<()> {
    let (store, authority, capabilities) = setup()?;
    capabilities.branch("main", "source")?;
    let competitor = Authority::open(store.clone(), authority.namespace().clone())?;
    *store.hook.lock().unwrap() = Some(Box::new(move || {
        Capabilities::new(competitor).delete_branch("source")?;
        Ok(())
    }));
    assert!(capabilities.branch("source", "target").is_err());
    assert!(!authority.snapshot()?.root.branches.contains_key("target"));
    let competitor = Authority::open(store.clone(), authority.namespace().clone())?;
    *store.hook.lock().unwrap() = Some(Box::new(move || {
        Capabilities::new(competitor).snapshot("main", "race")?;
        Ok(())
    }));
    assert!(capabilities.snapshot("main", "race").is_err());
    assert!(authority.snapshot()?.root.snapshots.contains_key("race"));
    Ok(())
}

#[test]
fn cas_retry_rejects_point_removed_between_read_and_publication() -> Result<()> {
    let (store, authority, capabilities) = setup()?;
    let point = capabilities.snapshot("main", "saved")?;
    capabilities.branch("main", "target")?;
    let before = authority.snapshot()?;
    let competitor = Authority::open(store.clone(), authority.namespace().clone())?;
    *store.hook.lock().unwrap() = Some(Box::new(move || {
        Capabilities::new(competitor).delete_snapshot("saved")?;
        Ok(())
    }));
    assert!(capabilities.restore_point("target", &point).is_err());
    assert_eq!(
        identity(&authority.snapshot()?, "target"),
        identity(&before, "target")
    );
    Ok(())
}

#[test]
fn deleted_and_recreated_branch_does_not_revalidate_old_sql_session() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let main = open(&authority, "main", &cache.path().join("main"))?;
    main.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    capabilities.branch("main", "feature")?;
    let feature = open(&authority, "feature", &cache.path().join("old"))?;
    feature.execute("INSERT INTO items VALUES(2)")?;
    let previous = authority.snapshot()?.root.branches["feature"].incarnation;
    capabilities.delete_branch("feature")?;
    assert!(feature.query("SELECT count(*) FROM items").is_err());
    let recreated = capabilities.branch("main", "feature")?;
    assert!(recreated.root.branches["feature"].incarnation > previous);
    assert!(feature.query("SELECT count(*) FROM items").is_err());
    assert!(feature.execute("INSERT INTO items VALUES(3)").is_err());
    let fresh = open(&authority, "feature", &cache.path().join("fresh"))?;
    assert_eq!(count(&fresh)?, 1);
    Ok(())
}

#[test]
fn empty_snapshot_clone_does_not_invent_a_durable_commit() -> Result<()> {
    let (_, _, capabilities) = setup()?;
    let saved = capabilities.snapshot("main", "empty")?;
    assert_eq!(saved.sequence, 0);
    capabilities.clone("empty", "copy")?;
    assert!(capabilities.pitr("copy", 0).is_err());
    assert!(capabilities.resolve_time("copy", 0).is_err());
    Ok(())
}

#[test]
fn restored_snapshot_clone_tracks_actual_commit_not_high_water() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let database = open(&authority, "main", &cache.path().join("old"))?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let original = capabilities.snapshot("main", "original")?;
    database.execute("INSERT INTO items VALUES(2); INSERT INTO items VALUES(3)")?;
    let high_water = authority.snapshot()?.root.branches["main"].sequence;
    assert!(high_water > original.sequence);
    let restored = capabilities.restore("main", "original")?;
    assert_eq!(restored.root.branches["main"].sequence, high_water);
    assert_eq!(
        restored.root.branches["main"].current_sequence,
        original.sequence
    );
    let saved = capabilities.snapshot("main", "restored")?;
    assert_eq!(saved.sequence, original.sequence);
    assert_eq!(saved.timestamp_ms, original.timestamp_ms);
    capabilities.clone("restored", "copy")?;
    assert_eq!(
        capabilities.pitr("copy", original.sequence)?.sequence,
        original.sequence
    );
    assert!(capabilities.pitr("copy", high_water).is_err());
    let cloned = open(&authority, "copy", &cache.path().join("copy"))?;
    assert_eq!(count(&cloned)?, 1);
    let fresh = open(&authority, "main", &cache.path().join("fresh"))?;
    fresh.execute("INSERT INTO items VALUES(4)")?;
    let reference = authority.snapshot()?.root.branches["main"].clone();
    assert!(reference.sequence > high_water);
    assert_eq!(reference.current_sequence, reference.sequence);
    assert_eq!(reference.current_commit_ms, reference.last_commit_ms);
    Ok(())
}

#[test]
fn newer_cross_branch_restore_merges_high_water_without_rewinding() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let main = open(&authority, "main", &cache.path().join("main"))?;
    main.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    capabilities.branch("main", "early")?;
    main.execute("INSERT INTO items VALUES(2); INSERT INTO items VALUES(3)")?;
    let point = capabilities.snapshot("main", "newer")?;
    let before = authority.snapshot()?.root.branches["early"].clone();
    assert!(before.sequence < point.sequence);
    let restored = capabilities.restore("early", "newer")?;
    let reference = &restored.root.branches["early"];
    assert_eq!(reference.current_sequence, point.sequence);
    assert_eq!(reference.sequence, before.sequence.max(point.sequence));
    assert_eq!(
        reference.last_commit_ms,
        before.last_commit_ms.max(point.timestamp_ms)
    );
    let early = open(&authority, "early", &cache.path().join("early"))?;
    assert_eq!(count(&early)?, 3);
    early.execute("INSERT INTO items VALUES(4)")?;
    assert!(authority.snapshot()?.root.branches["early"].sequence > point.sequence);
    Ok(())
}

#[test]
fn idle_time_after_last_commit_resolves_within_bounded_clock() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let (_, authority, capabilities) = setup()?;
    let database = open(&authority, "main", cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let last = authority.snapshot()?.root.branches["main"]
        .history
        .last()
        .unwrap()
        .clone();
    let target = last.timestamp_ms.checked_add(1).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let now_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        if now_ms >= target {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "clock did not advance within bound"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(capabilities.resolve_time("main", target)?, last);
    assert!(capabilities.resolve_time("main", u64::MAX).is_err());
    Ok(())
}
