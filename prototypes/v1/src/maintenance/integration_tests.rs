//! End-to-end checks for the scheduled units.
//!
//! The scheduling rules are tested against a fake target in `tests`; these tests take the one
//! step the fake cannot: a scheduler decision is executed by the real `RemoteDatabase` entry
//! point, against a real store, and the WAL index is inspected before and after. A regression
//! that queued the right lane but ran nothing, ran the other lane, or ran the unit on the
//! request path fails here and not in the rule tests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use super::budget::MaintenanceBudget;
use super::policy::{MaintenancePolicy, Watermark};
use super::{MaintenanceLane, MaintenanceScheduler, MaintenanceTarget, PreSuspendReason};
use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use crate::session::{Budget, RequestDeadline};
use crate::wal::RemoteWal;

const MIB: u64 = 1024 * 1024;

/// A lane whose high mark the test WAL crosses immediately and whose first slot is
/// `period` after the database opened.
fn armed_lane(period: Duration, phase: Duration) -> Watermark {
    Watermark {
        low_bytes: 0,
        high_bytes: 1,
        period,
        phase,
    }
}

fn policy_with(
    checkpoint: Watermark,
    index_compact: Watermark,
    cooldown: Duration,
    blocked_backoff: Duration,
) -> MaintenancePolicy {
    MaintenancePolicy::for_tests(true, checkpoint, index_compact, cooldown, blocked_backoff)
}

/// The compact slot is still ahead of the checkpoint slot, so the first pass can only pick
/// the checkpoint lane; the second pass can only pick the compaction lane.
fn two_lane_policy(cooldown: Duration, blocked_backoff: Duration) -> MaintenancePolicy {
    policy_with(
        armed_lane(Duration::from_secs(2), Duration::ZERO),
        armed_lane(Duration::from_secs(3), Duration::from_secs(1)),
        cooldown,
        blocked_backoff,
    )
}

struct Fixture {
    database: Arc<RemoteDatabase>,
    store: Arc<MemoryObjectStore>,
    namespace: Namespace,
    wal: RemoteWal,
    _cache: tempfile::TempDir,
}

impl Fixture {
    fn open(name: &str) -> Result<Self> {
        let cache = tempfile::tempdir()?;
        let namespace = Namespace::new("maintenance", name)?;
        let store = Arc::new(MemoryObjectStore::default());
        let database = Arc::new(RemoteDatabase::open(
            store.clone(),
            namespace.clone(),
            cache.path(),
            0,
        )?);
        database.execute("CREATE TABLE entries(value INTEGER)")?;
        for value in 0..4 {
            database.execute(&format!("INSERT INTO entries VALUES({value})"))?;
        }
        let wal = RemoteWal::new(database.storage().store());
        Ok(Self {
            database,
            store,
            namespace,
            wal,
            _cache: cache,
        })
    }

    fn target(&self) -> Arc<dyn MaintenanceTarget> {
        self.database.clone()
    }

    fn opened(&self) -> Instant {
        self.database.maintenance_ledger().opened()
    }

    fn counters(&self) -> super::ledger::MaintenanceLedgerCounters {
        self.database.maintenance_ledger().counters()
    }

    fn values(&self) -> Result<String> {
        Ok(self.database.query("SELECT count(*) FROM entries")?[0][0].to_string())
    }
}

/// Reopen the same namespace on a fresh cache and a fresh engine. This is the only check that
/// sees what a cold start sees: an in-memory page cache or a live connection can hide a
/// checkpoint that published the wrong image or a WAL reset whose baseline is not replayable.
fn reopen(
    store: &Arc<MemoryObjectStore>,
    namespace: &Namespace,
) -> Result<(RemoteDatabase, tempfile::TempDir)> {
    let cache = tempfile::tempdir()?;
    let database = RemoteDatabase::open(store.clone(), namespace.clone(), cache.path(), 0)?;
    Ok((database, cache))
}

fn assert_same_index(
    before: &crate::wal::WalMetadata,
    after: &crate::wal::WalMetadata,
    reason: &str,
) {
    assert_eq!(after.last_lsn, before.last_lsn, "{reason}");
    assert_eq!(
        after.replay_baseline_lsn, before.replay_baseline_lsn,
        "{reason}"
    );
    assert_eq!(after.entries.len(), before.entries.len(), "{reason}");
    for (before, after) in before.entries.iter().zip(&after.entries) {
        assert_eq!(after.lsn, before.lsn, "{reason}");
        assert_eq!(after.hash, before.hash, "{reason}");
    }
}

/// The two lanes run in their own slots and are executed by the real entry points: the Full
/// checkpoint neither rebases nor rewrites the index, and the compaction that follows advances
/// the baseline and collapses the index. A cold reopen after both proves the published image
/// and the reset baseline are replayable.
#[test]
fn scheduled_lanes_run_through_the_real_entry_points_and_compaction_reclaims_the_wal() -> Result<()>
{
    let fixture = Fixture::open("scheduled_compact")?;
    let before = fixture.wal.metadata(&fixture.namespace)?;
    assert!(
        before.entries.len() >= 2,
        "the fixture must publish more than the baseline entry"
    );
    let scheduler = MaintenanceScheduler::new(
        two_lane_policy(Duration::from_secs(60), Duration::from_secs(60)),
        MaintenanceBudget::new(1, 1, 4, 64 * MIB, Duration::from_secs(60)),
    );
    let checkpoint_slot = fixture.opened() + Duration::from_secs(2);
    let compaction_slot = fixture.opened() + Duration::from_secs(4);

    scheduler.observe(&fixture.target(), checkpoint_slot)?;
    let queued = fixture.counters();
    assert_eq!(queued.attempts, 1);
    assert_eq!(
        queued.blocks.get("unconfirmed_wal"),
        Some(&1),
        "the compaction lane is refused by its precondition before its slot, and the refusal \
         is recorded"
    );
    assert_eq!(
        queued.executions, 0,
        "observing may only queue, never run a unit"
    );
    let summary = scheduler.drain(1, checkpoint_slot);
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.refused, 0);

    let after_checkpoint = fixture.wal.metadata(&fixture.namespace)?;
    assert_same_index(
        &before,
        &after_checkpoint,
        "a Full checkpoint does not rewrite the WAL index",
    );
    assert_eq!(fixture.counters().executions, 1);
    assert_eq!(fixture.counters().completed, 1);

    scheduler.observe(&fixture.target(), compaction_slot)?;
    let queued = fixture.counters();
    assert_eq!(queued.attempts, 2);
    assert_eq!(
        queued.executions, 1,
        "the checkpoint lane is in its cooldown, only the compaction lane was picked"
    );
    let summary = scheduler.drain(1, compaction_slot);
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.refused, 0);
    assert_eq!(fixture.counters().failed, 0);

    let compacted = fixture.wal.metadata(&fixture.namespace)?;
    // The reset is itself a durable WAL record, so the last LSN moves by one; what matters is
    // that the index is rebased onto it and keeps nothing below it.
    assert!(compacted.last_lsn >= after_checkpoint.last_lsn);
    assert!(
        compacted.replay_baseline_lsn > after_checkpoint.replay_baseline_lsn,
        "compaction must advance the baseline"
    );
    assert_eq!(compacted.replay_baseline_lsn, compacted.last_lsn);
    assert_eq!(
        compacted.entries.len(),
        1,
        "compaction keeps only the baseline entry"
    );
    assert_eq!(compacted.entries[0].lsn, compacted.replay_baseline_lsn);
    assert_eq!(fixture.values()?, "4");
    assert_eq!(fixture.counters().executions, 2);
    assert_eq!(fixture.counters().completed, 2);

    // A new frame makes the checkpoint lane armed again. Its own slot has passed, so only the
    // cooldown can keep the attempt counter from moving; the compaction lane is still inside
    // its period (and unconfirmed again), so it cannot be picked either.
    fixture.database.execute("INSERT INTO entries VALUES(5)")?;
    let later = checkpoint_slot + Duration::from_secs(3);
    scheduler.observe(&fixture.target(), later)?;
    assert_eq!(
        fixture.counters().attempts,
        2,
        "a lane that just ran must stay in its cooldown"
    );
    let summary = scheduler.drain(1, later);
    assert_eq!(summary.completed, 0);
    assert_eq!(fixture.counters().executions, 2);

    let store = fixture.store.clone();
    let namespace = fixture.namespace.clone();
    drop(fixture.database);
    let (database, _cache) = reopen(&store, &namespace)?;
    assert_eq!(
        database.query("SELECT count(*) FROM entries")?[0][0].to_string(),
        "5",
        "a cold start replays the reset baseline and the frame appended after it"
    );
    let replayed = RemoteWal::new(database.storage().store()).metadata(&fixture.namespace)?;
    assert_eq!(replayed.replay_baseline_lsn, compacted.replay_baseline_lsn);
    assert!(replayed.last_lsn > compacted.last_lsn);
    assert!(replayed.entries.len() >= 2);
    Ok(())
}

/// A refused unit is skipped as a whole. The only window request admitted the checkpoint; the
/// compaction queued behind it is refused, nothing starts, and the index is byte-for-byte the
/// one the checkpoint left. The block covers the whole database, so the next pass does not
/// retry either lane.
#[test]
fn a_budget_refusal_skips_the_queued_unit_and_leaves_the_wal_in_place() -> Result<()> {
    let fixture = Fixture::open("scheduled_budget")?;
    let scheduler = MaintenanceScheduler::new(
        two_lane_policy(Duration::from_secs(60), Duration::from_secs(60)),
        MaintenanceBudget::new(1, 1, 1, 64 * MIB, Duration::from_secs(60)),
    );
    let checkpoint_slot = fixture.opened() + Duration::from_secs(2);
    let compaction_slot = fixture.opened() + Duration::from_secs(4);

    scheduler.observe(&fixture.target(), checkpoint_slot)?;
    assert_eq!(scheduler.drain(1, checkpoint_slot).completed, 1);
    let settled = fixture.wal.metadata(&fixture.namespace)?;

    scheduler.observe(&fixture.target(), compaction_slot)?;
    assert_eq!(
        fixture.counters().attempts,
        2,
        "the compaction lane was picked before the budget was charged"
    );
    let summary = scheduler.drain(1, compaction_slot);
    assert_eq!(summary.refused, 1);
    assert_eq!(summary.completed, 0);

    let counters = fixture.counters();
    assert_eq!(
        counters.executions, 1,
        "the refused unit never started; only the checkpoint is recorded"
    );
    assert_eq!(counters.refused, 1);
    assert_eq!(counters.blocks.get("budget"), Some(&1));

    let after = fixture.wal.metadata(&fixture.namespace)?;
    assert_same_index(&settled, &after, "a refusal must not rewrite the WAL");
    assert_eq!(fixture.values()?, "4");
    Ok(())
}

/// The backfill volume is charged against the byte budget before anything starts: a byte
/// budget below the WAL size refuses the unit, and the block backoff (which outlives the
/// cooldown) keeps the whole database ineligible after the cooldown has expired.
#[test]
fn a_backfill_charge_over_the_byte_budget_skips_the_unit() -> Result<()> {
    let fixture = Fixture::open("scheduled_bytes")?;
    let before = fixture.wal.metadata(&fixture.namespace)?;
    let cooldown = Duration::from_secs(5);
    let backoff = Duration::from_secs(30);
    let scheduler = MaintenanceScheduler::new(
        policy_with(
            armed_lane(Duration::from_secs(2), Duration::ZERO),
            armed_lane(Duration::from_secs(3), Duration::from_secs(1)),
            cooldown,
            backoff,
        ),
        // A non-empty WAL cannot be charged in full against a zero byte window.
        MaintenanceBudget::new(1, 1, 4, 0, Duration::from_secs(60)),
    );
    let slot = fixture.opened() + Duration::from_secs(2);

    scheduler.observe(&fixture.target(), slot)?;
    assert_eq!(fixture.counters().attempts, 1);
    let summary = scheduler.drain(1, slot);
    assert_eq!(summary.refused, 1);
    assert_eq!(summary.completed, 0);
    assert_eq!(fixture.counters().executions, 0);
    assert_eq!(fixture.counters().blocks.get("budget"), Some(&1));
    let after = fixture.wal.metadata(&fixture.namespace)?;
    assert_same_index(&before, &after, "a byte refusal must not touch the WAL");

    // Six seconds after the pick the five-second cooldown is over; only the thirty-second
    // block backoff can keep the checkpoint lane from being picked again.
    let later = slot + Duration::from_secs(6);
    scheduler.observe(&fixture.target(), later)?;
    assert_eq!(
        fixture.counters().attempts,
        1,
        "a budget block must outlive the cooldown and back the database off"
    );
    Ok(())
}

/// The admin entry point is not the scheduled lane. It may compact while the durable tail is
/// not yet published, because the truncate checkpoint is what confirms that tail; the reset
/// baseline then carries the rows. A cold reopen is the check that the reset did not drop the
/// tail, and the index check is the check that it did reclaim the old entries.
#[test]
fn the_admin_entry_point_compacts_an_unpublished_tail_without_losing_it() -> Result<()> {
    let fixture = Fixture::open("admin_tail")?;
    fixture.database.execute("INSERT INTO entries VALUES(4)")?;
    assert!(
        fixture.target().signals()?.unconfirmed_wal,
        "the fixture must carry an unpublished tail for this test to mean anything"
    );
    let before = fixture.wal.metadata(&fixture.namespace)?;

    fixture.database.compact_wal()?;

    let compacted = fixture.wal.metadata(&fixture.namespace)?;
    assert!(
        compacted.replay_baseline_lsn > before.replay_baseline_lsn,
        "the admin path must advance the baseline"
    );
    assert_eq!(compacted.replay_baseline_lsn, compacted.last_lsn);
    assert_eq!(compacted.entries.len(), 1);
    assert_eq!(
        fixture.values()?,
        "5",
        "the unpublished row survived the reset"
    );

    let store = fixture.store.clone();
    let namespace = fixture.namespace.clone();
    drop(fixture.database);
    let (database, _cache) = reopen(&store, &namespace)?;
    assert_eq!(
        database.query("SELECT count(*) FROM entries")?[0][0].to_string(),
        "5",
        "a cold start replays the baseline that absorbed the unpublished tail"
    );
    Ok(())
}

/// A reader that still needs the old WAL makes the scheduled compaction fail after dispatch,
/// and the failure is recorded without truncating the index. The reader keeps its snapshot.
#[test]
fn a_long_reader_blocks_the_scheduled_compaction_and_the_wal_survives() -> Result<()> {
    let fixture = Fixture::open("scheduled_reader")?;
    let scheduler = MaintenanceScheduler::new(
        two_lane_policy(Duration::from_secs(60), Duration::from_secs(60)),
        MaintenanceBudget::new(1, 1, 4, 64 * MIB, Duration::from_secs(60)),
    );
    let checkpoint_slot = fixture.opened() + Duration::from_secs(2);
    let compaction_slot = fixture.opened() + Duration::from_secs(4);

    scheduler.observe(&fixture.target(), checkpoint_slot)?;
    assert_eq!(scheduler.drain(1, checkpoint_slot).completed, 1);
    let settled = fixture.wal.metadata(&fixture.namespace)?;

    let reader = fixture.database.session()?;
    reader.execute("BEGIN")?;
    assert_eq!(reader.query("SELECT value FROM entries")?.len(), 4);
    assert!(reader.in_transaction()?);

    scheduler.observe(&fixture.target(), compaction_slot)?;
    let summary = scheduler.drain(1, compaction_slot);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.completed, 0);
    let counters = fixture.counters();
    assert_eq!(counters.failed, 1);
    assert_eq!(counters.blocks.get("long_reader"), Some(&1));

    let after = fixture.wal.metadata(&fixture.namespace)?;
    assert_same_index(&settled, &after, "a blocked compaction keeps the old WAL");
    assert!(reader.in_transaction()?);
    assert_eq!(reader.query("SELECT value FROM entries")?.len(), 4);
    assert_eq!(fixture.values()?, "4");
    Ok(())
}

/// The suspend-time unit takes the same precondition path as the scheduled one: a Full
/// checkpoint first makes the publication current, then a reader blocks the compaction, which
/// is reported as a failed unit with the long-reader block already recorded by the real
/// preflight. Nothing is truncated and the rows stay readable.
#[test]
fn a_suspend_pre_tidy_blocked_by_a_reader_records_the_long_reader() -> Result<()> {
    let fixture = Fixture::open("suspend_reader")?;
    let scheduler = MaintenanceScheduler::new(
        policy_with(
            armed_lane(Duration::from_secs(1), Duration::ZERO),
            armed_lane(Duration::from_secs(2), Duration::from_secs(1)),
            Duration::from_millis(1),
            Duration::from_millis(1),
        ),
        MaintenanceBudget::new(1, 1, 16, 64 * MIB, Duration::from_secs(60)),
    );

    let checkpoint = scheduler.pre_suspend(&fixture.target());
    assert_eq!(
        checkpoint.reason,
        PreSuspendReason::Completed,
        "{checkpoint:?}"
    );
    assert_eq!(checkpoint.lane, Some(MaintenanceLane::Checkpoint));
    let settled = fixture.wal.metadata(&fixture.namespace)?;

    let reader = fixture.database.session()?;
    reader.execute("BEGIN")?;
    assert_eq!(reader.query("SELECT value FROM entries")?.len(), 4);

    let compact = scheduler.pre_suspend(&fixture.target());
    assert_eq!(compact.reason, PreSuspendReason::Failed, "{compact:?}");
    assert_eq!(compact.lane, Some(MaintenanceLane::IndexCompact));
    assert!(compact.started);
    let counters = fixture.counters();
    assert_eq!(counters.failed, 1);
    assert_eq!(counters.completed, 1);
    assert_eq!(counters.blocks.get("long_reader"), Some(&1));

    let after = fixture.wal.metadata(&fixture.namespace)?;
    assert_same_index(&settled, &after, "a blocked pre-tidy keeps the old WAL");
    assert!(reader.in_transaction()?);
    assert_eq!(reader.query("SELECT value FROM entries")?.len(), 4);
    assert_eq!(fixture.values()?, "4");
    Ok(())
}

/// A checkpoint is background work. An operation that already holds the operations gate
/// must not wait for the backfill, and the checkpoint must not wait for that operation
/// before it may start.
#[test]
fn a_checkpoint_does_not_queue_behind_the_foreground_gate() -> Result<()> {
    let fixture = Fixture::open("checkpoint_gate")?;
    let deadline = RequestDeadline::new(&Budget::trusted_default())?;
    let operation = fixture.database.operation_until(&deadline, false)?;

    let (sender, receiver) = std::sync::mpsc::channel();
    let database = fixture.database.clone();
    let worker = std::thread::spawn(move || {
        let _ = sender.send(database.checkpoint());
    });
    let observed = receiver.recv_timeout(Duration::from_secs(5));
    // A timeout means the checkpoint waited for the foreground gate: release it so the
    // worker can finish before the assertion below reports the failure.
    if observed.is_err() {
        drop(operation);
    }
    worker.join().expect("checkpoint thread");
    let result = observed.expect("a checkpoint must not wait for the operations gate");
    result.expect("checkpoint");
    Ok(())
}

/// A commit that lands while the main database publication uploads is newer than the
/// checkpoint position the publication froze. It must survive the settle and the reopen;
/// it must not turn the publication into a failure or make the commit wait for the upload.
#[test]
fn a_commit_that_lands_during_a_publication_is_not_lost() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let namespace = Namespace::new("maintenance", "commit_during_publication")?;
    let store = Arc::new(GatedStore::default());
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        namespace.clone(),
        cache.path(),
        0,
    )?);
    database.execute("CREATE TABLE entries(value INTEGER)")?;
    for value in 0..4 {
        database.execute(&format!("INSERT INTO entries VALUES({value})"))?;
    }

    store.arm();
    let checkpoint = {
        let database = database.clone();
        std::thread::spawn(move || database.checkpoint())
    };
    assert!(
        store.wait_until_blocked(Duration::from_secs(10)),
        "the checkpoint never reached the segment upload"
    );

    let session = database.session()?;
    session.execute("INSERT INTO entries VALUES(5)")?;
    store.release();
    checkpoint.join().expect("checkpoint thread")?;

    // A reopen sees only what the publication settled: the concurrent commit has to be in
    // the WAL and replay on a cold engine with a fresh cache.
    let reopen_cache = tempfile::tempdir()?;
    let reopened = RemoteDatabase::open(store, namespace, reopen_cache.path(), 0)?;
    assert_eq!(
        reopened.query("SELECT count(*) FROM entries")?[0][0].to_string(),
        "5",
        "a commit newer than the checkpoint position stays in the WAL and replays"
    );
    Ok(())
}

#[derive(Default)]
struct GatedStore {
    inner: MemoryObjectStore,
    gate: std::sync::Mutex<Gate>,
    changed: std::sync::Condvar,
}

#[derive(Default)]
struct Gate {
    armed: bool,
    blocked: bool,
    released: bool,
}

impl GatedStore {
    fn arm(&self) {
        self.gate.lock().expect("upload gate").armed = true;
    }

    fn wait_until_blocked(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut gate = self.gate.lock().expect("upload gate");
        while !gate.blocked {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            gate = self
                .changed
                .wait_timeout(gate, remaining)
                .expect("upload gate")
                .0;
        }
        true
    }

    fn release(&self) {
        self.gate.lock().expect("upload gate").released = true;
        self.changed.notify_all();
    }
}

impl ObjectStore for GatedStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.contains("/segments/") {
            let mut gate = self.gate.lock().expect("upload gate");
            if gate.armed {
                gate.armed = false;
                gate.blocked = true;
                self.changed.notify_all();
                while !gate.released {
                    gate = self.changed.wait(gate).expect("upload gate");
                }
            }
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
