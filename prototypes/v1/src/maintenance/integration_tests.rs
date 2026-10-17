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
use super::{MaintenanceScheduler, MaintenanceTarget};
use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
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

fn policy(checkpoint: Watermark, index_compact: Watermark) -> MaintenancePolicy {
    MaintenancePolicy::for_tests(
        true,
        checkpoint,
        index_compact,
        Duration::from_secs(60),
        Duration::from_secs(60),
    )
}

struct Fixture {
    database: Arc<RemoteDatabase>,
    namespace: Namespace,
    wal: RemoteWal,
    _cache: tempfile::TempDir,
}

impl Fixture {
    fn open(name: &str) -> Result<Self> {
        let cache = tempfile::tempdir()?;
        let namespace = Namespace::new("maintenance", name)?;
        let database = Arc::new(RemoteDatabase::open(
            Arc::new(MemoryObjectStore::default()),
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

/// The two lanes run in their own slots and are executed by the real entry points: the Full
/// checkpoint neither rebases nor rewrites the index, and the compaction that follows advances
/// the baseline and collapses the index. The commit between the pick and the drain proves that
/// only `drain` executes a unit.
#[test]
fn scheduled_lanes_run_through_the_real_entry_points_and_compaction_reclaims_the_wal() -> Result<()>
{
    let fixture = Fixture::open("scheduled_compact")?;
    let before = fixture.wal.metadata(&fixture.namespace)?;
    assert!(
        before.entries.len() >= 2,
        "the fixture must publish more than the baseline entry"
    );
    // The checkpoint slot comes first; the compaction slot is still in the future and its
    // precondition (no unconfirmed WAL) does not hold until the checkpoint has run.
    let scheduler = MaintenanceScheduler::new(
        policy(
            armed_lane(Duration::from_secs(2), Duration::ZERO),
            armed_lane(Duration::from_secs(3), Duration::from_secs(1)),
        ),
        MaintenanceBudget::new(1, 1, 4, 64 * MIB, Duration::from_secs(60)),
    );
    let checkpoint_slot = fixture.opened() + Duration::from_secs(2);
    let compaction_slot = fixture.opened() + Duration::from_secs(4);

    scheduler.observe(&fixture.target(), checkpoint_slot)?;
    assert_eq!(fixture.counters().attempts, 1);
    assert_eq!(
        fixture.counters().executions,
        0,
        "observing may only queue, never run a unit"
    );
    let summary = scheduler.drain(1, checkpoint_slot);
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.refused, 0);

    let after_checkpoint = fixture.wal.metadata(&fixture.namespace)?;
    assert_eq!(after_checkpoint.last_lsn, before.last_lsn);
    assert_eq!(
        after_checkpoint.replay_baseline_lsn, before.replay_baseline_lsn,
        "a Full checkpoint does not rebase the WAL index"
    );
    assert_eq!(after_checkpoint.entries.len(), before.entries.len());
    for (before, after) in before.entries.iter().zip(&after_checkpoint.entries) {
        assert_eq!(after.lsn, before.lsn);
        assert_eq!(
            after.hash, before.hash,
            "a Full checkpoint writes the image"
        );
    }
    assert_eq!(fixture.counters().executions, 1);
    assert_eq!(fixture.counters().completed, 1);

    scheduler.observe(&fixture.target(), compaction_slot)?;
    let queued = fixture.counters();
    assert_eq!(queued.attempts, 2);
    assert_eq!(
        queued.executions, 1,
        "the checkpoint lane is in its cooldown, only the compaction lane was picked"
    );

    fixture.database.execute("INSERT INTO entries VALUES(5)")?;
    assert_eq!(
        fixture.counters().executions,
        1,
        "a commit must not drain the maintenance queue"
    );
    let before_drain = fixture.wal.metadata(&fixture.namespace)?;
    assert!(before_drain.last_lsn > after_checkpoint.last_lsn);

    let summary = scheduler.drain(1, compaction_slot);
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.refused, 0);
    assert_eq!(fixture.counters().failed, 0);

    let after = fixture.wal.metadata(&fixture.namespace)?;
    // The reset is itself a durable WAL record, so the last LSN moves by one; what matters is
    // that the index is rebased onto it and keeps nothing below it.
    assert!(after.last_lsn >= before_drain.last_lsn);
    assert!(
        after.replay_baseline_lsn > after_checkpoint.replay_baseline_lsn,
        "compaction must advance the baseline"
    );
    assert_eq!(after.replay_baseline_lsn, after.last_lsn);
    assert_eq!(
        after.entries.len(),
        1,
        "compaction keeps only the baseline entry"
    );
    assert_eq!(after.entries[0].lsn, after.replay_baseline_lsn);
    assert_eq!(fixture.values()?, "5");
    assert_eq!(fixture.counters().executions, 2);
    assert_eq!(fixture.counters().completed, 2);

    // A new frame makes the compaction lane eligible again by watermark and by slot; only
    // the cooldown can keep the attempt counter from moving.
    fixture.database.execute("INSERT INTO entries VALUES(6)")?;
    let later = compaction_slot + Duration::from_secs(3);
    scheduler.observe(&fixture.target(), later)?;
    assert_eq!(
        fixture.counters().attempts,
        2,
        "a lane that just ran must stay in its cooldown"
    );
    let summary = scheduler.drain(1, later);
    assert_eq!(summary.completed, 0);
    assert_eq!(fixture.counters().executions, 2);
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
        policy(
            armed_lane(Duration::from_secs(2), Duration::ZERO),
            armed_lane(Duration::from_secs(3), Duration::from_secs(1)),
        ),
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
    assert_eq!(after.last_lsn, settled.last_lsn);
    assert_eq!(after.replay_baseline_lsn, settled.replay_baseline_lsn);
    assert_eq!(after.entries.len(), settled.entries.len());
    for (settled, after) in settled.entries.iter().zip(&after.entries) {
        assert_eq!(after.lsn, settled.lsn);
        assert_eq!(
            after.hash, settled.hash,
            "a refusal must not rewrite chunks"
        );
    }
    assert_eq!(fixture.values()?, "4");

    let later = compaction_slot + Duration::from_secs(3);
    scheduler.observe(&fixture.target(), later)?;
    assert_eq!(
        fixture.counters().attempts,
        2,
        "the budget block backs the database off instead of retrying every tick"
    );
    Ok(())
}
