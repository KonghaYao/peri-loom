use std::sync::Arc;

use anyhow::{ensure, Result};
use turso_core::{NativeIoRetirementGuard, IO};

use super::retirement::DatabaseRetirementGuard;
use super::RemoteDatabase;
use crate::maintenance::BlockReason;
use crate::session::{AdmissionError, SessionLane};

impl RemoteDatabase {
    /// Compaction preflight. A lane that holds a native read lock is a *long reader*, not
    /// contention: it is still using a snapshot, and a truncate checkpoint cannot backfill
    /// past the frame that snapshot depends on.
    ///
    /// The error stays `Conflict` exactly as before, because callers already branch on it.
    /// What is new is that the reason and the reader's age are recorded before the error is
    /// returned, so a review can tell "blocked by a reader of four minutes" from "the lane
    /// mutex was momentarily busy". Nothing here releases, truncates or rewrites anything:
    /// the WAL index is untouched, so the frames the reader needs stay published.
    pub(crate) fn preflight_compaction(&self) -> Result<()> {
        let lanes = self.lanes.lock().map_err(|_| AdmissionError::Quarantine)?;
        for lane in lanes.iter().filter_map(std::sync::Weak::upgrade) {
            if Arc::ptr_eq(&lane, &self.lane) {
                continue;
            }
            let connection = lane.connection.try_lock().map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => AdmissionError::Conflict,
                std::sync::TryLockError::Poisoned(_) => AdmissionError::Quarantine,
            })?;
            if connection
                .as_ref()
                .is_some_and(|connection| connection.get_pager().holds_read_lock())
            {
                drop(connection);
                drop(lanes);
                // Records the age of the reader that is blocking this compaction, so the
                // block below names both the reason and how long the reader has been there.
                let age = self.oldest_reader_age(std::time::Instant::now());
                crate::work_metrics::observe_reader_block(age);
                self.record_maintenance_block(BlockReason::LongReader);
                return Err(AdmissionError::Conflict.into());
            }
        }
        Ok(())
    }

    /// Age of the oldest session that currently holds a native read lock, and the session
    /// whose age that is. Zero when no session holds one, which is the state a truncate
    /// checkpoint needs.
    pub(crate) fn oldest_reader_age(&self, now: std::time::Instant) -> std::time::Duration {
        let Ok(lanes) = self.lanes.lock() else {
            return std::time::Duration::ZERO;
        };
        let age = lanes
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .filter(|lane| lane.holds_read_lock())
            .map(|lane| now.saturating_duration_since(lane.opened()))
            .max()
            .unwrap_or(std::time::Duration::ZERO);
        self.maintenance_ledger().readers().observe(age);
        age
    }

    pub(crate) fn register_lane(&self, lane: &Arc<SessionLane>) -> Result<()> {
        self.admission.healthy()?;
        let mut lanes = self.lanes.lock().map_err(|_| AdmissionError::Quarantine)?;
        self.admission.healthy()?;
        lanes.retain(|lane| lane.strong_count() > 0);
        lanes.push(Arc::downgrade(lane));
        Ok(())
    }

    pub(crate) fn collect_lanes(
        &self,
        guard: &DatabaseRetirementGuard<'_>,
    ) -> Result<Vec<Arc<SessionLane>>> {
        let expected: Arc<dyn IO> = self.io.clone();
        ensure!(
            Arc::ptr_eq(guard.io(), &expected),
            "foreign native retirement guard"
        );
        let lanes = self.lanes.lock().map_err(|_| AdmissionError::Quarantine)?;
        Ok(lanes.iter().filter_map(std::sync::Weak::upgrade).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{bail, Result};
    use turso_core::StepResult;

    use crate::database::RemoteDatabase;
    use crate::namespace::Namespace;
    use crate::object_store::MemoryObjectStore;
    use crate::session::{AdmissionError, UnsettledSqlOutcome};

    #[test]
    fn partial_backfill_classification_preserves_fixed_source_and_unsettled_marker() {
        let error = RemoteDatabase::require_complete_backfill(false).unwrap_err();
        assert_eq!(
            error.root_cause().to_string(),
            "checkpoint did not backfill the complete WAL"
        );
        assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert!(error.downcast_ref::<AdmissionError>().is_none());
        assert!(RemoteDatabase::require_complete_backfill(true).is_ok());
    }

    /// The correctness red line for W4: a long reader blocks reclamation, the reason is
    /// recorded, and the references that reader depends on are still there afterwards.
    ///
    /// The assertion that matters is not that compaction failed — the test above already
    /// covers that — but that the WAL index is byte-for-byte the one the reader was using:
    /// a "record the reason and reclaim anyway" version of this change would truncate it and
    /// fail the index comparison, and a version that stopped recording the reason would fail
    /// the block counter and the reader age.
    #[test]
    fn a_long_reader_blocks_compaction_records_why_and_keeps_the_old_wal() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let namespace = Namespace::new("compaction", "long_reader")?;
        let database = Arc::new(RemoteDatabase::open(
            Arc::new(MemoryObjectStore::default()),
            namespace.clone(),
            directory.path(),
            0,
        )?);
        database.execute("CREATE TABLE ledger(value INTEGER)")?;
        let reader = database.session()?;
        reader.execute("BEGIN")?;
        reader.query("SELECT value FROM ledger")?;
        database.execute("INSERT INTO ledger VALUES(1)")?;
        // Long enough that the reader's age is a measurable number of milliseconds rather
        // than a rounding to zero.
        std::thread::sleep(std::time::Duration::from_millis(20));

        let wal = crate::wal::RemoteWal::new(database.storage().store());
        let index_before = wal.metadata(&namespace)?;
        let lsn_before = database.io().wal_lsn();
        let before = database.maintenance_ledger().counters();
        assert_eq!(before.blocks.get("long_reader"), None);
        assert_eq!(before.oldest_reader_peak_ms, 0);
        assert!(
            !index_before.entries.is_empty(),
            "the WAL has frames to keep"
        );

        let error = database.compact_wal().unwrap_err();
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Conflict)
        );
        assert!(!database.writer_admission_status()?.quarantined);

        let after = database.maintenance_ledger().counters();
        assert_eq!(
            after.blocks.get("long_reader"),
            Some(&1),
            "the reason the reclamation did not happen must be recorded"
        );
        assert_eq!(
            after.executions, 1,
            "the unit ran and was refused by the reader, so a block is not free"
        );
        assert!(
            after.oldest_reader_peak_ms >= 20,
            "the age of the reader that blocked reclamation is recorded, got {} ms",
            after.oldest_reader_peak_ms
        );

        let index_after = wal.metadata(&namespace)?;
        assert_eq!(
            index_after.replay_baseline_lsn, index_before.replay_baseline_lsn,
            "the WAL index must not be rebased while a reader still needs it"
        );
        assert_eq!(index_after.last_lsn, index_before.last_lsn);
        assert_eq!(index_after.entries.len(), index_before.entries.len());
        assert_eq!(database.io().wal_lsn(), lsn_before);

        // The reader still reads its own snapshot, and the committed write is still durable
        // for everyone else.
        assert!(reader.in_transaction()?);
        assert!(reader.query("SELECT value FROM ledger")?.is_empty());
        assert_eq!(
            database.query("SELECT value FROM ledger")?[0][0].to_string(),
            "1"
        );

        drop(reader);
        database.compact_wal()?;
        assert_eq!(
            database.query("SELECT value FROM ledger")?[0][0].to_string(),
            "1"
        );
        Ok(())
    }

    #[test]
    fn pinned_reader_rejects_compaction_before_dispatch_without_quarantine() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let database = Arc::new(RemoteDatabase::open(
            Arc::new(MemoryObjectStore::default()),
            Namespace::new("compaction", "reader_preflight")?,
            directory.path(),
            0,
        )?);
        database.execute("CREATE TABLE ledger(value INTEGER)")?;
        let reader = database.session()?;
        reader.execute("BEGIN")?;
        reader.query("SELECT value FROM ledger")?;
        database.execute("INSERT INTO ledger VALUES(1)")?;
        let error = database.compact_wal().unwrap_err();
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Conflict)
        );
        assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_none());
        assert!(!database.writer_admission_status()?.quarantined);
        assert!(reader.in_transaction()?);
        assert!(reader.query("SELECT value FROM ledger")?.is_empty());
        drop(reader);
        database.compact_wal()?;
        assert_eq!(
            database.query("SELECT value FROM ledger")?[0][0].to_string(),
            "1"
        );
        Ok(())
    }

    /// The preflight can only see lanes created through `RemoteDatabase::session()`. A native
    /// connection opened directly on the engine pins a read mark that the preflight cannot see,
    /// so admission dispatches and the truncate checkpoint itself returns `Busy`. That is an
    /// unknown outcome, not a classified reader block: the database must be quarantined, the
    /// WAL index kept, and no `long_reader` or `partial_backfill` claimed.
    ///
    /// This is the red-line test for the blind spot: a version that recorded the block without
    /// seeing the reader, or that treated the native `Busy` as a clean rejection, fails here.
    #[test]
    fn a_reader_outside_the_lane_registry_quarantines_instead_of_claiming_a_block() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let namespace = Namespace::new("compaction", "unregistered_reader")?;
        let database = Arc::new(RemoteDatabase::open(
            Arc::new(MemoryObjectStore::default()),
            namespace.clone(),
            directory.path(),
            0,
        )?);
        database.execute("CREATE TABLE ledger(value INTEGER)")?;

        // Connecting through the engine instead of `session()` skips `register_lane`, which is
        // exactly the blind spot: the connection holds a native read lock no lane knows about.
        let engine = {
            let engine = database
                .engine
                .lock()
                .map_err(|_| AdmissionError::Quarantine)?;
            engine.as_ref().cloned().ok_or(AdmissionError::Quarantine)?
        };
        let reader = engine.connect()?;
        reader.execute("BEGIN")?;
        let mut select = reader
            .query("SELECT value FROM ledger")?
            .ok_or(AdmissionError::Quarantine)?;
        loop {
            match select.step()? {
                StepResult::Row | StepResult::Done => break,
                StepResult::IO | StepResult::Yield => select._io().step()?,
                other => bail!("unexpected step while pinning a reader: {other:?}"),
            }
        }
        assert!(!reader.get_auto_commit());
        assert!(reader.get_pager().holds_read_lock());

        // The writer advances the WAL while the unregistered reader still pins its snapshot.
        database.execute("INSERT INTO ledger VALUES(1)")?;
        let wal = crate::wal::RemoteWal::new(database.storage().store());
        let index_before = wal.metadata(&namespace)?;
        let lsn_before = database.io().wal_lsn();
        let wal_bytes_before = database.resources()?.wal_bytes;
        assert!(
            !index_before.entries.is_empty(),
            "the WAL has frames to keep"
        );

        // The blind spot is real: the preflight passes even though a reader is pinned, so this
        // path would otherwise be recorded as a reader block after the fact.
        database.preflight_compaction()?;
        let before = database.maintenance_ledger().counters();
        assert_eq!(before.blocks.get("long_reader"), None);

        let error = database.compact_wal().unwrap_err();
        let chain = format!("{error:#}");
        assert!(chain.contains("Database is busy"), "{chain}");
        assert!(
            error.downcast_ref::<UnsettledSqlOutcome>().is_some(),
            "a checkpoint blocked by an invisible reader has an unknown outcome: {chain}"
        );
        assert!(
            database.writer_admission_status()?.quarantined,
            "an unsettled checkpoint must fail closed: {chain}"
        );

        let after = database.maintenance_ledger().counters();
        assert_eq!(
            after.blocks.get("long_reader"),
            None,
            "the reader was never seen, so claiming a measured long reader would be a lie"
        );
        assert_eq!(
            after.blocks.get("partial_backfill"),
            None,
            "a native Busy is an unsettled outcome, not a classified partial backfill"
        );
        assert_eq!(after.executions, 1);

        let index_after = wal.metadata(&namespace)?;
        assert_eq!(
            index_after.replay_baseline_lsn, index_before.replay_baseline_lsn,
            "the WAL index must not be rebased after an unsettled checkpoint"
        );
        assert_eq!(index_after.last_lsn, index_before.last_lsn);
        assert_eq!(index_after.entries.len(), index_before.entries.len());
        assert_eq!(database.io().wal_lsn(), lsn_before);
        assert_eq!(database.resources()?.wal_bytes, wal_bytes_before);
        assert!(reader.get_pager().holds_read_lock());

        // Fail closed without a retry: the quarantined database rejects the next unit.
        let retry = database.compact_wal().unwrap_err();
        assert_eq!(
            retry.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Quarantine)
        );
        Ok(())
    }
}
