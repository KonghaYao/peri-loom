use std::sync::Arc;

use anyhow::{ensure, Result};
use turso_core::{NativeIoRetirementGuard, IO};

use super::retirement::DatabaseRetirementGuard;
use super::RemoteDatabase;
use crate::session::{AdmissionError, SessionLane};

impl RemoteDatabase {
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
                return Err(AdmissionError::Conflict.into());
            }
        }
        Ok(())
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

    use anyhow::Result;

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
}
