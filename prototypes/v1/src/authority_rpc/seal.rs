use std::sync::Arc;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use crate::object_store::{ObjectStore, PutCondition};

const KEY: &str = "authority-control/single-authority-seal";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    owner: String,
    clean: bool,
}

pub(super) struct Seal {
    store: Arc<dyn ObjectStore>,
    record: Record,
    etag: String,
}

impl Seal {
    pub fn acquire(store: Arc<dyn ObjectStore>) -> Result<Self> {
        ensure!(
            store.supports_strict_fencing(),
            "authority seal requires strict conditional backend writes"
        );
        let previous = store.get(KEY)?;
        if let Some(previous) = &previous {
            let record: Record = serde_json::from_slice(&previous.bytes)?;
            ensure!(record.version == 1 && record.clean, "dirty backend authority seal; old remote terminal is unproven; state-dir replacement and --reconciled cannot authorize takeover");
        }
        let record = Record {
            version: 1,
            owner: crate::authority::unique_id()?,
            clean: false,
        };
        let condition = previous.map_or(PutCondition::Absent, |previous| {
            PutCondition::Match(previous.etag)
        });
        let etag = store.put(KEY, &serde_json::to_vec(&record)?, condition)?;
        Ok(Self {
            store,
            record,
            etag,
        })
    }

    pub fn mark_clean(&mut self) -> Result<()> {
        self.record.clean = true;
        self.etag = self.store.put(
            KEY,
            &serde_json::to_vec(&self.record)?,
            PutCondition::Match(self.etag.clone()),
        )?;
        Ok(())
    }

    /// Revokes the clean record when the write raced the shutdown budget.
    /// The conditional write pins the rollback to the row this authority itself
    /// wrote, so rolling back cannot reopen a takeover window for anyone else.
    pub fn mark_dirty(&mut self) -> Result<()> {
        self.record.clean = false;
        self.etag = self.store.put(
            KEY,
            &serde_json::to_vec(&self.record)?,
            PutCondition::Match(self.etag.clone()),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;

    #[test]
    fn rollback_closes_the_clean_window_a_restart_would_trust() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let mut seal = Seal::acquire(store.clone())?;
        seal.mark_clean()?;
        // The window this guards: a clean record is accepted by the next startup,
        // so an over-budget shutdown that lands it must be able to revoke it.
        let published: serde_json::Value =
            serde_json::from_slice(&store.get(KEY)?.expect("seal record").bytes)?;
        assert_eq!(published["clean"], true);
        seal.mark_dirty()?;
        assert!(Seal::acquire(store).is_err());
        Ok(())
    }

    #[test]
    fn rollback_requires_the_etag_of_the_record_this_authority_wrote() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let mut previous = Seal::acquire(store.clone())?;
        previous.mark_clean()?;
        let _takeover = Seal::acquire(store)?;
        assert!(previous.mark_dirty().is_err());
        Ok(())
    }
}
