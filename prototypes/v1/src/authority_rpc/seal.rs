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
}
