use anyhow::{ensure, Result};

use super::registry::{Entry, Registry};
use super::DatabaseId;
use crate::session::AdmissionError;

impl Registry {
    pub fn resident_count(&self) -> usize {
        self.entries.len().saturating_add(
            self.quarantined
                .keys()
                .filter(|identifier| !self.entries.contains_key(*identifier))
                .count(),
        )
    }

    pub fn check_capacity(&self, limit: usize) -> Result<()> {
        ensure!(
            self.resident_count() < limit,
            "worker database capacity exceeded"
        );
        Ok(())
    }

    pub fn observe_quarantine(&mut self, identifier: &DatabaseId) {
        let Some(Entry::Loaded(loaded)) = self.entries.get_mut(identifier) else {
            return;
        };
        if loaded
            .database
            .database()
            .and_then(|database| database.admission().healthy())
            .is_err()
        {
            self.quarantined
                .entry(identifier.clone())
                .or_insert(loaded.epoch);
            loaded.draining = true;
            loaded.idle_since = None;
        }
    }

    pub fn observe_all_quarantines(&mut self) {
        let identifiers: Vec<_> = self.entries.keys().cloned().collect();
        for identifier in identifiers {
            self.observe_quarantine(&identifier);
        }
    }

    pub fn check_available(&mut self, identifier: &DatabaseId) -> Result<()> {
        self.observe_quarantine(identifier);
        if self.quarantined.contains_key(identifier) {
            return Err(AdmissionError::Quarantine.into());
        }
        Ok(())
    }

    pub fn finish_retiring(&mut self, identifier: &DatabaseId) {
        if !self.quarantined.contains_key(identifier)
            && matches!(self.entries.get(identifier), Some(Entry::Retiring(_)))
        {
            self.entries.remove(identifier);
        }
    }
}

#[cfg(test)]
mod tests;
