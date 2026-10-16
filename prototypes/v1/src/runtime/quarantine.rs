use anyhow::{ensure, Result};

use super::registry::{Entry, Registry};
use super::DatabaseId;
use crate::session::AdmissionError;

impl Registry {
    /// Admission caliber: a database that could not prove a clean unload keeps
    /// consuming its slot until an operator retires it, so tombstones without a
    /// native entry count here.
    pub fn capacity_resident_count(&self) -> usize {
        self.entries.len().saturating_add(
            self.quarantined
                .keys()
                .filter(|identifier| !self.entries.contains_key(*identifier))
                .count(),
        )
    }

    /// Reporting caliber: only identities with a live native entry. Quarantined
    /// tombstones are reported by `quarantined_count`/`oldest_quarantined_age_ms`
    /// so the resident figures describe residency rather than retained capacity.
    pub fn live_resident_count(&self) -> usize {
        self.entries.len()
    }

    pub fn oldest_live_resident_age_ms(&self) -> u64 {
        self.resident_since
            .iter()
            .filter(|(identifier, _)| self.entries.contains_key(*identifier))
            .map(|(_, since)| since.elapsed().as_millis() as u64)
            .max()
            .unwrap_or(0)
    }

    pub fn oldest_quarantined_age_ms(&self) -> u64 {
        self.quarantined_since
            .values()
            .map(|since| since.elapsed().as_millis() as u64)
            .max()
            .unwrap_or(0)
    }

    pub fn check_capacity(&self, limit: usize) -> Result<()> {
        ensure!(
            self.capacity_resident_count() < limit,
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
            self.quarantined_since
                .entry(identifier.clone())
                .or_insert_with(std::time::Instant::now);
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
            self.resident_since.remove(identifier);
        }
    }
}

#[cfg(test)]
mod tests;
