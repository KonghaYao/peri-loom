use crate::authority::{semantic, BranchStore, CatalogRoot};
use crate::{
    manifest::{Head, Manifest},
    ownership::types::GrantState,
};
use anyhow::{ensure, Context, Result};

impl BranchStore {
    pub(in crate::authority) fn validate_publication(
        &self,
        root: &CatalogRoot,
        key: &str,
        bytes: &[u8],
    ) -> Result<()> {
        if self.authority.guard.is_none() || root.ownership.is_none() {
            return Ok(());
        }
        let catalog = root.ownership.as_ref().unwrap();
        let record = catalog
            .branches
            .get(&self.branch)
            .context("Fenced ownership missing")?;
        ensure!(
            record.state == GrantState::Active && record.branch_incarnation == self.incarnation,
            "Fenced branch ownership"
        );
        match key {
            "wal/index" => {
                ensure!(
                    semantic::index(bytes)?.epoch == record.epoch,
                    "Fenced WAL epoch"
                );
            }
            "HEAD" => {
                let next: Head = serde_json::from_slice(bytes)?;
                let entry = root
                    .objects
                    .get(&format!("manifests/{}", next.manifest))
                    .context("HEAD manifest missing")?;
                let manifest: Manifest = serde_json::from_slice(&self.authority.read_blob(entry)?)?;
                let previous = root.branches[&self.branch].head.as_ref();
                if let Some(previous) = previous {
                    let head: Head = serde_json::from_slice(&self.authority.read_blob(previous)?)?;
                    let entry = root
                        .objects
                        .get(&format!("manifests/{}", head.manifest))
                        .context("previous HEAD manifest missing")?;
                    let old: Manifest = serde_json::from_slice(&self.authority.read_blob(entry)?)?;
                    ensure!(
                        manifest.generation
                            == old
                                .generation
                                .checked_add(1)
                                .context("generation exhausted")?
                            && manifest.parent.as_ref() == Some(&head.manifest),
                        "Conflict expected HEAD generation or parent"
                    );
                } else {
                    ensure!(
                        manifest.generation == 0 && manifest.parent.is_none(),
                        "Conflict initial HEAD generation"
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }
}
