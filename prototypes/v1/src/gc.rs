use crate::authority::{Authority, Tombstone};
use crate::object_store::PreconditionFailed;
use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default, Serialize)]
pub struct GcStats {
    pub root_revision: u64,
    pub known_blobs: usize,
    pub reachable_blobs: usize,
    pub unreachable_blobs: usize,
    pub unresolved_uploads: usize,
    pub reader_pins: usize,
    pub pending_deletions: usize,
    pub completed_deletions: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GcResult {
    pub marked: usize,
    pub deleted: usize,
}

pub struct GarbageCollector {
    authority: Arc<Authority>,
    safety_window: Duration,
}

impl GarbageCollector {
    pub fn new(authority: Arc<Authority>, safety_window: Duration) -> Self {
        Self {
            authority,
            safety_window,
        }
    }

    pub fn inspect(&self) -> Result<GcStats> {
        let snapshot = self.authority.snapshot()?;
        let reachable = self.authority.reachable(&snapshot.root)?;
        let blobs = snapshot.root.blobs.entries()?;
        let tombstones = snapshot.root.tombstones.entries()?;
        Ok(GcStats {
            root_revision: snapshot.root.revision,
            known_blobs: blobs.len(),
            reachable_blobs: reachable.len(),
            unreachable_blobs: blobs
                .keys()
                .filter(|blob| !reachable.contains(*blob))
                .count(),
            unresolved_uploads: snapshot.root.uploads.entries()?.len(),
            reader_pins: snapshot.root.pins.len(),
            pending_deletions: tombstones
                .values()
                .filter(|tombstone| !tombstone.deleted)
                .count(),
            completed_deletions: tombstones
                .values()
                .filter(|tombstone| tombstone.deleted)
                .count(),
        })
    }

    pub fn mark(&self) -> Result<usize> {
        for _ in 0..64 {
            let snapshot = self.authority.snapshot()?;
            let reachable = self.authority.reachable(&snapshot.root)?;
            let tombstones = snapshot.root.tombstones.entries()?;
            let candidates: BTreeMap<_, _> = snapshot
                .root
                .blobs
                .entries()?
                .into_iter()
                .filter(|(blob, _)| !reachable.contains(blob) && !tombstones.contains_key(blob))
                .collect();
            if candidates.is_empty() {
                return Ok(0);
            }
            let marked_ms = now_ms()?;
            match self
                .authority
                .transact(Some(snapshot.root.revision), |root| {
                    let orphaned: Vec<String> = root
                        .objects
                        .entries()?
                        .into_iter()
                        .filter(|(_, entry)| candidates.contains_key(&entry.blob))
                        .map(|(key, _)| key)
                        .collect();
                    for key in orphaned {
                        root.objects.remove(&key)?;
                    }
                    for (blob, entry) in &candidates {
                        root.tombstones.insert(
                            blob.clone(),
                            Tombstone {
                                entry: entry.clone(),
                                marked_ms,
                                deleted: false,
                            },
                        )?;
                    }
                    Ok(())
                }) {
                Ok(_) => return Ok(candidates.len()),
                Err(error) if error.is::<PreconditionFailed>() => continue,
                Err(error) => return Err(error),
            }
        }
        bail!("GC root changed too frequently; no unsafe deletion attempted")
    }

    pub fn sweep(&self) -> Result<usize> {
        let snapshot = self.authority.snapshot()?;
        // Deletion must not act on a store whose reachable metadata no longer verifies:
        // the deep walk is what refuses to reap against corrupt dependencies.
        self.authority.reachable(&snapshot.root)?;
        let now = now_ms()?;
        let window = u64::try_from(self.safety_window.as_millis())?;
        let mut deleted = 0;
        for (blob, tombstone) in snapshot.root.tombstones.entries()? {
            if now.saturating_sub(tombstone.marked_ms) >= window {
                self.authority.delete_tombstone(&blob)?;
                deleted += usize::from(!tombstone.deleted);
            }
        }
        Ok(deleted)
    }

    pub fn collect(&self) -> Result<GcResult> {
        Ok(GcResult {
            marked: self.mark()?,
            deleted: self.sweep()?,
        })
    }

    pub(crate) fn collect_receipted(
        &self,
        request_id: &str,
        digest: &str,
        mut eligible: impl FnMut(&str, &Tombstone) -> Result<bool>,
    ) -> Result<serde_json::Value> {
        use crate::ownership::types::CommandReceipt;
        use serde_json::json;
        let mut started = None;
        for _ in 0..64 {
            let snapshot = self.authority.snapshot()?;
            self.authority.reachable(&snapshot.root)?;
            let catalog = snapshot
                .root
                .ownership
                .as_ref()
                .context("ownership missing")?;
            if let Some(receipt) = catalog.commands.get(request_id) {
                anyhow::ensure!(
                    receipt.digest == digest,
                    "Conflict request ID payload mismatch"
                );
                if receipt
                    .result
                    .get("status")
                    .and_then(|status| status.as_str())
                    == Some("completed_gc")
                {
                    return Ok(receipt.result.clone());
                }
                anyhow::ensure!(
                    receipt
                        .result
                        .get("status")
                        .and_then(|status| status.as_str())
                        == Some("pending_gc"),
                    "Conflict GC command status"
                );
                started = Some(receipt.result.clone());
                break;
            }
            let reachable = self.authority.reachable(&snapshot.root)?;
            let tombstones = snapshot.root.tombstones.entries()?;
            let candidates: BTreeMap<_, _> = snapshot
                .root
                .blobs
                .entries()?
                .into_iter()
                .filter(|(blob, _)| !reachable.contains(blob) && !tombstones.contains_key(blob))
                .collect();
            let targets: Vec<_> = tombstones
                .keys()
                .chain(candidates.keys())
                .cloned()
                .collect();
            let initial_pending: Vec<_> = tombstones
                .iter()
                .filter(|(_, tombstone)| !tombstone.deleted)
                .map(|(blob, _)| blob.clone())
                .chain(candidates.keys().cloned())
                .collect();
            let result = json!({"status": "pending_gc", "targets": targets, "initial_pending": initial_pending, "marked": candidates.len(), "deleted": 0});
            let marked_ms = now_ms()?;
            match self
                .authority
                .transact(Some(snapshot.root.revision), |root| {
                    let orphaned: Vec<String> = root
                        .objects
                        .entries()?
                        .into_iter()
                        .filter(|(_, entry)| candidates.contains_key(&entry.blob))
                        .map(|(key, _)| key)
                        .collect();
                    for key in orphaned {
                        root.objects.remove(&key)?;
                    }
                    for (blob, entry) in &candidates {
                        root.tombstones.insert(
                            blob.clone(),
                            Tombstone {
                                entry: entry.clone(),
                                marked_ms,
                                deleted: false,
                            },
                        )?;
                    }
                    root.ownership
                        .as_mut()
                        .context("ownership missing")?
                        .commands
                        .insert(
                            request_id.into(),
                            CommandReceipt {
                                digest: digest.into(),
                                result: result.clone(),
                            },
                        );
                    Ok(())
                }) {
                Ok(_) => {
                    started = Some(result);
                    break;
                }
                Err(error) if error.is::<PreconditionFailed>() => continue,
                Err(error) => return Err(error),
            }
        }
        let mut result = started.context("GC root changed too frequently")?;
        let snapshot = self.authority.snapshot()?;
        let now = now_ms()?;
        let window = u64::try_from(self.safety_window.as_millis())?;
        for target in result["targets"].as_array().context("GC targets missing")? {
            let blob = target.as_str().context("GC target invalid")?;
            let tombstone = snapshot
                .root
                .tombstones
                .get(blob)?
                .context("GC tombstone missing")?;
            let monotonic_ready = eligible(blob, &tombstone)?;
            if monotonic_ready && now.saturating_sub(tombstone.marked_ms) >= window {
                self.authority.delete_tombstone(blob)?;
            }
        }
        let acknowledged = self.authority.snapshot()?;
        let acknowledged_tombstones = acknowledged.root.tombstones.entries()?;
        let deleted = result["initial_pending"]
            .as_array()
            .context("GC initial pending targets missing")?
            .iter()
            .filter(|target| {
                target.as_str().is_some_and(|blob| {
                    acknowledged_tombstones
                        .get(blob)
                        .is_some_and(|tombstone| tombstone.deleted)
                })
            })
            .count();
        result["status"] = json!("completed_gc");
        result["deleted"] = json!(deleted);
        self.authority.transact(None, |root| {
            let receipt = root
                .ownership
                .as_mut()
                .context("ownership missing")?
                .commands
                .get_mut(request_id)
                .context("GC receipt missing")?;
            anyhow::ensure!(receipt.digest == digest, "Conflict GC receipt changed");
            receipt.result = result.clone();
            Ok(())
        })?;
        Ok(result)
    }
}

fn now_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("GC clock precedes Unix epoch")?
        .as_millis()
        .try_into()
        .context("GC clock overflow")
}
