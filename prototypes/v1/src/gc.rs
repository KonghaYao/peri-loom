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
        Ok(GcStats {
            root_revision: snapshot.root.revision,
            known_blobs: snapshot.root.blobs.len(),
            reachable_blobs: reachable.len(),
            unreachable_blobs: snapshot
                .root
                .blobs
                .keys()
                .filter(|blob| !reachable.contains(*blob))
                .count(),
            unresolved_uploads: snapshot.root.uploads.len(),
            reader_pins: snapshot.root.pins.len(),
            pending_deletions: snapshot
                .root
                .tombstones
                .values()
                .filter(|tombstone| !tombstone.deleted)
                .count(),
            completed_deletions: snapshot
                .root
                .tombstones
                .values()
                .filter(|tombstone| tombstone.deleted)
                .count(),
        })
    }

    pub fn mark(&self) -> Result<usize> {
        for _ in 0..64 {
            let snapshot = self.authority.snapshot()?;
            let reachable = self.authority.reachable(&snapshot.root)?;
            let candidates: BTreeMap<_, _> = snapshot
                .root
                .blobs
                .iter()
                .filter(|(blob, _)| {
                    !reachable.contains(*blob) && !snapshot.root.tombstones.contains_key(*blob)
                })
                .map(|(blob, entry)| (blob.clone(), entry.clone()))
                .collect();
            if candidates.is_empty() {
                return Ok(0);
            }
            let marked_ms = now_ms()?;
            match self
                .authority
                .transact(Some(snapshot.root.revision), |root| {
                    root.objects
                        .retain(|_, entry| !candidates.contains_key(&entry.blob));
                    for (blob, entry) in &candidates {
                        root.tombstones.insert(
                            blob.clone(),
                            Tombstone {
                                entry: entry.clone(),
                                marked_ms,
                                deleted: false,
                            },
                        );
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
        let now = now_ms()?;
        let window = u64::try_from(self.safety_window.as_millis())?;
        let mut deleted = 0;
        for (blob, tombstone) in &snapshot.root.tombstones {
            if now.saturating_sub(tombstone.marked_ms) >= window {
                self.authority.delete_tombstone(blob)?;
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
}

fn now_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("GC clock precedes Unix epoch")?
        .as_millis()
        .try_into()
        .context("GC clock overflow")
}
