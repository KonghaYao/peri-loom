use crate::authority::{Authority, BranchRef, CatalogRoot, RecoveryPoint, RootSnapshot};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize)]
pub struct CapabilityList {
    pub revision: u64,
    pub branches: BTreeMap<String, BranchRef>,
    pub snapshots: BTreeMap<String, RecoveryPoint>,
}

pub struct Capabilities {
    authority: Arc<Authority>,
}

impl Capabilities {
    pub fn new(authority: Arc<Authority>) -> Self {
        Self { authority }
    }

    pub fn branch(&self, source: &str, new: &str) -> Result<RootSnapshot> {
        validate_name(source)?;
        validate_name(new)?;
        self.authority.transact(None, |root| {
            ensure!(
                !root.branches.contains_key(new),
                "branch already exists: {new}"
            );
            let mut branch = root
                .branches
                .get(source)
                .context("source branch missing")?
                .clone();
            branch.incarnation = next_incarnation(root)?;
            branch.timeline = branch.incarnation;
            root.branches.insert(new.to_owned(), branch);
            Ok(())
        })
    }

    pub fn snapshot(&self, branch: &str, name: &str) -> Result<RecoveryPoint> {
        validate_name(branch)?;
        validate_name(name)?;
        let snapshot = self.authority.transact(None, |root| {
            ensure!(
                !root.snapshots.contains_key(name),
                "snapshot already exists: {name}"
            );
            let reference = root.branches.get(branch).context("branch missing")?;
            let identifier =
                serde_json::to_vec(&(&root.incarnation, root.revision, name, branch, reference))?;
            let point = RecoveryPoint {
                id: blake3::hash(&identifier).to_hex().to_string(),
                branch: branch.to_owned(),
                timeline: reference.timeline,
                sequence: reference.current_sequence,
                timestamp_ms: reference.current_commit_ms,
                head: reference.head.clone(),
                wal: reference.wal.clone(),
            };
            root.snapshots.insert(name.to_owned(), point);
            Ok(())
        })?;
        snapshot
            .root
            .snapshots
            .get(name)
            .cloned()
            .context("created snapshot missing")
    }

    pub fn clone(&self, snapshot: &str, new: &str) -> Result<RootSnapshot> {
        validate_name(snapshot)?;
        validate_name(new)?;
        self.authority.transact(None, |root| {
            ensure!(
                !root.branches.contains_key(new),
                "branch already exists: {new}"
            );
            let point = root
                .snapshots
                .get(snapshot)
                .context("snapshot missing")?
                .clone();
            let branch = BranchRef {
                incarnation: next_incarnation(root)?,
                timeline: next_incarnation(root)?,
                sequence: point.sequence,
                last_commit_ms: point.timestamp_ms,
                current_sequence: point.sequence,
                current_commit_ms: point.timestamp_ms,
                head: point.head.clone(),
                wal: point.wal.clone(),
                history: if point.sequence == 0 {
                    Vec::new()
                } else {
                    vec![point]
                },
            };
            root.branches.insert(new.to_owned(), branch);
            Ok(())
        })
    }

    pub fn restore(&self, branch: &str, snapshot: &str) -> Result<RootSnapshot> {
        validate_name(branch)?;
        validate_name(snapshot)?;
        self.authority.transact(None, |root| {
            let point = root
                .snapshots
                .get(snapshot)
                .context("snapshot missing")?
                .clone();
            restore(root, branch, &point)
        })
    }

    pub fn restore_point(&self, branch: &str, point: &RecoveryPoint) -> Result<RootSnapshot> {
        validate_name(branch)?;
        self.authority.transact(None, |root| {
            ensure!(
                reachable(root, point),
                "recovery point is no longer reachable"
            );
            restore(root, branch, point)
        })
    }

    pub fn pitr(&self, branch: &str, sequence: u64) -> Result<RecoveryPoint> {
        validate_name(branch)?;
        let snapshot = self.authority.snapshot()?;
        let reference = snapshot
            .root
            .branches
            .get(branch)
            .context("branch missing")?;
        reference
            .history
            .iter()
            .filter(|point| point.sequence == sequence)
            .max_by_key(|point| (point.timestamp_ms, point.timeline))
            .cloned()
            .context("exact committed sequence is not retained")
    }

    pub fn resolve_time(&self, branch: &str, timestamp_ms: u64) -> Result<RecoveryPoint> {
        validate_name(branch)?;
        let snapshot = self.authority.snapshot()?;
        let reference = snapshot
            .root
            .branches
            .get(branch)
            .context("branch missing")?;
        let first = reference
            .history
            .iter()
            .map(|point| point.timestamp_ms)
            .min()
            .context("no retained commit history")?;
        let now_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
            .context("system clock milliseconds overflow")?;
        let last = now_ms.max(reference.last_commit_ms);
        ensure!(
            (first..=last).contains(&timestamp_ms),
            "requested time is outside retained history"
        );
        reference
            .history
            .iter()
            .filter(|point| point.timestamp_ms <= timestamp_ms)
            .max_by_key(|point| (point.timestamp_ms, point.sequence, point.timeline))
            .cloned()
            .context("no retained commit at requested time")
    }

    pub fn delete_branch(&self, branch: &str) -> Result<RootSnapshot> {
        validate_name(branch)?;
        self.authority.transact(None, |root| {
            root.branches.remove(branch).context("branch missing")?;
            Ok(())
        })
    }

    pub fn delete_snapshot(&self, snapshot: &str) -> Result<RootSnapshot> {
        validate_name(snapshot)?;
        self.authority.transact(None, |root| {
            root.snapshots
                .remove(snapshot)
                .context("snapshot missing")?;
            Ok(())
        })
    }

    pub fn retain_after(&self, branch: &str, min_sequence: u64) -> Result<RootSnapshot> {
        validate_name(branch)?;
        self.authority.transact(None, |root| {
            let reference = root.branches.get_mut(branch).context("branch missing")?;
            reference
                .history
                .retain(|point| point.sequence >= min_sequence);
            Ok(())
        })
    }

    pub fn list(&self) -> Result<CapabilityList> {
        let snapshot = self.authority.snapshot()?;
        Ok(CapabilityList {
            revision: snapshot.root.revision,
            branches: snapshot.root.branches,
            snapshots: snapshot.root.snapshots,
        })
    }

    pub fn graph(&self) -> Result<CatalogRoot> {
        Ok(self.authority.snapshot()?.root)
    }
}

fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name.len() <= 128,
        "invalid reference name length"
    );
    ensure!(
        name.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
        "invalid ASCII reference name"
    );
    Ok(())
}

fn next_incarnation(root: &CatalogRoot) -> Result<u64> {
    root.revision.checked_add(1).context("incarnation overflow")
}

fn reachable(root: &CatalogRoot, point: &RecoveryPoint) -> bool {
    root.snapshots.values().any(|snapshot| snapshot == point)
        || root
            .branches
            .values()
            .any(|branch| branch.history.iter().any(|retained| retained == point))
}

pub(crate) fn restore(root: &mut CatalogRoot, branch: &str, point: &RecoveryPoint) -> Result<()> {
    let fresh = next_incarnation(root)?;
    let reference = root.branches.get_mut(branch).context("branch missing")?;
    ensure!(
        fresh > reference.incarnation && fresh > reference.timeline,
        "branch identity did not advance"
    );
    reference.incarnation = fresh;
    reference.timeline = fresh;
    reference.head = point.head.clone();
    reference.wal = point.wal.clone();
    reference.current_sequence = point.sequence;
    reference.current_commit_ms = point.timestamp_ms;
    reference.sequence = reference.sequence.max(point.sequence);
    reference.last_commit_ms = reference.last_commit_ms.max(point.timestamp_ms);
    Ok(())
}
