use crate::authority::{
    Authority, BranchRef, CatalogRoot, OutOfRetention, RecoveryPoint, RecoveryPointNotFound,
    RootSnapshot,
};
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
    /// The committed PITR contract per branch, so an operator can see the actual range
    /// instead of inferring a duration from a commit count.
    pub retention: BTreeMap<String, RetentionReport>,
}

/// What a branch currently promises. `oldest_*`/`newest_*` describe points that exist, not
/// the abstract window: a time window can end before the newest commit if the branch is
/// idle, and a sequence window is always a count.
#[derive(Clone, Debug, Serialize)]
pub struct RetentionReport {
    pub branch: String,
    pub retention_mode: String,
    pub indexed: bool,
    pub retention_ms: Option<u64>,
    pub retention_points: Option<u32>,
    pub max_bytes: Option<u64>,
    pub anchor_snapshots: Option<bool>,
    pub retained_points: u64,
    pub retained_bytes: u64,
    pub hot_points: usize,
    pub oldest_recoverable_sequence: Option<u64>,
    pub newest_recoverable_sequence: Option<u64>,
    pub oldest_recoverable_timestamp_ms: Option<u64>,
    pub newest_recoverable_timestamp_ms: Option<u64>,
    pub snapshot_anchors: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecoveryPointPage {
    pub branch: String,
    pub timeline: u64,
    pub points: Vec<RecoveryPoint>,
    pub next_after_sequence: Option<u64>,
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
            let history = if point.sequence == 0 {
                Vec::new()
            } else {
                vec![point.clone()]
            };
            let recovery = Some(self.authority.build_recovery_index(&history)?);
            let branch = BranchRef {
                incarnation: next_incarnation(root)?,
                timeline: next_incarnation(root)?,
                sequence: point.sequence,
                last_commit_ms: point.timestamp_ms,
                current_sequence: point.sequence,
                current_commit_ms: point.timestamp_ms,
                head: point.head.clone(),
                wal: point.wal.clone(),
                history,
                recovery,
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
                reachable(self.authority.as_ref(), root, point)?,
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
        resolve_sequence(self.authority.as_ref(), branch, reference, sequence)
    }

    pub fn resolve_time(&self, branch: &str, timestamp_ms: u64) -> Result<RecoveryPoint> {
        validate_name(branch)?;
        let snapshot = self.authority.snapshot()?;
        let reference = snapshot
            .root
            .branches
            .get(branch)
            .context("branch missing")?;
        resolve_timestamp(self.authority.as_ref(), branch, reference, timestamp_ms)
    }

    /// The committed PITR contract and the points that actually exist. This never scans the
    /// index: bounds and counts are durable summaries in the branch reference.
    pub fn retention(&self, branch: &str) -> Result<RetentionReport> {
        validate_name(branch)?;
        let snapshot = self.authority.snapshot()?;
        let reference = snapshot
            .root
            .branches
            .get(branch)
            .context("branch missing")?;
        Ok(retention_report(
            branch,
            reference,
            &snapshot.root.snapshots,
        ))
    }

    /// One bounded page of a timeline. The traversal skips subtrees that end before
    /// `after_sequence`, so cost follows the tree height plus the returned page.
    pub fn recovery_points(
        &self,
        branch: &str,
        timeline: u64,
        after_sequence: u64,
        limit: usize,
    ) -> Result<RecoveryPointPage> {
        validate_name(branch)?;
        ensure!(
            (1..=256).contains(&limit),
            "recovery page size is out of range"
        );
        ensure!(timeline > 0, "recovery timeline identity is zero");
        let snapshot = self.authority.snapshot()?;
        let reference = snapshot
            .root
            .branches
            .get(branch)
            .context("branch missing")?;
        let points = self
            .authority
            .recovery_range(reference, timeline, after_sequence, limit)?;
        let next_after_sequence = points
            .last()
            .filter(|_| points.len() == limit)
            .map(|point| point.sequence);
        Ok(RecoveryPointPage {
            branch: branch.to_owned(),
            timeline,
            points,
            next_after_sequence,
        })
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
            self.authority
                .retain_recovery_after(root, branch, min_sequence)
        })
    }

    pub fn list(&self) -> Result<CapabilityList> {
        let snapshot = self.authority.snapshot()?;
        let retention = snapshot
            .root
            .branches
            .iter()
            .map(|(name, branch)| {
                (
                    name.clone(),
                    retention_report(name, branch, &snapshot.root.snapshots),
                )
            })
            .collect();
        Ok(CapabilityList {
            revision: snapshot.root.revision,
            branches: snapshot.root.branches,
            snapshots: snapshot.root.snapshots,
            retention,
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

fn reachable(authority: &Authority, root: &CatalogRoot, point: &RecoveryPoint) -> Result<bool> {
    if root.snapshots.values().any(|snapshot| snapshot == point) {
        return Ok(true);
    }
    for branch in root.branches.values() {
        if branch.history.contains(point) {
            return Ok(true);
        }
        if authority
            .recovery_lookup_sequence(branch, point.sequence)?
            .iter()
            .any(|retained| retained == point)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

struct RecoveryBounds {
    mode: String,
    retained_points: u64,
    oldest_sequence: Option<u64>,
    newest_sequence: Option<u64>,
    oldest_timestamp_ms: Option<u64>,
    newest_timestamp_ms: Option<u64>,
}

fn recovery_bounds(branch: &BranchRef) -> RecoveryBounds {
    let mut bounds = RecoveryBounds {
        mode: "legacy_hot_window".into(),
        retained_points: 0,
        oldest_sequence: None,
        newest_sequence: None,
        oldest_timestamp_ms: None,
        newest_timestamp_ms: None,
    };
    if let Some(index) = &branch.recovery {
        bounds.mode = index.policy.mode.name().into();
        bounds.retained_points = index.count;
        if index.count > 0 {
            bounds.oldest_sequence = Some(index.oldest_sequence);
            bounds.newest_sequence = Some(index.newest_sequence);
            bounds.oldest_timestamp_ms = Some(index.oldest_timestamp_ms);
            bounds.newest_timestamp_ms = Some(index.newest_timestamp_ms);
        }
    }
    for point in &branch.history {
        bounds.oldest_sequence = Some(
            bounds
                .oldest_sequence
                .map_or(point.sequence, |value| value.min(point.sequence)),
        );
        bounds.newest_sequence = Some(
            bounds
                .newest_sequence
                .map_or(point.sequence, |value| value.max(point.sequence)),
        );
        bounds.oldest_timestamp_ms = Some(
            bounds
                .oldest_timestamp_ms
                .map_or(point.timestamp_ms, |value| value.min(point.timestamp_ms)),
        );
        bounds.newest_timestamp_ms = Some(
            bounds
                .newest_timestamp_ms
                .map_or(point.timestamp_ms, |value| value.max(point.timestamp_ms)),
        );
    }
    if branch.recovery.is_none() {
        bounds.retained_points = branch.history.len() as u64;
    } else {
        bounds.retained_points = bounds.retained_points.max(branch.history.len() as u64);
    }
    bounds
}

fn out_of_retention(
    branch: &str,
    bounds: &RecoveryBounds,
    requested_sequence: Option<u64>,
    requested_timestamp_ms: Option<u64>,
) -> anyhow::Error {
    anyhow::Error::new(OutOfRetention {
        branch: branch.to_owned(),
        requested_sequence,
        requested_timestamp_ms,
        retention_mode: bounds.mode.clone(),
        retained_points: bounds.retained_points,
        oldest_sequence: bounds.oldest_sequence,
        newest_sequence: bounds.newest_sequence,
        oldest_timestamp_ms: bounds.oldest_timestamp_ms,
        newest_timestamp_ms: bounds.newest_timestamp_ms,
    })
}

fn point_not_found(
    branch: &str,
    bounds: &RecoveryBounds,
    requested_sequence: Option<u64>,
    requested_timestamp_ms: Option<u64>,
) -> anyhow::Error {
    anyhow::Error::new(RecoveryPointNotFound {
        branch: branch.to_owned(),
        requested_sequence,
        requested_timestamp_ms,
        oldest_sequence: bounds.oldest_sequence,
        newest_sequence: bounds.newest_sequence,
    })
}

/// Resolve an exact sequence against the index, not the inline window. A miss inside the
/// committed bounds is a different error from a selector that aged out.
pub(crate) fn resolve_sequence(
    authority: &Authority,
    branch: &str,
    reference: &BranchRef,
    sequence: u64,
) -> Result<RecoveryPoint> {
    let bounds = recovery_bounds(reference);
    if let Some(point) = authority
        .recovery_lookup_sequence(reference, sequence)?
        .into_iter()
        .max_by_key(|point| (point.timestamp_ms, point.sequence, point.timeline))
    {
        return Ok(point);
    }
    match (bounds.oldest_sequence, bounds.newest_sequence) {
        (Some(oldest), Some(newest)) if (oldest..=newest).contains(&sequence) => {
            Err(point_not_found(branch, &bounds, Some(sequence), None))
        }
        _ => Err(out_of_retention(branch, &bounds, Some(sequence), None)),
    }
}

/// Resolve the latest commit at or before a timestamp. Requesting a time between the newest
/// commit and now resolves to the newest commit, which is the existing restore contract.
pub(crate) fn resolve_timestamp(
    authority: &Authority,
    branch: &str,
    reference: &BranchRef,
    timestamp_ms: u64,
) -> Result<RecoveryPoint> {
    let bounds = recovery_bounds(reference);
    let Some(oldest) = bounds.oldest_timestamp_ms else {
        return Err(out_of_retention(branch, &bounds, None, Some(timestamp_ms)));
    };
    let now_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
        .context("system clock milliseconds overflow")?;
    let last = now_ms.max(reference.last_commit_ms);
    if timestamp_ms < oldest || timestamp_ms > last {
        return Err(out_of_retention(branch, &bounds, None, Some(timestamp_ms)));
    }
    authority
        .recovery_lookup_time(reference, timestamp_ms)?
        .into_iter()
        .max_by_key(|point| (point.timestamp_ms, point.sequence, point.timeline))
        .ok_or_else(|| point_not_found(branch, &bounds, None, Some(timestamp_ms)))
}

fn retention_report(
    branch: &str,
    reference: &BranchRef,
    snapshots: &BTreeMap<String, RecoveryPoint>,
) -> RetentionReport {
    let bounds = recovery_bounds(reference);
    let (indexed, policy) = match &reference.recovery {
        Some(index) => (true, Some(index.policy.clone())),
        None => (false, None),
    };
    let (retention_ms, retention_points) = match policy.as_ref().map(|policy| &policy.mode) {
        Some(crate::authority::RetentionMode::TimeWindow { retain_ms }) => (Some(*retain_ms), None),
        Some(crate::authority::RetentionMode::SequenceWindow { points }) => (None, Some(*points)),
        None => (None, None),
    };
    RetentionReport {
        branch: branch.to_owned(),
        retention_mode: bounds.mode.clone(),
        indexed,
        retention_ms,
        retention_points,
        max_bytes: policy.as_ref().map(|policy| policy.max_bytes),
        anchor_snapshots: policy.as_ref().map(|policy| policy.anchor_snapshots),
        retained_points: bounds.retained_points,
        retained_bytes: reference.recovery.as_ref().map_or(0, |index| index.bytes),
        hot_points: reference.history.len(),
        oldest_recoverable_sequence: bounds.oldest_sequence,
        newest_recoverable_sequence: bounds.newest_sequence,
        oldest_recoverable_timestamp_ms: bounds.oldest_timestamp_ms,
        newest_recoverable_timestamp_ms: bounds.newest_timestamp_ms,
        snapshot_anchors: snapshots
            .values()
            .filter(|snapshot| snapshot.branch == branch && snapshot.sequence > 0)
            .count(),
    }
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
