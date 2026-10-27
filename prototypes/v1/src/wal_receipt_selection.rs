/// The archive is keyed by the full digest path, whereas receipt age is unrelated to
/// that path. A contiguous run in digest order targets fewer leaves than the oldest
/// equally-sized set, though individual page splits can still cost more. Always start
/// at the oldest eligible receipt so every roll makes
/// age progress, without changing the inline budget, roll cadence or archive format.
pub(super) fn select(candidates: Vec<(u64, String)>, target: usize) -> Vec<(u64, String)> {
    if candidates.len() <= target {
        let mut candidates = candidates;
        candidates.sort();
        return candidates;
    }
    if target == 0 {
        return Vec::new();
    }
    let mut ordered = candidates
        .into_iter()
        .map(|(lsn, identity)| (*blake3::hash(identity.as_bytes()).as_bytes(), lsn, identity))
        .collect::<Vec<_>>();
    // LSN and identity make even a digest collision deterministic. They do not
    // change which trie paths are adjacent.
    ordered.sort_unstable();
    let oldest = ordered
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| (left.1, &left.2).cmp(&(right.1, &right.2)))
        .map(|(position, _)| position)
        .unwrap();
    ordered.rotate_left(oldest);
    ordered.truncate(target);
    let mut selected = ordered
        .into_iter()
        .map(|(_, lsn, identity)| (lsn, identity))
        .collect::<Vec<_>>();
    // Recovery reconstructs the actual removed set in this canonical order,
    // using the existing archive batch size. Selection must not change that order.
    selected.sort();
    selected
}

#[cfg(test)]
#[path = "wal_receipt_selection_tests.rs"]
mod tests;
