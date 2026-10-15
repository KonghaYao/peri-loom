/// Reserve at most half of the container limit for database segments and
/// sessions. The rest remains available for WAL, SQL work, runtime overhead,
/// and the operating system. This is admission accounting, not an allocator cap.
pub(super) fn cgroup_reservation_budget() -> Option<usize> {
    let limit = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let bytes = limit.trim().parse::<usize>().ok()?;
    Some(bytes / 2)
}

/// The per-session reserve covers the default pager and its minimum safe
/// 200-page capacity even when SQLite page size is 64 KiB.
const SESSION_RESERVATION_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn fits_reservation(
    databases: usize,
    sessions: usize,
    segment_ram_bytes: usize,
    budget: usize,
) -> bool {
    databases
        .checked_mul(segment_ram_bytes)
        .and_then(|bytes| {
            sessions
                .checked_mul(SESSION_RESERVATION_BYTES)
                .and_then(|sessions| bytes.checked_add(sessions))
        })
        .is_some_and(|required| required <= budget)
}

#[cfg(test)]
mod tests {
    use super::fits_reservation;

    #[test]
    fn reservations_limit_512m_and_2g_workers_without_overflow() {
        let mib = 1024 * 1024;
        assert!(fits_reservation(10, 11, 8 * mib, 256 * mib));
        assert!(!fits_reservation(11, 11, 8 * mib, 256 * mib));
        assert!(!fits_reservation(32, 128, 8 * mib, 1024 * mib));
        assert!(fits_reservation(32, 48, 8 * mib, 1024 * mib));
        assert!(!fits_reservation(usize::MAX, 2, 8 * mib, usize::MAX));
    }
}
