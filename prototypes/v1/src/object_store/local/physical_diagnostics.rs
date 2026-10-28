//! Best-effort exposure of existing local-store counters. No durability decision
//! or business result depends on these observations.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

#[cfg(test)]
mod tests;

pub(super) struct CountersDiagnostic {
    instance: u64,
    sequence: AtomicU64,
}

#[derive(Clone, Copy)]
pub(super) enum SnapshotReason {
    SuccessfulGroupBoundary,
    StoreDrop,
}

impl SnapshotReason {
    fn name(self) -> &'static str {
        match self {
            Self::SuccessfulGroupBoundary => "successful_group_boundary",
            Self::StoreDrop => "store_drop",
        }
    }
}

impl CountersDiagnostic {
    pub(super) fn new(enabled: bool) -> Option<Self> {
        if !enabled {
            return None;
        }
        static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);
        Some(Self {
            instance: NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed),
            sequence: AtomicU64::new(0),
        })
    }

    fn observe(
        &self,
        reason: SnapshotReason,
        group_id: Option<u64>,
        snapshot: impl FnOnce() -> Value,
        output: impl FnOnce(&Value),
    ) {
        // JSON construction and sink writes are diagnostic-only. Normal Drop
        // uses this path; Drop during an existing unwind is skipped by its caller.
        // Do not propagate a diagnostic panic.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let record = serde_json::json!({
                "schema": "peri-local-storage-counters-v1",
                "event": "local_store_counters",
                "evidence_class": "diagnostic",
                "pid": std::process::id(),
                "store_instance_id": self.instance,
                "snapshot_sequence": sequence,
                "snapshot_reason": reason.name(),
                "snapshot_unix_ns": SystemTime::now().duration_since(UNIX_EPOCH)
                    .map_or(0, |time| time.as_nanos().min(u64::MAX as u128) as u64),
                "boundary_group_id": group_id,
                "scope": "store_instance_since_construction",
                "value_semantics": "counters_cumulative_cache_occupancy_gauges_limits_static",
                "atomic_snapshot": false,
                "group_delta": false,
                "coverage_complete": false,
                "durable_diagnostic_log": false,
                "unwinding": std::thread::panicking(),
                "metrics": snapshot(),
            });
            output(&record);
        }));
    }
}

pub(super) fn emit_if_enabled(
    diagnostic: Option<&CountersDiagnostic>,
    reason: SnapshotReason,
    group_id: Option<u64>,
    snapshot: impl FnOnce() -> Value,
) {
    let Some(diagnostic) = diagnostic else {
        // In particular do not evaluate snapshot(), acquire the cache lock,
        // allocate JSON, or read a clock when diagnostics are disabled.
        return;
    };
    diagnostic.observe(reason, group_id, snapshot, |record| {
        #[cfg(test)]
        if tests::capture(record) {
            return;
        }
        let _ = writeln!(std::io::stderr().lock(), "STORAGE_PHASE {record}");
    });
}
