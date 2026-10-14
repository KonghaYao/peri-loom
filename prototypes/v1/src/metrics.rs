use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Serialize;

pub const LATENCY_BOUNDS_NS: [u64; 6] = [
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    5_000_000_000,
];

#[derive(Clone, Copy)]
pub enum Operation {
    Query,
    Execute,
    Checkpoint,
    Get,
    Put,
    List,
    Delete,
    SegmentGet,
    SegmentPut,
    WalDurability,
    NativeCommitDurability,
    ColdOpen,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct OperationMetrics {
    pub total: u64,
    pub errors: u64,
    pub bytes: u64,
    pub latency_total_ns: u64,
    pub latency_max_ns: u64,
    pub latency_buckets: [u64; 7],
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PhysicalBackendMetrics {
    pub get_successes: u64,
    pub put_successes: u64,
    pub get_errors: u64,
    pub put_errors: u64,
    pub get_payload_bytes: u64,
    pub put_payload_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MetricsSnapshot {
    pub query: OperationMetrics,
    pub execute: OperationMetrics,
    pub checkpoint: OperationMetrics,
    pub get: OperationMetrics,
    pub put: OperationMetrics,
    pub list: OperationMetrics,
    pub delete: OperationMetrics,
    pub segment_get: OperationMetrics,
    pub segment_put: OperationMetrics,
    pub wal_durability: OperationMetrics,
    pub native_commit_durability: OperationMetrics,
    pub cold_open: OperationMetrics,
    pub measurement_scopes: MeasurementScopes,
    pub latency_bounds_ns: [u64; 6],
}

#[derive(Clone, Debug, Serialize)]
pub struct MeasurementScopes {
    pub segment_cache: &'static str,
    pub segment_io: &'static str,
    pub wal_durability: &'static str,
    pub native_commit_durability: &'static str,
    pub cold_open: &'static str,
    pub bytes: &'static str,
}

impl Default for MeasurementScopes {
    fn default() -> Self {
        Self {
            segment_cache: "ResourceUsage segment_cache_hit_ratio = (RAM hits + SSD hits) / segment cache lookups (RAM hits + SSD hits + misses); null before any lookup; not native pager hit ratio",
            segment_io: "logical RemoteStore segment session RPC including key/role validation, authority fencing, transport and response decoding; not raw backend IO",
            wal_durability: "RemoteWal.append_commit through confirmed durable WAL index publication (authority root CAS for RemoteStore); excludes preceding sync_wal preflight validation and SQL execution",
            native_commit_durability: "wal_durability subset with a newly validated native WAL commit position; identical append elapsed time; not full SQL COMMIT statement latency",
            cold_open: "one WorkerRuntime cold load including store admission, key resolution when encrypted, native open and recovery; excludes waiting/coalesced or reused sessions",
            bytes: "successful segment object payload or WAL batch Write payload bytes; excludes transport/envelope bytes; failed operations contribute zero",
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    state: Mutex<MetricsSnapshot>,
}

impl Metrics {
    pub fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<Metrics>> = OnceLock::new();
        GLOBAL.get_or_init(|| Arc::new(Self::default())).clone()
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut snapshot = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        snapshot.latency_bounds_ns = LATENCY_BOUNDS_NS;
        snapshot
    }

    pub fn observe<T>(
        &self,
        operation: Operation,
        action: impl FnOnce() -> anyhow::Result<T>,
        bytes: impl FnOnce(&T) -> u64,
    ) -> anyhow::Result<T> {
        let started = Instant::now();
        let result = action();
        let elapsed = started.elapsed();
        let transferred = result.as_ref().map(bytes).unwrap_or(0);
        self.record(operation, elapsed, result.is_ok(), transferred);
        result
    }

    pub(crate) fn record(
        &self,
        operation: Operation,
        elapsed: Duration,
        success: bool,
        bytes: u64,
    ) {
        let elapsed = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metric = match operation {
            Operation::Query => &mut state.query,
            Operation::Execute => &mut state.execute,
            Operation::Checkpoint => &mut state.checkpoint,
            Operation::Get => &mut state.get,
            Operation::Put => &mut state.put,
            Operation::List => &mut state.list,
            Operation::Delete => &mut state.delete,
            Operation::SegmentGet => &mut state.segment_get,
            Operation::SegmentPut => &mut state.segment_put,
            Operation::WalDurability => &mut state.wal_durability,
            Operation::NativeCommitDurability => &mut state.native_commit_durability,
            Operation::ColdOpen => &mut state.cold_open,
        };
        metric.total = metric.total.saturating_add(1);
        metric.errors = metric.errors.saturating_add(u64::from(!success));
        metric.bytes = metric.bytes.saturating_add(bytes);
        metric.latency_total_ns = metric.latency_total_ns.saturating_add(elapsed);
        metric.latency_max_ns = metric.latency_max_ns.max(elapsed);
        for (index, bound) in LATENCY_BOUNDS_NS.iter().enumerate() {
            if elapsed <= *bound {
                metric.latency_buckets[index] = metric.latency_buckets[index].saturating_add(1);
            }
        }
        metric.latency_buckets[6] = metric.latency_buckets[6].saturating_add(1);
    }
}
