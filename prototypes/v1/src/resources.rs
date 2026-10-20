use serde::Serialize;

#[derive(Clone, Debug, Default, Serialize)]
pub struct ResourceUsage {
    pub segment_ram_bytes: usize,
    pub segment_disk_bytes: u64,
    pub dirty_segment_bytes: u64,
    pub wal_bytes: usize,
    pub pending_wal_bytes: usize,
    /// Hard WAL capacity in bytes, or zero when no capacity budget is configured. This is a
    /// real refusal limit; the maintenance watermarks are not.
    pub wal_capacity_bytes: u64,
    /// True when the materialized WAL, including the pending batch, crossed the budget's soft
    /// mark. It arms maintenance early and never rejects a write by itself.
    pub wal_pressure: bool,
    pub segment_loads: u64,
    pub segment_cache_hits_ram: u64,
    pub segment_cache_hits_disk: u64,
    pub segment_cache_misses: u64,
    pub segment_cache_evictions: u64,
    pub segment_cache_lookups: u64,
    pub segment_cache_hit_ratio: Option<f64>,
}
