use serde::Serialize;

#[derive(Clone, Debug, Default, Serialize)]
pub struct ResourceUsage {
    pub segment_ram_bytes: usize,
    pub segment_disk_bytes: u64,
    pub dirty_segment_bytes: u64,
    pub wal_bytes: usize,
    pub pending_wal_bytes: usize,
    pub segment_loads: u64,
    pub segment_cache_hits_ram: u64,
    pub segment_cache_hits_disk: u64,
    pub segment_cache_misses: u64,
    pub segment_cache_evictions: u64,
    pub segment_cache_lookups: u64,
    pub segment_cache_hit_ratio: Option<f64>,
}
