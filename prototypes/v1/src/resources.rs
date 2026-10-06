use serde::Serialize;

#[derive(Clone, Debug, Default, Serialize)]
pub struct ResourceUsage {
    pub segment_ram_bytes: usize,
    pub segment_disk_bytes: u64,
    pub dirty_segment_bytes: u64,
    pub wal_bytes: usize,
    pub pending_wal_bytes: usize,
    pub segment_loads: u64,
}
