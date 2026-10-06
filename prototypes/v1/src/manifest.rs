use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SEGMENT_SIZE: usize = 128 * 1024;

#[derive(Default, Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WalPosition {
    pub salts: [u32; 2],
    pub frame_count: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub generation: u64,
    pub logical_db_size: u64,
    pub page_size: u32,
    pub segment_size: u32,
    pub segments: BTreeMap<u64, String>,
    pub applied_wal_position: WalPosition,
    pub applied_lsn: u64,
    pub parent: Option<String>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            generation: 0,
            logical_db_size: 0,
            page_size: 4096,
            segment_size: SEGMENT_SIZE as u32,
            segments: BTreeMap::new(),
            applied_wal_position: WalPosition::default(),
            applied_lsn: 0,
            parent: None,
        }
    }
}

impl Manifest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.segment_size == SEGMENT_SIZE as u32,
            "unsupported segment size"
        );
        ensure!(
            self.page_size.is_power_of_two() && (512..=65536).contains(&self.page_size),
            "invalid page size"
        );
        let count = self.logical_db_size.div_ceil(SEGMENT_SIZE as u64);
        for (index, hash) in &self.segments {
            ensure!(*index < count, "segment outside database");
            ensure!(valid_hash(hash), "invalid segment identifier");
        }
        if let Some(parent) = &self.parent {
            ensure!(valid_hash(parent), "invalid parent identifier");
        }
        Ok(())
    }
}

pub fn digest(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

pub(crate) fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn verify_segment(hash: &str, bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() == SEGMENT_SIZE, "invalid segment length");
    ensure!(digest(bytes) == hash, "segment integrity failure");
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Head {
    pub manifest: String,
}
