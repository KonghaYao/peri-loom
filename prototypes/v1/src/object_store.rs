mod local;
mod memory;
pub mod metrics;
#[path = "object_store/local/payload.rs"]
pub(crate) mod payload;
mod s3;
mod signing;
mod transport;
mod xml;

pub use local::LocalObjectStore;
pub use memory::MemoryObjectStore;
pub use s3::{S3Config, S3ObjectStore};

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredObject {
    pub bytes: Vec<u8>,
    pub etag: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "etag",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PutCondition {
    Any,
    Absent,
    Match(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreconditionFailed;

impl std::fmt::Display for PreconditionFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("object store precondition failed")
    }
}

impl std::error::Error for PreconditionFailed {}

pub trait ObjectStore: Send + Sync {
    fn append_wal(&self, _: &crate::wal::WalAppendRequest) -> Result<Option<u64>> {
        Ok(None)
    }

    fn retire_scope(&self, _: std::time::Instant) -> Result<()> {
        Ok(())
    }

    fn supports_strict_fencing(&self) -> bool {
        false
    }

    fn validate_session(&self) -> Result<()> {
        Ok(())
    }

    fn validate_local(&self) -> Result<()> {
        self.validate_session()
    }

    fn access_role(&self) -> crate::ownership::types::AccessRole {
        crate::ownership::types::AccessRole::Writer
    }

    fn validate_transaction(&self, _in_transaction: bool) -> Result<()> {
        self.validate_session()
    }

    fn checked_get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.validate_session()?;
        self.get(key)
    }

    fn checked_put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.validate_session()?;
        self.put(key, bytes, condition)
    }

    fn checked_delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.validate_session()?;
        self.delete(key, expected)
    }

    fn checked_list(&self, prefix: &str) -> Result<Vec<String>> {
        self.validate_session()?;
        self.list(prefix)
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>>;
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String>;
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()>;
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

fn validate_key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= 1024,
        "invalid object key length"
    );
    for component in key.split('/') {
        ensure!(
            !component.is_empty() && component != "." && component != "..",
            "invalid object key component"
        );
        ensure!(
            component
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte)),
            "object keys must be ASCII URL-safe paths"
        );
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        return Ok(());
    }
    validate_key(prefix.strip_suffix('/').unwrap_or(prefix))
}

fn check_condition(current: Option<&StoredObject>, condition: &PutCondition) -> Result<()> {
    let matches = match condition {
        PutCondition::Any => true,
        PutCondition::Absent => current.is_none(),
        PutCondition::Match(expected) => current.is_some_and(|object| object.etag == *expected),
    };
    ensure!(matches, PreconditionFailed);
    Ok(())
}

fn fresh_etag(bytes: &[u8]) -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let mut hash = blake3::Hasher::new();
    hash.update(bytes);
    hash.update(&std::process::id().to_le_bytes());
    hash.update(&SEQUENCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    hash.update(
        &std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    hash.finalize().to_hex().to_string()
}
