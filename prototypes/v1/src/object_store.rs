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
    #[serde(with = "payload")]
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

/// One write inside a durability group handed to [`ObjectStore::put_group`].
#[derive(Clone, Debug)]
pub struct GroupWrite {
    pub key: String,
    pub bytes: Vec<u8>,
    pub condition: PutCondition,
}

impl GroupWrite {
    pub fn new(key: impl Into<String>, bytes: Vec<u8>, condition: PutCondition) -> Self {
        Self {
            key: key.into(),
            bytes,
            condition,
        }
    }

    /// Immutable content-addressed body: already-present is success, not failure.
    pub fn absent(key: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self::new(key, bytes, PutCondition::Absent)
    }
}

pub trait ObjectStore: Send + Sync {
    fn upload_segments(&self, _: &[Vec<u8>]) -> Result<Vec<crate::authority::ObjectEntry>> {
        anyhow::bail!("batched segment upload unavailable")
    }

    fn register_segments(&self, _: &[crate::authority::ObjectEntry]) -> Result<()> {
        anyhow::bail!("batched segment registration unavailable")
    }

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

    /// Publish several objects as a single durability group.
    ///
    /// Caller contract — a group is a *durability* unit, not a transaction:
    ///
    /// * `writes` must be ordered so that the write which makes the group
    ///   visible (the root / CAS write) comes **last**. Every earlier write is
    ///   expected to be an immutable, content-addressed body that is harmless
    ///   to publish on its own.
    /// * On `Ok`, every write in the group has passed its precondition and is
    ///   durable before this call returns. The returned etags follow `writes`
    ///   order.
    /// * `Err(PreconditionFailed)`: all conditions are evaluated against the
    ///   pre-group state, so no write of the group became visible. That promise is
    ///   binding only for backends that declare [`Self::group_rejection_publishes_nothing`];
    ///   the portable fallback below cannot keep it.
    /// * Any other error means the group is **not** committed. Writes carrying
    ///   [`PutCondition::Absent`] may still have landed; repeating them is safe
    ///   because their key is derived from their content.
    /// * Fencing is unchanged. This is a storage-layer batching primitive: the
    ///   caller still holds the only trusted store, and the CAS write inside the
    ///   group stays the final fence.
    ///
    /// Implementations must not turn one group into `writes.len()` independent
    /// durability barriers when a single barrier can cover the group. The
    /// default below is the portable fallback: it preserves the contract but
    /// pays a full barrier per write.
    fn put_group(&self, writes: &[GroupWrite]) -> Result<Vec<String>> {
        writes
            .iter()
            .map(|write| self.put(&write.key, &write.bytes, write.condition.clone()))
            .collect()
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()>;
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Whether a rejected group proves that *no* write of the group became visible.
    ///
    /// A caller that turns `put_group`'s `PreconditionFailed` into "this attempt
    /// changed nothing" — and therefore retries it — is relying on a property the
    /// default implementation below does not have: it evaluates one condition per
    /// `put`, so a failure on a later write leaves the earlier, condition-free
    /// bodies behind. Those bodies are content-addressed and unreferenced, but the
    /// group is still partly published, and a caller that cannot tell the two
    /// situations apart has to keep treating the attempt as unsettled.
    ///
    /// The default is therefore the conservative answer: an undeclared backend is
    /// reported as if the rejection may have published part of the group. Backends
    /// that evaluate every condition against the pre-group state before writing the
    /// first body override this and let their callers retry.
    fn group_rejection_publishes_nothing(&self) -> bool {
        false
    }
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
