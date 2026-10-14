use std::sync::Arc;

use anyhow::Result;

use crate::metrics::{Metrics, Operation};
use crate::ownership::types::AccessRole;

use super::{ObjectStore, PutCondition, StoredObject};

pub struct MetricsObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<Metrics>,
}

impl MetricsObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }
}

impl ObjectStore for MetricsObjectStore {
    fn append_wal(&self, request: &crate::wal::WalAppendRequest) -> Result<Option<u64>> {
        self.inner.append_wal(request)
    }

    fn retire_scope(&self, deadline: std::time::Instant) -> Result<()> {
        self.inner.retire_scope(deadline)
    }

    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn validate_session(&self) -> Result<()> {
        self.inner.validate_session()
    }

    fn validate_local(&self) -> Result<()> {
        self.inner.validate_local()
    }

    fn access_role(&self) -> AccessRole {
        self.inner.access_role()
    }

    fn validate_transaction(&self, in_transaction: bool) -> Result<()> {
        self.inner.validate_transaction(in_transaction)
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.metrics.observe(
            Operation::Get,
            || self.inner.get(key),
            |object| {
                object
                    .as_ref()
                    .map_or(0, |object| object.bytes.len() as u64)
            },
        )
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.metrics.observe(
            Operation::Put,
            || self.inner.put(key, bytes, condition),
            |_| bytes.len() as u64,
        )
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.metrics.observe(
            Operation::Delete,
            || self.inner.delete(key, expected),
            |_| 0,
        )
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.metrics.observe(
            Operation::List,
            || self.inner.list(prefix),
            |keys| keys.iter().map(|key| key.len() as u64).sum(),
        )
    }
}
