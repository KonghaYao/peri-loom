use std::sync::Arc;

use anyhow::Result;

use crate::metrics::{Metrics, Operation};
use crate::ownership::types::AccessRole;

use super::{ObjectStore, PutCondition, StoredObject};

pub struct MetricsObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<Metrics>,
    backend: &'static str,
}

impl MetricsObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, metrics: Arc<Metrics>) -> Self {
        Self {
            inner,
            metrics,
            backend: "wrapped",
        }
    }

    pub(crate) fn with_backend(mut self, backend: &'static str) -> Self {
        self.backend = backend;
        self
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

    fn group_rejection_publishes_nothing(&self) -> bool {
        self.inner.group_rejection_publishes_nothing()
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

    /// Forwards the whole group in one call: splitting it per write would put
    /// the per-write durability barrier right back. The group is accounted as
    /// `writes.len()` puts sharing one elapsed measurement, so `total`, `bytes`
    /// and `latency_total_ns` stay comparable with the unbatched path.
    fn put_group(&self, writes: &[super::GroupWrite]) -> Result<Vec<String>> {
        let started = std::time::Instant::now();
        let timer = crate::slow_diagnostics::Timer::begin();
        let result = self.inner.put_group(writes);
        let share = started.elapsed() / writes.len().max(1) as u32;
        if let Some(timer) = timer {
            timer.finish(crate::slow_diagnostics::outcome(&result), None, |_| {
                crate::slow_diagnostics::object_io(
                    "backend",
                    "PUT_GROUP",
                    "other",
                    self.backend,
                    writes.iter().map(|write| write.bytes.len() as u64).sum(),
                    writes.len(),
                    None,
                )
            });
        }
        for write in writes {
            self.metrics.record(
                Operation::Put,
                share,
                result.is_ok(),
                write.bytes.len() as u64,
            );
        }
        result
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let timer = crate::slow_diagnostics::Timer::begin();
        let result = self.metrics.observe(
            Operation::Get,
            || self.inner.get(key),
            |object| {
                object
                    .as_ref()
                    .map_or(0, |object| object.bytes.len() as u64)
            },
        );
        if let Some(timer) = timer {
            timer.finish(crate::slow_diagnostics::outcome(&result), None, |_| {
                crate::slow_diagnostics::object_io(
                    "backend",
                    "GET",
                    crate::slow_diagnostics::object_class(key),
                    self.backend,
                    result
                        .as_ref()
                        .ok()
                        .and_then(|object| object.as_ref())
                        .map_or(0, |object| object.bytes.len() as u64),
                    0,
                    None,
                )
            });
        }
        result
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let timer = crate::slow_diagnostics::Timer::begin();
        let result = self.metrics.observe(
            Operation::Put,
            || self.inner.put(key, bytes, condition),
            |_| bytes.len() as u64,
        );
        if let Some(timer) = timer {
            timer.finish(crate::slow_diagnostics::outcome(&result), None, |_| {
                crate::slow_diagnostics::object_io(
                    "backend",
                    "PUT",
                    crate::slow_diagnostics::object_class(key),
                    self.backend,
                    bytes.len() as u64,
                    1,
                    None,
                )
            });
        }
        result
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

#[cfg(test)]
mod group_tests {
    use super::*;
    use crate::object_store::{GroupWrite, LocalObjectStore};

    /// The wrapper must forward the whole group: a per-write fan-out would
    /// restore the per-write durability barrier and lose the whole point.
    #[test]
    fn wrapper_forwards_the_group_without_resplitting_it() {
        let directory = tempfile::tempdir().unwrap();
        let inner =
            std::sync::Arc::new(LocalObjectStore::open(directory.path().join("objects")).unwrap());
        let metrics = std::sync::Arc::new(Metrics::default());
        let store = MetricsObjectStore::new(inner.clone(), metrics.clone());
        let writes: Vec<GroupWrite> = (0..6)
            .map(|index| GroupWrite::absent(format!("pages/{index:02}"), vec![index as u8; 1024]))
            .collect();

        let before = crate::object_store::local::barrier_count();
        let etags = store.put_group(&writes).unwrap();
        let bars = crate::object_store::local::barrier_count() - before;

        assert_eq!(etags.len(), 6);
        assert_eq!(
            bars, 1,
            "group must reach the store as one durability group"
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.put.total, 6, "each write still counted");
        assert_eq!(
            snapshot.put.bytes,
            writes
                .iter()
                .map(|write| write.bytes.len() as u64)
                .sum::<u64>()
        );
    }
}
