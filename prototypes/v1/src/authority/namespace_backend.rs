use crate::object_store::{ObjectStore, PreconditionFailed, PutCondition, StoredObject};
use anyhow::{ensure, Result};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

/// What the writes this backend dispatched proved about one initialization attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Disposition {
    /// Nothing was dispatched: a pure read or preflight failure.
    None,
    /// Every dispatched write was rejected without becoming visible. A group above one
    /// write only lands here when the backend declares
    /// [`crate::object_store::ObjectStore::group_rejection_publishes_nothing`].
    Rejected,
    /// Every dispatched write was acknowledged as durable.
    Committed,
    /// At least one write may have landed without proof of what happened.
    Unknown,
}

#[derive(Debug)]
pub(super) struct ReadFailure;

impl std::fmt::Display for ReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("namespace backend read failed")
    }
}

impl std::error::Error for ReadFailure {}

pub(super) struct NamespaceBackend {
    inner: Arc<dyn ObjectStore>,
    uncertain: AtomicBool,
    initializing: AtomicBool,
    dispatches: AtomicU64,
    rejected: AtomicU64,
    committed: AtomicU64,
    trace: Option<Arc<crate::capacity_trace::Trace>>,
}

impl NamespaceBackend {
    pub fn scoped(
        inner: Arc<dyn ObjectStore>,
        namespace: crate::namespace::Namespace,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            uncertain: AtomicBool::new(false),
            initializing: AtomicBool::new(true),
            dispatches: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            trace: crate::capacity_trace::Trace::new(Some(namespace)),
        })
    }

    fn start(&self, operation: &'static str) -> Option<u64> {
        self.trace.as_ref().map(|trace| {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
            let io_id = NEXT.fetch_add(1, Ordering::Relaxed);
            trace.record(crate::capacity_trace::Event {
                kind: "backend_start",
                monotonic_micros: crate::capacity_trace::micros(),
                operation: Some(operation),
                io_id: Some(io_id),
                ..Default::default()
            });
            io_id
        })
    }

    fn finish(
        &self,
        operation: &'static str,
        io_id: Option<u64>,
        success: bool,
        bytes: Option<u64>,
    ) {
        if let Some(trace) = &self.trace {
            trace.record(crate::capacity_trace::Event {
                kind: "backend_finish",
                monotonic_micros: crate::capacity_trace::micros(),
                operation: Some(operation),
                io_id,
                success: Some(success),
                bytes,
                ..Default::default()
            });
        }
    }
    pub fn available(&self) -> Result<()> {
        ensure!(
            !self.uncertain.load(Ordering::Acquire),
            "UnknownOutcome namespace backend blocked"
        );
        Ok(())
    }
    pub fn finish_initialization(&self) {
        self.initializing.store(false, Ordering::Release);
    }

    pub fn disposition(&self) -> Disposition {
        let dispatches = self.dispatches.load(Ordering::Acquire);
        let rejected = self.rejected.load(Ordering::Acquire);
        let committed = self.committed.load(Ordering::Acquire);
        if self.uncertain.load(Ordering::Acquire) || dispatches != rejected + committed {
            Disposition::Unknown
        } else if committed > 0 {
            Disposition::Committed
        } else if rejected > 0 {
            Disposition::Rejected
        } else {
            Disposition::None
        }
    }

    fn observe<T>(&self, outcome: Result<T>) -> Result<T> {
        match &outcome {
            Ok(_) => {
                self.committed.fetch_add(1, Ordering::Release);
            }
            Err(error) if error.is::<PreconditionFailed>() => {
                self.rejected.fetch_add(1, Ordering::Release);
            }
            Err(_) => self.uncertain.store(true, Ordering::Release),
        }
        outcome
    }
}

impl ObjectStore for NamespaceBackend {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }
    fn group_rejection_publishes_nothing(&self) -> bool {
        self.inner.group_rejection_publishes_nothing()
    }
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let result = self.inner.get(key);
        if let Some(trace) = &self.trace {
            let bytes = result.as_ref().ok().map(|object| {
                object
                    .as_ref()
                    .map_or(0, |object| object.bytes.len() as u64)
            });
            trace.backend_get(result.is_ok(), bytes);
        }
        result.map_err(|error| error.context(ReadFailure))
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.available()?;
        self.dispatches.fetch_add(1, Ordering::Release);
        let io_id = self.start("put");
        let result = self.inner.put(key, bytes, condition);
        self.finish(
            "put",
            io_id,
            result.is_ok(),
            result.as_ref().ok().map(|_| bytes.len() as u64),
        );
        self.observe(result)
    }
    /// The authority publishes through this backend, so a durability group has to reach
    /// the underlying store as a group: falling back to the trait default would silently
    /// turn one commit into one barrier per page.
    fn put_group(&self, writes: &[crate::object_store::GroupWrite]) -> Result<Vec<String>> {
        self.available()?;
        self.dispatches.fetch_add(1, Ordering::Release);
        let io_id = self.start("put_group");
        let bytes = writes.iter().map(|write| write.bytes.len() as u64).sum();
        let result = self.inner.put_group(writes);
        // A single write has nothing to publish before it fails, but above that the
        // rejection only proves "nothing became visible" when the backend declares it.
        // An undeclared backend may have published the earlier bodies, so the attempt
        // is not allowed back into the retryable path. Scoped to initialization: that
        // is where a rejection is read as "this attempt dispatched nothing", while the
        // established `observe` semantics of a running namespace stay unchanged.
        if self.initializing.load(Ordering::Acquire)
            && writes.len() > 1
            && !self.inner.group_rejection_publishes_nothing()
            && result
                .as_ref()
                .is_err_and(|error| error.is::<PreconditionFailed>())
        {
            self.uncertain.store(true, Ordering::Release);
        }
        self.finish("put_group", io_id, result.is_ok(), Some(bytes));
        self.observe(result)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.available()?;
        self.dispatches.fetch_add(1, Ordering::Release);
        self.observe(self.inner.delete(key, expected))
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::Namespace;
    use crate::object_store::MemoryObjectStore;

    #[test]
    fn disposition_counts_rejections_commits_and_partial_groups() -> Result<()> {
        use crate::object_store::GroupWrite;
        let raw = Arc::new(MemoryObjectStore::default());
        raw.put("root", b"initial", PutCondition::Absent)?;
        let backend = NamespaceBackend::scoped(raw.clone(), Namespace::new("init", "evidence")?);
        assert_eq!(backend.disposition(), Disposition::None);
        assert!(backend
            .put("root", b"rejected", PutCondition::Absent)
            .is_err());
        assert_eq!(backend.disposition(), Disposition::Rejected);
        assert!(backend.delete("root", Some("wrong-etag")).is_err());
        assert_eq!(backend.disposition(), Disposition::Rejected);
        backend.delete("root", None)?;
        assert_eq!(backend.disposition(), Disposition::Committed);
        raw.put("root", b"present", PutCondition::Absent)?;
        let partial = NamespaceBackend::scoped(raw.clone(), Namespace::new("init", "partial")?);
        assert!(partial
            .put_group(&[
                GroupWrite::absent("body", b"body".to_vec()),
                GroupWrite::absent("root", b"root".to_vec()),
            ])
            .is_err());
        assert!(raw.get("body")?.is_some());
        assert_eq!(partial.disposition(), Disposition::Unknown);
        assert!(partial.available().is_err());
        Ok(())
    }

    #[test]
    fn physical_put_windows_and_get_counters_use_only_successful_payloads() -> Result<()> {
        let trace = crate::capacity_trace::test_namespace_trace(Namespace::new("tenant", "db")?);
        let backend = NamespaceBackend {
            inner: Arc::new(MemoryObjectStore::default()),
            uncertain: AtomicBool::new(false),
            initializing: AtomicBool::new(true),
            dispatches: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            trace: Some(trace.clone()),
        };
        backend.put("secret/backend/key", b"abc", PutCondition::Absent)?;
        assert!(backend
            .put(
                "secret/backend/key",
                b"failed-payload",
                PutCondition::Absent
            )
            .is_err());
        assert_eq!(backend.get("secret/backend/key")?.unwrap().bytes, b"abc");
        let snapshot = trace.test_events();
        assert_eq!(snapshot["events"].as_array().unwrap().len(), 4);
        assert_eq!(snapshot["events"][0]["bytesBefore"], 0);
        assert_eq!(snapshot["events"][1]["bytesAfter"], 3);
        assert_eq!(snapshot["events"][1]["bytes"], 3);
        assert_eq!(snapshot["events"][3]["success"], false);
        assert!(snapshot["events"][3]["bytesAfter"].is_null());
        assert_eq!(snapshot["backendMetrics"]["get_successes"], 1);
        assert_eq!(snapshot["backendMetrics"]["put_successes"], 1);
        assert_eq!(snapshot["backendMetrics"]["put_errors"], 1);
        assert_eq!(snapshot["backendMetrics"]["get_payload_bytes"], 3);
        assert_eq!(snapshot["backendMetrics"]["put_payload_bytes"], 3);
        assert!(!serde_json::to_string(&snapshot)?.contains("secret/backend/key"));
        Ok(())
    }
}
