use crate::object_store::{ObjectStore, PreconditionFailed, PutCondition, StoredObject};
use anyhow::{ensure, Result};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub(super) struct NamespaceBackend {
    inner: Arc<dyn ObjectStore>,
    uncertain: AtomicBool,
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
    fn observe<T>(&self, outcome: Result<T>) -> Result<T> {
        if outcome
            .as_ref()
            .is_err_and(|error| !error.is::<PreconditionFailed>())
        {
            self.uncertain.store(true, Ordering::Release);
        }
        outcome
    }
}

impl ObjectStore for NamespaceBackend {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
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
        result
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.available()?;
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
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.available()?;
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
    fn physical_put_windows_and_get_counters_use_only_successful_payloads() -> Result<()> {
        let trace = crate::capacity_trace::test_namespace_trace(Namespace::new("tenant", "db")?);
        let backend = NamespaceBackend {
            inner: Arc::new(MemoryObjectStore::default()),
            uncertain: AtomicBool::new(false),
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
