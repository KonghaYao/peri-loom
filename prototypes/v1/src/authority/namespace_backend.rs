use crate::object_store::{ObjectStore, PreconditionFailed, PutCondition, StoredObject};
use anyhow::{ensure, Result};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub(super) struct NamespaceBackend {
    inner: Arc<dyn ObjectStore>,
    uncertain: AtomicBool,
}

impl NamespaceBackend {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            uncertain: AtomicBool::new(false),
        })
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
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.available()?;
        self.observe(self.inner.put(key, bytes, condition))
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.available()?;
        self.observe(self.inner.delete(key, expected))
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}
