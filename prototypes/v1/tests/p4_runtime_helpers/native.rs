use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{ensure, Result};
use peri_loom_v1::object_store::{ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::ownership::types::AccessRole;

pub struct PolicyStore {
    pub inner: Arc<dyn ObjectStore>,
    pub role: AccessRole,
    pub mode: AtomicU8,
    pub mutations: AtomicUsize,
    pub transaction_checks: AtomicUsize,
    pub session_checks: AtomicUsize,
    pub root_gets: AtomicUsize,
}

impl PolicyStore {
    pub fn new(inner: Arc<dyn ObjectStore>, role: AccessRole) -> Arc<Self> {
        Arc::new(Self {
            inner,
            role,
            mode: AtomicU8::new(0),
            mutations: AtomicUsize::new(0),
            transaction_checks: AtomicUsize::new(0),
            session_checks: AtomicUsize::new(0),
            root_gets: AtomicUsize::new(0),
        })
    }
}

impl ObjectStore for PolicyStore {
    fn access_role(&self) -> AccessRole {
        self.role
    }

    fn validate_session(&self) -> Result<()> {
        self.session_checks.fetch_add(1, Ordering::SeqCst);
        ensure!(self.mode.load(Ordering::SeqCst) != 2, "local lease revoked");
        self.inner.validate_session()
    }

    fn validate_local(&self) -> Result<()> {
        ensure!(self.mode.load(Ordering::SeqCst) != 2, "local lease revoked");
        self.inner.validate_local()
    }

    fn validate_transaction(&self, in_transaction: bool) -> Result<()> {
        self.transaction_checks.fetch_add(1, Ordering::SeqCst);
        self.validate_session()?;
        ensure!(
            self.mode.load(Ordering::SeqCst) != 1 || in_transaction,
            "draining cannot start a transaction"
        );
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.ends_with("/authority/root") {
            self.root_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.validate_session()?;
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        ensure!(self.role == AccessRole::Writer, "read-only backend");
        self.validate_session()?;
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        ensure!(self.role == AccessRole::Writer, "read-only backend");
        self.validate_session()?;
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.validate_session()?;
        self.inner.list(prefix)
    }
}
