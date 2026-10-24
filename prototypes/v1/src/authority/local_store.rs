#[cfg(test)]
#[path = "local_store_tests.rs"]
mod tests;

use super::BranchStore;
use crate::{
    namespace::Namespace,
    object_store::{GroupWrite, ObjectStore, PutCondition, StoredObject},
    wal::{RemoteWal, WalAppendRequest},
};
use anyhow::{ensure, Result};
use std::sync::Arc;

/// The branch owns the root, incarnation and reader pin; this adapter only binds the
/// local open epoch. It is not a lease authority and does not claim strict fencing.
pub(crate) struct LocalStore {
    inner: Arc<BranchStore>,
    namespace: Namespace,
    epoch: u64,
}

impl LocalStore {
    pub(crate) fn new(inner: Arc<BranchStore>, namespace: Namespace, epoch: u64) -> Self {
        Self {
            inner,
            namespace,
            epoch,
        }
    }
}

impl ObjectStore for LocalStore {
    fn append_wal(&self, request: &WalAppendRequest) -> Result<Option<u64>> {
        request.validate_local()?;
        ensure!(
            request.namespace == self.namespace && request.epoch == self.epoch,
            "local WAL append identity mismatch"
        );
        self.inner.validate_session()?;
        // Use the bound branch directly, not append_one/append_commit, which would
        // dispatch to this hook again. All publication validation remains in BranchStore.
        let lsn = RemoteWal::new(self.inner.clone()).append_using(
            request,
            |encoded, metadata, expected| {
                self.inner.publish_wal_append(encoded, metadata, expected)
            },
        )?;
        Ok(Some(lsn))
    }

    fn validate_local(&self) -> Result<()> {
        self.inner.validate_local()
    }

    fn validate_session(&self) -> Result<()> {
        self.inner.validate_session()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn get_retained(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get_retained(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }

    fn put_group(&self, writes: &[GroupWrite]) -> Result<Vec<String>> {
        self.inner.put_group(writes)
    }

    fn publish_pages(&self, writes: &[GroupWrite]) -> Result<bool> {
        self.inner.publish_pages(writes)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}
