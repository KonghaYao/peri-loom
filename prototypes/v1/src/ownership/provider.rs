use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, ensure, Context, Result};

use crate::authority_rpc::provider::RemoteAuthorityProvider;
use crate::object_store::{ObjectStore, PutCondition, StoredObject};
use crate::runtime::{DatabaseId, DatabaseProvider};

use super::context::LeaseContext;
use super::types::AccessRole;

pub(super) type Contexts = Arc<Mutex<BTreeMap<DatabaseId, Arc<LeaseContext>>>>;

pub(super) struct ContextProvider {
    pub remote: Arc<RemoteAuthorityProvider>,
    pub contexts: Contexts,
}

impl DatabaseProvider for ContextProvider {
    fn open_store(&self, identifier: &DatabaseId, epoch: u64) -> Result<Arc<dyn ObjectStore>> {
        let context = self
            .contexts
            .lock()
            .map_err(|_| anyhow!("ownership registry poisoned"))?
            .get(identifier)
            .cloned()
            .context("database ownership not acquired")?;
        context.check(Some(false))?;
        let (role, generation) = {
            let state = context.lock()?;
            ensure!(
                state.binding.epoch == epoch,
                "local ownership epoch mismatch"
            );
            (state.role, state.generation)
        };
        let inner = self.remote.open_store(identifier, epoch)?;
        context.check(Some(false))?;
        ensure!(inner.access_role() == role, "remote scope role mismatch");
        Ok(Arc::new(ContextStore {
            inner,
            context,
            role,
            generation,
        }))
    }
}

pub(super) struct ContextStore {
    pub inner: Arc<dyn ObjectStore>,
    pub context: Arc<LeaseContext>,
    pub role: AccessRole,
    pub generation: u64,
}

impl ContextStore {
    fn generation(&self) -> Result<()> {
        ensure!(
            self.context.lock()?.generation == self.generation,
            "scoped store generation revoked"
        );
        Ok(())
    }

    fn mutation(&self) -> Result<()> {
        ensure!(
            self.role == AccessRole::Writer,
            "read-only ownership capability"
        );
        self.validate_session()
    }
}

impl ObjectStore for ContextStore {
    fn access_role(&self) -> AccessRole {
        self.role
    }

    fn validate_local(&self) -> Result<()> {
        self.generation()?;
        self.context.check(None)?;
        self.generation()
    }

    fn validate_session(&self) -> Result<()> {
        self.generation()?;
        self.context.check(None)?;
        self.inner.validate_session()?;
        self.context.check(None)?;
        self.generation()
    }

    fn validate_transaction(&self, in_transaction: bool) -> Result<()> {
        self.generation()?;
        self.context.check(Some(in_transaction))?;
        self.inner.validate_transaction(in_transaction)?;
        self.context.check(Some(in_transaction))?;
        self.generation()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.validate_session()?;
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.mutation()?;
        self.inner.put(key, bytes, condition)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.mutation()?;
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.validate_session()?;
        self.inner.list(prefix)
    }
}
