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
        let (role, generation, binding) = {
            let state = context.lock()?;
            ensure!(
                state.binding.epoch == epoch,
                "local ownership epoch mismatch"
            );
            (state.role, state.generation, state.binding.clone())
        };
        let inner = self.remote.open_store(identifier, epoch)?;
        context.check(Some(false))?;
        ensure!(inner.access_role() == role, "remote scope role mismatch");
        Ok(Arc::new(ContextStore {
            inner,
            context,
            role,
            generation,
            binding,
        }))
    }
}

pub(super) struct ContextStore {
    pub inner: Arc<dyn ObjectStore>,
    pub context: Arc<LeaseContext>,
    pub role: AccessRole,
    pub generation: u64,
    pub binding: super::types::LeaseBinding,
}

impl ContextStore {
    fn generation(&self) -> Result<()> {
        let state = self.context.lock()?;
        ensure!(
            state.generation == self.generation
                && crate::authority_rpc::provider::same_identity(&self.binding, &state.binding)
                && state.binding.renewal_sequence >= self.binding.renewal_sequence,
            "scoped store generation revoked"
        );
        Ok(())
    }

    fn mutation(&self) -> Result<()> {
        ensure!(
            self.role == AccessRole::Writer,
            "read-only ownership capability"
        );
        self.validate_local()
    }
}

impl ObjectStore for ContextStore {
    fn append_wal(&self, request: &crate::wal::WalAppendRequest) -> Result<Option<u64>> {
        self.mutation()?;
        request.validate()?;
        {
            let state = self.context.lock()?;
            ensure!(
                request.namespace == state.binding.database.namespace
                    && request.epoch == state.binding.epoch,
                "Fenced WAL append identity"
            );
        }
        let result = self.inner.append_wal(request)?;
        self.validate_local()?;
        Ok(result)
    }

    fn retire_scope(&self, deadline: std::time::Instant) -> Result<()> {
        self.inner.retire_scope(deadline)
    }

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
        self.validate_local()?;
        let object = self.inner.checked_get(key)?;
        self.validate_local()?;
        Ok(object)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.mutation()?;
        let etag = self.inner.checked_put(key, bytes, condition)?;
        self.validate_local()?;
        Ok(etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.mutation()?;
        self.inner.checked_delete(key, expected)?;
        self.validate_local()
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.validate_local()?;
        let keys = self.inner.checked_list(prefix)?;
        self.validate_local()?;
        Ok(keys)
    }
}
