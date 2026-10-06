use super::{Permission, Service};
use crate::{
    authority::BranchStore,
    object_store::{ObjectStore, PutCondition, StoredObject},
    ownership::types::{AccessRole, LeaseBinding},
};
use anyhow::{anyhow, ensure, Result};
use std::sync::Arc;

pub(super) struct FencedStore {
    service: Arc<Service>,
    binding: LeaseBinding,
    role: AccessRole,
    inner: Option<Arc<BranchStore>>,
}

impl FencedStore {
    pub fn new(
        service: Arc<Service>,
        binding: LeaseBinding,
        role: AccessRole,
        inner: Arc<BranchStore>,
    ) -> Self {
        Self {
            service,
            binding,
            role,
            inner: Some(inner),
        }
    }

    fn key(&self, key: &str) -> Result<String> {
        let prefix = self.binding.database.namespace.prefix();
        let relative = key
            .strip_prefix(&prefix)
            .ok_or_else(|| anyhow!("Unauthorized namespace"))?;
        self.binding.database.namespace.key(relative)?;
        ensure!(
            relative == "HEAD"
                || relative == "wal/index"
                || ["segments/", "manifests/", "wal/chunks/"]
                    .iter()
                    .any(|prefix| relative
                        .strip_prefix(prefix)
                        .is_some_and(crate::manifest::valid_hash)),
            "Unauthorized engine key"
        );
        Ok(relative.to_owned())
    }
}

impl ObjectStore for FencedStore {
    fn access_role(&self) -> AccessRole {
        self.role
    }

    fn validate_local(&self) -> Result<()> {
        self.service.live(&self.binding)?;
        Ok(())
    }

    fn validate_session(&self) -> Result<()> {
        let _ticket = self.service.gate.read()?;
        self.service.check_binding(&self.binding)?;
        self.inner.as_ref().unwrap().validate_session()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.key(key)?;
        let _ticket = self.service.gate.read()?;
        self.service.check_binding(&self.binding)?;
        self.inner.as_ref().unwrap().get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let relative = self.key(key)?;
        ensure!(self.role == AccessRole::Writer, "ReadOnly");
        if relative != "HEAD" && relative != "wal/index" {
            ensure!(
                matches!(condition, PutCondition::Absent),
                "Unauthorized immutable condition"
            );
            let hash = relative.rsplit('/').next().unwrap();
            ensure!(
                blake3::hash(bytes).to_hex().as_str() == hash,
                "immutable checksum mismatch"
            );
            if relative.starts_with("wal/chunks/") {
                let record: crate::wal::WalRecord = serde_json::from_slice(bytes)?;
                ensure!(
                    record.namespace == self.binding.database.namespace
                        && record.epoch == self.binding.epoch
                        && record.lsn > 0,
                    "Fenced WAL chunk identity"
                );
            }
        } else {
            ensure!(
                !matches!(condition, PutCondition::Any),
                "Unauthorized unconditional mutable publication"
            );
        }
        let _ticket = self.service.gate.read()?;
        self.service.check_binding(&self.binding)?;
        self.inner.as_ref().unwrap().put(key, bytes, condition)
    }

    fn delete(&self, _: &str, _: Option<&str>) -> Result<()> {
        anyhow::bail!("Unauthorized worker delete")
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let namespace = self.binding.database.namespace.prefix();
        let relative = prefix
            .strip_prefix(&namespace)
            .ok_or_else(|| anyhow!("Unauthorized namespace"))?;
        ensure!(
            ["", "segments/", "manifests/", "wal/chunks/", "wal/"].contains(&relative),
            "Unauthorized list prefix"
        );
        let _ticket = self.service.gate.read()?;
        self.service.check_binding(&self.binding)?;
        self.inner.as_ref().unwrap().list(prefix)
    }
}

impl Drop for FencedStore {
    fn drop(&mut self) {
        if let Ok(_barrier) = self.service.gate.write_wait() {
            if self.service.available().is_ok() {
                let scoped = self.service.scoped(Permission::Pins);
                if let Some(inner) = &self.inner {
                    let _ = inner.release_pin(&scoped);
                }
            }
            drop(self.inner.take());
        }
    }
}
