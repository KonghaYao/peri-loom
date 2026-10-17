use super::{Permission, Service};
use crate::{
    authority::{branch::report_pin_release_failure, BranchStore},
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
                || [
                    "segments/",
                    "manifests/",
                    "wal/chunks/",
                    "wal/receipt-pages/",
                    "wal/index-segments/",
                ]
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
    fn upload_segments(&self, segments: &[Vec<u8>]) -> Result<Vec<crate::authority::ObjectEntry>> {
        crate::authority_rpc::protocol::validate_segment_batch(segments)?;
        ensure!(self.role == AccessRole::Writer, "ReadOnly");
        let _ticket = self.service.gate.read()?;
        self.service.check_binding(&self.binding)?;
        self.service
            .scoped(super::Permission::Session(self.binding.clone(), self.role))
            .upload_segments(segments)
    }

    fn register_segments(&self, entries: &[crate::authority::ObjectEntry]) -> Result<()> {
        crate::authority_rpc::protocol::validate_registration_batch(entries)?;
        ensure!(self.role == AccessRole::Writer, "ReadOnly");
        let _ticket = self.service.gate.read()?;
        self.service.check_binding(&self.binding)?;
        self.service
            .scoped(super::Permission::Session(self.binding.clone(), self.role))
            .register_segments(entries)
    }

    fn append_wal(&self, request: &crate::wal::WalAppendRequest) -> Result<Option<u64>> {
        request.validate()?;
        ensure!(
            self.role == AccessRole::Writer
                && request.namespace == self.binding.database.namespace
                && request.epoch == self.binding.epoch,
            "Fenced WAL append identity"
        );
        let _ticket = self.service.gate.read()?;
        self.service.check_binding(&self.binding)?;
        let store = Arc::new(GatedWalStore {
            service: self.service.clone(),
            binding: self.binding.clone(),
            inner: self.inner.as_ref().unwrap().clone(),
        });
        let lsn = crate::wal::RemoteWal::new(store).append_using(
            request,
            |encoded, metadata, expected| {
                self.inner
                    .as_ref()
                    .unwrap()
                    .publish_wal_append(encoded, metadata, expected)
            },
        )?;
        self.service.live(&self.binding)?;
        Ok(Some(lsn))
    }

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
        self.service.live(&self.binding)?;
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
        match self.service.gate.write_wait() {
            Ok(_barrier) => {
                match self.service.available() {
                    Ok(()) => {
                        let scoped = self.service.scoped(Permission::Pins);
                        if let Some(inner) = &self.inner {
                            if let Err(error) = inner.release_pin(&scoped) {
                                report_pin_release_failure(
                                    inner.pin_identity(),
                                    "fenced-release",
                                    &error,
                                );
                            }
                        }
                    }
                    Err(error) => report_pin_release_failure(
                        pin_identity(self.inner.as_deref()),
                        "fenced-availability",
                        &error,
                    ),
                }
                drop(self.inner.take());
            }
            Err(error) => {
                report_pin_release_failure(
                    pin_identity(self.inner.as_deref()),
                    "fenced-gate",
                    &error,
                );
                drop(self.inner.take());
            }
        }
    }
}

fn pin_identity(inner: Option<&BranchStore>) -> &str {
    inner.map(BranchStore::pin_identity).unwrap_or("<missing>")
}

struct GatedWalStore {
    service: Arc<Service>,
    binding: LeaseBinding,
    inner: Arc<BranchStore>,
}

impl ObjectStore for GatedWalStore {
    fn validate_session(&self) -> Result<()> {
        self.service.live(&self.binding)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.validate_session()?;
        let result = self.inner.get(key)?;
        self.validate_session()?;
        Ok(result)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.validate_session()?;
        let result = self.inner.put(key, bytes, condition)?;
        self.validate_session()?;
        Ok(result)
    }

    fn delete(&self, _: &str, _: Option<&str>) -> Result<()> {
        anyhow::bail!("Unauthorized WAL append delete")
    }

    fn list(&self, _: &str) -> Result<Vec<String>> {
        anyhow::bail!("Unauthorized WAL append list")
    }
}
