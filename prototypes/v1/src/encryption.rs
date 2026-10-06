mod broker;

pub use broker::BrokerDatabaseKeyProvider;

use crate::ownership::types::{AccessRole, LeaseBinding};
use crate::runtime::DatabaseId;
use anyhow::Result;
use turso_core::EncryptionKey;
use zeroize::{Zeroize, Zeroizing};

pub struct InstalledKeyLease {
    pub binding: LeaseBinding,
    pub access_role: AccessRole,
}

pub trait DatabaseKeyProvider: Send + Sync {
    fn resolve(&self, database: &DatabaseId, epoch: u64) -> Result<ResolvedDatabaseKey>;
}

pub struct ResolvedDatabaseKey {
    descriptor_id: String,
    descriptor_revision: u64,
    domain_id: String,
    dek_id: String,
    binding: LeaseBinding,
    access_role: AccessRole,
    dek: [u8; 32],
}

impl ResolvedDatabaseKey {
    pub fn descriptor_id(&self) -> &str {
        &self.descriptor_id
    }
    pub fn descriptor_revision(&self) -> u64 {
        self.descriptor_revision
    }
    pub fn domain_id(&self) -> &str {
        &self.domain_id
    }
    pub fn dek_id(&self) -> &str {
        &self.dek_id
    }

    pub(crate) fn validate_open(
        &self,
        namespace: &crate::namespace::Namespace,
        epoch: u64,
        role: AccessRole,
    ) -> Result<()> {
        anyhow::ensure!(
            self.binding.database.namespace == *namespace
                && self.binding.epoch == epoch
                && self.access_role == role,
            "encrypted open identity mismatch"
        );
        Ok(())
    }

    pub(crate) fn native_key(&self) -> EncryptionKey {
        EncryptionKey::new_256(self.dek)
    }

    pub(crate) fn hex_key(&self) -> Zeroizing<String> {
        use std::fmt::Write;
        let mut encoded = Zeroizing::new(String::with_capacity(64));
        for byte in &self.dek {
            write!(&mut *encoded, "{byte:02x}").expect("string formatting");
        }
        encoded
    }
}

impl Drop for ResolvedDatabaseKey {
    fn drop(&mut self) {
        self.dek.zeroize();
    }
}
