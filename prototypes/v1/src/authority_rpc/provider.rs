use super::client::AuthorityClient;
use super::protocol::{AuthorityCommand, SessionOperation};
use crate::object_store::{ObjectStore, PutCondition, StoredObject};
use crate::ownership::types::{validate_label, AccessRole, GrantReply, LeaseBinding};
use crate::runtime::{DatabaseId, DatabaseProvider};
use anyhow::{ensure, Context, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub struct RemoteAuthorityProvider {
    client: Arc<AuthorityClient>,
    worker_id: String,
    leases: Mutex<BTreeMap<DatabaseId, (LeaseBinding, AccessRole)>>,
}

pub(crate) fn same_identity(first: &LeaseBinding, second: &LeaseBinding) -> bool {
    first.database == second.database
        && first.role == second.role
        && first.root_incarnation == second.root_incarnation
        && first.authority_term == second.authority_term
        && first.branch_incarnation == second.branch_incarnation
        && first.epoch == second.epoch
        && first.worker_id == second.worker_id
        && first.lease_id == second.lease_id
        && first.grant_id == second.grant_id
}

impl RemoteAuthorityProvider {
    pub fn installed_key_lease(
        &self,
        database: &DatabaseId,
        epoch: u64,
    ) -> Result<crate::encryption::InstalledKeyLease> {
        database.validate()?;
        let (binding, access_role) = self
            .leases
            .lock()
            .map_err(|_| anyhow::anyhow!("lease registry unavailable"))?
            .get(database)
            .cloned()
            .context("database lease not installed")?;
        ensure!(binding.epoch == epoch, "lease epoch mismatch");
        Ok(crate::encryption::InstalledKeyLease {
            binding,
            access_role,
        })
    }
    pub fn new(client: Arc<AuthorityClient>, worker_id: String) -> Result<Arc<Self>> {
        validate_label(&worker_id)?;
        Ok(Arc::new(Self {
            client,
            worker_id,
            leases: Mutex::new(BTreeMap::new()),
        }))
    }

    pub fn install_lease(&self, lease: GrantReply, role: AccessRole) -> Result<()> {
        lease.binding.database.validate()?;
        ensure!(
            lease.binding.worker_id == self.worker_id && lease.remaining_ttl_ms > 0,
            "invalid worker lease"
        );
        ensure!(
            role == AccessRole::ReadOnly || lease.binding.role == AccessRole::Writer,
            "read-only lease cannot grant writer access"
        );
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| anyhow::anyhow!("lease registry unavailable"))?;
        if let Some((current, _)) = leases.get(&lease.binding.database) {
            ensure!(
                lease.binding.epoch >= current.epoch,
                "stale lease installation"
            );
            if lease.binding.epoch == current.epoch {
                ensure!(
                    same_identity(current, &lease.binding)
                        && lease.binding.renewal_sequence >= current.renewal_sequence,
                    "lease identity or renewal regressed"
                );
            }
        }
        leases.insert(lease.binding.database.clone(), (lease.binding, role));
        Ok(())
    }

    pub fn remove_lease(&self, database: &DatabaseId, epoch: u64) -> Result<()> {
        database.validate()?;
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| anyhow::anyhow!("lease registry unavailable"))?;
        if leases
            .get(database)
            .is_some_and(|(binding, _)| binding.epoch == epoch)
        {
            leases.remove(database);
        }
        Ok(())
    }

    pub fn validate_lease(&self, binding: &LeaseBinding, role: AccessRole) -> Result<GrantReply> {
        binding.database.validate()?;
        ensure!(binding.worker_id == self.worker_id, "lease worker mismatch");
        ensure!(
            role == AccessRole::ReadOnly || binding.role == AccessRole::Writer,
            "read-only lease cannot grant writer access"
        );
        let reply: GrantReply =
            serde_json::from_value(self.client.call(AuthorityCommand::ValidateLease {
                binding: binding.clone(),
                role,
            })?)?;
        ensure!(
            same_identity(binding, &reply.binding)
                && reply.binding.renewal_sequence >= binding.renewal_sequence
                && reply.remaining_ttl_ms > 0,
            "authority lease validation mismatch"
        );
        Ok(reply)
    }
}

impl DatabaseProvider for RemoteAuthorityProvider {
    fn open_store(&self, database: &DatabaseId, epoch: u64) -> Result<Arc<dyn ObjectStore>> {
        database.validate()?;
        let (binding, role) = self
            .leases
            .lock()
            .map_err(|_| anyhow::anyhow!("lease registry unavailable"))?
            .get(database)
            .cloned()
            .context("database lease not installed")?;
        ensure!(binding.epoch == epoch, "lease epoch mismatch");
        self.validate_lease(&binding, role)?;
        let reply = self.client.call(AuthorityCommand::OpenSession {
            binding: binding.clone(),
            role,
        })?;
        let session = reply["session"]
            .as_str()
            .context("authority session missing")?
            .to_owned();
        ensure!(
            !session.is_empty()
                && session.len() <= 128
                && session.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid opaque authority session"
        );
        Ok(Arc::new(RemoteStore {
            client: self.client.clone(),
            session,
            binding,
            role,
            retirement: std::sync::atomic::AtomicU8::new(0),
        }))
    }
}

struct RemoteStore {
    client: Arc<AuthorityClient>,
    session: String,
    binding: LeaseBinding,
    role: AccessRole,
    retirement: std::sync::atomic::AtomicU8,
}

impl RemoteStore {
    fn call(&self, operation: SessionOperation) -> Result<serde_json::Value> {
        ensure!(
            self.retirement.load(std::sync::atomic::Ordering::Acquire) == 0,
            "remote scope retired or unsettled"
        );
        self.client.call(AuthorityCommand::Session {
            session: self.session.clone(),
            operation,
        })
    }

    fn key(&self, key: &str) -> Result<()> {
        let prefix = self.binding.database.namespace.prefix();
        let relative = key
            .strip_prefix(&prefix)
            .context("unauthorized namespace")?;
        self.binding.database.namespace.key(relative)?;
        ensure!(
            relative == "HEAD"
                || relative == "wal/index"
                || ["segments/", "manifests/", "wal/chunks/"]
                    .iter()
                    .any(|prefix| relative
                        .strip_prefix(prefix)
                        .is_some_and(crate::manifest::valid_hash)),
            "unauthorized logical object key"
        );
        Ok(())
    }
}

impl ObjectStore for RemoteStore {
    fn upload_segments(&self, segments: &[Vec<u8>]) -> Result<Vec<crate::authority::ObjectEntry>> {
        super::protocol::validate_segment_batch(segments)?;
        ensure!(self.role == AccessRole::Writer, "ReadOnly");
        let entries: Vec<crate::authority::ObjectEntry> =
            serde_json::from_value(self.call(SessionOperation::UploadSegments {
                segments: segments.to_vec(),
            })?)?;
        ensure!(
            entries.len() == segments.len()
                && entries.iter().zip(segments).all(|(entry, bytes)| {
                    entry.hash == crate::manifest::digest(bytes)
                        && crate::manifest::valid_hash(&entry.blob)
                        && crate::manifest::valid_hash(&entry.etag)
                }),
            "invalid segment upload response"
        );
        Ok(entries)
    }

    fn register_segments(&self, entries: &[crate::authority::ObjectEntry]) -> Result<()> {
        super::protocol::validate_registration_batch(entries)?;
        ensure!(self.role == AccessRole::Writer, "ReadOnly");
        let reply = self.call(SessionOperation::RegisterSegments {
            entries: entries.to_vec(),
        })?;
        ensure!(reply.is_null(), "invalid segment registration response");
        Ok(())
    }

    fn append_wal(&self, request: &crate::wal::WalAppendRequest) -> Result<Option<u64>> {
        request.validate()?;
        ensure!(
            self.role == AccessRole::Writer
                && request.namespace == self.binding.database.namespace
                && request.epoch == self.binding.epoch,
            "Fenced WAL append identity"
        );
        let result = (|| {
            let lsn: u64 = serde_json::from_value(self.call(SessionOperation::AppendWal {
                request: request.clone(),
            })?)?;
            ensure!(lsn > 0, "invalid WAL append result");
            Ok(Some(lsn))
        })();
        if result.as_ref().is_err_and(|error: &anyhow::Error| {
            !error.is::<super::client::KnownAuthorityRejection>()
                && !error.is::<crate::object_store::PreconditionFailed>()
        }) {
            self.retirement
                .store(3, std::sync::atomic::Ordering::Release);
        }
        result
    }

    fn retire_scope(&self, deadline: std::time::Instant) -> Result<()> {
        use std::sync::atomic::Ordering;
        match self
            .retirement
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(2) => return Ok(()),
            Err(_) => anyhow::bail!("remote scope retirement pending or unknown; refusing retry"),
        }
        let result = (|| {
            let proof = self.client.call_until(
                AuthorityCommand::CloseSession {
                    session: self.session.clone(),
                },
                deadline,
            )?;
            let binding: LeaseBinding = serde_json::from_value(proof["binding"].clone())?;
            ensure!(
                proof["closed"] == true && proof["drained"] == true && binding == self.binding,
                "remote scope retirement proof mismatch"
            );
            Ok(())
        })();
        self.retirement
            .store(if result.is_ok() { 2 } else { 3 }, Ordering::Release);
        result
    }

    fn access_role(&self) -> AccessRole {
        self.role
    }

    fn checked_get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.get(key)
    }

    fn checked_put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.put(key, bytes, condition)
    }

    fn checked_delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.delete(key, expected)
    }

    fn checked_list(&self, prefix: &str) -> Result<Vec<String>> {
        self.list(prefix)
    }

    fn validate_session(&self) -> Result<()> {
        let reply: GrantReply = serde_json::from_value(self.call(SessionOperation::Validate)?)?;
        ensure!(
            same_identity(&self.binding, &reply.binding)
                && reply.binding.renewal_sequence >= self.binding.renewal_sequence
                && reply.remaining_ttl_ms > 0,
            "authority session lease mismatch"
        );
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let get = || {
            self.key(key)?;
            Ok(serde_json::from_value(self.call(
                SessionOperation::Get {
                    key: key.to_owned(),
                },
            )?)?)
        };
        let segment = key
            .strip_prefix(&self.binding.database.namespace.prefix())
            .is_some_and(|relative| relative.starts_with("segments/"));
        if segment {
            crate::metrics::Metrics::global().observe(
                crate::metrics::Operation::SegmentGet,
                get,
                |object: &Option<StoredObject>| {
                    object
                        .as_ref()
                        .map_or(0, |object| object.bytes.len() as u64)
                },
            )
        } else {
            get()
        }
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let put = || {
            self.key(key)?;
            ensure!(self.role == AccessRole::Writer, "ReadOnly");
            Ok(serde_json::from_value(self.call(
                SessionOperation::Put {
                    key: key.to_owned(),
                    bytes: bytes.to_vec(),
                    condition,
                },
            )?)?)
        };
        let segment = key
            .strip_prefix(&self.binding.database.namespace.prefix())
            .is_some_and(|relative| relative.starts_with("segments/"));
        if segment {
            crate::metrics::Metrics::global().observe(
                crate::metrics::Operation::SegmentPut,
                put,
                |_| bytes.len() as u64,
            )
        } else {
            put()
        }
    }

    fn delete(&self, _: &str, _: Option<&str>) -> Result<()> {
        anyhow::bail!("unauthorized worker delete")
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let namespace = self.binding.database.namespace.prefix();
        let relative = prefix
            .strip_prefix(&namespace)
            .context("unauthorized namespace")?;
        ensure!(
            ["", "segments/", "manifests/", "wal/chunks/", "wal/"].contains(&relative),
            "unauthorized list prefix"
        );
        Ok(serde_json::from_value(self.call(
            SessionOperation::List {
                prefix: prefix.to_owned(),
            },
        )?)?)
    }
}

impl Drop for RemoteStore {
    fn drop(&mut self) {
        if self.retirement.load(std::sync::atomic::Ordering::Acquire) != 0 {
            return;
        }
        let _ = self.client.call(AuthorityCommand::CloseSession {
            session: self.session.clone(),
        });
    }
}
