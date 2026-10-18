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
                || [
                    "segments/",
                    "manifests/",
                    "wal/chunks/",
                    "wal/receipt-pages/",
                    "wal/index-segments/"
                ]
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
        // Advisory pre-check only: the authority re-checks the binding on every
        // operation it serves and the head CAS is the final fence, so repeating
        // this round trip inside one request adds latency without adding
        // protection. The local checks in the wrapper store still run.
        if crate::session::lease_window::satisfied() {
            return Ok(());
        }
        let reply: GrantReply = serde_json::from_value(self.call(SessionOperation::Validate)?)?;
        ensure!(
            same_identity(&self.binding, &reply.binding)
                && reply.binding.renewal_sequence >= self.binding.renewal_sequence
                && reply.remaining_ttl_ms > 0,
            "authority session lease mismatch"
        );
        crate::session::lease_window::mark_satisfied();
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

    /// Batched immutable segment publication. One call carries up to `MAX_GROUP_WRITES`
    /// bodies and becomes one authority publication and one media barrier; the fallback
    /// keeps every other key or condition on the single-write path. The bodies are content
    /// addressed, so the caller's conditional check stays at the manifest/HEAD fence.
    fn put_group(&self, writes: &[crate::object_store::GroupWrite]) -> Result<Vec<String>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let prefix = self.binding.database.namespace.prefix();
        let segments = writes.iter().all(|write| {
            matches!(write.condition, PutCondition::Absent)
                && write.bytes.len() == crate::manifest::SEGMENT_SIZE
                && write
                    .key
                    .strip_prefix(&prefix)
                    .and_then(|relative| relative.strip_prefix("segments/"))
                    .is_some_and(crate::manifest::valid_hash)
        });
        if !segments {
            return writes
                .iter()
                .map(|write| self.put(&write.key, &write.bytes, write.condition.clone()))
                .collect();
        }
        ensure!(self.role == AccessRole::Writer, "ReadOnly");
        let mut etags = Vec::with_capacity(writes.len());
        let mut start = 0;
        while start < writes.len() {
            let mut end = start;
            let mut bytes = 0usize;
            while end < writes.len()
                && end - start < super::protocol::MAX_GROUP_WRITES
                && (end == start
                    || bytes + writes[end].bytes.len() <= super::protocol::MAX_GROUP_BYTES)
            {
                bytes += writes[end].bytes.len();
                end += 1;
            }
            let batch = &writes[start..end];
            for write in batch {
                self.key(&write.key)?;
            }
            let payload = batch
                .iter()
                .map(|write| super::protocol::SegmentGroupWrite {
                    key: write.key.clone(),
                    bytes: write.bytes.clone(),
                })
                .collect::<Vec<_>>();
            let started = std::time::Instant::now();
            let result = self.call(SessionOperation::PutGroup { writes: payload });
            let share = started.elapsed() / batch.len().max(1) as u32;
            for write in batch {
                crate::metrics::Metrics::global().record(
                    crate::metrics::Operation::SegmentPut,
                    share,
                    result.is_ok(),
                    write.bytes.len() as u64,
                );
            }
            let reply: Vec<String> = serde_json::from_value(result?)?;
            ensure!(reply.len() == batch.len(), "invalid segment group response");
            etags.extend(reply);
            start = end;
        }
        Ok(etags)
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

#[cfg(test)]
mod lease_window_tests {
    use super::*;
    use crate::namespace::Namespace;
    use crate::object_store::ObjectStore;
    use crate::ownership::types::AccessRole;
    use crate::runtime::DatabaseId;
    use crate::session::lease_window;
    use crate::wal::{WalAppendRequest, WalBatch};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Minimal authority that answers every request from a per-operation table,
    /// so a test can retire a lease in the middle of a request cycle.
    struct FakeAuthority {
        address: String,
        seen: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl FakeAuthority {
        fn start(
            reply: impl Fn(&str) -> Result<serde_json::Value> + Send + 'static,
        ) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let address = format!("http://{}/", listener.local_addr()?);
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let recorded = seen.clone();
            let requests = Arc::new(AtomicU64::new(0));
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(20);
                for stream in listener.incoming() {
                    if Instant::now() > deadline {
                        return;
                    }
                    let Ok(mut stream) = stream else { return };
                    let _ = handle(&mut stream, &reply, &recorded, &requests);
                    if requests.load(Ordering::SeqCst) >= 8 {
                        return;
                    }
                }
            });
            Ok(Self { address, seen })
        }
    }

    fn handle(
        stream: &mut TcpStream,
        reply: &(impl Fn(&str) -> Result<serde_json::Value> + Send + 'static),
        recorded: &Arc<std::sync::Mutex<Vec<String>>>,
        requests: &AtomicU64,
    ) -> Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let request = crate::authority_rpc::http::read_request(stream)?;
        let parsed: serde_json::Value = serde_json::from_slice(&request.body)?;
        let operation = parsed["command"]["operation"]["operation"]
            .as_str()
            .unwrap_or_else(|| parsed["command"]["command"].as_str().unwrap_or("?"))
            .to_owned();
        recorded.lock().unwrap().push(operation.clone());
        requests.fetch_add(1, Ordering::SeqCst);
        let body = match reply(&operation) {
            Ok(mut value) => {
                value["request_id"] = parsed["request_id"].clone();
                value
            }
            Err(error) => serde_json::json!({"version": 1, "request_id": parsed["request_id"],
                "ok": false, "result": null, "error": format!("{error:#}"),
                "precondition_failed": false}),
        };
        let bytes = serde_json::to_vec(&body)?;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            bytes.len()
        )?;
        stream.write_all(&bytes)?;
        stream.flush()?;
        let mut drain = [0u8; 1];
        let _ = stream.read(&mut drain);
        Ok(())
    }

    fn binding() -> LeaseBinding {
        LeaseBinding {
            database: DatabaseId::new(Namespace::new("tenant", "database").unwrap(), "main")
                .unwrap(),
            root_incarnation: "root".into(),
            authority_term: 1,
            branch_incarnation: 1,
            epoch: 7,
            worker_id: "worker".into(),
            lease_id: "lease".into(),
            grant_id: "grant".into(),
            renewal_sequence: 3,
            role: AccessRole::Writer,
        }
    }

    fn store(address: &str, binding: LeaseBinding) -> Result<RemoteStore> {
        Ok(RemoteStore {
            client: AuthorityClient::new(address, "synthetic_worker_credential_0123456789".into())?,
            session: "synthetic_session".into(),
            binding,
            role: AccessRole::Writer,
            retirement: std::sync::atomic::AtomicU8::new(0),
        })
    }

    fn validate_reply(binding: &LeaseBinding) -> serde_json::Value {
        serde_json::json!({"version": 1, "request_id": "r", "ok": true,
            "result": {"binding": serde_json::to_value(binding).unwrap(),
                "remaining_ttl_ms": 60_000},
            "error": null, "precondition_failed": false})
    }

    fn wal_request() -> WalAppendRequest {
        WalAppendRequest {
            namespace: Namespace::new("tenant", "database").unwrap(),
            epoch: 7,
            batch: WalBatch::default(),
            position: None,
            append_id: "append".into(),
            timestamp_millis: 1,
        }
    }

    /// The window merges validate round trips, so the fence has to be re-proved:
    /// a lease that dies in the middle of the cycle must still block the
    /// publication even though the window is already satisfied.
    #[test]
    fn satisfied_window_still_cannot_publish_after_the_lease_dies() -> Result<()> {
        let boundary = binding();
        let authority = FakeAuthority::start(move |operation| match operation {
            "validate" => Ok(validate_reply(&boundary)),
            // The authority retires the binding between the validation and the
            // write, exactly what an epoch change looks like to this worker.
            _ => anyhow::bail!("Fenced WAL append identity"),
        })?;
        let store = store(&authority.address, binding())?;

        let _window = lease_window::open();
        store
            .validate_session()
            .expect("first validation must reach authority");
        assert!(lease_window::satisfied());

        let error = store
            .append_wal(&wal_request())
            .expect_err("superseded lease still published");
        assert!(
            error.is::<crate::authority_rpc::client::KnownAuthorityRejection>(),
            "rejection was not surfaced as an authority refusal"
        );
        let seen = authority.seen.lock().unwrap().clone();
        assert_eq!(seen, vec!["validate".to_owned(), "append_wal".to_owned()]);
        Ok(())
    }

    /// The merged path must not turn a failed first validation into a silent
    /// pass: if the authority rejects the lease, the cycle never gets a window.
    #[test]
    fn failed_first_validation_leaves_the_window_unsatisfied() -> Result<()> {
        let authority = FakeAuthority::start(|operation| match operation {
            "validate" => anyhow::bail!("LeaseExpired"),
            _ => Ok(serde_json::Value::Null),
        })?;
        let store = store(&authority.address, binding())?;

        let _window = lease_window::open();
        let error = store
            .validate_session()
            .expect_err("expired lease accepted");
        assert!(
            error.is::<crate::authority_rpc::client::KnownAuthorityRejection>(),
            "expired lease was not rejected by the authority"
        );
        assert!(
            !lease_window::satisfied(),
            "window opened on a rejected validation"
        );

        store
            .validate_session()
            .expect_err("rejected lease accepted on the retry");
        let seen = authority.seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec!["validate".to_owned(), "validate".to_owned()],
            "a rejected cycle stopped reaching the authority"
        );
        Ok(())
    }

    /// Merging only applies inside a window; outside one every validation still
    /// reaches the authority.
    #[test]
    fn outside_a_window_every_validation_reaches_the_authority() -> Result<()> {
        let boundary = binding();
        let authority = FakeAuthority::start(move |_| Ok(validate_reply(&boundary)))?;
        let store = store(&authority.address, binding())?;
        store.validate_session()?;
        store.validate_session()?;
        let seen = authority.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "validation was merged without a window");
        Ok(())
    }
}
