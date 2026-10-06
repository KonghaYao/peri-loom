use anyhow::{Context, Result};
use peri_loom_v1::authority::ownership::OwnershipAuthority;
use peri_loom_v1::authority_rpc::{
    auth::AuthConfig, client::AuthorityClient, protocol::AuthorityCommand, server::Server,
    startup::AuthorityStartup,
};
use peri_loom_v1::object_store::MemoryObjectStore;
use peri_loom_v1::ownership::types::{
    AccessRole, GrantIntent, GrantReply, GrantRequest, LeaseClock, LeaseIntent, NonceReply,
};
use peri_loom_v1::runtime::DatabaseId;
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

pub fn token() -> Result<String> {
    let mut bytes = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn private_directory() -> Result<tempfile::TempDir> {
    let directory = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(directory)
}

pub struct Clock(pub AtomicU64);

impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

pub struct Harness {
    pub control: Arc<AuthorityClient>,
    pub worker: Arc<AuthorityClient>,
    pub other: Arc<AuthorityClient>,
    pub owner: Arc<OwnershipAuthority>,
    pub raw: Arc<MemoryObjectStore>,
    pub clock: Arc<Clock>,
    pub endpoint: String,
    pub control_token: String,
    pub worker_token: String,
    thread: Option<JoinHandle<Result<()>>>,
    _directory: Option<tempfile::TempDir>,
}

impl Harness {
    pub fn new() -> Result<Self> {
        let directory = private_directory()?;
        let mut server = Self::at(
            directory.path(),
            Arc::new(MemoryObjectStore::default()),
            false,
        )?;
        server._directory = Some(directory);
        Ok(server)
    }

    pub fn at(
        directory: &std::path::Path,
        raw: Arc<MemoryObjectStore>,
        reconciled: bool,
    ) -> Result<Self> {
        let startup = AuthorityStartup::acquire(directory, reconciled)?;
        let clock = Arc::new(Clock(AtomicU64::new(100)));
        let owner = OwnershipAuthority::new(raw.clone(), clock.clone());
        let control_token = token()?;
        let worker_token = token()?;
        let other_token = token()?;
        let auth = AuthConfig::new(
            control_token.clone(),
            BTreeMap::from([
                ("worker-a".to_owned(), worker_token.clone()),
                ("worker-b".to_owned(), other_token.clone()),
            ]),
        )?;
        let server = Server::bind("127.0.0.1:0".parse()?, owner.clone(), auth, startup)?;
        let endpoint = format!("http://{}/", server.local_addr());
        let control = AuthorityClient::new(&endpoint, control_token.clone())?;
        let worker = AuthorityClient::new(&endpoint, worker_token.clone())?;
        let other = AuthorityClient::new(&endpoint, other_token)?;
        let thread = thread::spawn(move || server.run());
        Ok(Self {
            control,
            worker,
            other,
            owner,
            raw,
            clock,
            endpoint,
            control_token,
            worker_token,
            thread: Some(thread),
            _directory: None,
        })
    }

    pub fn grant_request(
        &self,
        database: DatabaseId,
        worker: &str,
        epoch: u64,
        expected: u64,
    ) -> Result<GrantRequest> {
        let intent = GrantIntent {
            role: AccessRole::Writer,
            database,
            worker_id: worker.to_owned(),
            epoch,
            expected_epoch: expected,
            lease_id: token()?,
            ttl_ms: 10_000,
        };
        let nonce: NonceReply =
            serde_json::from_value(self.control.call(AuthorityCommand::Challenge {
                intent: LeaseIntent::Grant(intent.clone()),
            })?)?;
        Ok(GrantRequest {
            request_id: token()?,
            nonce: nonce.nonce,
            intent,
        })
    }

    pub fn grant(
        &self,
        database: DatabaseId,
        worker: &str,
        epoch: u64,
        expected: u64,
    ) -> Result<GrantReply> {
        let request = self.grant_request(database, worker, epoch, expected)?;
        Ok(serde_json::from_value(
            self.control.call(AuthorityCommand::Grant { request })?,
        )?)
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if self.thread.is_some() {
            self.control.call(AuthorityCommand::Shutdown)?;
            self.thread
                .take()
                .unwrap()
                .join()
                .map_err(|_| anyhow::anyhow!("authority server panicked"))??;
        }
        Ok(())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

pub fn identifier(name: &str) -> Result<DatabaseId> {
    DatabaseId::new(
        peri_loom_v1::namespace::Namespace::new("rpc-tests", name)?,
        "main",
    )
}

pub fn session(reply: serde_json::Value) -> Result<String> {
    Ok(reply["session"]
        .as_str()
        .context("opaque session missing")?
        .to_owned())
}
