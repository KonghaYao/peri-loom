use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use anyhow::{Context, Result};
use peri_loom_v1::authority::ownership::OwnershipAuthority;
use peri_loom_v1::authority::Authority;
use peri_loom_v1::authority_rpc::auth::AuthConfig;
use peri_loom_v1::authority_rpc::client::diagnostics::{
    rejection_observer, RejectionObserver, RejectionReceiver,
};
use peri_loom_v1::authority_rpc::protocol::AuthorityCommand;
use peri_loom_v1::authority_rpc::server::Server;
use peri_loom_v1::authority_rpc::startup::AuthorityStartup;
use peri_loom_v1::authority_rpc::{AuthorityClient, RemoteAuthorityProvider};
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::LocalObjectStore;
use peri_loom_v1::ownership::types::{
    AccessRole, GrantIntent, GrantReply, GrantRequest, LeaseIntent, NonceReply, OwnershipCommand,
    RenewIntent, RenewRequest,
};
use peri_loom_v1::ownership::OwnershipRuntime;
use peri_loom_v1::runtime::{DatabaseId, WorkerLimits};

use super::clock_gate::{BlockingStore, FakeClock, Gate};

pub struct Fixture {
    pub runtime: Arc<OwnershipRuntime>,
    pub control: Arc<AuthorityClient>,
    pub worker_clock: Arc<FakeClock>,
    pub authority_clock: Arc<FakeClock>,
    pub gate: Arc<Gate>,
    pub lease: GrantReply,
    pub identifier: DatabaseId,
    grant_rejections: Option<Mutex<RejectionReceiver>>,
    server: Option<JoinHandle<Result<()>>>,
    _objects: tempfile::TempDir,
    _cache: tempfile::TempDir,
    _state: tempfile::TempDir,
}

impl Fixture {
    pub fn new(role: AccessRole, ttl_ms: u64) -> Result<Self> {
        Self::configured(role, ttl_ms, None)
    }

    pub fn new_with_grant_observer(role: AccessRole, ttl_ms: u64) -> Result<Self> {
        Self::configured(role, ttl_ms, Some(rejection_observer()))
    }

    fn configured(
        role: AccessRole,
        ttl_ms: u64,
        observer: Option<(RejectionObserver, RejectionReceiver)>,
    ) -> Result<Self> {
        let objects = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let state = tempfile::tempdir()?;
        let worker_clock = Arc::new(FakeClock::default());
        let authority_clock = Arc::new(FakeClock::default());
        let gate = Arc::new(Gate::default());
        let identifier = DatabaseId::new(Namespace::new("p4", "runtime")?, "main")?;
        let backing = Arc::new(LocalObjectStore::open(objects.path())?);
        if role == AccessRole::ReadOnly {
            let legacy = Authority::open(backing.clone(), identifier.namespace.clone())?;
            let database = RemoteDatabase::open(
                legacy.branch_store("main")?,
                identifier.namespace.clone(),
                cache.path().join("bootstrap"),
                0,
            )?;
            database.execute(
                "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(7)",
            )?;
            database.compact_wal()?;
            drop(database);
        }
        let store = Arc::new(BlockingStore {
            inner: backing,
            gate: gate.clone(),
        });
        let authority = OwnershipAuthority::new(store, authority_clock.clone());
        let worker_id = "worker_c".to_owned();
        let control_token = "control_c_credential______________________".to_owned();
        let worker_token = "worker_c_credential_______________________".to_owned();
        let auth = AuthConfig::new(
            control_token.clone(),
            BTreeMap::from([(worker_id.clone(), worker_token.clone())]),
        )?;
        let startup = AuthorityStartup::acquire(state.path().join("authority"), false)?;
        let server = Server::bind("127.0.0.1:0".parse()?, authority, auth, startup)?;
        let endpoint = format!("http://{}/", server.local_addr());
        let server = std::thread::spawn(move || server.run());
        let (control, grant_rejections) = match observer {
            Some((observer, receiver)) => (
                AuthorityClient::new_with_rejection_observer(&endpoint, control_token, observer)?,
                Some(Mutex::new(receiver)),
            ),
            None => (AuthorityClient::new(&endpoint, control_token)?, None),
        };
        let provider = RemoteAuthorityProvider::new(
            AuthorityClient::new(&endpoint, worker_token)?,
            worker_id.clone(),
        )?;
        let runtime = OwnershipRuntime::new(
            worker_id.clone(),
            provider,
            cache.path(),
            WorkerLimits::default(),
            worker_clock.clone(),
        )?;
        let intent = GrantIntent {
            database: identifier.clone(),
            worker_id,
            epoch: 1,
            expected_epoch: 0,
            lease_id: "lease_c".into(),
            ttl_ms,
            role,
        };
        let nonce: NonceReply =
            serde_json::from_value(control.call(AuthorityCommand::Challenge {
                intent: LeaseIntent::Grant(intent.clone()),
            })?)?;
        let lease = serde_json::from_value(control.call(AuthorityCommand::Grant {
            request: GrantRequest {
                request_id: "grant_c".into(),
                nonce: nonce.nonce,
                intent,
            },
        })?)?;
        Ok(Self {
            runtime,
            control,
            worker_clock,
            authority_clock,
            gate,
            lease,
            identifier,
            grant_rejections,
            server: Some(server),
            _objects: objects,
            _cache: cache,
            _state: state,
        })
    }

    pub fn acquire(&self, role: AccessRole) -> Result<()> {
        self.runtime.command(OwnershipCommand::AcquireDatabase {
            lease: self.lease.clone(),
            role,
        })?;
        Ok(())
    }

    pub fn renew(&self, sequence: u64, ttl_ms: u64) -> Result<GrantReply> {
        let intent = RenewIntent {
            binding: self.lease.binding.clone(),
            sequence,
            ttl_ms,
        };
        let nonce: NonceReply =
            serde_json::from_value(self.control.call(AuthorityCommand::Challenge {
                intent: LeaseIntent::Renew(intent.clone()),
            })?)?;
        Ok(serde_json::from_value(self.control.call(
            AuthorityCommand::Renew {
                request: RenewRequest {
                    request_id: format!("renew_c_{sequence}"),
                    nonce: nonce.nonce,
                    intent,
                },
            },
        )?)?)
    }

    pub fn grant_next(&self, epoch: u64) -> Result<GrantReply> {
        let intent = GrantIntent {
            database: self.identifier.clone(),
            worker_id: "worker_c".into(),
            epoch,
            expected_epoch: epoch - 1,
            lease_id: format!("lease_c_{epoch}"),
            ttl_ms: 1000,
            role: AccessRole::Writer,
        };
        let nonce: NonceReply = serde_json::from_value(
            self.control
                .call(AuthorityCommand::Challenge {
                    intent: LeaseIntent::Grant(intent.clone()),
                })
                .context("next grant: challenge RPC")?,
        )
        .context("next grant: decode challenge")?;
        serde_json::from_value(
            self.control
                .call(AuthorityCommand::Grant {
                    request: GrantRequest {
                        request_id: format!("grant_c_{epoch}"),
                        nonce: nonce.nonce,
                        intent,
                    },
                })
                .context("next grant: grant RPC")?,
        )
        .context("next grant: decode grant")
    }
    pub fn grant_next_diagnosed(&self, epoch: u64) -> Result<GrantReply> {
        let receiver = self
            .grant_rejections
            .as_ref()
            .context("grant diagnostic not enabled")?;
        let capture = receiver
            .lock()
            .map_err(|_| anyhow::anyhow!("grant diagnostic receiver poisoned"))?
            .begin();
        let result = self.grant_next(epoch);
        let observed = receiver
            .lock()
            .map_err(|_| anyhow::anyhow!("grant diagnostic receiver poisoned"))?
            .finish(capture);
        result.map_err(|error| match observed {
            Some(cause) => error.context(format!("diagnosed grant rejection: {cause:?}")),
            None => error.context("grant diagnostic unavailable"),
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.gate.release();
        let _ = self.control.call(AuthorityCommand::Shutdown);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}
