use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use peri_loom_v1::authority::ownership::OwnershipAuthority;
use peri_loom_v1::authority::Authority;
use peri_loom_v1::authority_rpc::auth::AuthConfig;
use peri_loom_v1::authority_rpc::protocol::{
    AuthorityCommand, RpcRequest, RpcResponse, MAX_RPC_BYTES, PROTOCOL_VERSION,
};
use peri_loom_v1::authority_rpc::server::Server;
use peri_loom_v1::authority_rpc::startup::AuthorityStartup;
use peri_loom_v1::authority_rpc::{AuthorityClient, RemoteAuthorityProvider};
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, PreconditionFailed};
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
    control_endpoint: String,
    control_credential: String,
    server: Option<JoinHandle<Result<()>>>,
    _objects: tempfile::TempDir,
    _cache: tempfile::TempDir,
    _state: tempfile::TempDir,
}

impl Fixture {
    pub fn new(role: AccessRole, ttl_ms: u64) -> Result<Self> {
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
        let control = AuthorityClient::new(&endpoint, control_token.clone())?;
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
            control_endpoint: endpoint,
            control_credential: control_token,
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
        self.grant_next_inner(epoch, false)
    }

    pub fn grant_next_diagnosed(&self, epoch: u64) -> Result<GrantReply> {
        self.grant_next_inner(epoch, true)
    }

    fn grant_next_inner(&self, epoch: u64, diagnosed: bool) -> Result<GrantReply> {
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
        let request = GrantRequest {
            request_id: format!("grant_c_{epoch}"),
            nonce: nonce.nonce,
            intent,
        };
        let result = if diagnosed {
            self.diagnosed_grant(request)
        } else {
            self.control.call(AuthorityCommand::Grant { request })
        };
        serde_json::from_value(result.context("next grant: grant RPC")?)
            .context("next grant: decode grant")
    }

    fn diagnosed_grant(&self, request: GrantRequest) -> Result<serde_json::Value> {
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .http1_only()
            .pool_max_idle_per_host(0)
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| anyhow!("diagnostic transport initialization failed"))?;
        let request_id = request.request_id.clone();
        let bytes = serde_json::to_vec(&RpcRequest {
            version: PROTOCOL_VERSION,
            request_id: request_id.clone(),
            command: AuthorityCommand::Grant { request },
        })?;
        ensure!(
            bytes.len() <= MAX_RPC_BYTES,
            "diagnostic request body limit"
        );
        let response = client
            .post(&self.control_endpoint)
            .bearer_auth(&self.control_credential)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes)
            .send()
            .map_err(|_| anyhow!("diagnostic transport failed; outcome may be unknown"))?;
        ensure!(response.status().is_success(), "diagnostic HTTP rejected");
        ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= MAX_RPC_BYTES as u64),
            "diagnostic response body limit"
        );
        let mut bytes = Vec::new();
        response
            .take(MAX_RPC_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| anyhow!("diagnostic response transport failed; outcome may be unknown"))?;
        ensure!(
            bytes.len() <= MAX_RPC_BYTES,
            "diagnostic response body limit"
        );
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid diagnostic response"))?;
        ensure!(
            value
                .as_object()
                .is_some_and(|object| object.contains_key("result")),
            "diagnostic result field missing"
        );
        let response: RpcResponse =
            serde_json::from_slice(&bytes).map_err(|_| anyhow!("invalid diagnostic response"))?;
        ensure!(
            response.version == PROTOCOL_VERSION && response.request_id == request_id,
            "diagnostic response identity mismatch"
        );
        if response.ok {
            ensure!(
                !response.precondition_failed && response.error.is_none(),
                "inconsistent diagnostic response"
            );
            return Ok(response.result.unwrap_or(serde_json::Value::Null));
        }
        ensure!(
            response.result.is_none() && response.error.is_some(),
            "inconsistent diagnostic error response"
        );
        if response.precondition_failed {
            return Err(PreconditionFailed.into());
        }
        Err(anyhow!("authority operation rejected").context(format!(
            "diagnosed grant rejection: {:?}",
            grant_cause(response.error.as_deref().unwrap_or_default())
        )))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum GrantCause {
    ExpectedEpoch,
    NonceMissing,
    NonceStaleOrMismatch,
    NonceOwnershipChanged,
    LeaseExpired,
    LeaseUnavailable,
    LeaseIdentity,
    AuthorityTerm,
    NamespaceBlocked,
    ClockUnavailable,
    NonceCapacity,
    Unclassified,
}

fn grant_cause(message: &str) -> GrantCause {
    match message {
        "Fenced expected epoch" => GrantCause::ExpectedEpoch,
        "Unauthorized nonce missing" => GrantCause::NonceMissing,
        "Unauthorized stale or mismatched nonce" => GrantCause::NonceStaleOrMismatch,
        "Fenced nonce expected ownership changed" => GrantCause::NonceOwnershipChanged,
        "LeaseExpired" | "LeaseExpired grant completed too late" => GrantCause::LeaseExpired,
        "Fenced lease unavailable" => GrantCause::LeaseUnavailable,
        "Fenced lease identity" | "Fenced grant replay" => GrantCause::LeaseIdentity,
        "Fenced authority term" => GrantCause::AuthorityTerm,
        "UnknownOutcome namespace blocked" => GrantCause::NamespaceBlocked,
        "Unavailable lease clock" | "Unavailable monotonic clock regression" => {
            GrantCause::ClockUnavailable
        }
        "nonce capacity exceeded" => GrantCause::NonceCapacity,
        _ => GrantCause::Unclassified,
    }
}

#[test]
fn grant_cause_is_closed_and_never_includes_private_payload() {
    assert_eq!(
        grant_cause("Fenced expected epoch"),
        GrantCause::ExpectedEpoch
    );
    assert_eq!(
        grant_cause("Fenced nonce expected ownership changed"),
        GrantCause::NonceOwnershipChanged
    );
    for message in [
        "private SQL token namespace",
        "Fenced expected epoch secret",
        "private: LeaseExpired",
    ] {
        let cause = grant_cause(message);
        assert_eq!(cause, GrantCause::Unclassified);
        assert_eq!(format!("{cause:?}"), "Unclassified");
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
