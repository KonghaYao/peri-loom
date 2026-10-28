use super::protocol::{AuthorityCommand, RpcRequest, MAX_RPC_BYTES, PROTOCOL_VERSION};
use crate::object_store::PreconditionFailed;
use anyhow::{ensure, Context, Result};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::io::Read;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[doc(hidden)]
pub mod diagnostics;

#[cfg(test)]
#[path = "client/reuse_tests.rs"]
mod reuse_tests;

/// Sockets this client parks for the authority endpoint once they are idle.
///
/// Zero — the value this used to hold — meant every authority RPC built a fresh
/// connection, so the authority's keep-alive path was unreachable from the only
/// client that speaks it. A parked socket is only one the authority itself kept
/// open, so raising this cannot keep a connection the peer closed.
const IDLE_CONNECTIONS: usize = 8;

/// Calls this client runs at the same time, and therefore the sockets it can
/// have in flight. Together with `IDLE_CONNECTIONS` and the sockets the pool is
/// handing back this is what bounds the client; the authority sizes its own
/// connection budget against `LIVE_CONNECTIONS + IDLE_CONNECTIONS`.
pub(crate) const LIVE_CONNECTIONS: usize = 24;

/// How long a parked socket may sit idle before this side closes it.
///
/// Deliberately below the authority's own reclamation
/// (`http::KEEP_ALIVE_IDLE`): this side must close first, otherwise a request
/// could start on a socket the peer has already decided to retire.
const IDLE_TIMEOUT: Duration = Duration::from_secs(4);

/// Connecting to a loopback authority is the only step that can hang before the
/// request exists, so it gets its own bound.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Wall clock a single call may take. Also the longest a caller waits for a
/// socket slot, and running out of that wait is a definite "not sent" because
/// nothing has been written to any socket yet.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct AuthorityClient {
    endpoint: String,
    token: String,
    http: Client,
    connections: Connections,
    observer: Option<diagnostics::RejectionObserver>,
}

/// Bounded pool of in-flight calls, and through them of sockets.
///
/// `pool_max_idle_per_host` bounds parked sockets only; nothing in the HTTP
/// client bounds the ones a burst of callers would open. This cap turns "how
/// many sockets can this client ask the authority for" into a number.
///
/// The cap is on calls, not on sockets, because the pool hands a finished
/// socket back asynchronously: a caller that starts in that window connects
/// instead of reusing. Sockets are therefore bounded by `LIVE_CONNECTIONS`
/// (checked out) plus `IDLE_CONNECTIONS` (parked) plus the ones being handed
/// back, and the idle cap reclaims any excess the window produced.
struct Connections {
    available: Mutex<usize>,
    released: Condvar,
}

impl Connections {
    fn new(limit: usize) -> Self {
        Self {
            available: Mutex::new(limit),
            released: Condvar::new(),
        }
    }

    /// Returns `None` once `wait` elapses, and never while a slot is held.
    fn acquire(&self, wait: Duration) -> Option<ConnectionSlot<'_>> {
        let deadline = Instant::now().checked_add(wait)?;
        let mut available = self.available.lock().ok()?;
        loop {
            if *available > 0 {
                *available -= 1;
                return Some(ConnectionSlot(self));
            }
            let remaining = deadline.checked_duration_since(Instant::now())?;
            available = self.released.wait_timeout(available, remaining).ok()?.0;
        }
    }
}

struct ConnectionSlot<'client>(&'client Connections);

impl Drop for ConnectionSlot<'_> {
    fn drop(&mut self) {
        // A poisoned mutex would otherwise leak a slot permanently; the counter
        // is the only thing in here, so recovering it is safe.
        let mut available = match self.0.available.lock() {
            Ok(available) => available,
            Err(poisoned) => poisoned.into_inner(),
        };
        *available += 1;
        self.0.released.notify_one();
    }
}

#[derive(Debug)]
pub(crate) struct KnownAuthorityRejection;

impl std::fmt::Display for KnownAuthorityRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("authority operation rejected")
    }
}

impl std::error::Error for KnownAuthorityRejection {}

pub(crate) fn random_id() -> Result<String> {
    let mut bytes = [0; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut identifier = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut identifier, "{byte:02x}")?;
    }
    Ok(identifier)
}

impl AuthorityClient {
    pub fn new(endpoint: &str, token: String) -> Result<Arc<Self>> {
        Self::configured(endpoint, token, None)
    }

    #[doc(hidden)]
    pub fn new_with_rejection_observer(
        endpoint: &str,
        token: String,
        observer: diagnostics::RejectionObserver,
    ) -> Result<Arc<Self>> {
        Self::configured(endpoint, token, Some(observer))
    }

    fn configured(
        endpoint: &str,
        token: String,
        observer: Option<diagnostics::RejectionObserver>,
    ) -> Result<Arc<Self>> {
        let url = url::Url::parse(endpoint).context("invalid authority endpoint")?;
        let loopback = match url.host() {
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            _ => false,
        };
        ensure!(
            url.scheme() == "http" && loopback,
            "authority endpoint must use loopback HTTP"
        );
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/",
            "invalid authority endpoint components"
        );
        ensure!(
            token.len() >= 32
                && token.len() <= 4096
                && token.bytes().all(|byte| byte.is_ascii_graphic()),
            "invalid authority credential"
        );
        let http = Client::builder()
            .no_proxy()
            .http1_only()
            .pool_max_idle_per_host(IDLE_CONNECTIONS)
            .pool_idle_timeout(IDLE_TIMEOUT)
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("authority transport initialization failed")?;
        Ok(Arc::new(Self {
            endpoint: url.to_string(),
            token,
            http,
            connections: Connections::new(LIVE_CONNECTIONS),
            observer,
        }))
    }

    pub fn call(&self, command: AuthorityCommand) -> Result<serde_json::Value> {
        self.call_with_deadline(command, None)
    }

    pub(crate) fn call_until(
        &self,
        command: AuthorityCommand,
        deadline: std::time::Instant,
    ) -> Result<serde_json::Value> {
        self.call_with_deadline(command, Some(deadline))
    }

    fn call_with_deadline(
        &self,
        command: AuthorityCommand,
        deadline: Option<std::time::Instant>,
    ) -> Result<serde_json::Value> {
        let mut link = crate::slow_diagnostics::AuthorityRpcLink::begin(&command);
        let is_grant = matches!(&command, AuthorityCommand::Grant { .. });
        let request_id = match &command {
            AuthorityCommand::Grant { request } => request.request_id.clone(),
            AuthorityCommand::Renew { request } => request.request_id.clone(),
            AuthorityCommand::Revoke { request } => request.request_id.clone(),
            AuthorityCommand::Maintenance { request_id, .. } => request_id.clone(),
            _ => random_id()?,
        };
        link.child(&request_id);
        crate::ownership::types::validate_label(&request_id)?;
        link.phase(crate::slow_diagnostics::RpcPhase::EncodeRequest);
        let bytes = serde_json::to_vec(&RpcRequest {
            version: PROTOCOL_VERSION,
            request_id: request_id.clone(),
            command,
        })?;
        ensure!(
            bytes.len() <= MAX_RPC_BYTES,
            "authority request body limit exceeded"
        );
        let observation = if is_grant {
            self.observer
                .as_ref()
                .map(|observer| observer.begin_grant())
        } else {
            None
        };
        link.phase(crate::slow_diagnostics::RpcPhase::Deadline);
        let budget = match deadline {
            Some(deadline) => deadline
                .checked_duration_since(Instant::now())
                .context("scope retirement deadline")?
                .min(REQUEST_TIMEOUT),
            None => REQUEST_TIMEOUT,
        };
        // The slot is taken before the request exists. Taking it after `send`
        // would make the bound advisory, and taking it here is exactly why
        // running out of slots is a definite "not sent" instead of an unknown.
        link.phase(crate::slow_diagnostics::RpcPhase::AcquireSlot);
        let _slot = self
            .connections
            .acquire(budget)
            .context("authority client saturated; request not sent")?;
        let mut pending = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&self.token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes);
        if deadline.is_some() {
            pending = pending.timeout(budget);
        }
        link.phase(crate::slow_diagnostics::RpcPhase::Send);
        let response = pending
            .send()
            .map_err(|_| anyhow::anyhow!("authority transport failed; outcome may be unknown"))?;
        link.phase(crate::slow_diagnostics::RpcPhase::ResponseHeaders);
        ensure!(
            response.status().is_success(),
            "authority HTTP request rejected with status {}",
            response.status().as_u16()
        );
        if let Some(length) = response.content_length() {
            ensure!(
                length <= MAX_RPC_BYTES as u64,
                "authority response body limit exceeded"
            );
        }
        link.phase(crate::slow_diagnostics::RpcPhase::ReadBody);
        let mut bytes = Vec::new();
        response
            .take(MAX_RPC_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                anyhow::anyhow!("authority response transport failed; outcome may be unknown")
            })?;
        ensure!(
            bytes.len() <= MAX_RPC_BYTES,
            "authority response body limit exceeded"
        );
        // One strict parse covers identity, result presence and the result itself. The wire
        // protocol always writes `result`, so a reply that omits it is malformed rather than
        // a null result, while an explicit null stays legitimate.
        link.phase(crate::slow_diagnostics::RpcPhase::DecodeResponse);
        let response: ResponseEnvelope = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid authority response"))?;
        link.phase(crate::slow_diagnostics::RpcPhase::ValidateResponse);
        let result = match response.result {
            ResponseResult::Missing => {
                return Err(anyhow::anyhow!("authority result field missing"))
            }
            ResponseResult::Null => None,
            ResponseResult::Value(value) => Some(value),
        };
        ensure!(
            response.version == PROTOCOL_VERSION && response.request_id == request_id,
            "authority response identity mismatch"
        );
        if response.ok {
            ensure!(
                !response.precondition_failed && response.error.is_none(),
                "inconsistent authority response"
            );
            link.complete("ok");
            return Ok(result.unwrap_or(serde_json::Value::Null));
        }
        ensure!(
            result.is_none() && response.error.is_some(),
            "inconsistent authority error response"
        );
        if response.precondition_failed {
            link.complete("precondition_failed");
            return Err(PreconditionFailed.into());
        }
        if let Some(observation) = observation {
            observation.reject(response.error.as_deref().unwrap_or_default());
        }
        link.complete("rejected");
        Err(KnownAuthorityRejection.into())
    }
}

/// Presence of the wire `result` field, kept apart from its value: the protocol writes the
/// field even when the value is null, and serde's `Option` would otherwise accept a missing
/// field as `None`.
#[derive(Default)]
enum ResponseResult {
    #[default]
    Missing,
    Null,
    Value(serde_json::Value),
}

impl<'de> Deserialize<'de> for ResponseResult {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Ok(
            match Option::<serde_json::Value>::deserialize(deserializer)? {
                Some(value) => ResponseResult::Value(value),
                None => ResponseResult::Null,
            },
        )
    }
}

/// One strict pass covers every field the caller checks, so a large body is materialized
/// once instead of once per parse.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseEnvelope {
    version: u32,
    request_id: String,
    ok: bool,
    #[serde(default)]
    result: ResponseResult,
    error: Option<String>,
    precondition_failed: bool,
}
