use super::protocol::{AuthorityCommand, RpcRequest, RpcResponse, MAX_RPC_BYTES, PROTOCOL_VERSION};
use crate::object_store::PreconditionFailed;
use anyhow::{ensure, Context, Result};
use reqwest::blocking::Client;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

#[doc(hidden)]
pub mod diagnostics;

pub struct AuthorityClient {
    endpoint: String,
    token: String,
    http: Client,
    observer: Option<diagnostics::RejectionObserver>,
}

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
            .pool_max_idle_per_host(0)
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .context("authority transport initialization failed")?;
        Ok(Arc::new(Self {
            endpoint: url.to_string(),
            token,
            http,
            observer,
        }))
    }

    pub fn call(&self, command: AuthorityCommand) -> Result<serde_json::Value> {
        let is_grant = matches!(&command, AuthorityCommand::Grant { .. });
        let request_id = match &command {
            AuthorityCommand::Grant { request } => request.request_id.clone(),
            AuthorityCommand::Renew { request } => request.request_id.clone(),
            AuthorityCommand::Revoke { request } => request.request_id.clone(),
            AuthorityCommand::Maintenance { request_id, .. } => request_id.clone(),
            _ => random_id()?,
        };
        crate::ownership::types::validate_label(&request_id)?;
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
        let response = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&self.token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(bytes)
            .send()
            .map_err(|_| anyhow::anyhow!("authority transport failed; outcome may be unknown"))?;
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
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid authority response"))?;
        ensure!(
            value
                .as_object()
                .is_some_and(|object| object.contains_key("result")),
            "authority result field missing"
        );
        let response: RpcResponse = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid authority response"))?;
        ensure!(
            response.version == PROTOCOL_VERSION && response.request_id == request_id,
            "authority response identity mismatch"
        );
        if response.ok {
            ensure!(
                !response.precondition_failed && response.error.is_none(),
                "inconsistent authority response"
            );
            return Ok(response.result.unwrap_or(serde_json::Value::Null));
        }
        ensure!(
            response.result.is_none() && response.error.is_some(),
            "inconsistent authority error response"
        );
        if response.precondition_failed {
            return Err(PreconditionFailed.into());
        }
        if let Some(observation) = observation {
            observation.reject(response.error.as_deref().unwrap_or_default());
        }
        anyhow::bail!("authority operation rejected")
    }
}
