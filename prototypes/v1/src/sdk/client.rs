use std::fmt;
use std::io::Read;
use std::sync::Arc;

use reqwest::blocking::Client as HttpClient;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use zeroize::Zeroizing;

use super::session::Session;
use super::types::{label, safe_code, ClientOptions, Envelope, ManagementRequest, SdkError};

pub struct Client {
    endpoint: String,
    credential: Zeroizing<String>,
    http: HttpClient,
    options: ClientOptions,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GatewayClient { credentials: [redacted] }")
    }
}

impl Client {
    pub fn new(endpoint: &str, credential: String) -> Result<Arc<Self>, SdkError> {
        Self::with_options(endpoint, credential, ClientOptions::default())
    }

    pub fn with_options(
        endpoint: &str,
        credential: String,
        options: ClientOptions,
    ) -> Result<Arc<Self>, SdkError> {
        let endpoint =
            url::Url::parse(endpoint).map_err(|_| SdkError::new("invalid_endpoint", "", false))?;
        let local = endpoint
            .host_str()
            .and_then(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .ok()
            })
            .is_some_and(|address| address.is_loopback());
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !matches!(endpoint.path(), "" | "/")
            || !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && local)
        {
            return Err(SdkError::new("invalid_endpoint", "", false));
        }
        if !(32..=4096).contains(&credential.len())
            || !credential.bytes().all(|byte| (33..=126).contains(&byte))
        {
            return Err(SdkError::new("invalid_credential", "", false));
        }
        if options.timeout.is_zero()
            || !(1..=32 << 20).contains(&options.max_request_bytes)
            || !(1..=32 << 20).contains(&options.max_response_bytes)
        {
            return Err(SdkError::new("invalid_limits", "", false));
        }
        let mut builder = HttpClient::builder()
            .timeout(options.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .pool_max_idle_per_host(0);
        if let Some(certificate) = &options.trusted_ca_pem {
            let certificate = reqwest::Certificate::from_pem(certificate)
                .map_err(|_| SdkError::new("invalid_ca", "", false))?;
            builder = builder.add_root_certificate(certificate);
        }
        let http = builder
            .build()
            .map_err(|_| SdkError::new("client_unavailable", "", false))?;
        Ok(Arc::new(Self {
            endpoint: endpoint.as_str().trim_end_matches('/').into(),
            credential: Zeroizing::new(credential),
            http,
            options,
        }))
    }

    pub fn open(
        self: &Arc<Self>,
        database: &crate::runtime::DatabaseId,
    ) -> Result<Session, SdkError> {
        database
            .validate()
            .map_err(|_| SdkError::new("invalid_database", "", false))?;
        let request_id = random_id()?;
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Opened {
            session: String,
            readonly: bool,
        }
        let reply: Opened = self.call(
            "/v1/sql/open",
            &request_id,
            &serde_json::json!({"database": database, "request_id": request_id}),
        )?;
        if reply.session.len() != 32
            || !reply
                .session
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SdkError::new("outcome_unknown", &request_id, true));
        }
        Ok(Session::new(self.clone(), reply.session, reply.readonly))
    }

    pub fn management(
        &self,
        operation: &str,
        mut request: ManagementRequest,
    ) -> Result<Value, SdkError> {
        let database_operation = operation.strip_prefix("databases/").unwrap_or(operation);
        if !matches!(
            database_operation,
            "create"
                | "delete"
                | "snapshot"
                | "branch"
                | "clone"
                | "restore"
                | "restore-point"
                | "pitr"
                | "suspend"
                | "resume"
                | "migrate"
                | "status"
                | "checkpoint"
                | "schema"
                | "gc"
                | "gc-inspect"
                | "refs"
                | "compact"
                | "drop-snapshot"
                | "retain-after"
                | "rewrap"
        ) && !matches!(
            operation,
            "organizations/create" | "projects/create" | "keys/create" | "keys/revoke" | "tokens"
        ) {
            return Err(SdkError::new("invalid_operation", "", false));
        }
        let global = matches!(
            operation,
            "organizations/create" | "projects/create" | "keys/create" | "keys/revoke" | "tokens"
        );
        if !global || operation == "tokens" {
            request
                .database
                .as_ref()
                .ok_or_else(|| SdkError::new("invalid_database", "", false))?
                .validate()
                .map_err(|_| SdkError::new("invalid_database", "", false))?;
        }
        if request.request_id.is_empty() {
            request.request_id = random_id()?;
        }
        self.call(
            &if global {
                format!("/v1/{operation}")
            } else {
                format!("/v1/databases/{database_operation}")
            },
            &request.request_id,
            &request,
        )
    }

    pub(super) fn call<Input: Serialize, Output: DeserializeOwned>(
        &self,
        path: &str,
        request_id: &str,
        input: &Input,
    ) -> Result<Output, SdkError> {
        if !label(request_id) {
            return Err(SdkError::new("invalid_request_id", "", false));
        }
        let unknown = || SdkError::new("outcome_unknown", request_id, true);
        let bytes = serde_json::to_vec(input)
            .map_err(|_| SdkError::new("invalid_request", request_id, false))?;
        if bytes.len() > self.options.max_request_bytes {
            return Err(SdkError::new("request_limit", request_id, false));
        }
        let response = self
            .http
            .post(format!("{}{path}", self.endpoint))
            .bearer_auth(self.credential.as_str())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-Request-ID", request_id)
            .body(bytes)
            .send()
            .map_err(|_| unknown())?;
        let status = response.status();
        let mut bytes = Vec::new();
        response
            .take(self.options.max_response_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| unknown())?;
        if bytes.len() > self.options.max_response_bytes {
            return Err(unknown());
        }
        let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
        if envelope.version != 1 || envelope.request_id != request_id {
            return Err(unknown());
        }
        if !envelope.ok {
            if !envelope.result.is_null() {
                return Err(unknown());
            }
            let failure = envelope.error.ok_or_else(unknown)?;
            let code = safe_code(&failure.code);
            if code == "sql_error" && (!failure.execution_completed || !failure.may_have_committed)
            {
                return Err(unknown());
            }
            let mut failure_error = SdkError::new(code, request_id, code == "outcome_unknown");
            failure_error.execution_completed = failure.execution_completed;
            failure_error.may_have_committed = failure.may_have_committed;
            return Err(failure_error);
        }
        if !status.is_success() || envelope.error.is_some() {
            return Err(unknown());
        }
        serde_json::from_value(envelope.result).map_err(|_| unknown())
    }
}

pub(super) fn random_id() -> Result<String, SdkError> {
    let mut bytes = [0; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|_| SdkError::new("request_id_unavailable", "", false))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
