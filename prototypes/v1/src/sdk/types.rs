use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone)]
pub struct ClientOptions {
    pub timeout: Duration,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub trusted_ca_pem: Option<Vec<u8>>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_request_bytes: 1 << 20,
            max_response_bytes: 8 << 20,
            trusted_ca_pem: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SdkError {
    pub code: &'static str,
    pub request_id: String,
    pub outcome_unknown: bool,
    pub execution_completed: bool,
    pub may_have_committed: bool,
    pub operation_id: Option<Box<str>>,
    pub stage: Option<Box<str>>,
    pub business_outcome: Option<Box<str>>,
    pub cleanup_outcome: Option<Box<str>>,
    pub execution_proven: Option<bool>,
    pub ambiguous: Option<bool>,
    pub rejected: Option<bool>,
    pub retry_allowed: Option<bool>,
    pub possible_commit: Option<bool>,
    pub rpc_observation: Option<RpcObservation>,
}

impl SdkError {
    pub(super) fn new(code: &'static str, request_id: &str, unknown: bool) -> Self {
        Self {
            code,
            request_id: request_id.into(),
            outcome_unknown: unknown,
            execution_completed: false,
            may_have_committed: false,
            operation_id: None,
            stage: None,
            business_outcome: None,
            cleanup_outcome: None,
            execution_proven: None,
            ambiguous: None,
            rejected: None,
            retry_allowed: None,
            possible_commit: None,
            rpc_observation: None,
        }
    }
}

impl fmt::Display for SdkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "peri: {} (request {})",
            self.code, self.request_id
        )
    }
}

impl std::error::Error for SdkError {}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ManagementRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<crate::runtime::DatabaseId>,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safety_window_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quotas: Option<Value>,
}

impl ManagementRequest {
    pub fn new(database: crate::runtime::DatabaseId) -> Self {
        Self {
            database: Some(database),
            ..Self::default()
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Envelope {
    pub version: u32,
    pub request_id: String,
    pub ok: bool,
    pub result: Value,
    pub error: Option<Failure>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Failure {
    pub code: String,
    #[serde(default)]
    pub execution_completed: bool,
    #[serde(default)]
    pub may_have_committed: bool,
    #[serde(default, deserialize_with = "present")]
    pub operation_id: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub stage: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub business_outcome: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub cleanup_outcome: Option<String>,
    #[serde(default, deserialize_with = "present")]
    pub execution_proven: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    pub ambiguous: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    pub rejected: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    pub retry_allowed: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    pub possible_commit: Option<bool>,
    #[serde(default, deserialize_with = "present")]
    pub rpc_observation: Option<RpcObservation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcObservation {
    pub ambiguous: bool,
    pub rejected: bool,
    pub execution_completed: bool,
}

fn present<'de, Deserializer, Field>(
    deserializer: Deserializer,
) -> Result<Option<Field>, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
    Field: Deserialize<'de>,
{
    Field::deserialize(deserializer).map(Some)
}

impl Failure {
    pub(super) fn valid_metadata(&self) -> bool {
        self.stage.as_deref().is_none_or(|stage| {
            matches!(
                stage,
                "open_session" | "tool_rpc" | "close_session" | "unknown"
            )
        }) && self.business_outcome.as_deref().is_none_or(|outcome| {
            matches!(
                outcome,
                "not_proven" | "unknown" | "completed" | "not_executed" | "not_applicable"
            )
        }) && self.cleanup_outcome.as_deref().is_none_or(|outcome| {
            matches!(
                outcome,
                "not_proven" | "not_applicable" | "pending" | "unknown" | "completed"
            )
        })
    }
}

pub(super) fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
}

pub(super) fn safe_code(value: &str) -> &'static str {
    match value {
        "database_isolated" => "database_isolated",
        "operation_recorded" => "operation_recorded",
        "operation_registration_unavailable" => "operation_registration_unavailable",
        "session_unusable" => "session_unusable",
        "sql_error" => "sql_error",
        "outcome_unknown" => "outcome_unknown",
        "session_moved" => "session_moved",
        "invalid_request" => "invalid_request",
        "invalid_session" => "invalid_session",
        "unauthenticated" => "unauthenticated",
        "unauthorized" => "unauthorized",
        "forbidden" => "forbidden",
        "quota_exceeded" => "quota_exceeded",
        "request_limit" => "request_limit",
        "result_limit" => "result_limit",
        "sql_limit" => "sql_limit",
        "session_busy" => "session_busy",
        "upstream_rejected" => "upstream_rejected",
        "upstream_unavailable" => "upstream_unavailable",
        "placement_unavailable" => "placement_unavailable",
        "metadata_unavailable" => "metadata_unavailable",
        "audit_unavailable" => "audit_unavailable",
        "gateway_closing" => "gateway_closing",
        "operation_incomplete" => "operation_incomplete",
        "token_binding_mismatch" => "token_binding_mismatch",
        "readonly_placement_required" => "readonly_placement_required",
        _ => "remote_error",
    }
}
