//! 统一错误体（架构 §17.4，冻结契约）。
//!
//! 任何经 HTTP 出口返回的错误都必须是：
//!
//! ```json
//! {"error":{"code":"DB_NOT_FOUND","message":"...","request_id":"...","retryable":false}}
//! ```
//!
//! `code` 取 [`domain::error::ErrorCode::as_str()`]，与 proto `platform.common.v1.ErrorCode`
//! 逐值对应，也与 Worker / WAL 返回的结构化错误码同源。
//!
//! `request_id` 永远存在：请求进入时由 [`crate::middleware::REQUEST_META`] 注入，
//! 若错误产生于注入范围之外（后台任务）则现场生成一个，保证日志能对上响应。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use domain::error::{ErrorCode, PlatformError};

/// 对外错误体的 `error` 字段。
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct ErrorBody {
    /// 平台错误码（`ErrorCode::as_str()`）。
    pub code: String,
    /// 人类可读信息；不得包含 secret。
    pub message: String,
    /// 请求 ID，用于把响应与日志 / trace 对齐。
    pub request_id: String,
    /// 是否可安全重试（由错误码语义决定）。
    pub retryable: bool,
    /// 结构化附加信息（例如 fencing 的 epoch 期望值 / 实际值）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
    /// 本次请求内已发生的透明路由重试次数（stale route 场景最多 1）。
    pub route_retry_count: u32,
}

/// 对外错误体的最外层信封。
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct ErrorEnvelope {
    /// 错误详情。
    pub error: ErrorBody,
}

/// API 层错误：包装领域 [`PlatformError`]，负责转换成冻结的错误体。
#[derive(Debug, Clone)]
pub struct ApiError {
    /// 领域错误。
    pub error: PlatformError,
}

impl ApiError {
    /// 由错误码与消息构造。
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: PlatformError::new(code, message),
        }
    }

    /// 参数非法。
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    /// 内部错误。
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalError, message)
    }

    /// 未认证。
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, message)
    }

    /// 无权限。
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::PermissionDenied, message)
    }

    /// 资源不存在。
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DbNotFound, message)
    }

    /// 未实现。
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotImplemented, message)
    }

    /// 错误码。
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        self.error.code
    }

    /// 覆盖 / 注入 request_id。
    #[must_use]
    pub fn with_request_id(mut self, request_id: Option<String>) -> Self {
        if self.error.request_id.is_none() {
            self.error.request_id = request_id;
        }
        self
    }

    /// 记录透明路由重试次数（stale route 重试后仍失败时写入错误体）。
    #[must_use]
    pub fn with_route_retry(mut self, count: u32) -> Self {
        self.error.route_retry_count = count;
        self
    }

    /// 附加结构化 detail。
    #[must_use]
    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.error.detail = Some(detail);
        self
    }

    /// 转成冻结的错误体（`request_id` 一定非空）。
    #[must_use]
    pub fn to_envelope(&self) -> ErrorEnvelope {
        let request_id = self
            .error
            .request_id
            .clone()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(crate::middleware::current_request_id);
        ErrorEnvelope {
            error: ErrorBody {
                code: self.error.code.as_str().to_string(),
                message: self.error.message.clone(),
                request_id,
                retryable: self.error.retryable,
                detail: self.error.detail.clone(),
                route_retry_count: self.error.route_retry_count,
            },
        }
    }
}

impl From<PlatformError> for ApiError {
    fn from(error: PlatformError) -> Self {
        Self { error }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error.code.as_str(), self.error.message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // 状态码由错误码语义决定（domain 侧统一维护），避免每个 handler 各自映射。
        let status = StatusCode::from_u16(self.error.code.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        // 5xx 只记日志不外泄细节：message 可能含内部路径 / SQL 片段。
        if status.is_server_error() {
            tracing::error!(
                code = self.error.code.as_str(),
                message = %self.error.message,
                "请求处理失败（服务端错误）"
            );
        }
        (status, Json(self.to_envelope())).into_response()
    }
}

/// API 层结果别名。
pub type ApiResult<T> = Result<T, ApiError>;

/// 把 `Result<T, PlatformError>` 直接转成 `ApiResult<T>`。
///
/// 用扩展 trait 而不是 `impl<E: Into<PlatformError>> From<E> for ApiError`：
/// 后者会与标准库的反射式 `From` 实现冲突（ApiError 自身也可能实现 Into<PlatformError>）。
pub trait PlatformResultExt<T> {
    /// 转成 API 结果。
    fn api(self) -> ApiResult<T>;
}

impl<T> PlatformResultExt<T> for Result<T, PlatformError> {
    fn api(self) -> ApiResult<T> {
        self.map_err(ApiError::from)
    }
}

/// tonic 传输错误（连接失败 / 超时 / 未实现）-> API 错误。
///
/// 映射规则（保守，不把 Worker 的不可用伪装成参数错误）：
/// - `DeadlineExceeded` -> `DEADLINE_EXCEEDED`；
/// - `Unavailable` / 连接类错误 -> `WORKER_UNAVAILABLE`（可重试）；
/// - `Unimplemented` / `NotFound`（gRPC 语义）-> `NOT_IMPLEMENTED`；
/// - 其余 -> `INTERNAL_ERROR`，并把 gRPC code 放进 detail 便于排障。
pub fn api_error_from_status(status: &tonic::Status, worker_id: &str) -> ApiError {
    use tonic::Code;
    let code = match status.code() {
        Code::DeadlineExceeded => ErrorCode::DeadlineExceeded,
        Code::Cancelled => ErrorCode::Cancelled,
        Code::Unavailable | Code::Unknown => ErrorCode::WorkerUnavailable,
        Code::Unimplemented => ErrorCode::NotImplemented,
        Code::Unauthenticated => ErrorCode::Unauthenticated,
        Code::PermissionDenied => ErrorCode::PermissionDenied,
        Code::ResourceExhausted => ErrorCode::ResourceExhausted,
        Code::FailedPrecondition => ErrorCode::DatabaseNotReady,
        _ => ErrorCode::InternalError,
    };
    ApiError::new(
        code,
        format!("调用 Worker {worker_id} 失败: {}", status.message()),
    )
    .with_detail(serde_json::json!({
        "grpc_code": format!("{:?}", status.code()),
        "worker_id": worker_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_uses_error_code_string_and_keeps_request_id() {
        let err = ApiError::new(ErrorCode::DbNotFound, "database not found")
            .with_request_id(Some("req-1".to_string()));
        let envelope = err.to_envelope();
        assert_eq!(envelope.error.code, "DB_NOT_FOUND");
        assert_eq!(envelope.error.request_id, "req-1");
        assert!(!envelope.error.retryable);
        assert_eq!(envelope.error.route_retry_count, 0);

        let json = serde_json::to_value(&envelope).expect("序列化错误体");
        assert_eq!(json["error"]["code"], "DB_NOT_FOUND");
        assert_eq!(json["error"]["message"], "database not found");
        assert_eq!(json["error"]["request_id"], "req-1");
        assert_eq!(json["error"]["retryable"], false);
        // detail 为 None 时不出现（契约只要求 4 个核心字段 + route_retry_count）
        assert!(json["error"].get("detail").is_none());
    }

    #[test]
    fn envelope_always_has_request_id_even_outside_request_scope() {
        let err = ApiError::new(ErrorCode::InternalError, "boom");
        let envelope = err.to_envelope();
        assert!(!envelope.error.request_id.trim().is_empty());
    }

    #[test]
    fn retryable_flag_follows_error_code_semantics() {
        assert!(
            ApiError::new(ErrorCode::WorkerUnavailable, "x")
                .to_envelope()
                .error
                .retryable
        );
        assert!(
            !ApiError::new(ErrorCode::InvalidArgument, "x")
                .to_envelope()
                .error
                .retryable
        );
    }

    #[test]
    fn http_status_comes_from_error_code() {
        let response = ApiError::new(ErrorCode::DbNotFound, "nope").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = ApiError::new(ErrorCode::InvalidArgument, "bad").into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let response = ApiError::new(ErrorCode::WakeupTimeout, "slow").into_response();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    #[test]
    fn platform_result_ext_maps_platform_error() {
        let result: Result<(), PlatformError> =
            Err(PlatformError::new(ErrorCode::AdmissionDenied, "no room"));
        let mapped = result.api();
        assert!(matches!(mapped, Err(e) if e.code() == ErrorCode::AdmissionDenied));
    }

    #[test]
    fn route_retry_count_is_carried_into_body() {
        let err = ApiError::new(ErrorCode::RouteStale, "stale").with_route_retry(1);
        let json = serde_json::to_value(err.to_envelope()).expect("序列化");
        assert_eq!(json["error"]["route_retry_count"], 1);
    }
}
