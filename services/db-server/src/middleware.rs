//! 请求级上下文注入。
//!
//! 统一错误体与审计日志都需要 `request_id` / `source_ip`，而它们是在**很深**的调用栈里
//! 才被用到的（例如 `ApiError` 在数据面流式处理中被构造）。为了避免把 `RequestMeta`
//! 一路透传成参数，这里用 `tokio::task_local!` 作用域：
//!
//! ```text
//! middleware: REQUEST_META.scope(meta, next.run(request))  // 同一个 task
//! handler   : ApiError::to_envelope() / audit_entry() 直接读取
//! ```
//!
//! 作用域外的读取（后台任务、单元测试）安全返回兜底值，不 panic。

use axum::extract::Request;
use axum::http::HeaderName;
use axum::middleware::Next;
use axum::response::Response;
use domain::ids::RequestId;

/// 外部传入的请求 ID 头（网关 / SDK 都会带）。
pub const REQUEST_ID_HEADER: &str = "x-request-id";

tokio::task_local! {
    /// 当前请求的上下文；仅由 [`inject_request_meta`] 建立作用域。
    static REQUEST_META: RequestMeta;
}

/// 请求级元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestMeta {
    /// 请求 ID（外部传入或现场生成）。
    pub request_id: String,
    /// 客户端来源 IP（取自 `x-forwarded-for` 首段或 socket 地址）。
    pub source_ip: Option<String>,
    /// 客户端声明的 User-Agent（审计留痕）。
    pub user_agent: Option<String>,
}

impl RequestMeta {
    /// 生成新的请求上下文。
    #[must_use]
    pub fn new(request_id: String, source_ip: Option<String>, user_agent: Option<String>) -> Self {
        Self {
            request_id,
            source_ip,
            user_agent,
        }
    }
}

/// 从请求头 / 连接信息提取元数据；缺失时生成时间有序的 UUID v7。
#[must_use]
pub fn extract_request_meta(request: &Request) -> RequestMeta {
    let headers = request.headers();
    let request_id = headers
        .get(HeaderName::from_static(REQUEST_ID_HEADER))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty() && v.len() <= 128)
        .map(ToString::to_string)
        .unwrap_or_else(|| RequestId::new_v7().to_string());

    // x-forwarded-for 可能是逗号分隔链；取最左（最接近客户端）的一段。
    let source_ip = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            request
                .extensions()
                .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                .map(|info| info.0.ip().to_string())
        });

    let user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.chars().take(256).collect::<String>());

    RequestMeta::new(request_id, source_ip, user_agent)
}

/// 作用域内读取当前请求元数据；作用域外返回 `None`。
#[must_use]
pub fn try_current_meta() -> Option<RequestMeta> {
    REQUEST_META.try_with(Clone::clone).ok()
}

/// 当前 `request_id`；作用域外现场生成（保证错误体 / 审计永远有 ID）。
#[must_use]
pub fn current_request_id() -> String {
    try_current_meta()
        .map(|meta| meta.request_id)
        .unwrap_or_else(|| RequestId::new_v7().to_string())
}

/// 当前来源 IP。
#[must_use]
pub fn current_source_ip() -> Option<String> {
    try_current_meta().and_then(|meta| meta.source_ip)
}

/// 中间件：建立请求上下文作用域，并把 `x-request-id` 回写到响应头。
pub async fn inject_request_meta(request: Request, next: Next) -> Response {
    let meta = extract_request_meta(&request);
    let request_id = meta.request_id.clone();
    let mut response = REQUEST_META.scope(meta, next.run(request)).await;
    // 回写请求 ID：客户端报障时可以直接把响应头里的 ID 给运维。
    if let Ok(value) = axum::http::HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outside_scope_falls_back_without_panicking() {
        // 后台任务 / 单测不在作用域内，必须拿到兜底 ID 而不是 panic
        assert!(try_current_meta().is_none());
        assert!(!current_request_id().is_empty());
        assert!(current_source_ip().is_none());
    }

    #[tokio::test]
    async fn scope_exposes_meta_to_nested_futures() {
        let meta = RequestMeta::new("req-42".to_string(), Some("10.0.0.7".to_string()), None);
        REQUEST_META
            .scope(meta, async {
                assert_eq!(current_request_id(), "req-42");
                assert_eq!(current_source_ip().as_deref(), Some("10.0.0.7"));
            })
            .await;
        // 作用域结束后恢复兜底
        assert!(try_current_meta().is_none());
    }
}
