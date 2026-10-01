//! Worker gRPC 客户端与「客户端断开即取消」守卫。
//!
//! 两个职责：
//! 1. **连接复用**：`Channel` 内部自带多路复用与重连，按 endpoint 缓存即可，
//!    热路径不能每次请求都握手。
//! 2. **取消传播**（架构 §17.4 硬约束）：客户端断开 / 请求被 drop 时，必须在 1s 内
//!    把 `WorkerData.Cancel` 送到 Worker。用 [`CancelGuard`]（drop guard）实现 ——
//!    axum 在连接断开时会 drop 响应体，从而 drop 掉持有 guard 的流，
//!    此时 `Drop` 里起的取消任务把信号送达。

use dashmap::DashMap;
use domain::ids::{DatabaseId, WorkerId};
use protocol::common::RequestContext;
use protocol::control::worker_control_client::WorkerControlClient;
use protocol::data::{
    worker_data_client::WorkerDataClient, BatchStatement, CancelRequest, CloseSessionRequest,
    ExecuteBatchRequest, ExecuteRequest, OpenSessionRequest, SessionExecuteRequest,
};
use tonic::transport::{Channel, Endpoint};

use crate::config::{CANCEL_DISPATCH_TIMEOUT, WORKER_CONNECT_TIMEOUT};
use crate::error::{api_error_from_status, ApiError, ApiResult};

/// 控制路径 RPC 的本地兜底超时（毫秒）。
///
/// 真正的 deadline 通过 proto 的 `deadline_unix_ms` 传给 Worker；这里的兜底值必须
/// **大于**它，否则会变成「Worker 还没返回，Server 先放弃」，把正常的慢启动判成故障。
pub const CONTROL_RPC_FALLBACK_TIMEOUT_MS: u64 = 60_000;

/// 按 endpoint 缓存的 gRPC `Channel` 池。
///
/// Control Path 与 Data Path 共用同一批连接：两者都是 HTTP/2 多路复用，
/// 分开建池只会让连接数翻倍。`Channel` 自身会做重连，因此缓存永不过期；
/// 只有在「Worker 换了地址」或「连接被判定为坏」时才显式 [`ChannelPool::evict`]。
#[derive(Debug, Default)]
pub struct ChannelPool {
    channels: DashMap<String, Channel>,
}

impl ChannelPool {
    /// 构造空池。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 取（必要时建立）指定 endpoint 的连接。
    ///
    /// # Errors
    /// endpoint 非法或连接失败时返回 `WORKER_UNAVAILABLE`（可重试）。
    pub async fn get(&self, endpoint: &str) -> ApiResult<Channel> {
        if let Some(existing) = self.channels.get(endpoint) {
            return Ok(existing.clone());
        }
        let url = normalize_grpc_endpoint(endpoint)?;
        let channel = Endpoint::from_shared(url)
            .map_err(|err| ApiError::internal(format!("Worker endpoint '{endpoint}' 非法: {err}")))?
            .connect_timeout(WORKER_CONNECT_TIMEOUT)
            .timeout(WORKER_CONNECT_TIMEOUT)
            .connect()
            .await
            .map_err(|err| {
                tracing::warn!(endpoint, error = %err, "连接 Worker 失败");
                ApiError::new(
                    domain::error::ErrorCode::WorkerUnavailable,
                    format!("连接 Worker {endpoint} 失败: {err}"),
                )
            })?;
        // 并发首连时以先写入者为准，后来的连接直接丢弃（Channel 自身会重连）。
        Ok(self
            .channels
            .entry(endpoint.to_string())
            .or_insert(channel)
            .clone())
    }

    /// Data Path 客户端。
    ///
    /// # Errors
    /// 同 [`ChannelPool::get`]。
    pub async fn data(&self, endpoint: &str) -> ApiResult<WorkerDataClient<Channel>> {
        Ok(WorkerDataClient::new(self.get(endpoint).await?))
    }

    /// Control Path 客户端。
    ///
    /// # Errors
    /// 同 [`ChannelPool::get`]。
    pub async fn control(&self, endpoint: &str) -> ApiResult<WorkerControlClient<Channel>> {
        Ok(WorkerControlClient::new(self.get(endpoint).await?))
    }

    /// 丢弃某 endpoint 的缓存连接（Worker 重注册后调用）。
    pub fn evict(&self, endpoint: &str) {
        self.channels.remove(endpoint);
    }
}

/// 把 endpoint 规范化为 tonic 需要的 `http://host:port` 形式。
///
/// 数据库 / env 里习惯写 `host:port`，而 tonic 只接受带 scheme 的 URI。
///
/// # Errors
/// 空串或带不支持的 scheme 时返回参数错误。
pub fn normalize_grpc_endpoint(raw: &str) -> ApiResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ApiError::invalid_argument("Worker endpoint 为空"));
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Ok(trimmed.to_string());
    }
    if trimmed.contains("://") {
        return Err(ApiError::invalid_argument(format!(
            "Worker endpoint 协议不支持: {trimmed}"
        )));
    }
    Ok(format!("http://{trimmed}"))
}

/// 「客户端断开即取消」守卫。
///
/// 生命周期与一次数据面请求绑定：
/// - 请求正常结束时调用 [`CancelGuard::disarm`]，不产生多余 RPC；
/// - 请求被 drop（连接断开 / 超时 / 上层取消）时，`Drop` 里 spawn 一个带超时的
///   `WorkerData.Cancel`，把取消一路传播到 DB Process。
pub struct CancelGuard {
    client: WorkerDataClient<Channel>,
    context: RequestContext,
    armed: bool,
}

impl CancelGuard {
    /// 构造守卫（默认 armed）。
    #[must_use]
    pub fn new(client: WorkerDataClient<Channel>, context: RequestContext) -> Self {
        Self {
            client,
            context,
            armed: true,
        }
    }

    /// 请求已正常结束，解除守卫。
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    /// 该守卫是否仍会发送取消。
    #[must_use]
    pub fn is_armed(&self) -> bool {
        self.armed
    }
}

impl std::fmt::Debug for CancelGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelGuard")
            .field("request_id", &self.context.request_id)
            .field("armed", &self.armed)
            .finish()
    }
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // drop 可能发生在 runtime 之外（进程退出 / 同步测试线程）：此时无法 spawn，
        // 直接放弃取消 —— 进程都要没了，传播取消没有意义。
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!(
                request_id = %self.context.request_id,
                "无 tokio runtime，跳过取消传播"
            );
            return;
        };
        let mut client = self.client.clone();
        let request = CancelRequest {
            context: Some(protocol::convert::RequestContext::from(self.context.clone()).into()),
            // 空 target 表示「取消 context.request_id 对应的这次执行」
            target_request_id: self.context.request_id.clone(),
            target_session_id: self.context.session_id.clone(),
            reason: "client disconnected".to_string(),
        };
        let request_id = self.context.request_id.clone();
        handle.spawn(async move {
            // 预算 800ms：给「1s 内送达」的契约留出调度余量。
            match tokio::time::timeout(CANCEL_DISPATCH_TIMEOUT, client.cancel(request)).await {
                Ok(Ok(_)) => tracing::debug!(request_id, "已向 Worker 传播取消"),
                Ok(Err(status)) => {
                    tracing::debug!(request_id, code = ?status.code(), "取消传播失败")
                }
                Err(_) => tracing::warn!(request_id, "取消传播超时"),
            }
        });
    }
}

/// 把 DB / Worker / epoch 组装成一次数据面调用的 [`RequestContext`]。
#[must_use]
pub fn data_request_context(
    request_id: &str,
    database_id: DatabaseId,
    worker_id: &WorkerId,
    owner_epoch: u64,
    session_id: Option<String>,
    deadline_unix_ms: u64,
) -> RequestContext {
    RequestContext {
        request_id: request_id.to_string(),
        trace_id: String::new(),
        tenant_id: String::new(),
        database_id: database_id.to_string(),
        owner_epoch,
        deadline_unix_ms,
        session_id: session_id.unwrap_or_default(),
        transaction_id: String::new(),
        worker_id: worker_id.to_string(),
        idempotency_key: String::new(),
    }
}

/// 组装 `ExecuteStream` / `Execute` 请求（两者共用 `ExecuteRequest`）。
#[must_use]
pub fn execute_request(
    context: RequestContext,
    sql: &str,
    params: Vec<protocol::data::Value>,
    inline_row_limit: u32,
) -> ExecuteRequest {
    ExecuteRequest {
        context: Some(protocol::convert::RequestContext::from(context).into()),
        sql: sql.to_string(),
        params,
        inline_row_limit,
        // 单语句默认包一层原子事务（autocommit 语义）
        atomic: true,
    }
}

/// 组装 `ExecuteBatch` 请求。
#[must_use]
pub fn execute_batch_request(
    context: RequestContext,
    statements: Vec<(String, Vec<protocol::data::Value>)>,
    atomic: bool,
) -> ExecuteBatchRequest {
    ExecuteBatchRequest {
        context: Some(protocol::convert::RequestContext::from(context).into()),
        statements: statements
            .into_iter()
            .map(|(sql, params)| BatchStatement { sql, params })
            .collect(),
        atomic,
    }
}

/// 组装 `OpenSession` 请求。
#[must_use]
pub fn open_session_request(context: RequestContext, idle_timeout_ms: u32) -> OpenSessionRequest {
    OpenSessionRequest {
        context: Some(protocol::convert::RequestContext::from(context).into()),
        idle_timeout_ms,
    }
}

/// 组装 `CloseSession` 请求。
#[must_use]
pub fn close_session_request(context: RequestContext, session_id: &str) -> CloseSessionRequest {
    CloseSessionRequest {
        context: Some(protocol::convert::RequestContext::from(context).into()),
        session_id: session_id.to_string(),
    }
}

/// 组装会话内执行请求。
#[must_use]
pub fn session_execute_request(
    context: RequestContext,
    session_id: &str,
    sql: &str,
    params: Vec<protocol::data::Value>,
) -> SessionExecuteRequest {
    SessionExecuteRequest {
        context: Some(protocol::convert::RequestContext::from(context).into()),
        session_id: session_id.to_string(),
        sql: sql.to_string(),
        params,
    }
}

/// 校验 proto 返回的结构化错误：`None` 或 `OK` 视为成功。
///
/// # Errors
/// 返回 Worker 上报的领域错误（保留 code / retryable / detail）。
pub fn check_proto_error(
    error: Option<&protocol::common::PlatformError>,
    worker_id: &str,
) -> ApiResult<()> {
    match error {
        None => Ok(()),
        Some(err) if err.code == protocol::common::ErrorCode::Ok as i32 => Ok(()),
        Some(err) => {
            let platform = protocol::convert::platform_error_from_proto(err);
            tracing::debug!(
                worker_id,
                code = platform.code.as_str(),
                message = %platform.message,
                "Worker 返回结构化错误"
            );
            Err(ApiError::from(platform))
        }
    }
}

/// 把 tonic 状态错误包装为 API 错误。
#[must_use]
pub fn status_to_api_error(status: tonic::Status, worker_id: &str) -> ApiError {
    api_error_from_status(&status, worker_id)
}

/// 判断 Worker 回传的错误是否属于「路由过期」，可透明重试。
#[must_use]
pub fn is_stale_route_error(error: &domain::error::PlatformError) -> bool {
    use domain::error::ErrorCode;
    matches!(
        error.code,
        ErrorCode::NotOwner | ErrorCode::EpochMismatch | ErrorCode::RouteStale
    )
}

/// 判断错误是否可能是**存储层 fencing 拒绝**（owner epoch 落后于 Remote WAL）。
///
/// # 为什么需要「可能」这种弱判定
///
/// `SetOwnerEpoch` 被 WAL 拒时，WalClient 会给出 `WAL_APPEND_REJECTED` / `EPOCH_MISMATCH`，
/// 但 Worker 在启动路径上会把 WAL 故障统一收敛为 `STORAGE_UNAVAILABLE`（Worker 不区分
/// 「WAL 拒绝了我的 epoch」与「WAL 挂了」，两者的处置都是「本节点起不来」）。
/// 控制面拿不到原始码，因此这里只做**粗筛**：命中后再去 WAL 读一次真实 epoch 作证据
/// （见 `DbRouter::retry_start_after_epoch_realign`），只有证据成立才动手对齐 ——
/// 这样既不会被错误文案绑架，也不会把真实的存储故障误判成 epoch 问题。
#[must_use]
pub fn is_fencing_rejection(error: &crate::error::ApiError) -> bool {
    use domain::error::ErrorCode;
    matches!(
        error.code(),
        ErrorCode::WalAppendRejected | ErrorCode::EpochMismatch | ErrorCode::StorageUnavailable
    )
}

/// 判断 gRPC 状态是否属于「路由过期」。
///
/// Worker 侧把 fencing 失败映射成 `FAILED_PRECONDITION` / `ABORTED`，或把结构化错误码
/// 放进 trailing metadata；两种形式都识别。
#[must_use]
pub fn is_stale_route_status(status: &tonic::Status) -> bool {
    use tonic::Code;
    if matches!(status.code(), Code::FailedPrecondition | Code::Aborted) {
        return true;
    }
    status.metadata().iter().any(|kv| match kv {
        tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
            key.as_str() == "error-code"
                && value
                    .to_str()
                    .map(|value| matches!(value, "NOT_OWNER" | "EPOCH_MISMATCH" | "ROUTE_STALE"))
                    .unwrap_or(false)
        }
        tonic::metadata::KeyAndValueRef::Binary(_, _) => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::error::ErrorCode;

    #[test]
    fn endpoint_normalization_adds_http_scheme() {
        assert_eq!(
            normalize_grpc_endpoint("worker-1:9101").unwrap(),
            "http://worker-1:9101"
        );
        assert_eq!(
            normalize_grpc_endpoint("http://worker-1:9101").unwrap(),
            "http://worker-1:9101"
        );
        assert_eq!(
            normalize_grpc_endpoint("  https://w:1  ").unwrap(),
            "https://w:1"
        );
    }

    #[test]
    fn endpoint_normalization_rejects_empty_and_unknown_scheme() {
        assert!(normalize_grpc_endpoint("   ").is_err());
        assert!(normalize_grpc_endpoint("unix:///run/db.sock").is_err());
    }

    #[test]
    fn proto_error_none_or_ok_is_success() {
        assert!(check_proto_error(None, "w1").is_ok());
        let ok = protocol::common::PlatformError {
            code: protocol::common::ErrorCode::Ok as i32,
            ..Default::default()
        };
        assert!(check_proto_error(Some(&ok), "w1").is_ok());
    }

    #[test]
    fn proto_error_preserves_domain_code() {
        let err = protocol::common::PlatformError {
            code: protocol::common::ErrorCode::NotOwner as i32,
            message: "not owner".into(),
            retryable: true,
            ..Default::default()
        };
        let api = check_proto_error(Some(&err), "w1").unwrap_err();
        assert_eq!(api.code(), ErrorCode::NotOwner);
        assert!(is_stale_route_error(&api.error));
    }

    #[test]
    fn stale_route_error_does_not_include_storage_fencing() {
        use domain::error::ErrorCode;
        // 存储层 fencing 拒绝不是「路由过期」：它必须走 epoch 对齐路径，而不是刷新路由重试。
        let fencing = domain::error::PlatformError::new(ErrorCode::WalAppendRejected, "epoch 过期");
        assert!(!is_stale_route_error(&fencing));
        assert!(is_fencing_rejection(&crate::error::ApiError::from(fencing)));
    }

    #[test]
    fn fencing_rejection_screening_covers_worker_rewritten_codes() {
        use domain::error::ErrorCode;
        // Worker 在启动路径上把 WAL 故障统一收敛成 STORAGE_UNAVAILABLE，
        // 所以粗筛必须包含它 —— 真正的判定证据是随后对 WAL 的读取（见 router）。
        for code in [
            ErrorCode::WalAppendRejected,
            ErrorCode::EpochMismatch,
            ErrorCode::StorageUnavailable,
        ] {
            let api = crate::error::ApiError::from(domain::error::PlatformError::new(code, "x"));
            assert!(is_fencing_rejection(&api), "{code:?} 应进入 fencing 粗筛");
        }
        // 无关错误不得被误伤（否则会把 S3 故障当成 epoch 问题去修补）
        for code in [ErrorCode::AdmissionDenied, ErrorCode::WakeupTimeout] {
            let api = crate::error::ApiError::from(domain::error::PlatformError::new(code, "x"));
            assert!(
                !is_fencing_rejection(&api),
                "{code:?} 不应进入 fencing 粗筛"
            );
        }
    }

    #[test]
    fn stale_route_detection_covers_fencing_codes() {
        for code in [
            ErrorCode::NotOwner,
            ErrorCode::EpochMismatch,
            ErrorCode::RouteStale,
        ] {
            assert!(is_stale_route_error(&domain::error::PlatformError::new(
                code, "x"
            )));
        }
        assert!(!is_stale_route_error(&domain::error::PlatformError::new(
            ErrorCode::SqlError,
            "x"
        )));
    }

    #[test]
    fn stale_route_status_detection() {
        assert!(is_stale_route_status(&tonic::Status::failed_precondition(
            "stale"
        )));
        assert!(!is_stale_route_status(&tonic::Status::internal("boom")));
        assert!(!is_stale_route_status(&tonic::Status::not_found("nope")));
    }

    #[test]
    fn cancel_request_carries_target_request_id() {
        let context = data_request_context(
            "req-7",
            DatabaseId::new_v7(),
            &WorkerId::new("w1"),
            9,
            Some("sess-1".to_string()),
            1_700_000_000_000,
        );
        let request = CancelRequest {
            context: Some(protocol::convert::RequestContext::from(context.clone()).into()),
            target_request_id: context.request_id.clone(),
            target_session_id: context.session_id.clone(),
            reason: "client disconnected".to_string(),
        };
        assert_eq!(request.target_request_id, "req-7");
        // proto 里 target_session_id 是标量 string（空 = 未指定），因此直接比对字符串。
        assert_eq!(request.target_session_id, "sess-1");
    }
}
