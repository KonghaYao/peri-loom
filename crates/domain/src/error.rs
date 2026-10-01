//! 统一错误码与结构化错误体。
//!
//! `ErrorCode` 与 `proto/platform/common.proto` 的 `platform.common.v1.ErrorCode`
//! **逐值对应**；`as_str()` 返回的 SCREAMING_SNAKE 字符串就是对外 HTTP 错误体中的 `code`
//! （架构 §17.4 统一错误体），必须与 proto 完全一致 —— 该一致性由 `proto_contract` 测试
//! 直接解析 proto 源文件校验。

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// 从字符串 / 整数解析领域枚举失败（严格解析路径使用；宽松路径见各 `from_str_lossy`）。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown {kind} value: {value}")]
pub struct UnknownEnumValue {
    /// 枚举名（例如 `ErrorCode`）。
    pub kind: &'static str,
    /// 未能识别的原始取值。
    pub value: String,
}

impl UnknownEnumValue {
    /// 构造解析错误。
    pub fn new(kind: &'static str, value: impl Into<String>) -> Self {
        Self {
            kind,
            value: value.into(),
        }
    }
}

/// 平台统一错误码，与 proto `platform.common.v1.ErrorCode` 一一对应。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorCode {
    /// 未指定（等价于“缺失错误码”）。
    ErrorCodeUnspecified,
    /// 成功（仅用于 Result 载体，不作为错误返回）。
    Ok,

    /// 数据库不存在。
    DbNotFound,
    /// 数据库已存在。
    DbAlreadyExists,
    /// 数据库尚未就绪（COLD / STARTING / 恢复中）。
    ///
    /// **仅内部 / 管理面使用**：Query API（数据面）不得把该码返回给调用方 —— 冷启动由
    /// Transparent Wake 在请求 deadline 内等待 DB Ready，对调用方完全透明，不暴露
    /// 「DB_WAKING 请重试」协议（架构 §15.4）。管理面 / 运维接口需要展示「正在唤醒」时
    /// 才可以返回它。
    DatabaseNotReady,
    /// 当前节点不是该 DB 的有效 Owner（fencing 拒绝）。
    NotOwner,
    /// Owner Epoch 不匹配，请求携带的 epoch 已过期。
    EpochMismatch,
    /// 路由缓存过期（stale route），可透明重试。
    RouteStale,
    /// Worker 不可用。
    WorkerUnavailable,
    /// Worker 正在排空，拒绝新 placement。
    WorkerDraining,
    /// 准入被拒（资源预算 / 硬上限不足）。
    AdmissionDenied,
    /// 冷启动等待超时（Transparent Wake 在 deadline 内未 READY）。
    WakeupTimeout,

    /// 会话不存在。
    SessionNotFound,
    /// 会话丢失（failover 后原 connection context 不可恢复）。
    SessionLost,
    /// 事务丢失。
    TransactionLost,
    /// 事务状态非法（例如未 BEGIN 就 COMMIT）。
    TransactionStateInvalid,
    /// 会话空闲超时（默认 60s）。
    SessionIdleTimeout,
    /// 事务超过最大生命周期（默认 30s）。
    TransactionMaxLifetimeExceeded,

    /// SQL 执行错误。
    SqlError,
    /// SQL 语法 / 解析错误。
    SqlParseError,
    /// 约束冲突（UNIQUE / NOT NULL / FK 等）。
    ConstraintViolation,
    /// 请求 deadline 超时。
    DeadlineExceeded,
    /// 请求被取消（客户端断开或显式 Cancel）。
    Cancelled,
    /// 结果集过大。
    ResultTooLarge,
    /// 批处理部分失败。
    BatchPartialFailure,

    /// Remote WAL 未 durable，禁止返回 Commit Success。
    ///
    /// **不可自动重试**（`retryable = false`）：该码的语义是「本次没有拿到 durable 确认」，
    /// 而**不是**「数据一定没写进去」—— quorum 复制成功但 ACK 丢失时事务可能已经落地。
    /// 若对外置 `retryable = true`，客户端会重放**整个写事务**，于是出现
    /// 「报告失败但实际生效」+「重试再生效」的双写。
    /// （wal-client 需要的换端点重试是它自己的有限重试 + 幂等键，不依赖这个对外标志。）
    WalNotDurable,
    /// WAL 拒绝该次 append（多数为 fencing / 配额 / 背压）。
    ///
    /// **不可自动重试**（`retryable = false`）：fencing（旧 Owner Epoch）与幂等键复用都是
    /// 确定性问题，同样的参数重试永远不会成功（必须先重新获取 Owner / 换幂等键）；
    /// 而重放被拒的写事务同样会踩到 WalNotDurable 那类双写（可能已生效 + 再执行一次）。
    WalAppendRejected,
    /// 该节点不是 WAL leader。
    ///
    /// 可自动重试：换到真正的 leader 端点重试**不改变请求语义**（架构 §17.8），
    /// 也不会让同一个写事务被重复施加。
    WalNotLeader,
    /// Object Storage 不可用。
    ///
    /// **不可自动重试**（`retryable = false`）：这是跨面故障，恢复后重放整个写事务可能对
    /// 已经产生副作用的动作（快照落盘、目标 Worker 上已写入的恢复数据）再来一次，
    /// 形成重复快照 / 重复恢复与双写。正确动作是管理面带 `Idempotency-Key` 显式重试。
    StorageUnavailable,
    /// 快照不可用（不存在 / 未完成 / 已损坏）。
    ///
    /// **不可自动重试**（`retryable = false`）：这是确定性状态而不是瞬态依赖 ——
    /// snapshot 不存在、没写完或校验不通，用同样的参数重试都不会自愈，
    /// 需要先完成快照或走备份恢复流程（改变请求前提）。
    SnapshotUnavailable,
    /// 校验和不匹配（数据损坏）。
    ChecksumMismatch,

    /// 资源耗尽（CPU / 内存 / FD / 进程位等）。
    ResourceExhausted,
    /// 未认证。
    Unauthenticated,
    /// 无权限。
    PermissionDenied,
    /// 触发限流。
    RateLimited,
    /// 超出配额。
    QuotaExceeded,
    /// 幂等键冲突（同 key 不同请求体）。
    IdempotencyConflict,

    /// 内部错误（未知错误码一律归入此处，不得 panic）。
    InternalError,
    /// 能力未实现。
    NotImplemented,
    /// 参数非法。
    InvalidArgument,
}

impl ErrorCode {
    /// 全部错误码（字典序 = proto 声明顺序），供测试与监控标签枚举使用。
    pub const ALL: &'static [ErrorCode] = &[
        ErrorCode::ErrorCodeUnspecified,
        ErrorCode::Ok,
        ErrorCode::DbNotFound,
        ErrorCode::DbAlreadyExists,
        ErrorCode::DatabaseNotReady,
        ErrorCode::NotOwner,
        ErrorCode::EpochMismatch,
        ErrorCode::RouteStale,
        ErrorCode::WorkerUnavailable,
        ErrorCode::WorkerDraining,
        ErrorCode::AdmissionDenied,
        ErrorCode::WakeupTimeout,
        ErrorCode::SessionNotFound,
        ErrorCode::SessionLost,
        ErrorCode::TransactionLost,
        ErrorCode::TransactionStateInvalid,
        ErrorCode::SessionIdleTimeout,
        ErrorCode::TransactionMaxLifetimeExceeded,
        ErrorCode::SqlError,
        ErrorCode::SqlParseError,
        ErrorCode::ConstraintViolation,
        ErrorCode::DeadlineExceeded,
        ErrorCode::Cancelled,
        ErrorCode::ResultTooLarge,
        ErrorCode::BatchPartialFailure,
        ErrorCode::WalNotDurable,
        ErrorCode::WalAppendRejected,
        ErrorCode::WalNotLeader,
        ErrorCode::StorageUnavailable,
        ErrorCode::SnapshotUnavailable,
        ErrorCode::ChecksumMismatch,
        ErrorCode::ResourceExhausted,
        ErrorCode::Unauthenticated,
        ErrorCode::PermissionDenied,
        ErrorCode::RateLimited,
        ErrorCode::QuotaExceeded,
        ErrorCode::IdempotencyConflict,
        ErrorCode::InternalError,
        ErrorCode::NotImplemented,
        ErrorCode::InvalidArgument,
    ];

    /// 对外错误码字符串（与 proto 枚举值名完全一致）。
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::ErrorCodeUnspecified => "ERROR_CODE_UNSPECIFIED",
            ErrorCode::Ok => "OK",
            ErrorCode::DbNotFound => "DB_NOT_FOUND",
            ErrorCode::DbAlreadyExists => "DB_ALREADY_EXISTS",
            ErrorCode::DatabaseNotReady => "DATABASE_NOT_READY",
            ErrorCode::NotOwner => "NOT_OWNER",
            ErrorCode::EpochMismatch => "EPOCH_MISMATCH",
            ErrorCode::RouteStale => "ROUTE_STALE",
            ErrorCode::WorkerUnavailable => "WORKER_UNAVAILABLE",
            ErrorCode::WorkerDraining => "WORKER_DRAINING",
            ErrorCode::AdmissionDenied => "ADMISSION_DENIED",
            ErrorCode::WakeupTimeout => "WAKEUP_TIMEOUT",
            ErrorCode::SessionNotFound => "SESSION_NOT_FOUND",
            ErrorCode::SessionLost => "SESSION_LOST",
            ErrorCode::TransactionLost => "TRANSACTION_LOST",
            ErrorCode::TransactionStateInvalid => "TRANSACTION_STATE_INVALID",
            ErrorCode::SessionIdleTimeout => "SESSION_IDLE_TIMEOUT",
            ErrorCode::TransactionMaxLifetimeExceeded => "TRANSACTION_MAX_LIFETIME_EXCEEDED",
            ErrorCode::SqlError => "SQL_ERROR",
            ErrorCode::SqlParseError => "SQL_PARSE_ERROR",
            ErrorCode::ConstraintViolation => "CONSTRAINT_VIOLATION",
            ErrorCode::DeadlineExceeded => "DEADLINE_EXCEEDED",
            ErrorCode::Cancelled => "CANCELLED",
            ErrorCode::ResultTooLarge => "RESULT_TOO_LARGE",
            ErrorCode::BatchPartialFailure => "BATCH_PARTIAL_FAILURE",
            ErrorCode::WalNotDurable => "WAL_NOT_DURABLE",
            ErrorCode::WalAppendRejected => "WAL_APPEND_REJECTED",
            ErrorCode::WalNotLeader => "WAL_NOT_LEADER",
            ErrorCode::StorageUnavailable => "STORAGE_UNAVAILABLE",
            ErrorCode::SnapshotUnavailable => "SNAPSHOT_UNAVAILABLE",
            ErrorCode::ChecksumMismatch => "CHECKSUM_MISMATCH",
            ErrorCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
            ErrorCode::Unauthenticated => "UNAUTHENTICATED",
            ErrorCode::PermissionDenied => "PERMISSION_DENIED",
            ErrorCode::RateLimited => "RATE_LIMITED",
            ErrorCode::QuotaExceeded => "QUOTA_EXCEEDED",
            ErrorCode::IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
            ErrorCode::InternalError => "INTERNAL_ERROR",
            ErrorCode::NotImplemented => "NOT_IMPLEMENTED",
            ErrorCode::InvalidArgument => "INVALID_ARGUMENT",
        }
    }

    /// proto 枚举数值。
    #[must_use]
    pub const fn to_proto_i32(&self) -> i32 {
        match self {
            ErrorCode::ErrorCodeUnspecified => 0,
            ErrorCode::Ok => 1,
            ErrorCode::DbNotFound => 100,
            ErrorCode::DbAlreadyExists => 101,
            ErrorCode::DatabaseNotReady => 102,
            ErrorCode::NotOwner => 103,
            ErrorCode::EpochMismatch => 104,
            ErrorCode::RouteStale => 105,
            ErrorCode::WorkerUnavailable => 106,
            ErrorCode::WorkerDraining => 107,
            ErrorCode::AdmissionDenied => 108,
            ErrorCode::WakeupTimeout => 109,
            ErrorCode::SessionNotFound => 200,
            ErrorCode::SessionLost => 201,
            ErrorCode::TransactionLost => 202,
            ErrorCode::TransactionStateInvalid => 203,
            ErrorCode::SessionIdleTimeout => 204,
            ErrorCode::TransactionMaxLifetimeExceeded => 205,
            ErrorCode::SqlError => 300,
            ErrorCode::SqlParseError => 301,
            ErrorCode::ConstraintViolation => 302,
            ErrorCode::DeadlineExceeded => 303,
            ErrorCode::Cancelled => 304,
            ErrorCode::ResultTooLarge => 305,
            ErrorCode::BatchPartialFailure => 306,
            ErrorCode::WalNotDurable => 400,
            ErrorCode::WalAppendRejected => 401,
            ErrorCode::WalNotLeader => 402,
            ErrorCode::StorageUnavailable => 403,
            ErrorCode::SnapshotUnavailable => 404,
            ErrorCode::ChecksumMismatch => 405,
            ErrorCode::ResourceExhausted => 500,
            ErrorCode::Unauthenticated => 501,
            ErrorCode::PermissionDenied => 502,
            ErrorCode::RateLimited => 503,
            ErrorCode::QuotaExceeded => 504,
            ErrorCode::IdempotencyConflict => 505,
            ErrorCode::InternalError => 600,
            ErrorCode::NotImplemented => 601,
            ErrorCode::InvalidArgument => 602,
        }
    }

    /// 解析 proto 数值；未知值落到 [`ErrorCode::InternalError`]，绝不 panic。
    #[must_use]
    pub fn from_proto_i32(value: i32) -> Self {
        Self::try_from_proto_i32(value).unwrap_or(ErrorCode::InternalError)
    }

    /// 解析 proto 数值，未知值返回 `None`。
    #[must_use]
    pub fn try_from_proto_i32(value: i32) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|code| code.to_proto_i32() == value)
    }

    /// 宽松解析字符串（DB 中的 `error_code` 列、跨版本数据）；未知值落到
    /// [`ErrorCode::InternalError`]。严格解析请用 [`FromStr`]。
    #[must_use]
    pub fn from_str_lossy(value: &str) -> Self {
        Self::ALL
            .iter()
            .copied()
            .find(|code| code.as_str() == value)
            .unwrap_or(ErrorCode::InternalError)
    }

    /// 默认 retryable 语义：`true` 表示调用方在**不改变请求语义**的前提下可安全重试。
    ///
    /// 判定原则：状态会自行收敛（容量/领导者/水位/瞬态依赖）**且重放不会重复施加副作用**的
    /// 才可重试；客户端请求本身有错的（参数/SQL/权限/配额/校验和）、需要改变语义的
    /// （重新开 Session、重新提交事务），以及「可能已经生效」的（WAL 未确认 durable /
    /// WAL 拒绝 / 存储面报错）一律不可自动重试 —— 后者的重放会造成双写，详见各错误码的注释。
    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self {
            // 路由 / 生命周期：重新解析 owner 后可重试（Server 侧受 route_retry_count 限制）。
            ErrorCode::DatabaseNotReady
            | ErrorCode::NotOwner
            | ErrorCode::EpochMismatch
            | ErrorCode::RouteStale
            | ErrorCode::WorkerUnavailable
            | ErrorCode::WorkerDraining
            | ErrorCode::AdmissionDenied
            | ErrorCode::WakeupTimeout => true,
            // WAL 里唯一可自动重试的码：换 leader 端点重试不改变请求语义。
            ErrorCode::WalNotLeader => true,
            // 平台压力：退避后可重试。
            ErrorCode::ResourceExhausted | ErrorCode::RateLimited | ErrorCode::InternalError => {
                true
            }
            // 其余一律不可自动重试。
            //
            // 特别地，WalNotDurable / WalAppendRejected / StorageUnavailable /
            // SnapshotUnavailable **必须**留在 false：它们要么可能已经生效，要么是确定性失败，
            // 客户端按 retryable=true 重放整个写事务会得到「报告失败但实际生效 + 重试再生效」
            // 的双写。不要把它们挪到上面的 true 分支里。
            _ => false,
        }
    }

    /// 建议的 HTTP 状态码（架构 §17.4 统一错误体）。
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        match self {
            ErrorCode::Ok => 200,
            ErrorCode::InvalidArgument | ErrorCode::SqlParseError | ErrorCode::SqlError => 400,
            ErrorCode::Unauthenticated => 401,
            ErrorCode::PermissionDenied => 403,
            ErrorCode::DbNotFound | ErrorCode::SessionNotFound => 404,
            ErrorCode::DbAlreadyExists
            | ErrorCode::NotOwner
            | ErrorCode::EpochMismatch
            | ErrorCode::RouteStale
            | ErrorCode::ConstraintViolation
            | ErrorCode::TransactionStateInvalid
            | ErrorCode::TransactionMaxLifetimeExceeded
            | ErrorCode::IdempotencyConflict => 409,
            // 会话 / 事务已不可恢复：410 表示资源曾经存在但已消失。
            ErrorCode::SessionLost | ErrorCode::TransactionLost | ErrorCode::SessionIdleTimeout => {
                410
            }
            ErrorCode::ResultTooLarge => 413,
            ErrorCode::Cancelled => 499,
            ErrorCode::BatchPartialFailure => 207,
            ErrorCode::RateLimited | ErrorCode::QuotaExceeded => 429,
            ErrorCode::DeadlineExceeded | ErrorCode::WakeupTimeout => 504,
            ErrorCode::DatabaseNotReady
            | ErrorCode::WorkerUnavailable
            | ErrorCode::WorkerDraining
            | ErrorCode::AdmissionDenied
            | ErrorCode::WalNotDurable
            | ErrorCode::WalAppendRejected
            | ErrorCode::WalNotLeader
            | ErrorCode::StorageUnavailable
            | ErrorCode::SnapshotUnavailable
            | ErrorCode::ResourceExhausted => 503,
            ErrorCode::NotImplemented => 501,
            // ERROR_CODE_UNSPECIFIED / CHECKSUM_MISMATCH / INTERNAL_ERROR
            _ => 500,
        }
    }

    /// 是否为成功码。
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        matches!(self, ErrorCode::Ok)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ErrorCode {
    type Err = UnknownEnumValue;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|code| code.as_str() == s)
            .ok_or_else(|| UnknownEnumValue::new("ErrorCode", s))
    }
}

impl From<ErrorCode> for i32 {
    fn from(value: ErrorCode) -> Self {
        value.to_proto_i32()
    }
}

// serde 走字符串（HTTP 错误体契约）；反序列化宽松，未知码降级为 INTERNAL_ERROR，
// 与 from_proto_i32 的约定保持一致 —— 读取旧数据不能让整条记录解析失败。
impl Serialize for ErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(ErrorCode::from_str_lossy(&raw))
    }
}

/// 统一结构化错误体（对应 proto `PlatformError`，并扩展 JSON detail）。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PlatformError {
    /// 错误码。
    pub code: ErrorCode,
    /// 人类可读信息（不得包含 secret）。
    pub message: String,
    /// 是否可安全重试；默认由 `code` 决定，可用 [`PlatformError::with_retryable`] 覆盖。
    pub retryable: bool,
    /// 关联的请求 ID，便于日志与链路追踪对齐。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// 结构化附加信息（例如 `{"epoch_expected":834,"epoch_actual":835}`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
    /// 单次请求内已发生的透明路由重试次数（stale route 场景最多 1 次）。
    #[serde(skip_serializing_if = "is_zero")]
    pub route_retry_count: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl PlatformError {
    /// 构造错误；`retryable` 由错误码默认语义决定。
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: code.retryable(),
            request_id: None,
            detail: None,
            route_retry_count: 0,
        }
    }

    /// 资源不存在。
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DbNotFound, message)
    }

    /// 资源已存在。
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::DbAlreadyExists, message)
    }

    /// 内部错误。
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalError, message)
    }

    /// 参数非法。
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    /// 未认证。
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, message)
    }

    /// 无权限。
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::PermissionDenied, message)
    }

    /// 能力未实现。
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotImplemented, message)
    }

    /// 资源耗尽 / 服务暂时不可用。
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::WorkerUnavailable, message)
    }

    /// 冷启动等待超时。
    pub fn wakeup_timeout(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::WakeupTimeout, message)
    }

    /// 会话丢失。
    pub fn session_lost(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::SessionLost, message)
    }

    /// 事务丢失。
    pub fn transaction_lost(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::TransactionLost, message)
    }

    /// 路由失效。
    pub fn route_stale(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::RouteStale, message)
    }

    /// 附加请求 ID（builder 风格）。
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// 附加结构化 detail（builder 风格）。
    #[must_use]
    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = Some(detail);
        self
    }

    /// 显式覆盖默认的 retryable 语义（builder 风格）。
    #[must_use]
    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    /// 记录一次透明路由重试（builder 风格）。
    #[must_use]
    pub fn with_route_retry_count(mut self, count: u32) -> Self {
        self.route_retry_count = count;
        self
    }

    /// 是否还能再做一次透明路由重试（最多 1 次，架构 §15.6 / proto PlatformError）。
    #[must_use]
    pub const fn route_retry_allowed(&self) -> bool {
        self.retryable && self.route_retry_count < 1
    }

    /// 建议的 HTTP 状态码。
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        self.code.http_status()
    }

    /// 更换错误码并重算默认 retryable（用于错误归类）。
    #[must_use]
    pub fn reclassify(mut self, code: ErrorCode) -> Self {
        self.retryable = code.retryable();
        self.code = code;
        self
    }
}

impl fmt::Display for PlatformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)?;
        if let Some(request_id) = &self.request_id {
            write!(f, " (request_id={request_id})")?;
        }
        Ok(())
    }
}

impl std::error::Error for PlatformError {}

// retryable 缺省时按 code 推导，避免手工构造的 JSON 让重试语义失真。
impl<'de> Deserialize<'de> for PlatformError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            code: ErrorCode,
            #[serde(default)]
            message: String,
            retryable: Option<bool>,
            #[serde(default)]
            request_id: Option<String>,
            #[serde(default)]
            detail: Option<serde_json::Value>,
            #[serde(default)]
            route_retry_count: u32,
        }

        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            retryable: raw.retryable.unwrap_or_else(|| raw.code.retryable()),
            code: raw.code,
            message: raw.message,
            request_id: raw.request_id,
            detail: raw.detail,
            route_retry_count: raw.route_retry_count,
        })
    }
}

/// 平台统一 Result。
pub type Result<T> = std::result::Result<T, PlatformError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract_src::{proto_enum_values, COMMON_PROTO};

    #[test]
    fn as_str_matches_proto_exactly() {
        let proto = proto_enum_values(COMMON_PROTO, "ErrorCode");
        assert_eq!(
            proto.len(),
            ErrorCode::ALL.len(),
            "错误码数量与 proto 不一致"
        );
        for (proto_name, proto_value) in &proto {
            let code = ErrorCode::ALL
                .iter()
                .copied()
                .find(|c| c.as_str() == proto_name)
                .unwrap_or_else(|| panic!("proto 中的 {proto_name} 在 Rust 侧缺失"));
            assert_eq!(
                code.to_proto_i32(),
                *proto_value,
                "{proto_name} 数值与 proto 不一致"
            );
            assert_eq!(code.as_str(), proto_name);
        }
        for code in ErrorCode::ALL {
            assert!(
                proto.iter().any(|(name, _)| name == code.as_str()),
                "Rust 侧 {} 在 proto 中缺失",
                code.as_str()
            );
        }
    }

    #[test]
    fn worker_unavailable_wire_name_is_frozen() {
        // 对外 HTTP 错误体 code 字符串必须与 proto 完全一致。
        assert_eq!(ErrorCode::WorkerUnavailable.as_str(), "WORKER_UNAVAILABLE");
        assert_eq!(
            ErrorCode::TransactionMaxLifetimeExceeded.as_str(),
            "TRANSACTION_MAX_LIFETIME_EXCEEDED"
        );
        assert_eq!(ErrorCode::InternalError.as_str(), "INTERNAL_ERROR");
    }

    #[test]
    fn proto_i32_round_trip_and_unknown_fallback() {
        for code in ErrorCode::ALL {
            assert_eq!(ErrorCode::from_proto_i32(code.to_proto_i32()), *code);
        }
        assert_eq!(ErrorCode::from_proto_i32(-1), ErrorCode::InternalError);
        assert_eq!(ErrorCode::from_proto_i32(9999), ErrorCode::InternalError);
        assert_eq!(ErrorCode::try_from_proto_i32(9999), None);
        assert_eq!(
            ErrorCode::from_proto_i32(0),
            ErrorCode::ErrorCodeUnspecified
        );
    }

    #[test]
    fn strict_and_lossy_string_parsing() {
        assert_eq!(
            ErrorCode::from_str("DB_NOT_FOUND").unwrap(),
            ErrorCode::DbNotFound
        );
        assert!(ErrorCode::from_str("NOPE").is_err());
        assert_eq!(ErrorCode::from_str_lossy("NOPE"), ErrorCode::InternalError);
        assert_eq!(
            ErrorCode::from_str_lossy("WAL_NOT_LEADER"),
            ErrorCode::WalNotLeader
        );
    }

    #[test]
    fn retryable_and_http_status_semantics() {
        assert!(ErrorCode::RouteStale.retryable());
        assert!(ErrorCode::WorkerUnavailable.retryable());
        assert!(ErrorCode::RateLimited.retryable());
        assert!(!ErrorCode::InvalidArgument.retryable());
        assert!(!ErrorCode::TransactionLost.retryable());
        assert!(!ErrorCode::ChecksumMismatch.retryable());

        assert_eq!(ErrorCode::DbNotFound.http_status(), 404);
        assert_eq!(ErrorCode::WorkerUnavailable.http_status(), 503);
        assert_eq!(ErrorCode::SessionLost.http_status(), 410);
        assert_eq!(ErrorCode::InvalidArgument.http_status(), 400);
        assert_eq!(ErrorCode::InternalError.http_status(), 500);
        assert_eq!(ErrorCode::Ok.http_status(), 200);
        assert_eq!(ErrorCode::NotImplemented.http_status(), 501);
    }

    /// FIX-1：durability / 存储类的失败码**不得**默认 `retryable = true`。
    ///
    /// 这些码要么「可能已经生效」（WAL 未确认 durable 但数据可能已落地、WAL 拒绝、
    /// 存储面报错时快照/恢复的副作用可能已产生），要么是确定性失败（快照缺失/损坏）。
    /// 客户端拿到 `retryable = true` 会重放**整个写事务**，于是出现
    /// 「报告失败但实际生效」+「重试再生效」的双写 —— 这正好破坏架构 §11.1 的
    /// 「Commit Success ⇔ Remote WAL durable / RPO = 0 committed transaction」。
    #[test]
    fn durability_and_storage_codes_are_not_retryable_due_to_double_write_risk() {
        for code in [
            ErrorCode::WalNotDurable,
            ErrorCode::WalAppendRejected,
            ErrorCode::StorageUnavailable,
            ErrorCode::SnapshotUnavailable,
        ] {
            assert!(
                !code.retryable(),
                "{} 不得自动重试：重放整个写事务会造成双写",
                code.as_str()
            );

            // 对外错误体里的 retryable 与错误码语义必须一致（架构 §17.4 统一错误体）
            let body = PlatformError::new(code, "boom");
            assert!(
                !body.retryable,
                "{} 的错误体不得标成 retryable",
                code.as_str()
            );
            // 不可重试 => 不允许透明路由重试
            assert!(
                !body.route_retry_allowed(),
                "{} 不得触发透明重试",
                code.as_str()
            );

            // HTTP 503（服务端侧可恢复）与「调用方可安全重放写事务」是两件事，不能混为一谈
            assert_eq!(
                code.http_status(),
                503,
                "{} 仍应表达服务端暂时不可用",
                code.as_str()
            );
        }
    }

    /// FIX-1 的另一半：可换端点 / 可稍后重试的码必须保持 `retryable = true`，
    /// 否则 Server 的透明重试（stale route / leader 切换 / 冷启动等待）会失效。
    #[test]
    fn endpoint_switchable_and_later_retryable_codes_stay_retryable() {
        for code in [
            ErrorCode::WalNotLeader, // 换 leader 端点，不改变请求语义
            ErrorCode::WorkerUnavailable,
            ErrorCode::WorkerDraining,
            ErrorCode::DatabaseNotReady,
            ErrorCode::WakeupTimeout,
            ErrorCode::RouteStale,
            ErrorCode::NotOwner,
            ErrorCode::EpochMismatch,
            ErrorCode::AdmissionDenied,
            ErrorCode::ResourceExhausted,
            ErrorCode::RateLimited,
            ErrorCode::InternalError,
        ] {
            assert!(code.retryable(), "{} 必须保持可重试", code.as_str());
        }
    }

    /// 错误体反序列化（手工构造的 JSON / 旧版本载荷）同样不得把 WAL_NOT_DURABLE 洗成可重试。
    #[test]
    fn deserialized_wal_not_durable_body_is_not_retryable() {
        let parsed: PlatformError =
            serde_json::from_str(r#"{"code":"WAL_NOT_DURABLE","message":"append timeout"}"#)
                .unwrap();
        assert!(
            !parsed.retryable,
            "缺省 retryable 必须按错误码推导为 false（否则调用方会重放写事务）"
        );
        assert_eq!(parsed.http_status(), 503);
    }

    #[test]
    fn platform_error_constructors_and_builders() {
        let err = PlatformError::not_found("database not found");
        assert_eq!(err.code, ErrorCode::DbNotFound);
        assert!(!err.retryable);
        assert_eq!(err.http_status(), 404);
        assert!(!err.route_retry_allowed());

        let err = PlatformError::internal("boom").with_request_id("req-1");
        assert_eq!(err.request_id.as_deref(), Some("req-1"));
        assert!(err.retryable, "INTERNAL_ERROR 默认允许重试");
        assert!(err.to_string().contains("request_id=req-1"));

        // 默认 retryable 可显式覆盖
        let err = PlatformError::internal("boom").with_retryable(false);
        assert!(!err.retryable);
        assert!(!err.route_retry_allowed());

        let err = PlatformError::route_stale("stale route").with_route_retry_count(1);
        assert!(!err.route_retry_allowed(), "透明路由重试最多 1 次");

        let err = PlatformError::new(ErrorCode::AdmissionDenied, "no capacity")
            .with_detail(serde_json::json!({"utilization": 0.91}));
        assert_eq!(err.http_status(), 503);
        assert!(err.detail.is_some());
    }

    #[test]
    fn serde_contract_matches_http_error_body() {
        let err = PlatformError::new(ErrorCode::DbNotFound, "database not found")
            .with_request_id("req-42");
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "code": "DB_NOT_FOUND",
                "message": "database not found",
                "retryable": false,
                "request_id": "req-42"
            })
        );

        // 缺省 retryable 时按 code 推导
        let parsed: PlatformError =
            serde_json::from_str(r#"{"code":"WORKER_UNAVAILABLE","message":"down"}"#).unwrap();
        assert!(parsed.retryable);
        assert_eq!(parsed.http_status(), 503);

        // 未知错误码降级而非解析失败
        let parsed: PlatformError = serde_json::from_str(r#"{"code":"FUTURE_CODE"}"#).unwrap();
        assert_eq!(parsed.code, ErrorCode::InternalError);
    }

    #[test]
    fn result_alias_is_usable() {
        fn fallible() -> Result<u8> {
            Ok(7)
        }
        assert_eq!(fallible().unwrap(), 7);
    }
}
