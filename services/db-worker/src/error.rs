//! db-worker 统一错误类型。
//!
//! 约束（架构 §17.4）：对外错误必须映射到 [`ErrorCode`]，不得把裸 `io::Error` /
//! `anyhow` 泄漏给 Server。控制面 gRPC 响应的错误体走 `PlatformError`（proto 在
//! 响应 message 内定义 `error` 字段），数据面 gRPC 只在传输/本地故障时用
//! `tonic::Status`，业务错误一律以 in-band `PlatformError` 返回。
//!
//! 映射原则：
//! - 所有权/fencing 类错误必须准确（`NOT_OWNER` vs `EPOCH_MISMATCH`），Router 据此
//!   决定是「刷新路由重试」还是「放弃」。
//! - 依赖不可达（WAL / Object Store）映射为 `*_UNAVAILABLE` 且 `retryable = true`，
//!   避免 Server 把临时故障当永久错误处理。
//! - 其余未分类错误归入 `INTERNAL_ERROR`，绝不 panic。

use domain::error::{ErrorCode, PlatformError};

/// db-worker 内部错误。
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// 配置非法（启动阶段即失败，不进入服务循环）。
    #[error("配置错误：{0}")]
    Config(String),

    /// 目标 DB 未在本 Worker 注册。
    #[error("数据库 {db_id} 未在本 Worker 注册")]
    DbNotRegistered {
        /// 数据库 id。
        db_id: String,
    },

    /// 请求携带的 epoch 低于本地记录：这是过期的 Owner（fencing 拒绝）。
    #[error("owner epoch 过期：db={db_id} 请求 {requested} < 本地 {local}")]
    EpochStale {
        /// 数据库 id。
        db_id: String,
        /// 请求携带的 epoch。
        requested: u64,
        /// 本地记录的 epoch。
        local: u64,
    },

    /// 请求携带的 epoch 高于本地记录：本 Worker 已被取代，不再持有该 DB 所有权。
    #[error("本 Worker 不是 db={db_id} 的 Owner：请求 epoch {requested} > 本地 {local}")]
    NotOwner {
        /// 数据库 id。
        db_id: String,
        /// 请求携带的 epoch。
        requested: u64,
        /// 本地记录的 epoch。
        local: u64,
    },

    /// 请求被路由到了错误的 Worker。
    #[error("指令目标 Worker {target} 与本节点 {local} 不符")]
    WrongWorker {
        /// 指令中的 worker id。
        target: String,
        /// 本节点 worker id。
        local: String,
    },

    /// 显式会话不存在于本节点（未 OpenSession、已被 Close、或进程重启后丢失）。
    ///
    /// 语义是「会话没了」，不是「DB 不可用」—— Server 据此把错误透传给客户端并
    /// 要求重新建立会话，而不是刷新路由重试。
    #[error("会话不存在：{session_id}")]
    SessionNotFound {
        /// 会话 id。
        session_id: String,
    },

    /// Move 冲突：目标 Worker 上已有该 DB 的运行进程。
    ///
    /// 典型场景是 `source_worker_id` 指向本节点（自己搬给自己）或同一 DB 被并发
    /// 调度到同一 Worker。此时若继续准备新的工作集，会覆盖正在运行的 DB 的数据目录，
    /// 因此必须显式拒绝而不是「尽力而为」。
    #[error("Move 冲突：{0}")]
    MoveConflict(String),

    /// Worker 正在排空，拒绝新 Placement（架构 §12.3）。
    #[error("Worker {worker_id} 处于 {state}，拒绝新 Placement")]
    Draining {
        /// 本节点 worker id。
        worker_id: String,
        /// 当前状态。
        state: String,
    },

    /// 资源准入被拒（架构 §9 `CanStart`）。
    #[error("资源准入被拒：{0}")]
    AdmissionDenied(String),

    /// 冷启动在 deadline 内未 READY（架构 §8）。
    #[error("启动超时：{0}")]
    WakeupTimeout(String),

    /// DB 进程启动失败。
    #[error("DB 进程启动失败：{0}")]
    Spawn(String),

    /// 本地 UDS（Dispatcher <-> DB Process）故障。
    #[error("本地 UDS 通信失败：{0}")]
    Uds(String),

    /// DB Process 以**结构化错误**拒绝了请求（错误帧只带 `error`、不带 `message`）。
    ///
    /// 错误码原样透传：`NOT_IMPLEMENTED` 这类确定性原因若被压成 `INTERNAL_ERROR`，
    /// 上层就只能看到「内部错误」，无法区分「能力没实现」与「真的坏了」。
    #[error("DB Process 拒绝请求：{0}")]
    DbProcess(PlatformError),

    /// 恢复（Snapshot / WAL replay）失败。
    #[error("恢复失败：{0}")]
    Restore(String),

    /// Object Storage 不可用 / 快照不可用。
    #[error("对象存储故障：{0}")]
    Storage(String),

    /// Remote WAL 不可用 / 读取失败。
    #[error("Remote WAL 故障：{0}")]
    Wal(String),

    /// 状态机非法转换（例如对 COLD DB 发 Commit）。
    #[error("状态非法：{0}")]
    InvalidState(String),

    /// cgroup 操作失败（仅用于内部日志；降级路径不返回本错误）。
    #[error("cgroup 操作失败：{0}")]
    Cgroup(String),

    /// 文件系统操作失败。
    #[error("{context}失败：{source}")]
    Io {
        /// 操作描述（含路径）。
        context: String,
        /// 底层错误。
        #[source]
        source: std::io::Error,
    },

    /// 兜底内部错误。
    #[error("内部错误：{0}")]
    Internal(String),
}

impl WorkerError {
    /// 构造带路径上下文的 IO 错误。
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        WorkerError::Io {
            context: context.into(),
            source,
        }
    }

    /// 映射到平台统一错误码。
    pub fn code(&self) -> ErrorCode {
        match self {
            WorkerError::Config(_) => ErrorCode::InvalidArgument,
            WorkerError::DbNotRegistered { .. } => ErrorCode::DbNotFound,
            WorkerError::EpochStale { .. } => ErrorCode::EpochMismatch,
            WorkerError::NotOwner { .. } => ErrorCode::NotOwner,
            WorkerError::WrongWorker { .. } => ErrorCode::NotOwner,
            WorkerError::SessionNotFound { .. } => ErrorCode::SessionNotFound,
            WorkerError::MoveConflict(_) => ErrorCode::InvalidArgument,
            WorkerError::Draining { .. } => ErrorCode::WorkerDraining,
            WorkerError::AdmissionDenied(_) => ErrorCode::AdmissionDenied,
            WorkerError::WakeupTimeout(_) => ErrorCode::WakeupTimeout,
            WorkerError::Spawn(_) => ErrorCode::InternalError,
            WorkerError::Uds(_) => ErrorCode::InternalError,
            WorkerError::DbProcess(err) => err.code,
            WorkerError::Restore(_) => ErrorCode::InternalError,
            WorkerError::Storage(_) => ErrorCode::StorageUnavailable,
            WorkerError::Wal(_) => ErrorCode::StorageUnavailable,
            WorkerError::InvalidState(_) => ErrorCode::DatabaseNotReady,
            WorkerError::Cgroup(_) => ErrorCode::InternalError,
            WorkerError::Io { .. } => ErrorCode::InternalError,
            WorkerError::Internal(_) => ErrorCode::InternalError,
        }
    }

    /// 转为结构化错误体。
    pub fn to_platform_error(&self) -> PlatformError {
        PlatformError::new(self.code(), self.to_string())
    }

    /// 转为结构化错误体并附带结构化诊断信息（例如 epoch 期望/实际）。
    pub fn to_platform_error_with_detail(&self, detail: serde_json::Value) -> PlatformError {
        self.to_platform_error().with_detail(detail)
    }

    /// 转为 gRPC `Status`。
    ///
    /// 数据面/控制面绝大多数错误走 in-band `PlatformError`；本方法只用于「请求本身
    /// 无法映射到业务错误」的场景（例如请求体字段缺失、跨进程协议损坏）。
    pub fn to_status(&self) -> tonic::Status {
        let code = self.code();
        let grpc_code = match code {
            ErrorCode::InvalidArgument => tonic::Code::InvalidArgument,
            ErrorCode::DbNotFound => tonic::Code::NotFound,
            ErrorCode::SessionNotFound => tonic::Code::NotFound,
            ErrorCode::NotOwner | ErrorCode::EpochMismatch => tonic::Code::FailedPrecondition,
            ErrorCode::WorkerDraining | ErrorCode::AdmissionDenied => {
                tonic::Code::ResourceExhausted
            }
            ErrorCode::WakeupTimeout | ErrorCode::DeadlineExceeded => tonic::Code::DeadlineExceeded,
            ErrorCode::Cancelled => tonic::Code::Cancelled,
            ErrorCode::ResourceExhausted => tonic::Code::ResourceExhausted,
            ErrorCode::StorageUnavailable => tonic::Code::Unavailable,
            ErrorCode::NotImplemented => tonic::Code::Unimplemented,
            _ => tonic::Code::Internal,
        };
        // 把平台错误码放进 metadata，便于 Server 侧无需解析文本即可分类
        let mut status = tonic::Status::new(grpc_code, self.to_string());
        if let Ok(value) = code.as_str().parse() {
            status.metadata_mut().insert("platform-error-code", value);
        }
        status
    }
}

/// 便捷别名。
pub type Result<T> = std::result::Result<T, WorkerError>;

impl From<WorkerError> for PlatformError {
    fn from(err: WorkerError) -> Self {
        err.to_platform_error()
    }
}

/// 把平台错误体转回 `Status`（保留结构化错误码）。
#[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
pub fn platform_error_to_status(err: &PlatformError) -> tonic::Status {
    let code = err.code;
    let grpc_code = match code {
        ErrorCode::InvalidArgument => tonic::Code::InvalidArgument,
        ErrorCode::DbNotFound => tonic::Code::NotFound,
        ErrorCode::NotOwner | ErrorCode::EpochMismatch => tonic::Code::FailedPrecondition,
        ErrorCode::WorkerDraining | ErrorCode::AdmissionDenied => tonic::Code::ResourceExhausted,
        ErrorCode::WakeupTimeout | ErrorCode::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        ErrorCode::Cancelled => tonic::Code::Cancelled,
        ErrorCode::ResourceExhausted => tonic::Code::ResourceExhausted,
        ErrorCode::StorageUnavailable => tonic::Code::Unavailable,
        ErrorCode::NotImplemented => tonic::Code::Unimplemented,
        _ => tonic::Code::Internal,
    };
    let mut status = tonic::Status::new(grpc_code, err.message.clone());
    if let Ok(value) = code.as_str().parse() {
        status.metadata_mut().insert("platform-error-code", value);
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_errors_map_to_fencing_codes() {
        let stale = WorkerError::EpochStale {
            db_id: "db-1".into(),
            requested: 1,
            local: 2,
        };
        assert_eq!(stale.code(), ErrorCode::EpochMismatch);
        assert_eq!(stale.to_status().code(), tonic::Code::FailedPrecondition);

        let not_owner = WorkerError::NotOwner {
            db_id: "db-1".into(),
            requested: 3,
            local: 2,
        };
        assert_eq!(not_owner.code(), ErrorCode::NotOwner);
        // NotOwner / EpochMismatch 都是「重新解析 owner 后可重试」，但重试必须由 Router
        // 改变目标 Worker，不能原地重放同一个请求
        assert!(not_owner.code().retryable());
    }

    #[test]
    fn dependency_failures_follow_the_platform_retry_contract() {
        // STORAGE_UNAVAILABLE 明确不可自动重试（domain::ErrorCode::retryable 的契约：
        // 写路径重放可能造成双写），Worker 必须沿用同一判定，不能自行放宽。
        assert!(!WorkerError::Storage("s3 down".into()).code().retryable());
        assert!(!WorkerError::Wal("wal down".into()).code().retryable());
        // 冷启动超时可以由 Router 重新选点/重试，属可重试
        assert!(WorkerError::WakeupTimeout("too slow".into())
            .code()
            .retryable());
        // 排空拒绝同样是「换个 Worker 就好」
        assert!(WorkerError::Draining {
            worker_id: "worker-1".into(),
            state: "DRAINING".into(),
        }
        .code()
        .retryable());
    }

    /// DB Process 的错误帧必须把**它给的错误码**透传给上层：快照接口曾把
    /// `NOT_IMPLEMENTED`（DB Process 不生成快照）报成 `INTERNAL_ERROR` + 「空载荷」，
    /// 让人误以为是通信故障。
    #[test]
    fn db_process_error_keeps_its_code_and_message() {
        let err = WorkerError::DbProcess(PlatformError::not_implemented(
            "DB Process 不生成快照：快照由 Worker 侧的对象存储通道完成",
        ));
        assert_eq!(err.code(), ErrorCode::NotImplemented);
        assert_eq!(err.to_status().code(), tonic::Code::Unimplemented);
        assert!(
            err.to_string().contains("不生成快照"),
            "真因必须保留在错误信息里：{err}"
        );
    }

    #[test]
    fn draining_maps_to_worker_draining() {
        let err = WorkerError::Draining {
            worker_id: "worker-1".into(),
            state: "DRAINING".into(),
        };
        assert_eq!(err.code(), ErrorCode::WorkerDraining);
        assert_eq!(err.to_status().code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            err.to_status()
                .metadata()
                .get("platform-error-code")
                .and_then(|v| v.to_str().ok()),
            Some("WORKER_DRAINING")
        );
    }
}
