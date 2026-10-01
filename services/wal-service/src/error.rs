//! wal-service 内部错误类型及其到平台错误码 / proto 错误体的映射。
//!
//! 契约来源（架构 §11.1 / §11.3，proto `platform/common.proto`）：
//! - 非 leader：`WAL_NOT_LEADER`（客户端应重试到 leader，可重试）
//! - 旧 owner_epoch 的 Append：`WAL_APPEND_REJECTED`（Storage-level Fencing，**不可**重试到同一副本）
//! - 等待 quorum 超时：`WAL_NOT_DURABLE`（提交不得向客户端返回成功）
//! - SetOwnerEpoch 回退：`EPOCH_MISMATCH`（epoch 属于单调语义，回退等价于放行旧写 Owner）
//!
//! 库 crate 约定：内部错误一律 thiserror，跨进程边界只暴露 `PlatformError`。

use domain::error::{ErrorCode, PlatformError};
use protocol::common;

/// fencing 相关 detail 的字段名（跨服务契约，两侧同名）。
///
/// 取值语义（架构 §11.3）：
/// - [`FENCING_CURRENT_EPOCH_KEY`]：**服务端权威**的当前 epoch（已记录值）；
/// - [`FENCING_REQUESTED_EPOCH_KEY`]：本次请求携带的 epoch。
///
/// 早期实现写的是 `epoch_expected` / `epoch_actual`，且含义与客户端预期的
/// 「expected = 请求携带 / actual = 服务端当前」正好相反，排障时会把人引向错误结论，
/// 因此统一成不含歧义的 `current_*` / `requested_*`；客户端仍兼容读取旧键。
pub const FENCING_CURRENT_EPOCH_KEY: &str = "current_epoch";
/// 见 [`FENCING_CURRENT_EPOCH_KEY`]。
pub const FENCING_REQUESTED_EPOCH_KEY: &str = "requested_epoch";

/// wal-service 内部错误。
///
/// `Clone` 是必需的：停机时要给所有未决提案回同一个错误
/// （见 `raft_group::RaftGroup::fail_all_pending`），`oneshot` 只能把值发出去一次。
#[derive(Debug, Clone, thiserror::Error)]
pub enum WalError {
    /// 本节点不是该 shard 的 Raft leader，无法接受写请求。
    #[error("节点不是 shard {shard} 的 leader（已知 leader={leader:?}）")]
    NotLeader {
        /// WAL shard 标识（一个 shard = 一个 Raft Group）。
        shard: String,
        /// 当前已知 leader 的节点 ID；0 表示无 leader（选举中）。
        leader: Option<u64>,
    },

    /// Append 携带的 owner_epoch 已经落后于已记录的 epoch：fencing 拒绝。
    #[error("owner epoch 过期：已记录 {recorded}，请求 {requested}")]
    StaleEpoch {
        /// WAL 中已记录的 epoch。
        recorded: u64,
        /// 请求携带的 epoch。
        requested: u64,
    },

    /// SetOwnerEpoch 必须严格递增，回退或持平都是非法推进。
    #[error("owner epoch 必须严格递增：已记录 {recorded}，请求 {requested}")]
    EpochNotMonotonic {
        /// WAL 中已记录的 epoch。
        recorded: u64,
        /// 请求携带的 epoch。
        requested: u64,
    },

    /// 同一 append_id 被复用于不同内容（不同 start_lsn），属于客户端幂等键误用。
    #[error(
        "append_id {append_id} 已被 start_lsn={recorded_start_lsn} 使用，本次 start_lsn={requested_start_lsn}"
    )]
    IdempotencyConflict {
        /// 客户端幂等键。
        append_id: String,
        /// 首次使用该键时的 start_lsn。
        recorded_start_lsn: u64,
        /// 本次携带的 start_lsn。
        requested_start_lsn: u64,
    },

    /// 参数非法（空 database_id、epoch=0、snapshot_id 缺失等）。
    #[error("参数非法：{0}")]
    InvalidArgument(String),

    /// 该 DB 在 WAL Shard 上没有任何记录。
    #[error("WAL 中不存在 database {0}")]
    DbNotFound(String),

    /// 请求读取的区间尚未 quorum durable。
    #[error("区间尚未 durable：{0}")]
    NotDurable(String),

    /// 请求区间已被 Trim（快照已覆盖），调用方必须改用更新的快照基线。
    #[error("区间已被 Trim：start_lsn={start_lsn} < trimmed_before_lsn={trimmed_before_lsn}")]
    RangeTrimmed {
        /// 请求起点。
        start_lsn: u64,
        /// 已截断水位。
        trimmed_before_lsn: u64,
    },

    /// Append 等待 commit/apply 超时。
    #[error("Append 等待 quorum durable 超时（{timeout_ms}ms，log_index={log_index:?}）")]
    AppendTimeout {
        /// 等待超时上限（毫秒）。
        timeout_ms: u64,
        /// 提案对应的 Raft 日志索引；None 表示尚未完成提案阶段。
        log_index: Option<u64>,
    },

    /// 本地持久化层（raft-engine）故障。
    #[error("raft-engine 故障：{0}")]
    Storage(String),

    /// Raft 状态机 / 配置层错误。
    #[error("Raft 错误：{0}")]
    Raft(String),

    /// 其他内部错误。
    #[error("内部错误：{0}")]
    Internal(String),
}

/// 本模块便捷 `Result`。
pub type WalResult<T> = std::result::Result<T, WalError>;

impl WalError {
    /// 映射到平台统一错误码（跨服务契约，不能随手换）。
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            // 非 leader：客户端换 leader 重试即可（架构 §17.8）
            WalError::NotLeader { .. } => ErrorCode::WalNotLeader,
            // 旧 epoch：Storage-level Fencing 拒绝，客户端必须重新获取 Owner（架构 §11.3）
            WalError::StaleEpoch { .. } => ErrorCode::WalAppendRejected,
            // 幂等键被复用到不同的 start_lsn：这是**终态**，用同参数重试永远不会成功。
            // 不能折叠成 WAL_APPEND_REJECTED（那个码表示「所有权已变」），否则客户端会
            // 误以为是 fencing 问题去重新取 Owner，而真正的问题是幂等键被复用；
            // IDEMPOTENCY_CONFLICT（505）在 proto 里已存在，客户端按终态处理。
            WalError::IdempotencyConflict { .. } => ErrorCode::IdempotencyConflict,
            // SetOwnerEpoch 回退：epoch 语义冲突
            WalError::EpochNotMonotonic { .. } => ErrorCode::EpochMismatch,
            WalError::InvalidArgument(_) => ErrorCode::InvalidArgument,
            WalError::DbNotFound(_) => ErrorCode::DbNotFound,
            // 未 durable / 超时：绝不返回 Commit Success（架构 §11.1）
            WalError::NotDurable(_) | WalError::AppendTimeout { .. } => ErrorCode::WalNotDurable,
            // 数据已被更晚的快照覆盖：调用方需要更新的快照基线
            WalError::RangeTrimmed { .. } => ErrorCode::SnapshotUnavailable,
            WalError::Storage(_) => ErrorCode::StorageUnavailable,
            WalError::Raft(_) | WalError::Internal(_) => ErrorCode::InternalError,
        }
    }

    /// 是否可安全重试（由错误码语义决定，不在此处另立标准）。
    pub const fn retryable(&self) -> bool {
        self.error_code().retryable()
    }

    /// 转成 domain 结构化错误（带诊断 detail）。
    pub fn to_platform_error(&self) -> PlatformError {
        let mut err = PlatformError::new(self.error_code(), self.to_string());
        // 「旧 epoch 被 fencing 拒绝」不是瞬时状态：用同样的参数重试永远不会成功，
        // 客户端必须先重新拿 owner epoch。这里显式覆盖错误码表的默认可重试标记，
        // 避免上层按 retryable 自动重试造成无意义放大。
        // （IdempotencyConflict 已由错误码表本身归为不可重试。）
        if matches!(self, WalError::StaleEpoch { .. }) {
            err = err.with_retryable(false);
        }
        let detail = match self {
            // 字段名必须与客户端一致：current = 服务端权威值，requested = 请求携带值
            WalError::StaleEpoch {
                recorded,
                requested,
            } => Some(serde_json::json!({
                FENCING_CURRENT_EPOCH_KEY: recorded,
                FENCING_REQUESTED_EPOCH_KEY: requested,
            })),
            WalError::EpochNotMonotonic {
                recorded,
                requested,
            } => Some(serde_json::json!({
                "epoch_recorded": recorded,
                "epoch_requested": requested,
            })),
            // 幂等冲突必须让调用方看到「键属于哪个区间、本次要求哪个区间」：
            // 只有它能解释「为什么这次重试被判成冲突而不是去重命中」。
            WalError::IdempotencyConflict {
                append_id,
                recorded_start_lsn,
                requested_start_lsn,
            } => Some(serde_json::json!({
                "append_id": append_id,
                "recorded_start_lsn": recorded_start_lsn,
                "requested_start_lsn": requested_start_lsn,
            })),
            WalError::RangeTrimmed {
                start_lsn,
                trimmed_before_lsn,
            } => Some(serde_json::json!({
                "start_lsn": start_lsn,
                "trimmed_before_lsn": trimmed_before_lsn,
            })),
            WalError::NotLeader { leader, .. } => leader.map(|leader| {
                serde_json::json!({
                    "leader_id": leader,
                    // 提示客户端换节点重试：这是 WAL_NOT_LEADER 的既定用法
                    "retry_with_leader": true,
                })
            }),
            _ => None,
        };
        if let Some(detail) = detail {
            err = err.with_detail(detail);
        }
        err
    }

    /// 转成 proto 错误体（gRPC 响应内嵌 `PlatformError` 字段）。
    pub fn to_proto(&self) -> common::PlatformError {
        common::PlatformError::from(self.to_platform_error())
    }
}

impl From<WalError> for PlatformError {
    fn from(err: WalError) -> Self {
        err.to_platform_error()
    }
}

impl From<raft::Error> for WalError {
    fn from(err: raft::Error) -> Self {
        WalError::Raft(err.to_string())
    }
}

impl From<raft_engine::Error> for WalError {
    fn from(err: raft_engine::Error) -> Self {
        WalError::Storage(err.to_string())
    }
}

impl From<prost::DecodeError> for WalError {
    fn from(err: prost::DecodeError) -> Self {
        WalError::Internal(format!("命令解码失败：{err}"))
    }
}

impl From<std::io::Error> for WalError {
    fn from(err: std::io::Error) -> Self {
        WalError::Internal(format!("IO 错误：{err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 错误码映射是跨服务契约：变更必须是有意为之。
    #[test]
    fn error_codes_match_platform_contract() {
        assert_eq!(
            WalError::NotLeader {
                shard: "shard-0".into(),
                leader: Some(2)
            }
            .error_code(),
            ErrorCode::WalNotLeader
        );
        assert_eq!(
            WalError::StaleEpoch {
                recorded: 7,
                requested: 6
            }
            .error_code(),
            ErrorCode::WalAppendRejected
        );
        assert_eq!(
            WalError::AppendTimeout {
                timeout_ms: 5000,
                log_index: Some(9)
            }
            .error_code(),
            ErrorCode::WalNotDurable
        );
        assert_eq!(
            WalError::EpochNotMonotonic {
                recorded: 7,
                requested: 7
            }
            .error_code(),
            ErrorCode::EpochMismatch
        );
        assert_eq!(
            WalError::IdempotencyConflict {
                append_id: "ap-1".into(),
                recorded_start_lsn: 0,
                requested_start_lsn: 8
            }
            .error_code(),
            ErrorCode::IdempotencyConflict,
            "幂等冲突必须保留自己的错误码，不能折叠成 WAL_APPEND_REJECTED（fencing 语义）"
        );
    }

    /// proto 错误体的 code 字符串必须与 domain::ErrorCode::as_str 一致。
    #[test]
    fn proto_error_carries_contract_code() {
        let proto = WalError::NotLeader {
            shard: "shard-0".into(),
            leader: Some(3),
        }
        .to_proto();
        let code = common::ErrorCode::try_from(proto.code).expect("proto 错误码必须可解析");
        assert_eq!(code.as_str_name(), "WAL_NOT_LEADER");
        assert!(proto.retryable, "WAL_NOT_LEADER 必须可重试");
        assert!(proto.detail_json.contains("\"leader_id\":3"));
    }

    #[test]
    fn stale_epoch_is_not_retryable_on_same_replica() {
        // fencing 拒绝是终态语义：同一参数重试不会变好（客户端必须重新拿 epoch），
        // 因此这里显式覆盖了 WAL_APPEND_REJECTED 的默认可重试标记。
        let err = WalError::StaleEpoch {
            recorded: 7,
            requested: 6,
        };
        let proto = err.to_proto();
        assert_eq!(proto.code, common::ErrorCode::WalAppendRejected as i32);
        assert!(!proto.retryable);
        // 字段名两侧统一：current = 服务端已记录值，requested = 请求携带值
        assert!(
            proto
                .detail_json
                .contains(&format!("\"{FENCING_CURRENT_EPOCH_KEY}\":7")),
            "{}",
            proto.detail_json
        );
        assert!(
            proto
                .detail_json
                .contains(&format!("\"{FENCING_REQUESTED_EPOCH_KEY}\":6")),
            "{}",
            proto.detail_json
        );
        // 旧键名不得再出现（它把服务端当前值与请求值写反了）
        assert!(!proto.detail_json.contains("epoch_expected"));
        assert!(!proto.detail_json.contains("epoch_actual"));
    }

    /// 幂等冲突必须原样带出 `IDEMPOTENCY_CONFLICT` 与冲突明细。
    #[test]
    fn idempotency_conflict_carries_code_and_detail() {
        let err = WalError::IdempotencyConflict {
            append_id: "ap-7".into(),
            recorded_start_lsn: 128,
            requested_start_lsn: 4096,
        };
        let proto = err.to_proto();
        let code = common::ErrorCode::try_from(proto.code).expect("proto 错误码必须可解析");
        assert_eq!(code.as_str_name(), "IDEMPOTENCY_CONFLICT");
        assert!(
            !proto.retryable,
            "幂等键复用是终态：重试同样参数永远不会成功"
        );
        assert!(proto.detail_json.contains("\"append_id\":\"ap-7\""));
        assert!(proto.detail_json.contains("\"recorded_start_lsn\":128"));
        assert!(proto.detail_json.contains("\"requested_start_lsn\":4096"));
    }
}
