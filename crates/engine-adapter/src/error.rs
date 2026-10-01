//! 引擎错误 → 平台错误码映射（架构 §17.4 统一错误体）。
//!
//! 对外错误必须映射到 [`domain::error::ErrorCode`]：调用方（db-runtime / Server / Worker）
//! 只理解平台错误码，不理解 `turso_core::LimboError` 的 60 多个变体。
//!
//! 这里有一条容易写错的语义：**durability 失败必须能被区分出来**。
//! 引擎只回传 `LimboError`，原始的平台错误在穿越引擎时丢失了，因此
//! [`crate::durable`] 会把「远程 WAL 未 durable」编码成特定的
//! [`turso_core::CompletionError::IOError`] 标签；本模块再按标签还原成正确的错误码。
//! 标签是 crate 内部契约（`pub(crate)`），不是对外 API。

use domain::error::{ErrorCode, PlatformError};
use turso_core::{CompletionError, LimboError};

/// 远程 WAL 未拿到 quorum durable 确认（可重试语义由平台外部决定，见 `ErrorCode::WalNotDurable`）。
pub(crate) const LABEL_WAL_NOT_DURABLE: &str = "platform: remote wal append not durable";
/// 远程 WAL 确定性拒绝（fencing / 幂等冲突 / 参数非法）：重试不会成功。
pub(crate) const LABEL_WAL_REJECTED: &str = "platform: remote wal append rejected";
/// durable IO 已进入 fail-stop（本进程不能再提交，必须重启 / 故障转移）。
pub(crate) const LABEL_DURABLE_STOPPED: &str = "platform: durable io stopped";
/// WAL 字节流不符合 SQLite/Turso 帧格式，无法证明 durability。
pub(crate) const LABEL_WAL_FORMAT: &str = "platform: wal byte format unsupported";

/// durability 失败标记：WAL 写入路径上一律以该标签终止 parent completion。
///
/// 语义与 [`LABEL_DURABLE_STOPPED`] 相同（durable IO 已 fail-stop，本进程不能再提交，
/// 必须重启 / 故障转移），这里只是给「写入被终止」这件事一个自解释的名字 ——
/// 复用同一个字符串而不是另写一份字面量，映射表里也就不会出现「同名不同码」。
pub(crate) const DURABLE_FAILURE_MARKER: &str = LABEL_DURABLE_STOPPED;

/// durability 相关标签全集，供测试与诊断使用。
pub const DURABILITY_ERROR_LABELS: [&str; 4] = [
    LABEL_WAL_NOT_DURABLE,
    LABEL_WAL_REJECTED,
    LABEL_DURABLE_STOPPED,
    LABEL_WAL_FORMAT,
];

/// 把引擎错误映射为平台错误。
///
/// `message` 保留引擎原始信息（排障需要），但**不得**包含 secret：引擎错误串来自
/// SQL 与存储层，本 crate 不会向其中注入连接串 / 凭据。
pub fn map_engine_error(err: &LimboError) -> PlatformError {
    let code = engine_error_code(err);
    let message = match code {
        ErrorCode::WalNotDurable => format!(
            "引擎写入失败：Remote WAL 未确认 quorum durable，禁止向客户端返回 Commit Success（{err}）"
        ),
        ErrorCode::WalAppendRejected => {
            format!("引擎写入被 Remote WAL 拒绝（多数为 owner epoch 过期或幂等冲突）：{err}")
        }
        ErrorCode::ConstraintViolation => format!("约束冲突：{err}"),
        _ => err.to_string(),
    };
    PlatformError::new(code, message)
}

/// 引擎错误 → 平台错误码（纯函数，便于单测与排障）。
pub fn engine_error_code(err: &LimboError) -> ErrorCode {
    match err {
        // ---- SQL 解析 ----
        LimboError::ParseError(_) | LimboError::LexerError(_) => ErrorCode::SqlParseError,

        // ---- 约束：整类归到 ConstraintViolation，调用方可据此返回 409 ----
        LimboError::Constraint(_) | LimboError::ForeignKeyConstraint(_) => {
            ErrorCode::ConstraintViolation
        }

        // ---- 取消 / 中断（架构 §15.6 Cancel）----
        LimboError::Interrupt => ErrorCode::Cancelled,

        // ---- 资源 ----
        LimboError::DatabaseFull | LimboError::OutOfMemory | LimboError::TooBig => {
            ErrorCode::ResourceExhausted
        }

        // ---- 存储层：本地文件不可用 / 损坏，属于存储面故障 ----
        LimboError::Corrupt(_) | LimboError::NotADB | LimboError::IoBackendUnavailable(_) => {
            ErrorCode::StorageUnavailable
        }

        // ---- durable IO：durability 契约的破坏点，必须精确还原 ----
        LimboError::CompletionError(err) => completion_error_code(err),

        // ---- 事务状态 ----
        // `StatementsInProgress` 是「同一连接上还有未结束语句就发起事务控制」，属于调用方
        // 违反事务状态机；`BusySnapshot` 需要在同一事务内回滚重试，归到 SqlError 更贴近现象。
        LimboError::StatementsInProgress(_) => ErrorCode::TransactionStateInvalid,
        LimboError::TxTerminated | LimboError::TxError(_) => ErrorCode::TransactionLost,

        LimboError::InvalidArgument(_) => ErrorCode::InvalidArgument,

        // 引擎内部错误：不得伪装成 SQL 错误，否则会掩盖 bug。
        LimboError::InternalError(_) => ErrorCode::InternalError,

        // ---- 其余全部按 SQL 执行错误上报（带原始 message）----
        _ => ErrorCode::SqlError,
    }
}

/// 该错误是否指示 durability 契约被破坏（db-runtime 据此决定杀进程 / 故障转移）。
pub fn is_durability_failure(err: &LimboError) -> bool {
    matches!(
        engine_error_code(err),
        ErrorCode::WalNotDurable | ErrorCode::WalAppendRejected | ErrorCode::StorageUnavailable
    )
}

fn completion_error_code(err: &CompletionError) -> ErrorCode {
    match err {
        CompletionError::IOError(_, op) => match *op {
            LABEL_WAL_NOT_DURABLE => ErrorCode::WalNotDurable,
            LABEL_WAL_REJECTED => ErrorCode::WalAppendRejected,
            LABEL_DURABLE_STOPPED | LABEL_WAL_FORMAT => ErrorCode::StorageUnavailable,
            // 普通本地 IO 错误：本地 NVMe 是工作集，坏掉同样属于存储面不可用
            _ => ErrorCode::StorageUnavailable,
        },
        CompletionError::Aborted => ErrorCode::Cancelled,
        // 短读 / 校验和 / 页不匹配：数据或介质出问题，不能当 SQL 错误糊过去
        _ => ErrorCode::StorageUnavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraint_maps_to_constraint_violation() {
        let err = LimboError::Constraint("UNIQUE constraint failed: t.id".into());
        assert_eq!(engine_error_code(&err), ErrorCode::ConstraintViolation);
    }

    #[test]
    fn parse_error_maps_to_sql_parse_error() {
        assert_eq!(
            engine_error_code(&LimboError::ParseError("near \"SELEC\"".into())),
            ErrorCode::SqlParseError
        );
    }

    #[test]
    fn durability_labels_survive_round_trip() {
        for (label, expected) in [
            (LABEL_WAL_NOT_DURABLE, ErrorCode::WalNotDurable),
            (LABEL_WAL_REJECTED, ErrorCode::WalAppendRejected),
            (LABEL_DURABLE_STOPPED, ErrorCode::StorageUnavailable),
            (LABEL_WAL_FORMAT, ErrorCode::StorageUnavailable),
        ] {
            let err = LimboError::CompletionError(CompletionError::IOError(
                std::io::ErrorKind::Other,
                label,
            ));
            assert_eq!(engine_error_code(&err), expected, "标签 {label} 映射错误");
            assert!(
                is_durability_failure(&err),
                "标签 {label} 必须被识别为 durability 失败"
            );
        }
    }

    #[test]
    fn unknown_sql_error_stays_sql_error_with_original_message() {
        let err = LimboError::SqlError("no such table: nope".into());
        let mapped = map_engine_error(&err);
        assert_eq!(mapped.code, ErrorCode::SqlError);
        assert!(mapped.message.contains("no such table: nope"));
    }

    #[test]
    fn interrupt_maps_to_cancelled() {
        assert_eq!(
            engine_error_code(&LimboError::Interrupt),
            ErrorCode::Cancelled
        );
    }
}
