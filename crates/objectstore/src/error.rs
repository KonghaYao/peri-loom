//! 对象存储错误类型与到 [`domain::error::ErrorCode`] 的映射。
//!
//! 对外所有 `ObjectStore` / snapshot 方法统一返回 [`domain::error::PlatformError`]，
//! 内部用 thiserror 定义 [`StorageError`] 保留语义细节（哪条 key、期望/实际 checksum），
//! 再由 [`StorageError::to_platform_error`] 统一映射错误码与 retryable 语义。

use std::io;
use std::path::Path;

use domain::error::{ErrorCode, PlatformError};

/// 对象存储层语义错误。所有变体都能映射到 [`ErrorCode`]。
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// 对象不存在（S3 `404` / `NotFound` / `NoSuchKey`）。
    #[error("object not found: {key}")]
    NotFound {
        /// 对象 key。
        key: String,
    },

    /// snapshot artifact 损坏 / 无法解析（manifest JSON 非法等）。
    #[error("snapshot artifact is corrupt: {0}")]
    SnapshotCorrupt(String),

    /// 内容校验不一致：sha256、字节数不符，或压缩数据无法解压（等价于损坏）。
    #[error("checksum mismatch for object {key}: expected {expected}, actual {actual}")]
    ChecksumMismatch {
        /// 对象 key。
        key: String,
        /// 期望值（manifest 中记录的 checksum 或大小）。
        expected: String,
        /// 实际值。
        actual: String,
    },

    /// 对象存储不可用：网络、连接、5xx、瞬时本地 IO 故障。可重试。
    #[error("object storage unavailable: {0}")]
    Unavailable(String),

    /// 对象存储拒绝访问（凭证失效 / 权限不足）。
    #[error("object storage permission denied: {0}")]
    PermissionDenied(String),

    /// 调用方参数非法（路径穿越、空 id、源文件不存在、未知压缩算法）。
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// 内部错误（响应缺少必要字段、阻塞任务被取消等编程/环境异常），不可重试。
    #[error("internal object storage error: {0}")]
    Internal(String),
}

impl StorageError {
    /// 由 `std::io::Error` 构造，按错误类型分流。
    ///
    /// IO 失败不都是一个语义：源文件/目录**不存在**是调用方参数问题（不该重试），
    /// 权限错误应暴露为 `PERMISSION_DENIED`，其余（EIO / ENOSPC / 网络文件系统抖动）
    /// 归入可用性故障，允许上层退避重试。
    pub(crate) fn io(path: impl AsRef<Path>, source: io::Error) -> Self {
        let path = path.as_ref().display();
        match source.kind() {
            io::ErrorKind::NotFound => {
                Self::InvalidArgument(format!("path does not exist: {path}"))
            }
            io::ErrorKind::PermissionDenied => {
                Self::PermissionDenied(format!("permission denied on {path}: {source}"))
            }
            _ => Self::Unavailable(format!("io failure on {path}: {source}")),
        }
    }

    /// 映射到 proto ErrorCode（与 platform.common.v1 一一对应）。
    pub fn code(&self) -> ErrorCode {
        match self {
            // 契约：对象不存在 -> SNAPSHOT_UNAVAILABLE。
            StorageError::NotFound { .. } | StorageError::SnapshotCorrupt(_) => {
                ErrorCode::SnapshotUnavailable
            }
            StorageError::ChecksumMismatch { .. } => ErrorCode::ChecksumMismatch,
            StorageError::Unavailable(_) => ErrorCode::StorageUnavailable,
            StorageError::PermissionDenied(_) => ErrorCode::PermissionDenied,
            StorageError::InvalidArgument(_) => ErrorCode::InvalidArgument,
            StorageError::Internal(_) => ErrorCode::InternalError,
        }
    }

    /// 是否可安全重试（不改变请求语义）。
    ///
    /// 只有存储层可用性故障可重试：对象不存在、artifact 损坏、参数错误重试都不会自愈。
    pub fn retryable(&self) -> bool {
        matches!(self, StorageError::Unavailable(_))
    }

    /// 转换为统一错误体。
    pub fn to_platform_error(&self) -> PlatformError {
        let mut err = PlatformError::new(self.code(), self.to_string());
        // domain 认为 SNAPSHOT_UNAVAILABLE / INTERNAL_ERROR 可重试（整体压力模型），
        // 但这里出现的这两种错误要么是 artifact 真的没了/坏了，要么是编程错误，
        // 重试不会自愈，因此显式覆盖 retryable。
        err.retryable = self.retryable();
        err
    }
}

impl From<StorageError> for PlatformError {
    fn from(value: StorageError) -> Self {
        value.to_platform_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_follow_contract() {
        let cases = [
            (
                StorageError::NotFound { key: "k".into() },
                ErrorCode::SnapshotUnavailable,
                false,
            ),
            (
                StorageError::SnapshotCorrupt("bad json".into()),
                ErrorCode::SnapshotUnavailable,
                false,
            ),
            (
                StorageError::ChecksumMismatch {
                    key: "k".into(),
                    expected: "a".into(),
                    actual: "b".into(),
                },
                ErrorCode::ChecksumMismatch,
                false,
            ),
            (
                StorageError::Unavailable("conn reset".into()),
                ErrorCode::StorageUnavailable,
                true,
            ),
            (
                StorageError::PermissionDenied("access denied".into()),
                ErrorCode::PermissionDenied,
                false,
            ),
            (
                StorageError::InvalidArgument("empty id".into()),
                ErrorCode::InvalidArgument,
                false,
            ),
            (
                StorageError::Internal("broken".into()),
                ErrorCode::InternalError,
                false,
            ),
        ];
        for (err, code, retryable) in cases {
            assert_eq!(err.code(), code, "错误码映射不符: {err}");
            assert_eq!(err.retryable(), retryable, "retryable 判定不符: {err}");
            let platform = err.to_platform_error();
            assert_eq!(platform.code, code);
            assert_eq!(platform.retryable, retryable);
            // 错误体是可序列化的（HTTP 统一错误体），且不得泄漏 detail。
            let json = serde_json::to_value(&platform).expect("PlatformError 可序列化");
            assert_eq!(json["code"], code.as_str());
        }
    }

    #[test]
    fn io_errors_are_classified_by_kind() {
        let missing = StorageError::io("a.txt", io::Error::from(io::ErrorKind::NotFound));
        assert_eq!(missing.code(), ErrorCode::InvalidArgument);

        let denied = StorageError::io("a.txt", io::Error::from(io::ErrorKind::PermissionDenied));
        assert_eq!(denied.code(), ErrorCode::PermissionDenied);

        let other = StorageError::io("a.txt", io::Error::from(io::ErrorKind::Other));
        assert_eq!(other.code(), ErrorCode::StorageUnavailable);
        assert!(other.retryable());
    }
}
