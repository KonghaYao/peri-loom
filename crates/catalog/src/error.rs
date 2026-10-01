//! Catalog 错误类型与 PostgreSQL 错误映射。
//!
//! 对外所有 Catalog 方法统一返回 [`domain::error::PlatformError`]（错误码与 proto
//! `platform.common.v1.ErrorCode` 一一对应），内部用 thiserror 定义 [`CatalogError`]
//! 保留语义细节，再由 [`CatalogError::to_platform_error`] 统一映射。

use domain::error::{ErrorCode, PlatformError};

/// 行不存在（RowNotFound）时映射的目标错误码。
///
/// proto 的 ErrorCode 没有通用 `NOT_FOUND`：数据库用 `DB_NOT_FOUND`，Worker 用
/// `WORKER_UNAVAILABLE`（Worker 不在 inventory 中即不可用），Snapshot 用
/// `SNAPSHOT_UNAVAILABLE`；其余控制面实体（Operation/Job/User/Token）退化为
/// `INVALID_ARGUMENT` —— 调用方传入的 id 不存在，属于请求参数问题。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NotFoundAs {
    Database,
    Worker,
    Operation,
    Job,
    User,
    Token,
    Snapshot,
}

impl NotFoundAs {
    pub(crate) fn code(self) -> ErrorCode {
        match self {
            NotFoundAs::Database => ErrorCode::DbNotFound,
            NotFoundAs::Worker => ErrorCode::WorkerUnavailable,
            NotFoundAs::Operation => ErrorCode::InvalidArgument,
            NotFoundAs::Job => ErrorCode::InvalidArgument,
            NotFoundAs::User => ErrorCode::InvalidArgument,
            NotFoundAs::Token => ErrorCode::Unauthenticated,
            NotFoundAs::Snapshot => ErrorCode::SnapshotUnavailable,
        }
    }

    pub(crate) fn entity(self) -> &'static str {
        match self {
            NotFoundAs::Database => "database",
            NotFoundAs::Worker => "worker",
            NotFoundAs::Operation => "operation",
            NotFoundAs::Job => "job",
            NotFoundAs::User => "user",
            NotFoundAs::Token => "api token",
            NotFoundAs::Snapshot => "snapshot",
        }
    }
}

/// 唯一约束冲突（SQLSTATE 23505）时映射的目标错误码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConflictAs {
    /// (tenant_id, name) 冲突
    Database,
    /// idempotency_keys.key / jobs.idempotency_key 冲突
    Idempotency,
    /// users.username / users.oidc_subject 冲突
    User,
    /// api_tokens.token_hash 冲突
    ApiToken,
    /// snapshots.id 冲突
    Snapshot,
}

impl ConflictAs {
    pub(crate) fn code(self) -> ErrorCode {
        match self {
            ConflictAs::Database => ErrorCode::DbAlreadyExists,
            ConflictAs::Idempotency => ErrorCode::IdempotencyConflict,
            ConflictAs::User => ErrorCode::InvalidArgument,
            ConflictAs::ApiToken => ErrorCode::InvalidArgument,
            ConflictAs::Snapshot => ErrorCode::InvalidArgument,
        }
    }

    pub(crate) fn entity(self) -> &'static str {
        match self {
            ConflictAs::Database => "database",
            ConflictAs::Idempotency => "idempotency key",
            ConflictAs::User => "user",
            ConflictAs::ApiToken => "api token",
            ConflictAs::Snapshot => "snapshot",
        }
    }
}

/// Catalog 语义错误。所有变体都能映射到 [`ErrorCode`]。
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    #[error("database already exists: tenant={tenant_id} name={name}")]
    DatabaseAlreadyExists { tenant_id: String, name: String },

    #[error("worker not found: {0}")]
    WorkerNotFound(String),

    #[error("operation not found: {0}")]
    OperationNotFound(String),

    #[error("job not found: {0}")]
    JobNotFound(String),

    #[error("user not found: {0}")]
    UserNotFound(String),

    #[error("token not found: {0}")]
    TokenNotFound(String),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    /// Split Brain 防护的关键错误：epoch 不等于 Catalog 当前值，说明调用方持有过期所有权。
    #[error("owner epoch mismatch: expected={expected} actual={actual}")]
    EpochMismatch { expected: u64, actual: u64 },

    /// 同一 Idempotency-Key 携带了不同的请求体。
    #[error("idempotency key conflict: key={key}")]
    IdempotencyConflict { key: String },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("check constraint violated: {constraint}")]
    ConstraintViolation { constraint: String },

    #[error("foreign key violated: {constraint}")]
    ForeignKeyViolation { constraint: String },

    /// 连接 / 池 / 网络类故障：可安全重试。
    #[error("catalog unavailable: {0}")]
    Unavailable(String),

    #[error("internal catalog error: {0}")]
    Internal(String),

    #[error("migration failed: {0}")]
    Migration(String),
}

impl CatalogError {
    /// 映射到 proto ErrorCode。
    pub fn code(&self) -> ErrorCode {
        match self {
            CatalogError::DatabaseNotFound(_) => ErrorCode::DbNotFound,
            CatalogError::DatabaseAlreadyExists { .. } => ErrorCode::DbAlreadyExists,
            CatalogError::WorkerNotFound(_) => ErrorCode::WorkerUnavailable,
            CatalogError::OperationNotFound(_)
            | CatalogError::JobNotFound(_)
            | CatalogError::UserNotFound(_)
            | CatalogError::InvalidArgument(_) => ErrorCode::InvalidArgument,
            CatalogError::TokenNotFound(_) => ErrorCode::Unauthenticated,
            CatalogError::SnapshotNotFound(_) => ErrorCode::SnapshotUnavailable,
            CatalogError::EpochMismatch { .. } => ErrorCode::EpochMismatch,
            CatalogError::IdempotencyConflict { .. } => ErrorCode::IdempotencyConflict,
            CatalogError::ConstraintViolation { .. } | CatalogError::ForeignKeyViolation { .. } => {
                ErrorCode::ConstraintViolation
            }
            CatalogError::Unavailable(_)
            | CatalogError::Internal(_)
            | CatalogError::Migration(_) => ErrorCode::InternalError,
        }
    }

    /// 只有连接类故障才允许调用方直接重试（不改变语义）。
    pub fn retryable(&self) -> bool {
        matches!(self, CatalogError::Unavailable(_))
    }

    pub fn to_platform_error(&self) -> PlatformError {
        let mut err = PlatformError::new(self.code(), self.to_string());
        err.retryable = self.retryable();
        err
    }
}

impl From<CatalogError> for PlatformError {
    fn from(value: CatalogError) -> Self {
        value.to_platform_error()
    }
}

/// 构造「不可重试」的 PlatformError。
///
/// domain 的 `ErrorCode::retryable()` 把 INTERNAL_ERROR 视为可重试（整体压力模型），
/// 但 Catalog 侧出现 INTERNAL_ERROR 基本都是解码 / 协议 / 编程错误，重试不会自愈；
/// 因此这里显式置 false，可重试场景必须走 [`platform_error_retryable`] 单独声明。
pub(crate) fn platform_error(code: ErrorCode, message: impl Into<String>) -> PlatformError {
    let mut err = PlatformError::new(code, message);
    err.retryable = false;
    err
}

/// 可重试错误：仅用于连接 / 池 / 网络故障，业务语义错误一律不可重试。
pub(crate) fn platform_error_retryable(
    code: ErrorCode,
    message: impl Into<String>,
) -> PlatformError {
    let mut err = PlatformError::new(code, message);
    err.retryable = true;
    err
}

pub(crate) fn catalog_error(err: CatalogError) -> PlatformError {
    err.to_platform_error()
}

/// `sqlx::Error` -> `PlatformError` 的分类映射。
///
/// 分类依据是 SQLSTATE：唯一冲突 / 外键 / CHECK / 序列化失败 / 连接类故障分别对应不同
/// 的错误码，避免把可重试的故障和调用方参数错误混为一谈。
pub(crate) fn map_sqlx_error(
    err: sqlx::Error,
    not_found: NotFoundAs,
    conflict: ConflictAs,
) -> PlatformError {
    match &err {
        sqlx::Error::RowNotFound => platform_error(
            not_found.code(),
            format!("{} not found", not_found.entity()),
        ),
        sqlx::Error::Database(db) => {
            let sqlstate = db.code().unwrap_or_default().to_string();
            let constraint = db.constraint().unwrap_or_default().to_string();
            match sqlstate.as_str() {
                "23505" => platform_error(
                    conflict.code(),
                    format!(
                        "{} already exists (constraint={})",
                        conflict.entity(),
                        constraint
                    ),
                ),
                "23503" => {
                    // Worker 外键缺失等价于 Worker 不可用
                    if constraint.contains("worker") {
                        platform_error(
                            ErrorCode::WorkerUnavailable,
                            format!("referenced worker does not exist (constraint={constraint})"),
                        )
                    } else {
                        platform_error(
                            ErrorCode::InvalidArgument,
                            format!("referenced row does not exist (constraint={constraint})"),
                        )
                    }
                }
                "23514" => platform_error(
                    ErrorCode::ConstraintViolation,
                    format!("check constraint violated (constraint={constraint})"),
                ),
                "23502" => platform_error(
                    ErrorCode::InvalidArgument,
                    format!("not null constraint violated (constraint={constraint})"),
                ),
                // 序列化失败 / 死锁：重试可解
                "40001" | "40P01" => platform_error_retryable(
                    ErrorCode::InternalError,
                    format!("transaction conflict, retryable (sqlstate={sqlstate})"),
                ),
                // statement timeout
                "57014" => {
                    platform_error(ErrorCode::DeadlineExceeded, "query cancelled by timeout")
                }
                _ => platform_error(
                    ErrorCode::InternalError,
                    format!("postgres error sqlstate={sqlstate}: {}", db.message()),
                ),
            }
        }
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => platform_error_retryable(
            ErrorCode::InternalError,
            format!("catalog connection pool unavailable: {err}"),
        ),
        sqlx::Error::Io(io) => platform_error_retryable(
            ErrorCode::StorageUnavailable,
            format!("catalog io error: {io}"),
        ),
        _ => platform_error(ErrorCode::InternalError, format!("catalog error: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;

    /// 无真实数据库即可验证 SQLSTATE 分类的假 PgDatabaseError。
    #[derive(Debug)]
    struct FakeDbError {
        code: &'static str,
        constraint: Option<&'static str>,
    }

    impl fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "fake db error {}", self.code)
        }
    }

    impl StdError for FakeDbError {}

    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            "fake"
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }
        fn constraint(&self) -> Option<&str> {
            self.constraint
        }
        fn table(&self) -> Option<&str> {
            None
        }
        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }
    }

    fn fake_err(code: &'static str, constraint: Option<&'static str>) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError { code, constraint }))
    }

    #[test]
    fn row_not_found_maps_to_entity_specific_code() {
        let e = map_sqlx_error(
            sqlx::Error::RowNotFound,
            NotFoundAs::Database,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::DbNotFound);
        assert!(!e.retryable);

        let e = map_sqlx_error(
            sqlx::Error::RowNotFound,
            NotFoundAs::Worker,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::WorkerUnavailable);

        let e = map_sqlx_error(
            sqlx::Error::RowNotFound,
            NotFoundAs::Snapshot,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::SnapshotUnavailable);
    }

    #[test]
    fn unique_violation_maps_to_conflict_kind() {
        let e = map_sqlx_error(
            fake_err("23505", Some("databases_tenant_id_name_key")),
            NotFoundAs::Database,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::DbAlreadyExists);

        let e = map_sqlx_error(
            fake_err("23505", Some("idempotency_keys_pkey")),
            NotFoundAs::Database,
            ConflictAs::Idempotency,
        );
        assert_eq!(e.code, ErrorCode::IdempotencyConflict);
    }

    #[test]
    fn foreign_key_and_check_violations_map_to_argument_errors() {
        let e = map_sqlx_error(
            fake_err("23503", Some("databases_owner_worker_id_fkey")),
            NotFoundAs::Database,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::WorkerUnavailable);

        let e = map_sqlx_error(
            fake_err("23503", Some("databases_tenant_id_fkey")),
            NotFoundAs::Database,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::InvalidArgument);

        let e = map_sqlx_error(
            fake_err("23514", Some("databases_state_check")),
            NotFoundAs::Database,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::ConstraintViolation);
    }

    #[test]
    fn connection_failures_are_retryable_and_others_are_not() {
        let e = map_sqlx_error(
            sqlx::Error::PoolTimedOut,
            NotFoundAs::Database,
            ConflictAs::Database,
        );
        assert!(e.retryable);

        let e = map_sqlx_error(
            sqlx::Error::Protocol("bad".into()),
            NotFoundAs::Database,
            ConflictAs::Database,
        );
        assert_eq!(e.code, ErrorCode::InternalError);
        assert!(!e.retryable);
    }

    #[test]
    fn catalog_error_codes_are_stable() {
        assert_eq!(
            CatalogError::EpochMismatch {
                expected: 1,
                actual: 2
            }
            .code(),
            ErrorCode::EpochMismatch
        );
        assert_eq!(
            CatalogError::IdempotencyConflict { key: "k".into() }.code(),
            ErrorCode::IdempotencyConflict
        );
        assert_eq!(
            CatalogError::DatabaseNotFound("d".into()).code(),
            ErrorCode::DbNotFound
        );
        assert!(CatalogError::Unavailable("x".into()).retryable());
        assert!(!CatalogError::Internal("x".into()).retryable());

        // 错误码字符串必须与 proto 一致（对外 HTTP 错误体）
        assert_eq!(
            CatalogError::EpochMismatch {
                expected: 1,
                actual: 2
            }
            .code()
            .as_str(),
            "EPOCH_MISMATCH"
        );
    }

    #[test]
    fn not_found_and_conflict_codes_cover_all_entities() {
        for k in [
            NotFoundAs::Database,
            NotFoundAs::Worker,
            NotFoundAs::Operation,
            NotFoundAs::Job,
            NotFoundAs::User,
            NotFoundAs::Token,
            NotFoundAs::Snapshot,
        ] {
            assert!(!k.entity().is_empty());
        }
        for k in [
            ConflictAs::Database,
            ConflictAs::Idempotency,
            ConflictAs::User,
            ConflictAs::ApiToken,
            ConflictAs::Snapshot,
        ] {
            assert!(!k.entity().is_empty());
        }
    }
}
