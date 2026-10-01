//! Operations：长操作跟踪（架构 §17.4 的 `202 Accepted + operation_id` 语义）。
//!
//! operation 是「异步副作用」的对外句柄，必须可查询、可幂等、可审计，
//! 因此写入都带 idempotency_key 唯一约束与状态机约束（DB 侧 CHECK 兜底）。

use domain::error::{ErrorCode, Result};
use domain::ids::{DatabaseId, OperationId, UserId, WorkerId};
use domain::records::OperationRecord;

use crate::error::{
    catalog_error, map_sqlx_error, platform_error, CatalogError, ConflictAs, NotFoundAs,
};
use crate::pg::{invalid_argument, OPERATION_COLUMNS};
use crate::Catalog;

/// `operations.kind` 的合法取值（与 migrations 的 CHECK 完全一致）。
pub const OPERATION_KINDS: [&str; 11] = [
    "CREATE_DB",
    "DELETE_DB",
    "START_DB",
    "STOP_DB",
    "RESTART_DB",
    "MOVE_DB",
    "BACKUP_DB",
    "RESTORE_DB",
    "SNAPSHOT_DB",
    "DRAIN_WORKER",
    "CREATE_TOKEN",
];

/// `operations.state` 的合法取值。
pub const OPERATION_STATES: [&str; 5] = ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"];

pub fn is_valid_operation_kind(kind: &str) -> bool {
    OPERATION_KINDS.contains(&kind)
}

pub fn is_valid_operation_state(state: &str) -> bool {
    OPERATION_STATES.contains(&state)
}

/// 终态：进入后不可再变更（幂等重放时用于判定是否已完成）。
pub fn is_terminal_operation_state(state: &str) -> bool {
    matches!(state, "SUCCEEDED" | "FAILED" | "CANCELLED")
}

/// 待创建的 Operation 描述（**不含 id**）。
///
/// id 由调用方在写库前统一分配，这样 `idempotency_keys.operation_id` 与
/// `operations.id` 天然指向同一行 —— 见 [`Catalog::begin_idempotent_operation`]：
/// 幂等记录与 Operation 必须在同一个事务里落库，不能靠「先提交幂等键、再去建 Operation」
/// 两步走（中间崩溃会留下「重放命中但 Operation 根本不存在」的死记录）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOperation {
    pub kind: String,
    pub database_id: Option<DatabaseId>,
    pub worker_id: Option<WorkerId>,
    pub requested_by: Option<UserId>,
}

impl NewOperation {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            database_id: None,
            worker_id: None,
            requested_by: None,
        }
    }

    pub fn with_database(mut self, database_id: DatabaseId) -> Self {
        self.database_id = Some(database_id);
        self
    }

    pub fn with_worker(mut self, worker_id: WorkerId) -> Self {
        self.worker_id = Some(worker_id);
        self
    }

    pub fn with_requested_by(mut self, requested_by: UserId) -> Self {
        self.requested_by = Some(requested_by);
        self
    }

    /// 写库前的参数校验（kind 白名单与 migration 的 CHECK 一致）。
    pub fn validate(&self) -> Result<()> {
        if !is_valid_operation_kind(&self.kind) {
            return Err(invalid_argument(format!(
                "unknown operation kind '{}', expected one of {OPERATION_KINDS:?}",
                self.kind
            )));
        }
        Ok(())
    }
}

/// 插入 operations 行，返回新建的行；`Ok(None)` 表示同 `idempotency_key` 已存在。
///
/// 单独抽成函数是为了让调用方把 executor 传成 `&mut *tx`：幂等组合入口必须把
/// `idempotency_keys` 与 `operations` 写在**同一个事务**里（FIX-B），
/// 任何一步失败都整事务回滚，不会留下半截状态。
///
/// `ON CONFLICT DO NOTHING` 对应 migration 里
/// `operations_idempotency_idx (idempotency_key) WHERE idempotency_key IS NOT NULL`：
/// 「同 key 只能有 1 个 Operation」由 DB 兜底，冲突时返回 `None` 让调用方回读已存在的行
/// （自愈），而不是把 23505 抛成对用户永久可见的错误。
pub(crate) async fn insert_operation<'e, E>(
    executor: E,
    operation_id: OperationId,
    spec: &NewOperation,
    idempotency_key: Option<&str>,
) -> Result<Option<OperationRecord>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = format!(
        "INSERT INTO operations
             (id, kind, state, database_id, worker_id, tenant_id, requested_by, idempotency_key)
         VALUES (
             $1::uuid, $2::text, 'PENDING', $3::uuid, $4::text,
             (SELECT tenant_id FROM databases WHERE id = $3::uuid),
             $5::uuid, $6::text
         )
         ON CONFLICT DO NOTHING
         RETURNING {OPERATION_COLUMNS}"
    );
    let db_uuid = match spec.database_id {
        Some(id) => Some(crate::pg::id_to_uuid(&id)?),
        None => None,
    };
    let row = sqlx::query_as::<_, crate::pg::OperationRow>(&sql)
        .bind(crate::pg::id_to_uuid(&operation_id)?)
        .bind(&spec.kind)
        .bind(db_uuid)
        .bind(spec.worker_id.as_ref().map(|w| w.to_string()))
        .bind(match &spec.requested_by {
            Some(u) => Some(crate::pg::id_to_uuid(u)?),
            None => None,
        })
        .bind(idempotency_key)
        .fetch_optional(executor)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Idempotency))?;
    Ok(row.map(|r| r.0))
}

/// 按 `idempotency_key` 回读已存在的行（幂等自愈路径）。
pub(crate) async fn find_operation_by_idempotency_key<'e, E>(
    executor: E,
    idempotency_key: &str,
) -> Result<Option<OperationRecord>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql =
        format!("SELECT {OPERATION_COLUMNS} FROM operations WHERE idempotency_key = $1::text");
    let row = sqlx::query_as::<_, crate::pg::OperationRow>(&sql)
        .bind(idempotency_key)
        .fetch_optional(executor)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Idempotency))?;
    Ok(row.map(|r| r.0))
}

impl Catalog {
    /// 创建 operation（自动分配 v7 id）。
    #[tracing::instrument(skip(self), fields(kind, database_id = ?database_id))]
    pub async fn create_operation(
        &self,
        kind: &str,
        database_id: Option<DatabaseId>,
        worker_id: Option<WorkerId>,
        requested_by: Option<UserId>,
        idempotency_key: Option<&str>,
    ) -> Result<OperationRecord> {
        self.create_operation_with_id(
            OperationId::new_v7(),
            kind,
            database_id,
            worker_id,
            requested_by,
            idempotency_key,
        )
        .await
    }

    /// 使用调用方指定的 id 创建 operation（幂等流程使用预分配 id）。
    ///
    /// tenant_id 由 database 反查后写入（单条 INSERT ... SELECT，无需额外事务），
    /// 保证审计记录里 operation 与 tenant 的归属一致。
    ///
    /// 同 `idempotency_key` 已存在时返回 `IDEMPOTENCY_CONFLICT`（唯一性的判定在 DB 侧完成）。
    /// 需要「幂等记录 + Operation 同事务」的调用方请用
    /// [`Catalog::begin_idempotent_operation`]。
    pub async fn create_operation_with_id(
        &self,
        operation_id: OperationId,
        kind: &str,
        database_id: Option<DatabaseId>,
        worker_id: Option<WorkerId>,
        requested_by: Option<UserId>,
        idempotency_key: Option<&str>,
    ) -> Result<OperationRecord> {
        let spec = NewOperation {
            kind: kind.to_string(),
            database_id,
            worker_id,
            requested_by,
        };
        spec.validate()?;
        let created = insert_operation(self.pool(), operation_id, &spec, idempotency_key).await?;
        created.ok_or_else(|| {
            // `ON CONFLICT DO NOTHING` 把唯一冲突（idempotency_key 部分索引 / 主键）变成了 None，
            // 这里还原成对调用方可见的冲突错误
            let detail = match idempotency_key {
                Some(key) => format!("idempotency_key '{key}' 已经绑定到另一个 operation"),
                None => format!("operation {operation_id} 已存在"),
            };
            platform_error(ErrorCode::IdempotencyConflict, detail)
        })
    }

    /// 推进 operation 状态。进入终态时补 finished_at。
    ///
    /// `error` 为 None 表示清除历史错误（成功路径），保证「成功但残留 error_message」
    /// 这种自相矛盾的状态不会出现在对外查询里。
    #[tracing::instrument(skip(self, error, result), fields(operation_id = %id, state))]
    pub async fn update_operation(
        &self,
        id: OperationId,
        state: &str,
        progress: i16,
        error: Option<(ErrorCode, String)>,
        result: serde_json::Value,
    ) -> Result<OperationRecord> {
        if !is_valid_operation_state(state) {
            return Err(invalid_argument(format!(
                "unknown operation state '{state}', expected one of {OPERATION_STATES:?}"
            )));
        }
        if !(0..=100).contains(&progress) {
            return Err(invalid_argument(format!(
                "operation progress 必须在 0..=100，收到 {progress}"
            )));
        }

        let (error_code, error_message) = match error {
            Some((code, message)) => (Some(code.as_str().to_string()), Some(message)),
            None => (None, None),
        };
        let sql = format!(
            "UPDATE operations SET
                 state = $2::text,
                 progress = $3::smallint,
                 error_code = $4::text,
                 error_message = $5::text,
                 result = $6::jsonb,
                 finished_at = CASE WHEN $2::text IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                                    THEN now() ELSE finished_at END,
                 updated_at = now()
             WHERE id = $1::uuid
             RETURNING {OPERATION_COLUMNS}"
        );
        let row = sqlx::query_as::<_, crate::pg::OperationRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .bind(state)
            .bind(progress)
            .bind(error_code)
            .bind(error_message)
            .bind(result)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Database))?
            .ok_or_else(|| catalog_error(CatalogError::OperationNotFound(id.to_string())))?;
        Ok(row.0)
    }

    pub async fn get_operation(&self, id: OperationId) -> Result<OperationRecord> {
        let sql = format!("SELECT {OPERATION_COLUMNS} FROM operations WHERE id = $1::uuid");
        let row = sqlx::query_as::<_, crate::pg::OperationRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Database))?
            .ok_or_else(|| catalog_error(CatalogError::OperationNotFound(id.to_string())))?;
        Ok(row.0)
    }

    pub async fn list_operations(&self, limit: i64, offset: i64) -> Result<Vec<OperationRecord>> {
        let sql = format!(
            "SELECT {OPERATION_COLUMNS} FROM operations
             ORDER BY created_at DESC LIMIT $1::bigint OFFSET $2::bigint"
        );
        let rows = sqlx::query_as::<_, crate::pg::OperationRow>(&sql)
            .bind(limit.clamp(1, 1000))
            .bind(offset.max(0))
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Operation, ConflictAs::Database))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_kind_whitelist_matches_schema_check() {
        assert!(is_valid_operation_kind("CREATE_DB"));
        assert!(is_valid_operation_kind("DRAIN_WORKER"));
        assert!(is_valid_operation_kind("CREATE_TOKEN"));
        assert!(!is_valid_operation_kind("create_db"));
        assert!(!is_valid_operation_kind("DROP_DB"));
        assert_eq!(OPERATION_KINDS.len(), 11);
    }

    #[test]
    fn operation_state_validation_and_terminality() {
        assert!(is_valid_operation_state("PENDING"));
        assert!(is_valid_operation_state("RUNNING"));
        assert!(!is_valid_operation_state("SUCCESS"));
        assert!(is_terminal_operation_state("SUCCEEDED"));
        assert!(is_terminal_operation_state("FAILED"));
        assert!(is_terminal_operation_state("CANCELLED"));
        assert!(!is_terminal_operation_state("RUNNING"));
    }
}
