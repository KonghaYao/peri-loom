//! PostgreSQL 行映射与 SQL 组装工具。
//!
//! domain 的 records 不依赖 sqlx，因此这里为每个 domain 记录定义本地行包装类型
//! （newtype + 手写 `FromRow`），把「列 -> 领域类型」的转换集中在一处，避免散落在各
//! 个查询里。

use std::fmt::Display;
use std::str::FromStr;

use domain::error::{ErrorCode, Result};
use domain::ids::{DatabaseId, SnapshotId, TenantId, UserId, WorkerId};
use domain::lifecycle::{LifecycleState, WorkerState};
use domain::records::{DatabaseRecord, JobRecord, OperationRecord, SnapshotRecord, WorkerRecord};
use domain::wal::{Lsn, OwnerEpoch};
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Postgres, Row};
use uuid::Uuid;

use crate::error::platform_error;

// ------------------------------------------------------------------ 转换工具

/// domain id -> PostgreSQL uuid。id 均为 UUID newtype，解析失败说明调用方传错类型，
/// 不做静默降级。
pub(crate) fn id_to_uuid<T: Display>(id: &T) -> Result<Uuid> {
    Uuid::parse_str(&id.to_string()).map_err(|e| {
        platform_error(
            ErrorCode::InvalidArgument,
            format!("invalid uuid id {id}: {e}"),
        )
    })
}

/// 反序列化失败（列类型/取值与领域类型不匹配）。
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct DecodeError(String);

pub(crate) fn decode_err(msg: impl Into<String>) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(DecodeError(msg.into())))
}

/// Duration -> 毫秒。SQL 侧统一用 `now() + N * INTERVAL '1 millisecond'`，
/// 避免 INTERVAL 绑定类型歧义，也让 TTL 语义在 Rust 与 SQL 之间保持一致。
pub(crate) fn millis(ttl: std::time::Duration) -> i64 {
    i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX)
}

/// u64 -> i64 饱和转换：容量 / epoch 数值不可能接近 i64 上限，溢出意味着上游有 bug。
pub(crate) fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// 读取列（列名寻址，配合显式 SELECT 列清单）。
pub(crate) fn col<'r, T>(row: &'r PgRow, name: &str) -> std::result::Result<T, sqlx::Error>
where
    T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get::<T, _>(name)
}

/// TEXT 列 -> domain id（WorkerId 等以 TEXT 存储的 id）。
pub(crate) fn decode_text_id<T: FromStr>(
    value: &str,
    column: &str,
) -> std::result::Result<T, sqlx::Error>
where
    T::Err: Display,
{
    value
        .parse::<T>()
        .map_err(|e| decode_err(format!("column {column}: invalid id '{value}': {e}")))
}

/// UUID 列 -> domain id。
pub(crate) fn decode_uuid_id<T: FromStr>(
    value: Uuid,
    column: &str,
) -> std::result::Result<T, sqlx::Error>
where
    T::Err: Display,
{
    value
        .to_string()
        .parse::<T>()
        .map_err(|e| decode_err(format!("column {column}: invalid uuid id '{value}': {e}")))
}

/// 文本存储的 nullable domain id。
pub(crate) fn decode_opt_text_id<T: FromStr>(
    value: Option<&str>,
    column: &str,
) -> std::result::Result<Option<T>, sqlx::Error>
where
    T::Err: Display,
{
    match value {
        Some(v) => Ok(Some(decode_text_id::<T>(v, column)?)),
        None => Ok(None),
    }
}

fn parse_lifecycle(value: &str, column: &str) -> std::result::Result<LifecycleState, sqlx::Error> {
    LifecycleState::from_db_str(value).map_err(|e| decode_err(format!("column {column}: {e}")))
}

fn parse_worker_state(value: &str, column: &str) -> std::result::Result<WorkerState, sqlx::Error> {
    WorkerState::from_db_str(value).map_err(|e| decode_err(format!("column {column}: {e}")))
}

/// TEXT 列 -> ErrorCode（operations.error_code 存的是 proto 字符串）。
fn parse_error_code(value: &str, column: &str) -> std::result::Result<ErrorCode, sqlx::Error> {
    value
        .parse::<ErrorCode>()
        .map_err(|e| decode_err(format!("column {column}: {e}")))
}

/// u64 列以 i64 读取后转换（PostgreSQL bigint 无符号语义），负数视为数据损坏。
fn u64_from_i64(value: i64, column: &str) -> std::result::Result<u64, sqlx::Error> {
    u64::try_from(value)
        .map_err(|_| decode_err(format!("column {column}: negative value {value} for u64")))
}

fn owner_epoch_from_i64(value: i64, column: &str) -> std::result::Result<OwnerEpoch, sqlx::Error> {
    Ok(OwnerEpoch::new(u64_from_i64(value, column)?))
}

fn lsn_from_i64(value: i64, column: &str) -> std::result::Result<Lsn, sqlx::Error> {
    Ok(Lsn::new(u64_from_i64(value, column)?))
}

// ------------------------------------------------------------------ 列清单

pub(crate) const DATABASE_COLUMNS: &str = "id, tenant_id, name, state, owner_worker_id, owner_epoch, \
     lease_expires_at, wakeup_in_progress, wakeup_started_at, storage_region, storage_prefix, \
     last_snapshot_id, last_snapshot_lsn, cpu_milli, memory_mib, fd_limit, disk_mib, iops_limit, \
     priority, evictable, engine_version, schema_version, affinity_worker_id, anti_affinity_worker_id, \
     labels, created_at, updated_at, deleted_at";

pub(crate) const WORKER_COLUMNS: &str = "id, endpoint, control_endpoint, data_endpoint, state, region, zone, \
     version, cpu_milli_total, memory_mib_total, fd_total, disk_mib_total, process_slots_total, iops_total, \
     cpu_milli_used, memory_mib_used, fd_used, disk_mib_used, process_slots_used, iops_used, \
     last_heartbeat_at, missed_heartbeats, inventory_version, reserved_for_failover, labels, \
     created_at, updated_at";

pub(crate) const OPERATION_COLUMNS: &str = "id, kind, state, database_id, worker_id, tenant_id, requested_by, \
     idempotency_key, progress, error_code, error_message, result, created_at, updated_at, finished_at";

pub(crate) const SNAPSHOT_COLUMNS: &str =
    "id, database_id, base_lsn, checksum, size_bytes, object_key, \
     compression, owner_epoch, engine_version, schema_version, state, created_at, verified_at";

pub(crate) const JOB_COLUMNS: &str = "id, kind, payload, state, priority, run_after, lease_owner, \
     lease_expires_at, attempts, max_attempts, last_error, idempotency_key, created_at, updated_at, finished_at";

// ------------------------------------------------------------------ 行类型

pub(crate) struct DatabaseRow(pub DatabaseRecord);

impl FromRow<'_, PgRow> for DatabaseRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let state: String = col(row, "state")?;
        let last_snapshot_id: Option<String> = col(row, "last_snapshot_id")?;
        let affinity: Option<String> = col(row, "affinity_worker_id")?;
        let anti_affinity: Option<String> = col(row, "anti_affinity_worker_id")?;
        Ok(DatabaseRow(DatabaseRecord {
            id: decode_uuid_id(col(row, "id")?, "id")?,
            tenant_id: decode_uuid_id(col(row, "tenant_id")?, "tenant_id")?,
            name: col(row, "name")?,
            state: parse_lifecycle(&state, "state")?,
            owner_worker_id: decode_opt_text_id(col(row, "owner_worker_id")?, "owner_worker_id")?,
            owner_epoch: owner_epoch_from_i64(col(row, "owner_epoch")?, "owner_epoch")?,
            lease_expires_at: col(row, "lease_expires_at")?,
            wakeup_in_progress: col(row, "wakeup_in_progress")?,
            wakeup_started_at: col(row, "wakeup_started_at")?,
            storage_region: col(row, "storage_region")?,
            storage_prefix: col(row, "storage_prefix")?,
            last_snapshot_id: match last_snapshot_id {
                Some(v) => Some(decode_text_id::<SnapshotId>(&v, "last_snapshot_id")?),
                None => None,
            },
            last_snapshot_lsn: match col::<Option<i64>>(row, "last_snapshot_lsn")? {
                Some(v) => Some(lsn_from_i64(v, "last_snapshot_lsn")?),
                None => None,
            },
            cpu_milli: u64_from_i64(col(row, "cpu_milli")?, "cpu_milli")?,
            memory_mib: u64_from_i64(col(row, "memory_mib")?, "memory_mib")?,
            fd_limit: u64_from_i64(col(row, "fd_limit")?, "fd_limit")?,
            disk_mib: u64_from_i64(col(row, "disk_mib")?, "disk_mib")?,
            iops_limit: u64_from_i64(col(row, "iops_limit")?, "iops_limit")?,
            priority: col(row, "priority")?,
            evictable: col(row, "evictable")?,
            engine_version: col(row, "engine_version")?,
            schema_version: col(row, "schema_version")?,
            affinity_worker_id: decode_opt_text_id(affinity.as_deref(), "affinity_worker_id")?,
            anti_affinity_worker_id: decode_opt_text_id(
                anti_affinity.as_deref(),
                "anti_affinity_worker_id",
            )?,
            labels: col(row, "labels")?,
            created_at: col(row, "created_at")?,
            updated_at: col(row, "updated_at")?,
            deleted_at: col(row, "deleted_at")?,
        }))
    }
}

pub(crate) struct WorkerRow(pub WorkerRecord);

impl FromRow<'_, PgRow> for WorkerRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let state: String = col(row, "state")?;
        let id: String = col(row, "id")?;
        Ok(WorkerRow(WorkerRecord {
            id: decode_text_id(&id, "id")?,
            endpoint: col(row, "endpoint")?,
            control_endpoint: col(row, "control_endpoint")?,
            data_endpoint: col(row, "data_endpoint")?,
            state: parse_worker_state(&state, "state")?,
            region: col(row, "region")?,
            zone: col(row, "zone")?,
            version: col(row, "version")?,
            cpu_milli_total: u64_from_i64(col(row, "cpu_milli_total")?, "cpu_milli_total")?,
            memory_mib_total: u64_from_i64(col(row, "memory_mib_total")?, "memory_mib_total")?,
            fd_total: u64_from_i64(col(row, "fd_total")?, "fd_total")?,
            disk_mib_total: u64_from_i64(col(row, "disk_mib_total")?, "disk_mib_total")?,
            process_slots_total: u64_from_i64(
                col(row, "process_slots_total")?,
                "process_slots_total",
            )?,
            iops_total: u64_from_i64(col(row, "iops_total")?, "iops_total")?,
            cpu_milli_used: u64_from_i64(col(row, "cpu_milli_used")?, "cpu_milli_used")?,
            memory_mib_used: u64_from_i64(col(row, "memory_mib_used")?, "memory_mib_used")?,
            fd_used: u64_from_i64(col(row, "fd_used")?, "fd_used")?,
            disk_mib_used: u64_from_i64(col(row, "disk_mib_used")?, "disk_mib_used")?,
            process_slots_used: u64_from_i64(
                col(row, "process_slots_used")?,
                "process_slots_used",
            )?,
            iops_used: u64_from_i64(col(row, "iops_used")?, "iops_used")?,
            last_heartbeat_at: col(row, "last_heartbeat_at")?,
            missed_heartbeats: col(row, "missed_heartbeats")?,
            inventory_version: col(row, "inventory_version")?,
            reserved_for_failover: col(row, "reserved_for_failover")?,
            labels: col(row, "labels")?,
            created_at: col(row, "created_at")?,
            updated_at: col(row, "updated_at")?,
        }))
    }
}

pub(crate) struct OperationRow(pub OperationRecord);

impl FromRow<'_, PgRow> for OperationRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let database_id: Option<Uuid> = col(row, "database_id")?;
        let worker_id: Option<String> = col(row, "worker_id")?;
        let tenant_id: Option<Uuid> = col(row, "tenant_id")?;
        let requested_by: Option<Uuid> = col(row, "requested_by")?;
        Ok(OperationRow(OperationRecord {
            id: decode_uuid_id(col(row, "id")?, "id")?,
            kind: col(row, "kind")?,
            state: col(row, "state")?,
            database_id: match database_id {
                Some(v) => Some(decode_uuid_id::<DatabaseId>(v, "database_id")?),
                None => None,
            },
            worker_id: decode_opt_text_id::<WorkerId>(worker_id.as_deref(), "worker_id")?,
            tenant_id: match tenant_id {
                Some(v) => Some(decode_uuid_id::<TenantId>(v, "tenant_id")?),
                None => None,
            },
            requested_by: match requested_by {
                Some(v) => Some(decode_uuid_id::<UserId>(v, "requested_by")?),
                None => None,
            },
            idempotency_key: col(row, "idempotency_key")?,
            progress: col(row, "progress")?,
            error_code: match col::<Option<String>>(row, "error_code")? {
                Some(v) => Some(parse_error_code(&v, "error_code")?),
                None => None,
            },
            error_message: col(row, "error_message")?,
            result: col(row, "result")?,
            created_at: col(row, "created_at")?,
            updated_at: col(row, "updated_at")?,
            finished_at: col(row, "finished_at")?,
        }))
    }
}

pub(crate) struct SnapshotRow(pub SnapshotRecord);

impl FromRow<'_, PgRow> for SnapshotRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let id: String = col(row, "id")?;
        Ok(SnapshotRow(SnapshotRecord {
            id: decode_text_id(&id, "id")?,
            database_id: decode_uuid_id(col(row, "database_id")?, "database_id")?,
            base_lsn: lsn_from_i64(col(row, "base_lsn")?, "base_lsn")?,
            checksum: col(row, "checksum")?,
            size_bytes: u64_from_i64(col(row, "size_bytes")?, "size_bytes")?,
            object_key: col(row, "object_key")?,
            compression: col(row, "compression")?,
            owner_epoch: owner_epoch_from_i64(col(row, "owner_epoch")?, "owner_epoch")?,
            engine_version: col(row, "engine_version")?,
            schema_version: col(row, "schema_version")?,
            state: col(row, "state")?,
            created_at: col(row, "created_at")?,
            verified_at: col(row, "verified_at")?,
        }))
    }
}

pub(crate) struct JobRow(pub JobRecord);

impl FromRow<'_, PgRow> for JobRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(JobRow(JobRecord {
            id: decode_uuid_id(col(row, "id")?, "id")?,
            kind: col(row, "kind")?,
            payload: col(row, "payload")?,
            state: col(row, "state")?,
            priority: col(row, "priority")?,
            run_after: col(row, "run_after")?,
            lease_owner: col(row, "lease_owner")?,
            lease_expires_at: col(row, "lease_expires_at")?,
            attempts: col(row, "attempts")?,
            max_attempts: col(row, "max_attempts")?,
            last_error: col(row, "last_error")?,
            idempotency_key: col(row, "idempotency_key")?,
            created_at: col(row, "created_at")?,
            updated_at: col(row, "updated_at")?,
            finished_at: col(row, "finished_at")?,
        }))
    }
}

// ------------------------------------------------------------------ SQL 组装

/// 绑定参数（值全部走占位符，禁止拼接到 SQL 文本里）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SqlParam {
    Uuid(Uuid),
    Text(String),
    Int(i64),
}

/// 动态 WHERE 组装器：只拼条件骨架，参数按顺序收集。
#[derive(Debug, Default)]
pub(crate) struct SqlBuilder {
    clauses: Vec<String>,
    params: Vec<SqlParam>,
}

impl SqlBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 追加一个条件；`template` 中必须使用 `$N` 占位符（N 由调用方按 `params().len()+1` 计算）。
    pub(crate) fn push(
        &mut self,
        template: impl Into<String>,
        param: Option<SqlParam>,
    ) -> &mut Self {
        self.clauses.push(template.into());
        if let Some(p) = param {
            self.params.push(p);
        }
        self
    }

    /// 下一个占位符序号（1-based）。
    pub(crate) fn next_placeholder(&self) -> usize {
        self.params.len() + 1
    }

    pub(crate) fn params(&self) -> &[SqlParam] {
        &self.params
    }

    /// 生成 ` WHERE a AND b`，无条件时返回空串。
    pub(crate) fn where_clause(&self) -> String {
        if self.clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", self.clauses.join(" AND "))
        }
    }
}

/// 把收集到的参数按顺序绑定到查询上。
pub(crate) fn bind_all<'q, O>(
    mut query: sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments>,
    params: &[SqlParam],
) -> sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments> {
    for p in params {
        query = match p {
            SqlParam::Uuid(v) => query.bind(*v),
            SqlParam::Text(v) => query.bind(v.clone()),
            SqlParam::Int(v) => query.bind(*v),
        };
    }
    query
}

/// 拒绝非法枚举取值（operation state / job state / snapshot state 等）。
pub(crate) fn invalid_argument(message: impl Into<String>) -> domain::error::PlatformError {
    platform_error(ErrorCode::InvalidArgument, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_builder_keeps_placeholder_order() {
        let mut b = SqlBuilder::new();
        assert_eq!(b.next_placeholder(), 1);
        b.push("tenant_id = $1", Some(SqlParam::Uuid(Uuid::nil())));
        b.push("state = $2", Some(SqlParam::Text("COLD".into())));
        assert_eq!(b.next_placeholder(), 3);
        assert_eq!(
            b.where_clause(),
            " WHERE tenant_id = $1 AND state = $2".to_string()
        );
        assert_eq!(b.params().len(), 2);
    }

    #[test]
    fn sql_builder_without_filters_has_no_where() {
        let b = SqlBuilder::new();
        assert!(b.where_clause().is_empty());
        assert!(b.params().is_empty());
    }

    #[test]
    fn id_conversion_rejects_non_uuid() {
        let err = id_to_uuid(&"not-a-uuid").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);

        let id = DatabaseId::new_v7();
        let uuid = id_to_uuid(&id).unwrap();
        assert_eq!(uuid.to_string(), id.to_string());
    }

    #[test]
    fn text_id_roundtrip() {
        let w: WorkerId = "worker-1".parse().unwrap();
        let parsed: WorkerId = decode_text_id(w.as_ref(), "id").unwrap();
        assert_eq!(parsed, w);
        assert!(decode_text_id::<DatabaseId>("not-a-uuid", "id").is_err());
    }

    #[test]
    fn u64_column_rejects_negative() {
        assert_eq!(u64_from_i64(7, "owner_epoch").unwrap(), 7);
        assert!(u64_from_i64(-1, "owner_epoch").is_err());
    }

    #[test]
    fn duration_millis_saturates() {
        assert_eq!(millis(std::time::Duration::from_millis(0)), 0);
        assert_eq!(millis(std::time::Duration::from_secs(30)), 30_000);
        assert_eq!(
            millis(std::time::Duration::from_secs(u64::MAX / 1000)),
            i64::MAX
        );
    }
}
