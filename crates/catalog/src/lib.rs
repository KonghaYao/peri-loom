//! catalog —— PostgreSQL Catalog 访问层（架构 §17.5 / §17.5.1）。
//!
//! 职责：
//! - DB catalog / Ownership + Epoch / Worker inventory / Operations / Jobs / Snapshot
//!   / RBAC / Audit / Panel 数据的权威读写；
//! - 用 SQLx 运行期 API（无 ORM、无编译期宏），所有值走绑定参数；
//! - 多步状态变更放进显式事务 + 行锁，保证 ownership 与 lease 的原子性。
//!
//! 调用约定：
//! - 所有公共方法返回 [`domain::error::Result`]（`PlatformError`），错误码可直接映射为 HTTP 状态；
//! - epoch / lease 相关方法失败必须视为「失去所有权」，调用方不得继续写入。

mod backup;
mod databases;
mod error;
mod idempotency;
mod jobs;
mod metadata;
mod operations;
mod panel;
mod pg;
mod rbac;
mod sqlite;
mod watcher;
mod workers;

use std::path::{Path, PathBuf};

use domain::error::{ErrorCode, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};

pub use backup::{
    is_valid_backup_job_kind, is_valid_backup_job_state, is_valid_snapshot_state, BackupJobRecord,
    BackupJobUpdate, CreateBackupJobParams, BACKUP_JOB_KINDS, BACKUP_JOB_STATES, SNAPSHOT_STATES,
};
pub use databases::{
    can_take_ownership, default_tenant_id, validate_lifecycle_transition, CreateDatabaseParams,
    DatabaseFilter, OwnershipEvent, RoutingEntry, WakeupDecision, DEFAULT_OWNER_LEASE,
    DEFAULT_TENANT_UUID, DEFAULT_WAKEUP_LEASE, REASON_EPOCH_REALIGN_FROM_STORAGE,
};
pub use error::CatalogError;
pub use idempotency::{IdempotencyOperationOutcome, IdempotencyOutcome};
pub use jobs::{
    is_valid_job_state, job_retry_backoff_millis, DEFAULT_JOB_LEASE, JOB_RETRY_BASE_BACKOFF_MS,
    JOB_RETRY_MAX_BACKOFF_MS, JOB_STATES,
};
pub use metadata::{BackupStore, DatabaseStore, IdentityStore, Metadata, PanelStore, TaskStore};
pub use operations::{
    is_terminal_operation_state, is_valid_operation_kind, is_valid_operation_state, NewOperation,
    OPERATION_KINDS, OPERATION_STATES,
};
pub use panel::{NewSavedQuery, NewSlowQuery, PreferenceRecord, SavedQueryRecord, SlowQueryRecord};
pub use rbac::{
    ApiTokenRecord, AuditEntry, AuditFilter, AuditLogRecord, AuthenticatedToken, NewApiToken,
    NewUser, UserRecord, AUDIT_RESULTS, PERMISSION_WILDCARD, USER_STATUSES,
};
pub use sqlite::{LocalSubmission, LocalSubmissionOutcome, SqliteCatalog};
pub use watcher::{CatalogChange, CatalogWatcher, CATALOG_CHANGES_CHANNEL};
pub use workers::{
    inventory_missing_grace, reclaim_reason, LocalDatabaseState, ReclaimedOwnership,
    RecordHeartbeatOutcome, UpsertWorkerParams, DEFAULT_HEARTBEAT_TIMEOUT, HEARTBEAT_INTERVAL,
    MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM,
};

use crate::error::{catalog_error, platform_error_retryable};

/// migrations 目录。
///
/// 解析顺序（为什么需要这样）：
/// 1. `PLATFORM_MIGRATIONS_DIR` 环境变量 —— 容器镜像里只拷贝了 migrations 目录
///    （见 Dockerfile 的 `/opt/db-platform/migrations`），运行时不包含 crate 源码树，
///    因此编译期路径在容器里必然不存在；
/// 2. 编译期路径 `CARGO_MANIFEST_DIR/../../migrations` —— 本地开发与测试用。
///
/// 两处都不存在时返回编译期路径（让 sqlx 报出可读的“目录不存在”错误，而不是在这里 panic）。
pub fn migrations_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("PLATFORM_MIGRATIONS_DIR") {
        let path = PathBuf::from(dir);
        if path.is_dir() {
            return path;
        }
        tracing::warn!(
            dir = %path.display(),
            "PLATFORM_MIGRATIONS_DIR 指向的目录不存在，回退到编译期 paths"
        );
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations")
}

/// PostgreSQL Catalog 客户端。内部是连接池（`PgPool` 为 Arc 语义），可自由 clone 共享。
#[derive(Clone)]
pub struct Catalog {
    pool: PgPool,
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("pool_size", &self.pool.size())
            .field("idle", &self.pool.num_idle())
            .finish()
    }
}

impl Catalog {
    /// 建立连接池。`max_connections` 是控制面共享的上限，超出会排队等待。
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(database_url)
            .await
            .map_err(|e| {
                // PostgreSQL 未就绪属于 bootstrap 阶段的可重试故障（架构 §17.5.1）
                platform_error_retryable(
                    ErrorCode::StorageUnavailable,
                    format!("连接 PostgreSQL catalog 失败: {e}"),
                )
            })?;
        Ok(Self { pool })
    }

    /// 用已有连接池构造（便于调用方统一管理连接池生命周期 / 测试注入）。
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 运行 migrations（运行期加载 `migrations/`，不使用编译期嵌入）。
    ///
    /// 控制面启动顺序依赖：PostgreSQL Ready -> Catalog 迁移完成 -> Server Ready。
    pub async fn migrate(&self) -> Result<()> {
        let dir = migrations_dir();
        let migrator = sqlx::migrate::Migrator::new(dir.as_path())
            .await
            .map_err(|e| {
                catalog_error(CatalogError::Migration(format!(
                    "加载 migrations 目录 {} 失败: {e}",
                    dir.display()
                )))
            })?;
        migrator
            .run(&self.pool)
            .await
            .map_err(|e| catalog_error(CatalogError::Migration(format!("执行迁移失败: {e}"))))?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// 轻量健康检查（供 /readyz 使用）。
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| {
                platform_error_retryable(
                    ErrorCode::StorageUnavailable,
                    format!("catalog health check 失败: {e}"),
                )
            })
    }
}

/// 真实数据库测试。默认 `#[ignore]`，需要 PostgreSQL 时显式运行：
///
/// ```bash
/// DATABASE_URL=postgres://user:pass@localhost:5432/postgres \
///   cargo test -p catalog -- --ignored --test-threads=1
/// ```
#[cfg(test)]
mod db_tests {
    use super::*;
    use domain::ids::{DatabaseId, WorkerId};
    use domain::lifecycle::{LifecycleState, WorkerState};
    use domain::records::SnapshotRecord;
    use domain::resources::WorkerResourceUsage;
    use domain::wal::{Lsn, OwnerEpoch};
    use std::time::Duration;

    async fn test_catalog() -> Catalog {
        let url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL 未设置：真实数据库测试需要 PostgreSQL");
        let catalog = Catalog::connect(&url, 5).await.expect("connect");
        catalog.migrate().await.expect("migrate");
        catalog
    }

    /// 每轮测试唯一的后缀：测试共享同一个 schema，固定名字会被上一轮遗留状态污染
    /// （例如上一轮把 Worker 置为 DRAINING 会让本轮心跳直接返回 draining）。
    fn run_nonce() -> String {
        uuid::Uuid::now_v7().to_string()
    }

    fn worker_id(suffix: &str) -> WorkerId {
        format!("test-worker-{suffix}-{}", run_nonce())
            .parse()
            .unwrap()
    }

    async fn register_worker(catalog: &Catalog, suffix: &str) -> WorkerId {
        let id = worker_id(suffix);
        catalog
            .upsert_worker(UpsertWorkerParams::new(id.clone(), "http://127.0.0.1:9090"))
            .await
            .expect("upsert worker");
        id
    }

    /// 测试用：把 `wakeup_started_at` 挪到 `age` 之前，等价于「Leader 置位后随即崩溃」。
    ///
    /// 走的是真实 SQL（而不是内存状态），这样验证的是 Catalog 的 SQL 判定条件本身。
    async fn age_wakeup_start(catalog: &Catalog, id: DatabaseId, age: Duration) {
        sqlx::query(
            "UPDATE databases SET wakeup_started_at = now() - ($2::bigint * INTERVAL '1 millisecond')
             WHERE id = $1::uuid",
        )
        .bind(crate::pg::id_to_uuid(&id).expect("database id 是 uuid"))
        .bind(crate::pg::millis(age))
        .execute(catalog.pool())
        .await
        .expect("模拟 wakeup 超时");
    }

    /// 测试用：把 owner 租约挪到 `age` 之前，等价于「Worker 崩溃后租约过期」。
    async fn expire_lease(catalog: &Catalog, id: DatabaseId, age: Duration) {
        sqlx::query(
            "UPDATE databases SET lease_expires_at = now() - ($2::bigint * INTERVAL '1 millisecond')
             WHERE id = $1::uuid",
        )
        .bind(crate::pg::id_to_uuid(&id).expect("database id 是 uuid"))
        .bind(crate::pg::millis(age))
        .execute(catalog.pool())
        .await
        .expect("模拟租约过期");
    }

    /// 测试用：读取某个 DB 当前的 Route Cache 快照条目。
    async fn routing_entry(catalog: &Catalog, id: DatabaseId) -> Option<RoutingEntry> {
        catalog
            .list_routing_entries()
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.database_id == id)
    }

    /// 测试用：心跳载荷里的资源用量（内容无关紧要，但必须合法）。
    fn heartbeat_usage() -> WorkerResourceUsage {
        WorkerResourceUsage {
            cpu_milli_used: 100,
            cpu_milli_total: 1_000,
            memory_mib_used: 10,
            memory_mib_total: 100,
            fd_used: 5,
            fd_total: 50,
            disk_mib_used: 1,
            disk_mib_total: 10,
            iops_used: 1,
            iops_total: 10,
            db_process_count: 1,
            db_process_limit: 10,
        }
    }

    /// 测试用：把 `updated_at` 挪到 `age` 之前 —— 等价于「Worker 已经这么久没有把该 DB
    /// 报进 inventory 了」。走真实 SQL，验的就是对账 SQL 的判定条件本身。
    async fn age_database_updated_at(catalog: &Catalog, id: DatabaseId, age: Duration) {
        sqlx::query(
            "UPDATE databases SET updated_at = now() - ($2::bigint * INTERVAL '1 millisecond')
             WHERE id = $1::uuid",
        )
        .bind(crate::pg::id_to_uuid(&id).expect("database id 是 uuid"))
        .bind(crate::pg::millis(age))
        .execute(catalog.pool())
        .await
        .expect("模拟心跳缺失");
    }

    /// 测试用：把 `lease_expires_at` 挪到 `ago` 之前 —— 等价于「Owner 已经这么久没续约了」。
    /// 走真实 SQL，验的就是 `clear_stale_ownership` 的判定条件本身。
    async fn age_database_lease(catalog: &Catalog, id: DatabaseId, ago: Duration) {
        sqlx::query(
            "UPDATE databases SET lease_expires_at = now() - ($2::bigint * INTERVAL '1 millisecond')
             WHERE id = $1::uuid",
        )
        .bind(crate::pg::id_to_uuid(&id).expect("database id 是 uuid"))
        .bind(crate::pg::millis(ago))
        .execute(catalog.pool())
        .await
        .expect("模拟租约未续约");
    }

    /// 测试用：把某个 DB 的 ownership 推到「以当前 epoch 认领的条目已缺失超过阈值」，
    /// 即同时满足对账的 `updated_at` 窗口与租约条件。
    async fn expire_reclaim_window(catalog: &Catalog, id: DatabaseId) {
        age_database_updated_at(
            catalog,
            id,
            MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM * HEARTBEAT_INTERVAL
                + Duration::from_secs(1),
        )
        .await;
        age_database_lease(catalog, id, DEFAULT_OWNER_LEASE).await;
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn migrate_sets_initial_catalog_version() {
        let catalog = test_catalog().await;
        let version = catalog.current_catalog_version().await.unwrap();
        assert!(version >= 1);
        catalog.health_check().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn create_get_and_soft_delete_database() {
        let catalog = test_catalog().await;
        let name = format!("itest-{}", uuid::Uuid::now_v7());
        let created = catalog
            .create_database(CreateDatabaseParams::new(name.clone()))
            .await
            .unwrap();
        assert_eq!(created.name, name);
        assert_eq!(created.state, LifecycleState::Cold);
        assert_eq!(created.owner_epoch, OwnerEpoch::ZERO);
        assert!(created.owner_worker_id.is_none());

        let fetched = catalog.get_database(created.id).await.unwrap();
        assert_eq!(fetched.id, created.id);
        let by_name = catalog
            .get_database_by_name(fetched.tenant_id, &name)
            .await
            .unwrap();
        assert_eq!(by_name.id, created.id);

        // 同名重复创建 -> DB_ALREADY_EXISTS
        let dup = catalog
            .create_database(CreateDatabaseParams::new(name.clone()))
            .await;
        assert_eq!(dup.unwrap_err().code, ErrorCode::DbAlreadyExists);

        let listed = catalog
            .list_databases(DatabaseFilter {
                tenant_id: Some(created.tenant_id),
                name_prefix: Some(name.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(listed.iter().any(|d| d.id == created.id));

        let deleted = catalog.soft_delete_database(created.id).await.unwrap();
        assert!(deleted.deleted_at.is_some());
        assert_eq!(deleted.state, LifecycleState::Stopping);
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn bump_ownership_rejects_wrong_epoch_and_audits_change() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "bump").await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-own-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        // epoch 不匹配必须失败（防 Split Brain 关键路径）
        let err = catalog
            .bump_ownership(db.id, 7, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::EpochMismatch);

        let epoch = catalog
            .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(epoch, 1);

        // 旧 epoch 再次接管必须失败
        let err = catalog
            .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::EpochMismatch);

        assert!(catalog
            .renew_lease(db.id, 1, Duration::from_secs(30))
            .await
            .unwrap());
        assert!(!catalog
            .renew_lease(db.id, 0, Duration::from_secs(30))
            .await
            .unwrap());

        let events = catalog.list_ownership_events(db.id, 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].from_epoch, 0);
        assert_eq!(events[0].to_epoch, 1);

        let routing = catalog.list_routing_entries().await.unwrap();
        assert!(routing.iter().any(|r| r.database_id == db.id));
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn wakeup_coalescing_allows_single_leader() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-wake-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Leader
        );
        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Waiter
        );

        catalog.end_wakeup(db.id).await.unwrap();

        // FIX-D：§7 状态机在生产写入路径上强制 —— COLD 不能直接跳到 WARM，
        // 并且错误里必须能看出 from/to
        let illegal = catalog
            .set_lifecycle_state(db.id, LifecycleState::Warm, None)
            .await
            .unwrap_err();
        assert_eq!(illegal.code, ErrorCode::InvalidArgument);
        assert!(illegal.message.contains("COLD"), "{}", illegal.message);
        assert!(illegal.message.contains("WARM"), "{}", illegal.message);
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().state,
            LifecycleState::Cold,
            "非法转换不得留下任何写入"
        );

        // 合法链路：COLD -> STARTING -> WARM
        catalog
            .set_lifecycle_state(db.id, LifecycleState::Starting, None)
            .await
            .unwrap();
        catalog
            .set_lifecycle_state(db.id, LifecycleState::Warm, None)
            .await
            .unwrap();
        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::AlreadyRunning(LifecycleState::Warm)
        );
    }

    /// FIX-A 回归：Leader 在 `bump_ownership` 之前崩溃时，`wakeup_in_progress` 会永久停在 TRUE，
    /// 旧实现下该 DB 的冷启动永久卡死（后续请求全部 Waiter 直到超时）。
    ///
    /// 修复由两部分组成，这里两条路径都验证：
    /// 1) `try_begin_wakeup` 支持租约式抢占（同一原子 UPDATE 里刷新 wakeup_started_at）；
    /// 2) `reclaim_stale_wakeups` janitor 不受 owner 非空约束，能把超时标志清回 FALSE。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn crashed_wakeup_leader_does_not_deadlock_cold_start() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-wake-crash-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Leader
        );
        // 崩溃现场：标志已置位、state 仍是 COLD、owner 仍是 NULL
        let stuck = catalog.get_database(db.id).await.unwrap();
        assert!(stuck.wakeup_in_progress);
        assert_eq!(stuck.state, LifecycleState::Cold);
        assert!(stuck.owner_worker_id.is_none());

        // 未超时之前，其他人不得抢走：仍是 Waiter
        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Waiter
        );

        // 用 SQL 把 wakeup_started_at 挪到过去，模拟「Leader 崩溃 + 租约到期」
        age_wakeup_start(
            &catalog,
            db.id,
            DEFAULT_WAKEUP_LEASE + Duration::from_secs(1),
        )
        .await;

        // 修复点 1：超过租约后允许接手，并且是同一条原子 UPDATE 里刷新时间戳
        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Leader,
            "wakeup 租约过期后必须允许新调用方接手，否则冷启动永久卡死"
        );
        // 接手后时间戳已被刷新：其他人重新回到 Waiter（不会出现双 Leader）
        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Waiter
        );

        // 修复点 2：janitor 兜底。旧实现的 clear_stale_ownership 要求
        // state <> 'COLD' AND owner_worker_id IS NOT NULL，永远扫不到这一行。
        age_wakeup_start(
            &catalog,
            db.id,
            DEFAULT_WAKEUP_LEASE + Duration::from_secs(1),
        )
        .await;
        let reclaimed = catalog
            .reclaim_stale_wakeups(DEFAULT_WAKEUP_LEASE)
            .await
            .unwrap();
        // janitor 是全局扫描：它只认「标志 + 超时」，因此也会顺手回收其它被卡住的行
        // （共享 schema 下同一轮里其它用例留下的 Leader 标志正是这个缺陷的产物）。
        assert!(
            reclaimed.contains(&db.id),
            "超时的 wakeup 应被 janitor 回收，实际回收 {reclaimed:?}"
        );

        let after = catalog.get_database(db.id).await.unwrap();
        assert!(!after.wakeup_in_progress);
        assert!(after.wakeup_started_at.is_none());
        assert_eq!(after.state, LifecycleState::Cold);
        assert_eq!(
            after.owner_epoch,
            OwnerEpoch::ZERO,
            "回收 wakeup 标志没有发生所有权变更，不得动 epoch"
        );

        // 回收后必须能重新发起冷启动
        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Leader
        );

        // 未超时的 wakeup 不会被 janitor 误伤（否则多个 Leader 会同时启动同一个 DB）
        let none = catalog
            .reclaim_stale_wakeups(DEFAULT_WAKEUP_LEASE)
            .await
            .unwrap();
        assert!(!none.contains(&db.id));
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn heartbeat_resets_misses_and_fences_stale_reports() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "hb").await;
        let usage = WorkerResourceUsage {
            cpu_milli_used: 100,
            cpu_milli_total: 1_000,
            memory_mib_used: 10,
            memory_mib_total: 100,
            fd_used: 5,
            fd_total: 50,
            disk_mib_used: 1,
            disk_mib_total: 10,
            iops_used: 1,
            iops_total: 10,
            db_process_count: 1,
            db_process_limit: 10,
        };

        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 3, &[], WorkerState::Active)
            .await
            .unwrap();
        assert!(!outcome.draining);
        assert!(outcome.catalog_version >= 1);

        let record = catalog.get_worker(worker.clone()).await.unwrap();
        assert_eq!(record.missed_heartbeats, 0);
        assert_eq!(record.cpu_milli_used, 100);
        assert!(record.last_heartbeat_at.is_some());

        // 未被排除的 Worker 若心跳超时则 +1
        let missed = catalog
            .mark_missed_heartbeats_with_timeout(&[], Duration::from_secs(0))
            .await
            .unwrap();
        assert!(missed.iter().any(|(id, n)| id == &worker && *n == 1));

        // 排除后不再累加
        let missed = catalog
            .mark_missed_heartbeats_with_timeout(
                std::slice::from_ref(&worker),
                Duration::from_secs(0),
            )
            .await
            .unwrap();
        assert!(!missed.iter().any(|(id, _)| id == &worker));

        // DRAINING 不被心跳覆盖
        catalog
            .mark_worker_state(worker.clone(), WorkerState::Draining)
            .await
            .unwrap();
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 4, &[], WorkerState::Draining)
            .await
            .unwrap();
        assert!(outcome.draining);
        assert_eq!(
            catalog.get_worker(worker).await.unwrap().state,
            WorkerState::Draining
        );
    }

    /// §12.2 / §16：Worker 重启后本地注册表为空，Catalog 必须依据心跳 inventory 自行把
    /// ownership 收回（清 owner + epoch+1 + COLD + 审计），否则该 DB 永远只能拿到 NOT_OWNER。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn heartbeat_reclaims_ownership_missing_from_inventory() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "inv").await;
        let usage = heartbeat_usage();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-inv-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        // 正常态：这次接管 + WARM，Worker 本地确实有这个进程
        catalog
            .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        catalog
            .set_lifecycle_state(db.id, LifecycleState::Warm, Some(worker.clone()))
            .await
            .unwrap();

        // 1) inventory 里带着它 -> 什么都不做
        let present = LocalDatabaseState {
            database_id: db.id,
            state: LifecycleState::Warm,
            owner_epoch: OwnerEpoch::new(1),
            pid: Some(42),
        };
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 5, &[present], WorkerState::Active)
            .await
            .unwrap();
        assert!(outcome.reclaimed.is_empty());

        // 2) 只有一轮缺失 -> 不得回收：Server 可能刚下达 StartDatabase，
        //    而 Worker 要等进程 READY 才写入注册表（这是「有界」的核心）。
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 6, &[], WorkerState::Active)
            .await
            .unwrap();
        assert!(
            outcome.reclaimed.is_empty(),
            "一次心跳缺失不足以判定 Worker 真的没有这个 DB"
        );
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().state,
            LifecycleState::Warm
        );

        // 3) 缺失持续超过阈值 -> 回收
        age_database_updated_at(
            &catalog,
            db.id,
            MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM * HEARTBEAT_INTERVAL
                + Duration::from_secs(1),
        )
        .await;
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 7, &[], WorkerState::Active)
            .await
            .unwrap();
        assert_eq!(outcome.reclaimed.len(), 1);
        let reclaimed = &outcome.reclaimed[0];
        assert_eq!(reclaimed.database_id, db.id);
        assert_eq!(reclaimed.worker_id, worker);
        assert_eq!(reclaimed.from_epoch, 1);
        assert_eq!(reclaimed.to_epoch, 2);

        let record = catalog.get_database(db.id).await.unwrap();
        assert_eq!(
            record.state,
            LifecycleState::Cold,
            "必须回到可被重新放置的 COLD"
        );
        assert!(record.owner_worker_id.is_none(), "owner 必须清空");
        assert_eq!(
            record.owner_epoch.get(),
            2,
            "epoch 必须推进以 fences 旧进程"
        );

        // 4) 幂等：同一条缺失心跳再来一次不会重复回收，也不产生重复审计
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 8, &[], WorkerState::Active)
            .await
            .unwrap();
        assert!(outcome.reclaimed.is_empty());
        let events = catalog.list_ownership_events(db.id, 10).await.unwrap();
        assert_eq!(events.len(), 2, "只应有 bump + reclaim 两条审计");
        assert_eq!(events[0].reason, reclaim_reason::INVENTORY_MISSING);
        assert_eq!(events[0].from_epoch, 1);
        assert_eq!(events[0].to_epoch, 2);
        assert!(events[0].to_worker_id.is_none());

        // 5) 回收后必须能重新被放置（这正是「自愈」的定义）
        let next = catalog
            .bump_ownership(db.id, 2, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(next, 3);
    }

    /// 心跳必须续约 ownership 租约 —— 这是「健康库不被 GC 误杀」的唯一保证。
    ///
    /// `bump_ownership` 发出的租约若无人续约，`clear_stale_ownership` 会在 TTL + grace
    /// 之后把**正在服务**的 DB 一并回收：DB 被反复踢回 COLD，epoch 空转，
    /// 每次请求都被迫重新冷启动（表现为数据面周期性不可用）。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn heartbeat_renews_ownership_lease() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "renew").await;
        let usage = heartbeat_usage();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-renew-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let epoch = catalog
            .bump_ownership(db.id, 0, worker.clone(), DEFAULT_OWNER_LEASE)
            .await
            .unwrap();

        // 模拟「bump 之后已经很久」：租约只剩 1s 寿命
        age_database_lease(
            &catalog,
            db.id,
            DEFAULT_OWNER_LEASE - Duration::from_secs(1),
        )
        .await;
        let before = catalog
            .get_database(db.id)
            .await
            .unwrap()
            .lease_expires_at
            .expect("bump 之后必须有租约");

        // Worker 以当前 epoch 认领它 -> 必须续约
        let present = LocalDatabaseState {
            database_id: db.id,
            state: LifecycleState::Warm,
            owner_epoch: OwnerEpoch::new(epoch),
            pid: Some(4242),
        };
        catalog
            .record_heartbeat(worker.clone(), &usage, 1, &[present], WorkerState::Active)
            .await
            .unwrap();

        let after = catalog
            .get_database(db.id)
            .await
            .unwrap()
            .lease_expires_at
            .expect("续约之后仍然有租约");
        assert!(
            after > before,
            "心跳必须把 ownership 租约向后推（before={before} after={after}）"
        );

        // 用最激进的过期口径做 GC：刚被心跳续约的库一个都不许被回收
        let reclaimed = catalog.clear_stale_ownership(Duration::ZERO).await.unwrap();
        assert!(
            !reclaimed.contains(&db.id),
            "刚被心跳续约的库不得被 ownership GC 回收，实际回收了 {reclaimed:?}"
        );
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            epoch,
            "续约只是延长租约，不推进 epoch"
        );
    }

    /// 续约只认当前 Owner：携带过期 epoch 的心跳既不能改 Catalog 状态，也不能给租约续命。
    ///
    /// 否则「被取代的旧 Owner 只要还在发心跳就能永久占住 DB」，Fencing 就形同虚设。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn heartbeat_with_stale_epoch_does_not_renew_lease() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "stale-renew").await;
        let usage = heartbeat_usage();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-stale-renew-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        // epoch=1 的 Owner，随后被 epoch=2 取代（旧 Owner 已经失去所有权）
        catalog
            .bump_ownership(db.id, 0, worker.clone(), DEFAULT_OWNER_LEASE)
            .await
            .unwrap();
        let current = catalog
            .bump_ownership(db.id, 1, worker.clone(), DEFAULT_OWNER_LEASE)
            .await
            .unwrap();
        assert_eq!(current, 2);

        age_database_lease(&catalog, db.id, DEFAULT_OWNER_LEASE).await;
        let before = catalog
            .get_database(db.id)
            .await
            .unwrap()
            .lease_expires_at
            .expect("租约仍在，只是已过期");

        let stale = LocalDatabaseState {
            database_id: db.id,
            state: LifecycleState::Warm,
            owner_epoch: OwnerEpoch::new(1),
            pid: Some(1),
        };
        catalog
            .record_heartbeat(worker.clone(), &usage, 1, &[stale], WorkerState::Active)
            .await
            .unwrap();

        let after = catalog
            .get_database(db.id)
            .await
            .unwrap()
            .lease_expires_at
            .expect("租约不应被清空");
        assert_eq!(before, after, "已被取代的 epoch 不得借心跳给自己的租约续命");
    }

    /// 已软删除的 DB 不得被心跳续租。
    ///
    /// `soft_delete_database` 只置 `deleted_at` + `state = 'STOPPING'`，**不清** owner 与租约，
    /// 所以 Worker 仍以当前 epoch 上报时，那条「既是状态回写、又是唯一续约点」的 UPDATE
    /// 会照样命中。命中的后果是无限续期：`clear_stale_ownership` 只认「租约已过期」，
    /// 于是这一行长期带着 owner + 有效租约残留（`state` 还会被上报值覆盖掉 STOPPING），
    /// 而按 `deleted_at IS NULL` 过滤的对账回收又够不着它。租约必须能自然过期。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn heartbeat_does_not_renew_lease_of_soft_deleted_database() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "deleted-renew").await;
        let usage = heartbeat_usage();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-deleted-renew-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let epoch = catalog
            .bump_ownership(db.id, 0, worker.clone(), DEFAULT_OWNER_LEASE)
            .await
            .unwrap();

        // 控制面软删除：deleted_at 置位、state -> STOPPING，owner 与租约保持原样
        let deleted = catalog.soft_delete_database(db.id).await.unwrap();
        assert!(deleted.deleted_at.is_some());
        assert_eq!(deleted.state, LifecycleState::Stopping);

        // 模拟「租约已过期」：这正是必须让它保持过期的现场
        age_database_lease(&catalog, db.id, DEFAULT_OWNER_LEASE).await;
        let before = catalog
            .get_database(db.id)
            .await
            .unwrap()
            .lease_expires_at
            .expect("软删除不清租约");

        // Worker 仍带着**当前 epoch** 上报它（本地进程尚未停完）——不得因此续租
        let present = LocalDatabaseState {
            database_id: db.id,
            state: LifecycleState::Warm,
            owner_epoch: OwnerEpoch::new(epoch),
            pid: Some(4242),
        };
        catalog
            .record_heartbeat(worker.clone(), &usage, 1, &[present], WorkerState::Active)
            .await
            .unwrap();

        let after = catalog.get_database(db.id).await.unwrap();
        assert_eq!(
            after.lease_expires_at.expect("软删除不清租约"),
            before,
            "已软删除的 DB 不得被心跳续租，租约必须保持已过期"
        );
        assert_eq!(
            after.state,
            LifecycleState::Stopping,
            "软删除置的 STOPPING 不得被心跳上报的本地状态覆盖"
        );
    }

    /// Worker 注册表里**保留的旧 epoch 条目**不得阻止对账回收。
    ///
    /// db-worker 的正常停止路径有意「保留 epoch 记录，所有权不变」
    /// （见 `supervisor::on_process_exit`），因此停止过的 DB 会一直出现在心跳
    /// inventory 里。若对账只比 id，它会一直认为「Worker 还持有这个 DB」，
    /// 于是启动被中断、Catalog 停在 STARTING 的 DB 永远回收不掉，
    /// 数据面只能等 ownership GC 按租约兜底（数十秒不可用）。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn inventory_entry_with_stale_epoch_does_not_block_reclaim() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "stale-inv").await;
        let usage = heartbeat_usage();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-stale-inv-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        // 上一次启动尝试拿到 epoch=1，被中断后重新接管推进到 epoch=2：
        // 这正是「Catalog 停在 STARTING、Worker 只剩旧条目」的现场
        catalog
            .bump_ownership(db.id, 0, worker.clone(), DEFAULT_OWNER_LEASE)
            .await
            .unwrap();
        let current = catalog
            .bump_ownership(db.id, 1, worker.clone(), DEFAULT_OWNER_LEASE)
            .await
            .unwrap();
        assert_eq!(current, 2);

        expire_reclaim_window(&catalog, db.id).await;

        // 心跳仍带着上个进程留下的 COLD 条目（epoch=1，pid 已清空）
        let stale = LocalDatabaseState {
            database_id: db.id,
            state: LifecycleState::Cold,
            owner_epoch: OwnerEpoch::new(1),
            pid: None,
        };
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 1, &[stale], WorkerState::Active)
            .await
            .unwrap();
        assert_eq!(
            outcome.reclaimed.len(),
            1,
            "携带过期 epoch 的 inventory 条目不能证明 Worker 仍持有该 DB"
        );
        assert_eq!(outcome.reclaimed[0].from_epoch, 2);
        assert_eq!(outcome.reclaimed[0].to_epoch, 3);
        assert_eq!(
            outcome.reclaimed[0].reason,
            reclaim_reason::INVENTORY_MISSING
        );

        let record = catalog.get_database(db.id).await.unwrap();
        assert_eq!(
            record.state,
            LifecycleState::Cold,
            "回收后必须回到可被重新放置的 COLD"
        );
        assert!(record.owner_worker_id.is_none(), "owner 必须清空");
    }

    /// 对照：以**当前 epoch** 认领的条目仍然算「Worker 本地存在」，不得被回收。
    /// 没有这一条，「只比 id」的旧实现固然能通过上面的测试，却会误杀正在服务的库。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn inventory_entry_with_current_epoch_still_blocks_reclaim() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "cur-inv").await;
        let usage = heartbeat_usage();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-cur-inv-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let epoch = catalog
            .bump_ownership(db.id, 0, worker.clone(), DEFAULT_OWNER_LEASE)
            .await
            .unwrap();
        expire_reclaim_window(&catalog, db.id).await;

        let present = LocalDatabaseState {
            database_id: db.id,
            state: LifecycleState::Warm,
            owner_epoch: OwnerEpoch::new(epoch),
            pid: Some(7),
        };
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 1, &[present], WorkerState::Active)
            .await
            .unwrap();
        assert!(
            outcome.reclaimed.is_empty(),
            "以当前 epoch 认领的 DB 仍在 Worker 手上，不得被对账回收"
        );
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            epoch
        );
    }

    /// §11.3：存储层是 Fencing 的权威。Catalog 落后时只能向它对齐，且对齐必须幂等、留审计。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn align_ownership_epoch_with_storage_follows_storage_authority() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "align").await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-align-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let epoch = catalog
            .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(epoch, 1);

        // 存储层已记录到 9（手工干预 / 从备份恢复的典型现场）
        let aligned = catalog
            .align_ownership_epoch_with_storage(db.id, 9, REASON_EPOCH_REALIGN_FROM_STORAGE)
            .await
            .unwrap();
        assert_eq!(aligned, 9);
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            9
        );

        // 幂等：同样的对齐再来一次不改 epoch，也不写第二条审计
        assert_eq!(
            catalog
                .align_ownership_epoch_with_storage(db.id, 9, REASON_EPOCH_REALIGN_FROM_STORAGE)
                .await
                .unwrap(),
            9
        );
        // 只前进：存储层给的值更低时不回退（否则会把 fencing 语义打破）
        assert_eq!(
            catalog
                .align_ownership_epoch_with_storage(db.id, 3, REASON_EPOCH_REALIGN_FROM_STORAGE)
                .await
                .unwrap(),
            9
        );
        // 对齐不得改变 owner：这只是一次「认知修正」，不是所有权转移
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_worker_id,
            Some(worker.clone())
        );

        // 对齐后接管必须严格大于存储层已记录值（这正是 WAL 接受的条件）
        let next = catalog
            .bump_ownership(db.id, aligned, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        assert!(next > 9, "对齐后的接管 epoch 必须严格大于 WAL 已记录值");

        let events = catalog.list_ownership_events(db.id, 10).await.unwrap();
        let realign: Vec<_> = events
            .iter()
            .filter(|e| e.reason == REASON_EPOCH_REALIGN_FROM_STORAGE)
            .collect();
        assert_eq!(realign.len(), 1, "对齐必须留审计，且幂等调用不产生重复审计");
        assert_eq!(realign[0].from_epoch, 1);
        assert_eq!(realign[0].to_epoch, 9);
    }

    /// 缺陷 2 的回归：`owner_epoch` 只增不减 —— 连**绕过 Catalog API 的裸 SQL** 也改不回去。
    ///
    /// 现场故障是「Catalog 被改到低位、Remote WAL 记着高位」：同一份落后的 epoch 会被
    /// Storage-level Fencing 反复拒绝，DB 永久卡在 STARTING。因此不变量必须落在 schema 层
    /// （`migrations/0002_owner_epoch_monotonic.sql` 的触发器），而不是只靠调用方自觉。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn owner_epoch_cannot_be_lowered_by_raw_sql() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "epoch-guard").await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-epoch-guard-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        // 把 epoch 推到 104（模拟「Remote WAL 已记录 104」的现场）
        let mut epoch = catalog
            .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        while epoch < 104 {
            epoch = catalog
                .bump_ownership(db.id, epoch, worker.clone(), Duration::from_secs(30))
                .await
                .unwrap();
        }
        assert_eq!(epoch, 104);

        // 1) 裸 SQL 把它改回低位：必须失败（trigger，SQLSTATE 23514）
        let lowered = sqlx::query("UPDATE databases SET owner_epoch = 1 WHERE id = $1::uuid")
            .bind(crate::pg::id_to_uuid(&db.id).expect("database id 是 uuid"))
            .execute(catalog.pool())
            .await;
        let err = lowered.expect_err("降低 owner_epoch 必须被拒绝");
        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("23514"),
            "{err}"
        );
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            104,
            "被拒绝的写入不得留下任何痕迹"
        );

        // 2) 同一列的「只增」写入（+1）仍然畅通：护栏不能误伤正常路径
        let bumped = catalog
            .bump_ownership(db.id, 104, worker.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(bumped, 105);

        // 3) 对齐路径同样只能前进（幂等 + 拒绝回退），确认两条「只增」路径一致
        assert_eq!(
            catalog
                .align_ownership_epoch_with_storage(db.id, 2, REASON_EPOCH_REALIGN_FROM_STORAGE)
                .await
                .unwrap(),
            105
        );
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            105
        );
    }

    /// 显式重置 API 的两道闸门：必须给出理由、且不得低于存储层已记录的 epoch。
    ///
    /// 这是「确需回退」（开发环境清理）的唯一合法入口：名字里带 `reset`，
    /// 调用点一眼可见，且每一次回退都留审计。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn reset_owner_epoch_requires_reason_and_storage_floor() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "epoch-reset").await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-epoch-reset-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        for expected in 1..=5u64 {
            let epoch = catalog
                .bump_ownership(db.id, expected - 1, worker.clone(), Duration::from_secs(30))
                .await
                .unwrap();
            assert_eq!(epoch, expected);
        }

        // 闸门 1：目标低于存储层已记录值 -> 拒绝（否则该库会被 fencing 永久拒绝）
        let err = catalog
            .reset_owner_epoch_with_reason(db.id, 1, 104, "开发环境清理")
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("104"), "{}", err.message);
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            5
        );

        // 闸门 2：没理由 -> 拒绝（回退必须可审计）
        let err = catalog
            .reset_owner_epoch_with_reason(db.id, 2, 0, "   ")
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            5
        );

        // 合法回退：目标 4 >= 存储层 0，且确实低于当前值
        let after = catalog
            .reset_owner_epoch_with_reason(db.id, 4, 0, "开发环境清理：重建工作集")
            .await
            .unwrap();
        assert_eq!(after, 4);
        let record = catalog.get_database(db.id).await.unwrap();
        assert_eq!(record.owner_epoch.get(), 4);
        // 重置不是所有权转移：owner 不变
        assert_eq!(record.owner_worker_id, Some(worker.clone()));

        let events = catalog.list_ownership_events(db.id, 20).await.unwrap();
        let reset = events
            .iter()
            .find(|e| e.reason.starts_with("epoch_reset:"))
            .expect("显式重置必须留审计");
        assert_eq!(reset.from_epoch, 5);
        assert_eq!(reset.to_epoch, 4);
        assert!(reset.reason.contains("开发环境清理"), "{}", reset.reason);

        // 目标不低于当前值：no-op（本 API 只负责回退，抬升交给 bump/align）
        assert_eq!(
            catalog
                .reset_owner_epoch_with_reason(db.id, 9, 0, "开发环境清理")
                .await
                .unwrap(),
            4
        );
        assert_eq!(
            catalog.get_database(db.id).await.unwrap().owner_epoch.get(),
            4
        );
    }

    /// 缺陷 1（控制面一侧）：排空完成后 Server 必须**明确授权** Worker 重新接纳，
    /// 并且只有 Worker 自报已经回到 ACTIVE 时才把 Catalog 写回 ACTIVE。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn heartbeat_readmits_drained_worker_only_after_it_reports_active() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "readmit").await;
        let usage = heartbeat_usage();

        // 排空中：不授权（§12.3），且 DRAINING 不被心跳覆盖
        catalog
            .mark_worker_state(worker.clone(), WorkerState::Draining)
            .await
            .unwrap();
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 1, &[], WorkerState::Draining)
            .await
            .unwrap();
        assert!(outcome.draining);
        assert!(!outcome.accept_new_placement);
        assert_eq!(
            catalog.get_worker(worker.clone()).await.unwrap().state,
            WorkerState::Draining
        );

        // 排空完成（EMPTY）：Server 授权，但 Catalog 此刻仍是 EMPTY
        catalog
            .mark_worker_state(worker.clone(), WorkerState::Empty)
            .await
            .unwrap();
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 2, &[], WorkerState::Empty)
            .await
            .unwrap();
        assert!(outcome.accept_new_placement);
        assert_eq!(
            catalog.get_worker(worker.clone()).await.unwrap().state,
            WorkerState::Empty,
            "授权不等于已经接纳：Catalog 要等 Worker 自报 ACTIVE"
        );

        // inventory 不为空时不得授权（节点并不空闲）
        let stray = LocalDatabaseState {
            database_id: domain::DatabaseId::new_v7(),
            state: LifecycleState::Warm,
            owner_epoch: OwnerEpoch::new(1),
            pid: Some(7),
        };
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 3, &[stray], WorkerState::Empty)
            .await
            .unwrap();
        assert!(!outcome.accept_new_placement);

        // Worker 收到授权后自行回到 ACTIVE 并如实上报 -> Catalog 重新纳入 Placement
        let outcome = catalog
            .record_heartbeat(worker.clone(), &usage, 4, &[], WorkerState::Active)
            .await
            .unwrap();
        assert!(!outcome.draining);
        assert!(!outcome.accept_new_placement);
        let record = catalog.get_worker(worker).await.unwrap();
        assert_eq!(record.state, WorkerState::Active);
        assert!(record.state.accepts_new_placement());
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn idempotent_begin_creates_single_operation() {
        let catalog = test_catalog().await;
        let key = format!("itest-idem-{}", uuid::Uuid::now_v7());

        let first = catalog.begin_idempotent(&key, "hash-1").await.unwrap();
        let operation_id = first.operation_id().expect("first has operation id");
        assert!(first.is_first());

        let second = catalog.begin_idempotent(&key, "hash-1").await.unwrap();
        assert!(second.is_replay());
        assert_eq!(second.operation_id(), Some(operation_id));

        let conflict = catalog.begin_idempotent(&key, "hash-2").await.unwrap();
        assert!(conflict.is_conflict());

        catalog
            .complete_idempotent(
                &key,
                202,
                serde_json::json!({"operation_id": operation_id.to_string()}),
            )
            .await
            .unwrap();
        match catalog.begin_idempotent(&key, "hash-1").await.unwrap() {
            IdempotencyOutcome::Replay { status, body, .. } => {
                assert_eq!(status, 202);
                assert_eq!(body["operation_id"], operation_id.to_string());
            }
            other => panic!("expected replay, got {other:?}"),
        }
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn job_lease_is_exclusive_and_retryable() {
        let catalog = test_catalog().await;
        let key = format!("itest-job-{}", uuid::Uuid::now_v7());
        // 用本轮唯一的 kind 隔离：jobs 队列是共享的，历史遗留的 READY 行会先于本轮的 job 被租出
        let kind = format!("ITEST_JOB_{}", run_nonce());
        let job = catalog
            .enqueue_job(&kind, serde_json::json!({"db": "x"}), 100, None, Some(&key))
            .await
            .unwrap();
        assert_eq!(job.state, "READY");

        // 幂等入队返回同一 job
        let again = catalog
            .enqueue_job(&kind, serde_json::json!({}), 100, None, Some(&key))
            .await
            .unwrap();
        assert_eq!(again.id, job.id);

        let leased = catalog
            .lease_job("owner-a", Duration::from_secs(60), &[kind.as_str()])
            .await
            .unwrap()
            .expect("lease some job");
        assert_eq!(leased.id, job.id);
        assert_eq!(leased.state, "LEASED");
        assert_eq!(leased.attempts, 1);

        // 已租出，第二个 Worker 抢不到同一行
        let other = catalog
            .lease_job("owner-b", Duration::from_secs(60), &[kind.as_str()])
            .await
            .unwrap();
        assert!(other.is_none_or(|j| j.id != job.id));

        // 完成后不再可租
        let done = catalog.complete_job(job.id, true, None).await.unwrap();
        assert_eq!(done.state, "DONE");

        let failed = catalog
            .enqueue_job(&kind, serde_json::json!({}), 100, None, None)
            .await
            .unwrap();
        let leased = catalog
            .lease_job("owner-a", Duration::from_secs(60), &[kind.as_str()])
            .await
            .unwrap()
            .expect("lease failed job");
        assert_eq!(leased.id, failed.id);
        let retried = catalog
            .complete_job(failed.id, false, Some("boom".into()))
            .await
            .unwrap();
        assert_eq!(retried.state, "READY");
        assert_eq!(retried.last_error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn snapshot_registration_and_audit_append() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-snap-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let snapshot = SnapshotRecord {
            // snapshots.id 在 schema 中是 TEXT，但 domain 的 SnapshotId 是 UUID newtype
            id: domain::ids::SnapshotId::new_v7()
                .to_string()
                .parse()
                .unwrap(),
            database_id: db.id,
            base_lsn: Lsn::new(42),
            checksum: "deadbeef".into(),
            size_bytes: 1_024,
            object_key: "snapshots/x".into(),
            compression: "zstd".into(),
            owner_epoch: OwnerEpoch::new(1),
            engine_version: "0.1.0".into(),
            schema_version: 0,
            state: "AVAILABLE".into(),
            created_at: domain::time::unix_ms_to_datetime(domain::time::now_unix_ms()),
            verified_at: None,
        };
        let stored = catalog.insert_snapshot(snapshot.clone()).await.unwrap();
        assert_eq!(stored.id, snapshot.id);
        let latest = catalog.latest_snapshot(db.id).await.unwrap().unwrap();
        assert_eq!(latest.id, snapshot.id);

        let audit_id = catalog
            .append_audit(
                AuditEntry::success("DB_CREATE")
                    .with_database(db.id)
                    .with_tenant(db.tenant_id)
                    .with_target("database", db.id.to_string()),
            )
            .await
            .unwrap();
        assert!(audit_id > 0);

        let entries = catalog
            .list_audit(
                10,
                0,
                AuditFilter {
                    database_id: Some(db.id),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "DB_CREATE");
    }

    /// §16 契约：同一个 Idempotency-Key 并发提交只能产生一个 Operation（只允许一个 First）。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn concurrent_idempotent_submissions_produce_single_first() {
        let catalog = test_catalog().await;
        let key = format!("itest-conc-idem-{}", uuid::Uuid::now_v7());

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let catalog = catalog.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move {
                catalog.begin_idempotent(&key, "same-hash").await
            }));
        }

        let mut firsts = 0;
        let mut replays = 0;
        let mut op_ids = std::collections::HashSet::new();
        for task in tasks {
            match task.await.unwrap().unwrap() {
                IdempotencyOutcome::First { operation_id } => {
                    firsts += 1;
                    op_ids.insert(operation_id);
                }
                IdempotencyOutcome::Replay { operation_id, .. } => {
                    replays += 1;
                    op_ids.insert(operation_id);
                }
                IdempotencyOutcome::Conflict => panic!("相同 request_hash 不应判定为 Conflict"),
            }
        }
        assert_eq!(firsts, 1, "并发提交只允许一个 First");
        assert_eq!(replays, 15);
        assert_eq!(op_ids.len(), 1, "所有重放必须指向同一个 operation_id");
    }

    /// §10 契约：并发接管同一个 DB 只允许一个成功（防 Split Brain），其余必须 EPOCH_MISMATCH。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn concurrent_ownership_bumps_have_exactly_one_winner() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-conc-own-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let mut workers = Vec::new();
        for i in 0..4 {
            workers.push(register_worker(&catalog, &format!("own-{i}")).await);
        }

        let mut tasks = Vec::new();
        for worker in workers {
            let catalog = catalog.clone();
            let db_id = db.id;
            tasks.push(tokio::spawn(async move {
                catalog
                    .bump_ownership(db_id, 0, worker, Duration::from_secs(60))
                    .await
            }));
        }

        let mut winners = 0;
        let mut mismatches = 0;
        for task in tasks {
            match task.await.unwrap() {
                Ok(epoch) => {
                    assert_eq!(epoch, 1);
                    winners += 1;
                }
                Err(err) => {
                    assert_eq!(err.code, ErrorCode::EpochMismatch);
                    mismatches += 1;
                }
            }
        }
        assert_eq!(winners, 1, "同一 epoch 只能有一个接管者");
        assert_eq!(mismatches, 3);

        // 每次成功接管都必须留下审计行（用于事后 Split Brain 校验）
        let events = catalog.list_ownership_events(db.id, 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].from_epoch, 0);
        assert_eq!(events[0].to_epoch, 1);
    }

    /// §8 契约：并发冷启动只允许一个 Leader，其余等待同一个 READY 结果。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn concurrent_wakeup_has_single_leader() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-conc-wake-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let catalog = catalog.clone();
            let db_id = db.id;
            tasks.push(tokio::spawn(async move {
                catalog.try_begin_wakeup(db_id).await
            }));
        }

        let mut leaders = 0;
        let mut waiters = 0;
        for task in tasks {
            match task.await.unwrap().unwrap() {
                WakeupDecision::Leader => leaders += 1,
                WakeupDecision::Waiter => waiters += 1,
                WakeupDecision::AlreadyRunning(state) => {
                    panic!("COLD DB 的并发唤醒不应看到 {state:?}")
                }
            }
        }
        assert_eq!(leaders, 1, "Coalesce Wakeup 只允许一个 Leader");
        assert_eq!(waiters, 7);

        catalog.end_wakeup(db.id).await.unwrap();
        assert_eq!(
            catalog.try_begin_wakeup(db.id).await.unwrap(),
            WakeupDecision::Leader,
            "end_wakeup 后应可再次发起冷启动"
        );
    }

    /// Job 队列契约：并发抢占不会把同一行租给两个 Worker。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn concurrent_job_leases_are_exclusive() {
        let catalog = test_catalog().await;
        let kind = format!("ITEST_CONC_LEASE_{}", run_nonce());
        let total = 8;
        for i in 0..total {
            catalog
                .enqueue_job(&kind, serde_json::json!({"i": i}), 100, None, None)
                .await
                .unwrap();
        }

        let mut tasks = Vec::new();
        for i in 0..total {
            let catalog = catalog.clone();
            let kind = kind.clone();
            tasks.push(tokio::spawn(async move {
                catalog
                    .lease_job(
                        &format!("owner-{i}"),
                        Duration::from_secs(60),
                        &[kind.as_str()],
                    )
                    .await
            }));
        }

        let mut leased_ids = std::collections::HashSet::new();
        for task in tasks {
            let job = task
                .await
                .unwrap()
                .unwrap()
                .expect("每个 Worker 都应抢到任务");
            assert_eq!(job.state, "LEASED");
            assert_eq!(job.attempts, 1);
            assert!(leased_ids.insert(job.id), "同一个 job 被租给了两个 Worker");
        }
        assert_eq!(leased_ids.len(), total as usize);
    }

    /// 租约过期的 Ownership 必须能被回收，并带上 epoch 递增与新审计行。
    ///
    /// FIX-C：Route Cache 快照只允许下发**租约未过期**的 owner，
    /// 否则 Server 会把请求路由到 Catalog 已认为失去所有权的进程上（Worker 侧 EPOCH_MISMATCH 抖动）。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn stale_ownership_is_reclaimed_and_routing_snapshot_updates() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "stale").await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-stale-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        // 租约给足：本用例手动把租约挪到过去来制造「过期」，不依赖 sleep，避免时序抖动
        let epoch = catalog
            .bump_ownership(db.id, 0, worker, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(epoch, 1);

        // STARTING 的路由必须出现在快照里（Transparent Wake 需要它作为等待目标）
        let entry = routing_entry(&catalog, db.id)
            .await
            .expect("STARTING 的路由必须在快照里");
        assert_eq!(entry.owner_epoch, 1);
        assert_eq!(entry.state, LifecycleState::Starting);
        assert!(
            entry.lease_expires_at.expect("接管后必须带租约") > chrono::Utc::now(),
            "租约未过期的 owner 才允许下发"
        );

        // 模拟 Worker 崩溃 -> 租约过期：该 owner 不得再出现在快照里
        expire_lease(&catalog, db.id, Duration::from_secs(5)).await;
        assert!(
            routing_entry(&catalog, db.id).await.is_none(),
            "租约已过期的 owner 不得下发给 Route Cache（FIX-C）"
        );

        let cleared = catalog
            .clear_stale_ownership(Duration::from_millis(0))
            .await
            .unwrap();
        assert!(cleared.contains(&db.id), "过期租约的 DB 应被回收");

        let after = catalog.get_database(db.id).await.unwrap();
        assert_eq!(after.state, LifecycleState::Cold);
        assert!(after.owner_worker_id.is_none());
        assert!(after.lease_expires_at.is_none());
        assert_eq!(
            after.owner_epoch.get(),
            2,
            "回收必须递增 epoch 以 fencing 旧 Owner"
        );

        let routing = catalog.list_routing_entries().await.unwrap();
        assert!(
            !routing.iter().any(|e| e.database_id == db.id),
            "失去 Owner 的 DB 不应再出现在 Route Cache 快照里"
        );
        let events = catalog.list_ownership_events(db.id, 10).await.unwrap();
        assert_eq!(events[0].reason, "lease_expired");
        assert_eq!(events[0].to_epoch, 2);
    }

    /// FIX-C 的反向约束：过滤只针对「租约过期」，不得把 STARTING / DRAINING / STOPPING / FAILED
    /// 从快照里抹掉 —— Server 需要这些非可服务态来决定等待（STARTING）、
    /// 停止发新流量（DRAINING / STOPPING）与触发 refresh（FAILED）。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn routing_snapshot_keeps_non_serving_states_with_valid_lease() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "route-states").await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-route-state-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        catalog
            .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(60))
            .await
            .unwrap();

        // 沿 §7 的合法链路走一遍：STARTING -> WARM -> DRAINING -> STOPPING -> FAILED
        for state in [
            LifecycleState::Starting,
            LifecycleState::Warm,
            LifecycleState::Draining,
            LifecycleState::Stopping,
            LifecycleState::Failed,
        ] {
            if state != LifecycleState::Starting {
                catalog
                    .set_lifecycle_state(db.id, state, Some(worker.clone()))
                    .await
                    .unwrap();
            }
            let entry = routing_entry(&catalog, db.id)
                .await
                .unwrap_or_else(|| panic!("{state} 的路由必须在快照里（FIX-C 只按租约过滤）"));
            assert_eq!(entry.state, state);
            assert_eq!(entry.owner_worker_id, worker);
        }
    }

    /// FIX-D 回归：§7 状态机必须约束 ownership 接管的前驱
    /// （HOT / DRAINING 下旧 Owner 可能仍在写数据，直接接管会制造双进程）。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn ownership_bump_rejects_hot_and_draining_predecessors() {
        let catalog = test_catalog().await;
        let worker = register_worker(&catalog, "lifecycle-guard").await;

        // 拒绝路径：直接构造带 owner 的活跃态（模拟 Catalog 里遗留的 HOT / DRAINING 行）
        for state in [LifecycleState::Hot, LifecycleState::Draining] {
            let db = catalog
                .create_database(CreateDatabaseParams {
                    state: Some(state),
                    ..CreateDatabaseParams::new(format!(
                        "itest-guard-{}-{}",
                        state.to_db_str(),
                        uuid::Uuid::now_v7()
                    ))
                })
                .await
                .unwrap();

            let err = catalog
                .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(30))
                .await
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidArgument);
            assert!(err.message.contains(state.to_db_str()), "{}", err.message);
            assert!(err.message.contains("STARTING"), "{}", err.message);

            // 拒绝必须零副作用：epoch / owner 都不能变
            let after = catalog.get_database(db.id).await.unwrap();
            assert_eq!(after.owner_epoch, OwnerEpoch::ZERO);
            assert!(after.owner_worker_id.is_none());
            assert_eq!(after.state, state);
        }

        // 允许路径：进程已失去的前驱（COLD / FAILED / WARM / STARTING / STOPPING）
        for state in [
            LifecycleState::Cold,
            LifecycleState::Failed,
            LifecycleState::Warm,
            LifecycleState::Starting,
            LifecycleState::Stopping,
        ] {
            let db = catalog
                .create_database(CreateDatabaseParams {
                    state: Some(state),
                    ..CreateDatabaseParams::new(format!(
                        "itest-guard-ok-{}-{}",
                        state.to_db_str(),
                        uuid::Uuid::now_v7()
                    ))
                })
                .await
                .unwrap();

            let epoch = catalog
                .bump_ownership(db.id, 0, worker.clone(), Duration::from_secs(30))
                .await
                .unwrap_or_else(|e| panic!("{state} 应当允许被接管: {}", e.message));
            assert_eq!(epoch, 1);
            let after = catalog.get_database(db.id).await.unwrap();
            assert_eq!(after.state, LifecycleState::Starting);
            assert_eq!(after.owner_worker_id.as_ref(), Some(&worker));
        }
    }

    /// FIX-B 回归：幂等记录与 Operation 必须在同一个事务里落库；
    /// 「插了 idempotency 但没插 operation」的历史脏数据必须能自愈，而不是永久吐死。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn idempotent_operation_is_atomic_and_heals_dirty_rows() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-idem-op-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();
        let key = format!("itest-idem-op-key-{}", uuid::Uuid::now_v7());

        // 1) First：幂等记录与本条 Operation 同事务提交 —— 拿到 id 的瞬间它必须已经可查
        let first = catalog
            .begin_idempotent_operation(
                &key,
                "hash-1",
                NewOperation::new("START_DB").with_database(db.id),
            )
            .await
            .unwrap();
        let operation_id = match first {
            IdempotencyOperationOutcome::First { operation_id } => operation_id,
            other => panic!("首次请求应为 First，得到 {other:?}"),
        };
        let stored = catalog
            .get_operation(operation_id)
            .await
            .expect("First 返回的 Operation 必须真实存在（同事务提交，FIX-B）");
        assert_eq!(stored.kind, "START_DB");
        assert_eq!(stored.state, "PENDING");
        assert_eq!(stored.idempotency_key.as_deref(), Some(key.as_str()));
        assert_eq!(stored.tenant_id, Some(db.tenant_id));

        // 2) Replay：同 key 同 hash 不再创建 Operation
        match catalog
            .begin_idempotent_operation(&key, "hash-1", NewOperation::new("START_DB"))
            .await
            .unwrap()
        {
            IdempotencyOperationOutcome::Replay {
                operation_id: replay_id,
                status,
                ..
            } => {
                assert_eq!(replay_id, operation_id);
                assert_eq!(status, 202);
            }
            other => panic!("重复提交应为 Replay，得到 {other:?}"),
        }

        // 3) Conflict：同 key 不同请求体
        let conflict = catalog
            .begin_idempotent_operation(&key, "hash-2", NewOperation::new("START_DB"))
            .await
            .unwrap();
        assert!(conflict.is_conflict());

        // 4) 非法 kind 在写库前就被拒（不占用事务）
        let bad = catalog
            .begin_idempotent_operation("itest-bad-kind", "h", NewOperation::new("BOGUS"))
            .await
            .unwrap_err();
        assert_eq!(bad.code, ErrorCode::InvalidArgument);

        // 5) 脏数据自愈：旧实现「先提交 idempotency_keys、再建 operations」中途崩溃的现场
        let dirty_key = format!("itest-idem-dirty-{}", uuid::Uuid::now_v7());
        let dangling = domain::ids::OperationId::new_v7();
        sqlx::query(
            "INSERT INTO idempotency_keys
                 (key, request_hash, operation_id, response_status, response_body)
             VALUES ($1::text, $2::text, $3::uuid, 202, '{}'::jsonb)",
        )
        .bind(&dirty_key)
        .bind("hash-dirty")
        .bind(crate::pg::id_to_uuid(&dangling).unwrap())
        .execute(catalog.pool())
        .await
        .unwrap();
        assert!(
            catalog.get_operation(dangling).await.is_err(),
            "脏数据现场：幂等记录指向的 Operation 并不存在"
        );

        // 自愈必须复用脏记录里已有的 operation_id（不能凭空再造一个，否则客户端拿到的 id 会漂移）
        match catalog
            .begin_idempotent_operation(
                &dirty_key,
                "hash-dirty",
                NewOperation::new("BACKUP_DB").with_database(db.id),
            )
            .await
            .unwrap()
        {
            IdempotencyOperationOutcome::First { operation_id } => {
                assert_eq!(
                    operation_id, dangling,
                    "自愈必须复用幂等记录中的 operation_id"
                );
            }
            other => panic!("脏数据应当回退为 First 并补建 Operation，得到 {other:?}"),
        }
        assert_eq!(catalog.get_operation(dangling).await.unwrap().id, dangling);

        // 自愈只发生一次：之后回到正常 Replay，不会每次重试都重建
        match catalog
            .begin_idempotent_operation(&dirty_key, "hash-dirty", NewOperation::new("BACKUP_DB"))
            .await
            .unwrap()
        {
            IdempotencyOperationOutcome::Replay { operation_id, .. } => {
                assert_eq!(operation_id, dangling);
            }
            other => panic!("自愈后应回到 Replay，得到 {other:?}"),
        }
    }

    /// Operations 生命周期 + 幂等键唯一约束。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn operation_lifecycle_and_idempotency_key_uniqueness() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-op-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let op = catalog
            .create_operation("START_DB", Some(db.id), None, None, None)
            .await
            .unwrap();
        assert_eq!(op.state, "PENDING");
        assert_eq!(op.progress, 0);
        assert_eq!(op.tenant_id, Some(db.tenant_id), "tenant 应从 DB 反查填入");

        let running = catalog
            .update_operation(op.id, "RUNNING", 50, None, serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(running.progress, 50);
        assert!(running.finished_at.is_none());

        let failed = catalog
            .update_operation(
                op.id,
                "FAILED",
                100,
                Some((ErrorCode::WakeupTimeout, "cold start timeout".into())),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(failed.error_code, Some(ErrorCode::WakeupTimeout));
        assert!(failed.finished_at.is_some());

        // 非法 kind / state / progress 必须在写库前被拒
        assert_eq!(
            catalog
                .create_operation("BOGUS", None, None, None, None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            catalog
                .update_operation(op.id, "BOGUS", 10, None, serde_json::json!({}))
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );

        // idempotency_key 唯一索引：同一 key 不能创建第二个 Operation
        let key = format!("itest-op-key-{}", uuid::Uuid::now_v7());
        catalog
            .create_operation("BACKUP_DB", Some(db.id), None, None, Some(&key))
            .await
            .unwrap();
        let dup = catalog
            .create_operation("BACKUP_DB", Some(db.id), None, None, Some(&key))
            .await;
        assert_eq!(dup.unwrap_err().code, ErrorCode::IdempotencyConflict);

        let listed = catalog.list_operations(50, 0).await.unwrap();
        assert!(listed.iter().any(|o| o.id == op.id));
        assert_eq!(catalog.get_operation(op.id).await.unwrap().id, op.id);
    }

    /// RBAC 全链路：用户 -> 角色权限合并 -> token 认证 -> 吊销。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn rbac_user_token_and_permission_resolution() {
        let catalog = test_catalog().await;
        let username = format!("itest-user-{}", run_nonce());
        let user = catalog
            .create_user(NewUser::new(username.clone()))
            .await
            .unwrap();
        assert!(user.is_active());
        assert_eq!(
            catalog
                .find_user_by_username(&username)
                .await
                .unwrap()
                .unwrap()
                .id,
            user.id
        );
        assert!(catalog
            .find_user_by_username(&format!("missing-{username}"))
            .await
            .unwrap()
            .is_none());
        // 无角色绑定 -> 无权限
        assert!(catalog
            .resolve_permissions(user.id)
            .await
            .unwrap()
            .is_empty());

        // 绑定 schema 内置的 dba 角色（00000000-0000-0000-0000-000000000011）
        sqlx::query(
            "INSERT INTO role_bindings (id, user_id, role_id, tenant_id, database_id)
             VALUES ($1::uuid, $2::uuid, $3::uuid, NULL, NULL)",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(user.id.as_uuid())
        .bind(
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000011")
                .expect("内置角色 id 必须是合法 uuid"),
        )
        .execute(catalog.pool())
        .await
        .unwrap();
        let permissions = catalog.resolve_permissions(user.id).await.unwrap();
        assert!(permissions.contains(&"db:admin".to_string()));
        assert!(permissions.contains(&"audit:read".to_string()));
        let token_database = catalog
            .create_database(CreateDatabaseParams::new(format!("token-{}", run_nonce())))
            .await
            .unwrap();

        let token_hash = format!("hash-{}", run_nonce());
        let token = catalog
            .create_api_token(NewApiToken {
                user_id: user.id,
                name: "itest".into(),
                token_hash: token_hash.clone(),
                tenant_id: None,
                database_id: Some(token_database.id),
                permissions: serde_json::json!([]),
                expires_at: None,
            })
            .await
            .unwrap();
        assert_eq!(token.token_hash, token_hash, "只存哈希，不回显明文");

        let authed = catalog
            .find_user_by_token_hash(&token_hash)
            .await
            .unwrap()
            .expect("有效 token 应能认证");
        assert_eq!(authed.user.id, user.id);
        assert_eq!(authed.token.id, token.id);

        assert!(catalog.revoke_api_token(token.id).await.unwrap());
        assert!(
            !catalog.revoke_api_token(token.id).await.unwrap(),
            "吊销是幂等的"
        );
        assert!(
            catalog
                .find_user_by_token_hash(&token_hash)
                .await
                .unwrap()
                .is_none(),
            "已吊销 token 不得通过认证"
        );

        let tokens = catalog.list_tokens_for_user(user.id).await.unwrap();
        assert_eq!(tokens.len(), 1);
        assert!(tokens[0].is_revoked());
        assert!(catalog
            .list_users(100, 0)
            .await
            .unwrap()
            .iter()
            .any(|u| u.id == user.id));
    }

    #[tokio::test]
    #[ignore = "需要隔离 PostgreSQL 测试库：DATABASE_URL"]
    async fn concurrent_token_create_and_rotation_leave_one_active_per_database() {
        let catalog = test_catalog().await;
        let suffix = run_nonce();
        let user = catalog
            .create_user(NewUser::new(format!("itest-token-race-{suffix}")))
            .await
            .unwrap();
        let database = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-token-race-{suffix}"
            )))
            .await
            .unwrap();
        let other_database = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-token-other-{suffix}"
            )))
            .await
            .unwrap();
        let make_token = |database_id, hash: String| NewApiToken {
            user_id: user.id,
            name: "race".into(),
            token_hash: hash,
            tenant_id: None,
            database_id: Some(database_id),
            permissions: serde_json::json!(["db:read"]),
            expires_at: None,
        };
        let other = catalog
            .create_api_token(make_token(
                other_database.id,
                format!("{suffix}-other-token"),
            ))
            .await
            .unwrap();

        let (a, b) = tokio::join!(
            catalog.create_api_token(make_token(database.id, format!("{suffix}-create-a"))),
            catalog.create_api_token(make_token(database.id, format!("{suffix}-create-b"))),
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let tokens = catalog.list_tokens_for_user(user.id).await.unwrap();
        assert_eq!(
            tokens
                .iter()
                .filter(|token| token.database_id == Some(database.id) && !token.is_revoked())
                .count(),
            1
        );

        let (a, b) = tokio::join!(
            catalog.rotate_api_token(make_token(database.id, format!("{suffix}-rotate-a"))),
            catalog.rotate_api_token(make_token(database.id, format!("{suffix}-rotate-b"))),
        );
        assert!(a.is_ok() && b.is_ok());
        let tokens = catalog.list_tokens_for_user(user.id).await.unwrap();
        assert_eq!(
            tokens
                .iter()
                .filter(|token| token.database_id == Some(database.id) && !token.is_revoked())
                .count(),
            1
        );
        assert!(tokens
            .iter()
            .any(|token| token.id == other.id && !token.is_revoked()));
    }

    /// Panel 数据：preferences / saved queries / slow queries。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn panel_preferences_saved_queries_and_slow_queries() {
        let catalog = test_catalog().await;
        let user = catalog
            .create_user(NewUser::new(format!("itest-panel-{}", run_nonce())))
            .await
            .unwrap();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-panel-db-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        assert!(catalog
            .get_preference(user.id, "theme")
            .await
            .unwrap()
            .is_none());
        catalog
            .set_preference(user.id, "theme", &serde_json::json!({"mode": "dark"}))
            .await
            .unwrap();
        catalog
            .set_preference(user.id, "theme", &serde_json::json!({"mode": "light"}))
            .await
            .unwrap();
        assert_eq!(
            catalog
                .get_preference(user.id, "theme")
                .await
                .unwrap()
                .unwrap()["mode"],
            "light",
            "同 key 写入必须 upsert"
        );
        assert_eq!(catalog.list_preferences(user.id).await.unwrap().len(), 1);

        let saved = catalog
            .create_saved_query(NewSavedQuery {
                user_id: user.id,
                database_id: Some(db.id),
                name: "top tables".into(),
                sql: "SELECT 1".into(),
                description: None,
                tags: vec!["perf".into()],
            })
            .await
            .unwrap();
        let listed = catalog.list_saved_queries(user.id, 10).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, saved.id);
        assert_eq!(listed[0].tags, vec!["perf".to_string()]);
        assert!(catalog.delete_saved_query(saved.id, user.id).await.unwrap());
        assert!(
            !catalog.delete_saved_query(saved.id, user.id).await.unwrap(),
            "重复删除返回 false"
        );

        catalog
            .insert_slow_query(NewSlowQuery {
                database_id: db.id,
                worker_id: None,
                session_id: None,
                fingerprint: None,
                sql_text: "SELECT pg_sleep(1)".into(),
                duration_micros: 1_500_000,
                rows_returned: 0,
                error_code: None,
            })
            .await
            .unwrap();
        let slow = catalog
            .list_slow_queries(db.id, 10, 1_000_000)
            .await
            .unwrap();
        assert_eq!(slow.len(), 1);
        assert_eq!(slow[0].duration_micros, 1_500_000);
        assert!(
            catalog
                .list_slow_queries(db.id, 10, 10_000_000)
                .await
                .unwrap()
                .is_empty(),
            "低于阈值的慢查询不应返回"
        );
    }

    /// Snapshot / Backup：状态推进与列表。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn backup_job_lifecycle() {
        let catalog = test_catalog().await;
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-backup-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        assert!(catalog.latest_snapshot(db.id).await.unwrap().is_none());
        assert_eq!(
            catalog
                .create_backup_job(CreateBackupJobParams {
                    database_id: db.id,
                    kind: "BOGUS".into(),
                    operation_id: None,
                    snapshot_id: None,
                    target_time: None,
                })
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );

        let job = catalog
            .create_backup_job(CreateBackupJobParams {
                database_id: db.id,
                kind: "BACKUP".into(),
                operation_id: None,
                snapshot_id: None,
                target_time: None,
            })
            .await
            .unwrap();
        assert_eq!(job.state, "PENDING");
        assert!(job.finished_at.is_none());

        let snapshot_id = domain::ids::SnapshotId::new_v7().to_string();
        let done = catalog
            .update_backup_job_state(
                job.id,
                BackupJobUpdate {
                    state: "SUCCEEDED".into(),
                    snapshot_id: Some(snapshot_id.clone()),
                    actual_point: Some(domain::time::unix_ms_to_datetime(
                        domain::time::now_unix_ms(),
                    )),
                    bytes_transferred: Some(2_048),
                    error_message: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(done.state, "SUCCEEDED");
        assert_eq!(done.snapshot_id.as_deref(), Some(snapshot_id.as_str()));
        assert_eq!(done.bytes_transferred, 2_048);
        assert!(done.finished_at.is_some());

        let jobs = catalog.list_backup_jobs(db.id, 10).await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, job.id);
    }

    /// §17.4：LISTEN/NOTIFY 作为加速提示必须真的送达（正确性由 catalog_version 兜底）。
    #[tokio::test]
    #[ignore = "需要 PostgreSQL：DATABASE_URL"]
    async fn watcher_receives_catalog_change_notification() {
        let catalog = test_catalog().await;
        let mut watcher = catalog.watch_catalog_changes().await.unwrap();
        let db = catalog
            .create_database(CreateDatabaseParams::new(format!(
                "itest-watch-{}",
                uuid::Uuid::now_v7()
            )))
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let expected = db.id.to_string();
        let mut matched = None;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, watcher.recv()).await {
                Ok(Ok(change)) => {
                    if change.id == expected {
                        matched = Some(change);
                        break;
                    }
                }
                Ok(Err(err)) => panic!("watcher 报错: {err:?}"),
                Err(_) => break,
            }
        }
        let change = matched.expect("应在超时前收到目标 DB 的 catalog_changes 通知");
        assert_eq!(change.table, "databases");
        assert_eq!(change.op, "INSERT");
        assert!(change.version >= 1);
        assert_eq!(watcher.last_version(), Some(change.version));
        assert!(catalog.current_catalog_version().await.unwrap() >= change.version);
    }
}
