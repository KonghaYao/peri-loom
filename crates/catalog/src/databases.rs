//! Database catalog：生命周期、Ownership / Epoch、Coalesce Wakeup、Route Cache 全量视图。
//!
//! 所有状态变更都走显式事务 + 行锁，epoch 校验在 SQL 的 WHERE 子句内完成，
//! 保证「同一个 DB 同时只有一个有效 Owner」（架构 §10 / §15.2）。

use std::time::Duration;

use chrono::{DateTime, Utc};
use domain::error::{ErrorCode, Result};
use domain::ids::{DatabaseId, TenantId, WorkerId};
use domain::lifecycle::LifecycleState;
use domain::records::DatabaseRecord;
use uuid::Uuid;

use crate::error::{
    catalog_error, map_sqlx_error, platform_error, CatalogError, ConflictAs, NotFoundAs,
};
use crate::pg::{
    bind_all, id_to_uuid, invalid_argument, millis, SqlBuilder, SqlParam, DATABASE_COLUMNS,
};
use crate::Catalog;

/// 默认 tenant（migrations/0001_init.sql 内置的 `default` tenant）。
pub const DEFAULT_TENANT_UUID: &str = "00000000-0000-0000-0000-000000000001";

/// 默认 tenant id。字符串为编译期常量，解析不会失败。
pub fn default_tenant_id() -> TenantId {
    DEFAULT_TENANT_UUID
        .parse()
        .expect("DEFAULT_TENANT_UUID 必须是合法 uuid")
}

/// 全局版本号所在的单行表主键。
const CATALOG_VERSION_ROW: i32 = 1;

// ------------------------------------------------------------------ 参数 / 结果

/// 创建 DB 的参数；未显式给出的字段沿用 migrations 中的默认值。
#[derive(Debug, Clone, Default)]
pub struct CreateDatabaseParams {
    pub id: Option<DatabaseId>,
    pub tenant_id: Option<TenantId>,
    pub name: String,
    pub state: Option<LifecycleState>,
    pub cpu_milli: Option<u64>,
    pub memory_mib: Option<u64>,
    pub fd_limit: Option<u64>,
    pub disk_mib: Option<u64>,
    pub iops_limit: Option<u64>,
    pub priority: Option<i32>,
    pub evictable: Option<bool>,
    pub storage_region: Option<String>,
    pub storage_prefix: Option<String>,
    pub engine_version: Option<String>,
    pub schema_version: Option<i32>,
    pub labels: Option<serde_json::Value>,
}

impl CreateDatabaseParams {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }
}

/// 列表过滤条件（全部字段走绑定参数）。
#[derive(Debug, Clone, Default)]
pub struct DatabaseFilter {
    pub tenant_id: Option<TenantId>,
    /// 按 owner_worker_id 过滤
    pub worker_id: Option<WorkerId>,
    pub state: Option<LifecycleState>,
    /// 名称前缀匹配（LIKE 元字符会被转义）
    pub name_prefix: Option<String>,
    /// 是否包含已软删除的 DB（默认不包含）
    pub include_deleted: bool,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// 冷启动并发合并（架构 §8）的判定结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeupDecision {
    /// 赢得 Coalesce Wakeup，负责真正执行冷启动，结束后必须调用 `end_wakeup`
    Leader,
    /// 已有其他 Wakeup 在进行：等待同一个 READY 结果
    Waiter,
    /// DB 已在服务或已在启动流程中
    AlreadyRunning(LifecycleState),
}

impl WakeupDecision {
    pub fn is_leader(&self) -> bool {
        matches!(self, WakeupDecision::Leader)
    }

    /// 是否需要等待 READY（Waiter 或正在 STARTING）。
    pub fn should_wait(&self) -> bool {
        match self {
            WakeupDecision::Leader => false,
            WakeupDecision::Waiter => true,
            WakeupDecision::AlreadyRunning(state) => !state.is_serving(),
        }
    }
}

/// list_routing_entries 的 `query_as` 行类型（列顺序必须与 SELECT 一致）。
type RoutingRow = (Uuid, String, i64, String, Option<DateTime<Utc>>);

/// Server Route Cache 的全量 reconcile 视图（db -> owner）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingEntry {
    pub database_id: DatabaseId,
    pub owner_worker_id: WorkerId,
    pub owner_epoch: u64,
    pub state: LifecycleState,
    /// Owner 租约到期时间（`None` = 该 Owner 没有租约）。
    ///
    /// 调用方需要它来判断「这条路由还能用多久」：`None` 与「即将过期」都不代表路由不可用，
    /// 但可以让 Server 提前预热 / 提前 refresh。租约**已经**过期的 owner 不会再被下发
    /// （见 [`Catalog::list_routing_entries`]）。
    pub lease_expires_at: Option<DateTime<Utc>>,
}

/// Coalesce Wakeup 的默认租约（架构 §8）。
///
/// Wakeup 是「某个人正在把 COLD DB 拉起来」的独占标记，它必须在持有者崩溃后能自动失效：
/// 若持有者在推进到 `bump_ownership` 之前消失，标记就会永远停在 TRUE，
/// 该 DB 的冷启动将永久卡死（FIX-A）。超过本租约仍未推进的 wakeup 视为失效：
/// 其他调用方可以接手（[`Catalog::try_begin_wakeup_with_lease`]），
/// janitor 也可以直接回收（[`Catalog::reclaim_stale_wakeups`]）。
///
/// 15s 的取值：§16 冷启动 P99 <= 1s，留一个数量级余量覆盖「恢复 Snapshot + Replay WAL」；
/// 同时远小于调用方 Deadline 的量级，保证卡死的 wakeup 不会长时间堵住冷启动。
pub const DEFAULT_WAKEUP_LEASE: Duration = Duration::from_secs(15);

/// Ownership 租约的默认 TTL：Owner 必须在此之前**续约**，否则由
/// [`Catalog::clear_stale_ownership`] 回收（架构 §10 / §16）。
///
/// 与 [`DEFAULT_WAKEUP_LEASE`] 分工不同：wakeup 租约是「某个人正在把 COLD DB 拉起来」
/// 的**启动**独占标记；本租约是「某台 Worker 正在为这个 DB 提供进程」的**存活**标记。
///
/// **发放点与续约点必须用同一个值**：发放点是 [`Catalog::bump_ownership`]，
/// 续约点是 Worker 心跳（[`Catalog::record_heartbeat`]）。心跳若用更短的 TTL 续约，
/// 刚发出的租约会在下一个心跳到达之前就过期；用更长的，Worker 掉线后的回收会被推迟。
/// 生产端以 `db-server::config::OWNERSHIP_LEASE_TTL` 引用的正是本常量。
///
/// 30s 的取值：心跳 1 s / 次，30 个心跳周期足以吸收抖动与短暂网络分区；
/// 与 [`DEFAULT_WAKEUP_LEASE`] 同量级，且「TTL + 回收 grace」仍留在 §16 的
/// 30s 收敛目标的一个可解释倍数内（Worker 整机故障时按心跳 miss 另行判定）。
pub const DEFAULT_OWNER_LEASE: Duration = Duration::from_secs(30);

/// [`Catalog::align_ownership_epoch_with_storage`] 的标准审计原因：控制面发现自己的
/// `owner_epoch` 落后于 Remote WAL，向存储层权威对齐（架构 §11.3）。
pub const REASON_EPOCH_REALIGN_FROM_STORAGE: &str = "epoch_realign_from_storage";

// ------------------------------------------------------------------ 生命周期状态机（架构 §7）

/// §7 生命周期状态转换校验（纯函数，可在写库前调用，也便于单测）。
///
/// 规则：
/// - `from == to` 视为幂等更新放行 —— domain 的 `can_transition_to` 明确说明
///   「同一状态不算转换」，而「把已经是 WARM 的 DB 再置一次 WARM」是正常写法；
/// - 其余一律以 [`LifecycleState::can_transition_to`] 为唯一权威，
///   非法转换返回 `INVALID_ARGUMENT` 且 message 里带 from/to，
///   便于直接看出是谁写出了 `COLD -> WARM` 这类跳步。
pub fn validate_lifecycle_transition(from: LifecycleState, to: LifecycleState) -> Result<()> {
    if from == to || from.can_transition_to(to) {
        return Ok(());
    }
    Err(invalid_argument(format!(
        "非法的生命周期转换 {from} -> {to}（架构 §7 状态机不存在这条边；同一状态重复写入是允许的）"
    )))
}

/// Ownership 接管后统一进入 STARTING：判断「当前态」是否允许被接管。
///
/// §7 的主链路是 `COLD -> STARTING -> WARM -> HOT -> WARM -> COLD`（另有 `FAILED -> STARTING/COLD`）。
/// `bump_ownership` 的语义是「旧 Owner 的进程已经（或即将）不存在，新 Owner 要在自己的
/// Worker 上重新拉起进程」，因此**进程已经失去的态**都允许进入 STARTING：
///
/// - `COLD`：冷启动（§8 Transparent Wake）；
/// - `STARTING`：上一次启动中断（持锁 Worker 崩溃后重新接管），幂等重入；
/// - `WARM`：Worker 故障 / Planned Move 的 Cutover 阶段，进程随旧 Worker 消失（§11.2 / §12.3）；
/// - `STOPPING`：停机过程被中断（Worker 崩溃），新 Owner 必须重新拉起；
/// - `FAILED`：启动失败 / 进程崩溃后的重试（§12.1）。
///
/// 明确**拒绝** `HOT` 与 `DRAINING`：这两态下旧 Owner 的进程可能仍在写数据
/// （HOT = 持续活跃；DRAINING = 在途请求收敛中），直接让第二个 Owner 接管会制造双进程，
/// 违反 §15.2 的 Single DB Process Owner。正确路径是先收敛生命周期：
/// `HOT -> WARM/DRAINING/STOPPING`、`DRAINING -> STOPPING -> COLD`，
/// 或由 [`Catalog::clear_stale_ownership`]（租约过期 -> COLD + epoch+1）把故障 Worker 的 DB 收回后再接管。
pub fn can_take_ownership(current: LifecycleState) -> bool {
    matches!(
        current,
        LifecycleState::Cold
            | LifecycleState::Starting
            | LifecycleState::Warm
            | LifecycleState::Stopping
            | LifecycleState::Failed
    )
}

// ------------------------------------------------------------------ 全局版本

impl Catalog {
    /// 当前 Catalog 全局版本。LISTEN/NOTIFY 只是加速提示，正确性由本版本号 reconcile 保证。
    pub async fn current_catalog_version(&self) -> Result<i64> {
        let row: Option<i64> =
            sqlx::query_scalar("SELECT version FROM catalog_version WHERE id = $1")
                .bind(CATALOG_VERSION_ROW)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        row.ok_or_else(|| {
            platform_error(
                ErrorCode::InternalError,
                "catalog_version 单行记录缺失，migrations 未正确执行",
            )
        })
    }
}

// ------------------------------------------------------------------ Database

impl Catalog {
    /// 创建 DB。重复的 (tenant_id, name) 返回 `DB_ALREADY_EXISTS`。
    #[tracing::instrument(skip(self, params), fields(name = %params.name))]
    pub async fn create_database(&self, params: CreateDatabaseParams) -> Result<DatabaseRecord> {
        if params.name.trim().is_empty() {
            return Err(invalid_argument("database name 不能为空"));
        }
        let id = match params.id {
            Some(id) => id,
            None => DatabaseId::new_v7(),
        };
        let tenant_id = params.tenant_id.unwrap_or_else(default_tenant_id);
        let state = params.state.unwrap_or(LifecycleState::Cold);

        let sql = format!(
            "INSERT INTO databases (
                 id, tenant_id, name, state,
                 cpu_milli, memory_mib, fd_limit, disk_mib, iops_limit, priority, evictable,
                 storage_region, storage_prefix, engine_version, schema_version, labels
             ) VALUES (
                 $1::uuid, $2::uuid, $3::text, COALESCE($4::text, 'COLD'),
                 COALESCE($5::bigint, 500), COALESCE($6::bigint, 256), COALESCE($7::bigint, 512),
                 COALESCE($8::bigint, 1024), COALESCE($9::bigint, 2000), COALESCE($10::int, 100),
                 COALESCE($11::boolean, TRUE),
                 COALESCE($12::text, 'default'), COALESCE($13::text, ''), COALESCE($14::text, ''),
                 COALESCE($15::int, 0), COALESCE($16::jsonb, '{{}}'::jsonb)
             ) RETURNING {DATABASE_COLUMNS}"
        );

        let row = sqlx::query_as::<_, crate::pg::DatabaseRow>(&sql)
            .bind(id_to_uuid(&id)?)
            .bind(id_to_uuid(&tenant_id)?)
            .bind(params.name.as_str())
            .bind(state.to_db_str())
            .bind(params.cpu_milli.map(crate::pg::saturating_i64))
            .bind(params.memory_mib.map(crate::pg::saturating_i64))
            .bind(params.fd_limit.map(crate::pg::saturating_i64))
            .bind(params.disk_mib.map(crate::pg::saturating_i64))
            .bind(params.iops_limit.map(crate::pg::saturating_i64))
            .bind(params.priority)
            .bind(params.evictable)
            .bind(params.storage_region.as_deref())
            .bind(params.storage_prefix.as_deref())
            .bind(params.engine_version.as_deref())
            .bind(params.schema_version)
            .bind(params.labels.clone())
            .fetch_one(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        Ok(row.0)
    }

    pub async fn get_database(&self, id: DatabaseId) -> Result<DatabaseRecord> {
        let sql = format!("SELECT {DATABASE_COLUMNS} FROM databases WHERE id = $1::uuid");
        let row = sqlx::query_as::<_, crate::pg::DatabaseRow>(&sql)
            .bind(id_to_uuid(&id)?)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?
            .ok_or_else(|| catalog_error(CatalogError::DatabaseNotFound(id.to_string())))?;
        Ok(row.0)
    }

    /// 按 (tenant, name) 查找未删除的 DB。
    pub async fn get_database_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<DatabaseRecord> {
        let sql = format!(
            "SELECT {DATABASE_COLUMNS} FROM databases
             WHERE tenant_id = $1::uuid AND name = $2::text AND deleted_at IS NULL"
        );
        let row = sqlx::query_as::<_, crate::pg::DatabaseRow>(&sql)
            .bind(id_to_uuid(&tenant_id)?)
            .bind(name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?
            .ok_or_else(|| {
                CatalogError::DatabaseNotFound(format!("tenant={tenant_id} name={name}"))
            })?;
        Ok(row.0)
    }

    pub async fn list_databases(&self, filter: DatabaseFilter) -> Result<Vec<DatabaseRecord>> {
        let (sql, params) = build_database_list_query(&filter)?;
        let query = bind_all(sqlx::query_as::<_, crate::pg::DatabaseRow>(&sql), &params);
        let rows = query
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// 软删除：置 deleted_at 并进入 STOPPING（实际停机由控制面推进）。
    /// 对已删除的 DB 重复调用返回 DB_NOT_FOUND。
    ///
    /// 这是生命周期状态机的**有意例外**：删除是控制面意图（§17.5.1 的 DELETE 语义），
    /// 而 §7 的图描述的是「进程按需存在」的运行期转换，没有建模 DELETE；
    /// 若强行用 `can_transition_to` 校验，COLD（本来就没有进程）的 DB 将永远无法删除。
    /// 因此这里保持单条 SQL，且 `wakeup_in_progress` 等标记留给 `state = 'STOPPING'`
    /// 之后的停机流程（或 `clear_stale_ownership` / `reclaim_stale_wakeups`）收拾。
    #[tracing::instrument(skip(self), fields(database_id = %id))]
    pub async fn soft_delete_database(&self, id: DatabaseId) -> Result<DatabaseRecord> {
        let sql = format!(
            "UPDATE databases SET deleted_at = now(), state = 'STOPPING', updated_at = now()
             WHERE id = $1::uuid AND deleted_at IS NULL
             RETURNING {DATABASE_COLUMNS}"
        );
        let row = sqlx::query_as::<_, crate::pg::DatabaseRow>(&sql)
            .bind(id_to_uuid(&id)?)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?
            .ok_or_else(|| CatalogError::DatabaseNotFound(format!("{id} (不存在或已删除)")))?;
        Ok(row.0)
    }

    /// 设置生命周期状态，并强制 §7 状态机。
    ///
    /// owner 语义：显式给出 owner 则覆盖；未给出时，若目标状态不占用进程（COLD/FAILED）
    /// 则清空 owner 与租约，避免 Route Cache 保留指向不存在进程的 owner。
    ///
    /// 状态机：先 `SELECT ... FOR UPDATE` 读出当前态，再用
    /// [`validate_lifecycle_transition`] 校验（读与写在同一事务内，
    /// 两个并发请求不会各自基于旧状态通过校验后写出非法序列）。
    /// 非法转换返回 `INVALID_ARGUMENT`，message 里带 from/to。
    /// 需要「跳过状态机强制回收」的场景请走 [`Catalog::clear_stale_ownership`]
    /// （租约过期回收）或 [`Catalog::soft_delete_database`]（软删除 -> STOPPING）。
    #[tracing::instrument(skip(self), fields(database_id = %id, state = state.to_db_str()))]
    pub async fn set_lifecycle_state(
        &self,
        id: DatabaseId,
        state: LifecycleState,
        owner_worker: Option<WorkerId>,
    ) -> Result<DatabaseRecord> {
        let db_uuid = id_to_uuid(&id)?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let current: Option<String> =
            sqlx::query_scalar("SELECT state FROM databases WHERE id = $1::uuid FOR UPDATE")
                .bind(db_uuid)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let from_raw =
            current.ok_or_else(|| catalog_error(CatalogError::DatabaseNotFound(id.to_string())))?;
        let from = LifecycleState::from_db_str(&from_raw).map_err(|e| {
            platform_error(
                ErrorCode::InternalError,
                format!("database {id} 的 state 无法识别: {e}"),
            )
        })?;
        // 非法转换直接返回：tx 在 drop 时回滚，行锁随之释放
        validate_lifecycle_transition(from, state)?;

        let release_owner = !state.occupies_process();
        let sql = format!(
            "UPDATE databases SET
                 state = $2::text,
                 owner_worker_id = CASE
                     WHEN $3::text IS NOT NULL THEN $3::text
                     WHEN $4::boolean THEN NULL
                     ELSE owner_worker_id END,
                 lease_expires_at = CASE WHEN $4::boolean THEN NULL ELSE lease_expires_at END,
                 wakeup_in_progress = CASE WHEN $2::text = 'COLD' THEN FALSE ELSE wakeup_in_progress END,
                 wakeup_started_at = CASE WHEN $2::text = 'COLD' THEN NULL ELSE wakeup_started_at END,
                 updated_at = now()
             WHERE id = $1::uuid
             RETURNING {DATABASE_COLUMNS}"
        );
        let row = sqlx::query_as::<_, crate::pg::DatabaseRow>(&sql)
            .bind(db_uuid)
            .bind(state.to_db_str())
            .bind(owner_worker.map(|w| w.to_string()))
            .bind(release_owner)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?
            .ok_or_else(|| catalog_error(CatalogError::DatabaseNotFound(id.to_string())))?;

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(row.0)
    }

    /// 接管 Ownership（架构 §10）：epoch 必须与 Catalog 当前值一致，成功后 epoch 单调 +1。
    ///
    /// 全过程在单个事务内完成，并用 `SELECT ... FOR UPDATE` 锁住目标行：
    /// - 并发 Start / Failover 不会同时成功（epoch 校验串行化）；
    /// - epoch 更新与 ownership_events 审计行同事务提交，保证事后可校验 Split Brain。
    #[tracing::instrument(skip(self), fields(database_id = %db_id, expected_epoch, worker_id = %new_worker))]
    pub async fn bump_ownership(
        &self,
        db_id: DatabaseId,
        expected_epoch: u64,
        new_worker: WorkerId,
        lease_ttl: Duration,
    ) -> Result<u64> {
        self.bump_ownership_with_reason(
            db_id,
            expected_epoch,
            new_worker,
            lease_ttl,
            "ownership_bump",
        )
        .await
    }

    /// 同 [`Catalog::bump_ownership`]，额外记录变更原因。
    ///
    /// 接管后 DB 统一进入 STARTING（新 Owner 必须重新拉起进程），因此写库前用
    /// [`can_take_ownership`] 校验当前态：`HOT` / `DRAINING` 会被拒绝（§7 + §15.2），
    /// 详见该函数的注释。
    pub async fn bump_ownership_with_reason(
        &self,
        db_id: DatabaseId,
        expected_epoch: u64,
        new_worker: WorkerId,
        lease_ttl: Duration,
        reason: &str,
    ) -> Result<u64> {
        let db_uuid = id_to_uuid(&db_id)?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        // 行锁：把 epoch 校验与更新放在同一事务内，防止两个 Worker 用同一个 epoch 同时接管。
        let current: Option<(Option<String>, i64, String)> = sqlx::query_as(
            "SELECT owner_worker_id, owner_epoch, state FROM databases WHERE id = $1::uuid FOR UPDATE",
        )
        .bind(db_uuid)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let (from_worker, current_epoch, current_state) = current.ok_or_else(|| {
            let err = CatalogError::DatabaseNotFound(db_id.to_string());
            err.to_platform_error()
        })?;
        let current_epoch_u64 = u64::try_from(current_epoch).unwrap_or(0);
        if current_epoch_u64 != expected_epoch {
            // 0 行/epoch 不一致都必须显式失败：这是防 Split Brain 的关键路径
            return Err(catalog_error(CatalogError::EpochMismatch {
                expected: expected_epoch,
                actual: current_epoch_u64,
            }));
        }

        // §7 状态机：接管只允许从「进程已失去」的前驱进入 STARTING（见 can_take_ownership）。
        let from_state = LifecycleState::from_db_str(&current_state).map_err(|e| {
            platform_error(
                ErrorCode::InternalError,
                format!("database {db_id} 的 state 无法识别: {e}"),
            )
        })?;
        if !can_take_ownership(from_state) {
            return Err(invalid_argument(format!(
                "database {db_id} 当前处于 {from_state}，不允许被接管启动：{from_state} -> {} \
                 不是 §7 允许的转换；请先收敛生命周期（HOT -> WARM/DRAINING/STOPPING，\
                 DRAINING -> STOPPING -> COLD）或由 clear_stale_ownership 回收后再接管",
                LifecycleState::Starting
            )));
        }

        let new_epoch: Option<i64> = sqlx::query_scalar(
            "UPDATE databases SET
                 owner_worker_id = $3::text,
                 owner_epoch = owner_epoch + 1,
                 lease_expires_at = now() + ($4::bigint * INTERVAL '1 millisecond'),
                 state = 'STARTING',
                 wakeup_in_progress = FALSE,
                 wakeup_started_at = NULL,
                 updated_at = now()
             WHERE id = $1::uuid AND owner_epoch = $2::bigint
             RETURNING owner_epoch",
        )
        .bind(db_uuid)
        .bind(expected_epoch as i64)
        .bind(new_worker.to_string())
        .bind(millis(lease_ttl))
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let new_epoch = match new_epoch {
            Some(v) => u64::try_from(v).unwrap_or(current_epoch_u64),
            None => {
                // 防御性分支：行锁下不应发生；一旦发生说明 epoch 已被其他事务推进
                return Err(catalog_error(CatalogError::EpochMismatch {
                    expected: expected_epoch,
                    actual: current_epoch_u64,
                }));
            }
        };

        sqlx::query(
            "INSERT INTO ownership_events
                 (database_id, from_worker_id, to_worker_id, from_epoch, to_epoch, reason)
             VALUES ($1::uuid, $2::text, $3::text, $4::bigint, $5::bigint, $6::text)",
        )
        .bind(db_uuid)
        .bind(from_worker)
        .bind(new_worker.to_string())
        .bind(current_epoch_u64 as i64)
        .bind(new_epoch as i64)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        Ok(new_epoch)
    }

    /// 续租。epoch 不匹配（已被重新 placement）返回 `false`，调用方必须视为失去所有权。
    ///
    /// # 生产路径的续租点不在本函数
    ///
    /// ownership 租约在**生产环境**是由 Worker 心跳续的：[`Catalog::record_heartbeat`]
    /// 在「按 `(owner_worker_id, owner_epoch)` 精确匹配」回写本地状态时，用同一条 SQL
    /// 一并把 `lease_expires_at` 推到 [`DEFAULT_OWNER_LEASE`] 之后。放在那里而不是调用本函数，
    /// 是因为心跳的四步（Worker 用量 / DB 状态回写 / inventory 对账 / 释放 wakeup）必须在
    /// **同一个事务**内提交，而本函数走自己的连接池事务。
    ///
    /// 本函数保留为**显式续租**的入口（Owner 主动声明「我还活着且仍持有它」），
    /// 返回 `false` 时调用方必须停止服务该 DB。新代码若只是想让正常心跳维持租约，
    /// 不要调用它 —— 心跳路径已经覆盖，重复调用只会多一次无谓的写。
    #[tracing::instrument(skip(self), fields(database_id = %db_id, epoch))]
    pub async fn renew_lease(
        &self,
        db_id: DatabaseId,
        epoch: u64,
        lease_ttl: Duration,
    ) -> Result<bool> {
        let updated: Option<Uuid> = sqlx::query_scalar(
            "UPDATE databases
             SET lease_expires_at = now() + ($3::bigint * INTERVAL '1 millisecond'), updated_at = now()
             WHERE id = $1::uuid AND owner_epoch = $2::bigint AND owner_worker_id IS NOT NULL
             RETURNING id",
        )
        .bind(id_to_uuid(&db_id)?)
        .bind(epoch as i64)
        .bind(millis(lease_ttl))
        .fetch_optional(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(updated.is_some())
    }

    /// 清理租约过期的 Ownership：`older_than` 表示「租约已过期多久」。
    ///
    /// 事务 + 行锁内完成「查找 -> 清 owner -> epoch+1 -> 写审计」：
    /// epoch 递增确保旧 Worker 即使短暂恢复，其携带旧 epoch 的写入也会被拒绝（Storage Fencing）。
    /// 返回被清理的 DB 列表，由 Scheduler 重新 placement。
    ///
    /// 范围说明：本函数的 WHERE（`state <> 'COLD' AND owner_worker_id IS NOT NULL`）
    /// 只针对**已经拿到 owner 的**行。COLD 且无 owner、仅 `wakeup_in_progress` 被卡住的行
    /// 不在其覆盖范围内（那正是 FIX-A 的缺陷），由
    /// [`Catalog::reclaim_stale_wakeups`] 负责回收。
    #[tracing::instrument(skip(self), fields(expired_for_ms = millis(older_than)))]
    pub async fn clear_stale_ownership(&self, older_than: Duration) -> Result<Vec<DatabaseId>> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let stale: Vec<(Uuid, Option<String>, i64)> = sqlx::query_as(
            "SELECT id, owner_worker_id, owner_epoch FROM databases
             WHERE state <> 'COLD'
               AND owner_worker_id IS NOT NULL
               AND lease_expires_at IS NOT NULL
               AND lease_expires_at < now() - ($1::bigint * INTERVAL '1 millisecond')
             FOR UPDATE",
        )
        .bind(millis(older_than))
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let mut cleared = Vec::with_capacity(stale.len());
        for (db_uuid, from_worker, from_epoch) in stale {
            let new_epoch: i64 = sqlx::query_scalar(
                "UPDATE databases SET
                     owner_worker_id = NULL,
                     owner_epoch = owner_epoch + 1,
                     lease_expires_at = NULL,
                     wakeup_in_progress = FALSE,
                     wakeup_started_at = NULL,
                     state = 'COLD',
                     updated_at = now()
                 WHERE id = $1::uuid
                 RETURNING owner_epoch",
            )
            .bind(db_uuid)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

            sqlx::query(
                "INSERT INTO ownership_events
                     (database_id, from_worker_id, to_worker_id, from_epoch, to_epoch, reason)
                 VALUES ($1::uuid, $2::text, NULL, $3::bigint, $4::bigint, 'lease_expired')",
            )
            .bind(db_uuid)
            .bind(from_worker)
            .bind(from_epoch)
            .bind(new_epoch)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

            cleared.push(
                crate::pg::decode_uuid_id::<DatabaseId>(db_uuid, "id").map_err(|e| {
                    platform_error(ErrorCode::InternalError, format!("decode database id: {e}"))
                })?,
            );
        }

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(cleared)
    }

    /// 把 Catalog 的 `owner_epoch` 抬到**存储层已记录的 epoch**（架构 §11.3）。
    ///
    /// ## 为什么需要它
    ///
    /// Storage-level Fencing 的权威是 Remote WAL 自己记住的 epoch：任何
    /// `SetOwnerEpoch` / 写入都必须携带**严格更大**的 epoch，否则被拒
    /// （`WAL_APPEND_REJECTED`）。一旦 Catalog 的 epoch 因为手工干预、从备份恢复、
    /// 开发期重置数据等原因落后于 WAL，启动路径会稳定失败并且**重试永远不会成功**
    /// —— 同一份落后的 epoch 会被反复拒绝，DB 从此起不来。
    ///
    /// 控制面必须向存储层对齐（§11.3），而不是无视它或用「跳一个很大的数」蒙混：
    /// 对齐后 Catalog 记的就是存储层已确认的值，下一次 `SetOwnerEpoch`
    /// （= `aligned + 1`）必然严格更大，fencing 语义得以保持。
    ///
    /// ## 语义
    ///
    /// - **只前进**：`aligned = max(当前 owner_epoch, storage_epoch)`；相等时是 no-op（幂等）；
    /// - **不动 owner / state / 租约**：本函数只修 epoch 这一个事实，避免把一次
    ///   「认知修正」意外变成一次所有权转移；
    /// - **写审计**：`ownership_events` 落一行（`from_epoch -> to_epoch`，reason 由调用方给定），
    ///   这是「控制面向存储层权威对齐」的可追溯事件。
    ///
    /// 返回对齐后的 epoch（即当前 Catalog 值，调用方可直接作为下一次
    /// `bump_ownership` 的 `expected_epoch`）。
    #[tracing::instrument(skip(self), fields(database_id = %db_id, storage_epoch, reason))]
    pub async fn align_ownership_epoch_with_storage(
        &self,
        db_id: DatabaseId,
        storage_epoch: u64,
        reason: &str,
    ) -> Result<u64> {
        let db_uuid = id_to_uuid(&db_id)?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        // 行锁：与 bump_ownership 用同一把锁，避免「对齐」与「接管」交叉导致 epoch 回退。
        let current: Option<(Option<String>, i64)> = sqlx::query_as(
            "SELECT owner_worker_id, owner_epoch FROM databases WHERE id = $1::uuid FOR UPDATE",
        )
        .bind(db_uuid)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let (owner, current_epoch) = current.ok_or_else(|| {
            let err = CatalogError::DatabaseNotFound(db_id.to_string());
            err.to_platform_error()
        })?;
        let current_epoch = u64::try_from(current_epoch).unwrap_or(0);

        if storage_epoch <= current_epoch {
            // 已经不低于存储层：什么都不做（幂等），也不写审计噪音。
            tx.commit()
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
            return Ok(current_epoch);
        }

        sqlx::query(
            "UPDATE databases SET owner_epoch = $2::bigint, updated_at = now() WHERE id = $1::uuid",
        )
        .bind(db_uuid)
        .bind(storage_epoch as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        sqlx::query(
            "INSERT INTO ownership_events
                 (database_id, from_worker_id, to_worker_id, from_epoch, to_epoch, reason)
             VALUES ($1::uuid, $2::text, $2::text, $3::bigint, $4::bigint, $5::text)",
        )
        .bind(db_uuid)
        .bind(owner)
        .bind(current_epoch as i64)
        .bind(storage_epoch as i64)
        .bind(reason)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        tracing::warn!(
            database_id = %db_id,
            from_epoch = current_epoch,
            to_epoch = storage_epoch,
            reason,
            "Catalog owner_epoch 落后于 Remote WAL 已记录值，已向存储层对齐（架构 §11.3）"
        );
        Ok(storage_epoch)
    }

    /// **显式**重置 `owner_epoch`（开发环境清理等**回退**场景的唯一合法入口）。
    ///
    /// 名字里带 `reset` 是刻意的：调用点必须一眼看出这是一次「有意回退」，而不是普通写入。
    /// 生产路径一律只增不减（[`Catalog::bump_ownership`] / [`Catalog::clear_stale_ownership`] /
    /// [`Catalog::align_ownership_epoch_with_storage`]），schema 层还有
    /// `migrations/0002_owner_epoch_monotonic.sql` 的触发器兜底：任何绕过本函数的降低写入
    /// 都会被数据库拒绝。
    ///
    /// ## 两道闸门
    ///
    /// 1. `reason` 必须非空 —— 回退会写进 `ownership_events` 审计（`epoch_reset: <reason>`），
    ///    这是事后判断「谁在什么时候把库改回去了」的唯一线索；
    /// 2. `storage_epoch` 是 **Remote WAL 已记录的 epoch**（epoch 的权威，由持有 WAL 客户端的
    ///    控制面读回，见 `DbRouter::retry_start_after_epoch_realign`）。`target_epoch` 低于它
    ///    直接拒绝：那样的「重置」会让该库被 Storage-level Fencing 永久拒绝
    ///    （"owner epoch 必须严格递增：已记录 N，请求 M"），正是要修的那类故障。
    ///
    /// 目标不低于当前值时为 no-op（回退 API 不做无意义的抬升，返回当前值）。
    #[tracing::instrument(skip(self), fields(database_id = %db_id, target_epoch, storage_epoch))]
    pub async fn reset_owner_epoch_with_reason(
        &self,
        db_id: DatabaseId,
        target_epoch: u64,
        storage_epoch: u64,
        reason: &str,
    ) -> Result<u64> {
        if reason.trim().is_empty() {
            return Err(invalid_argument(
                "reset owner_epoch 必须给出理由（写入 ownership_events 审计）",
            ));
        }
        if target_epoch < storage_epoch {
            return Err(invalid_argument(format!(
                "拒绝把 database {db_id} 的 owner_epoch 重置为 {target_epoch}：Remote WAL 已记录 \
                 {storage_epoch}，低于存储层权威的 epoch 会被 Storage-level Fencing 永久拒绝 \
                 （架构 §11.3）；请先清理存储层状态，或改用 >= {storage_epoch} 的目标值"
            )));
        }

        let db_uuid = id_to_uuid(&db_id)?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let current: Option<(Option<String>, i64)> = sqlx::query_as(
            "SELECT owner_worker_id, owner_epoch FROM databases WHERE id = $1::uuid FOR UPDATE",
        )
        .bind(db_uuid)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let (owner, current_epoch) = current.ok_or_else(|| {
            let err = CatalogError::DatabaseNotFound(db_id.to_string());
            err.to_platform_error()
        })?;
        let current_epoch = u64::try_from(current_epoch).unwrap_or(0);

        if target_epoch >= current_epoch {
            // 不是回退：交给 bump/align 那类只增路径，本 API 不做抬升。
            tx.commit()
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
            return Ok(current_epoch);
        }

        // 两道闸门通过后，以会话级 GUC 让 schema 层触发器放行这一次**有理由、且不低于
        // 存储层权威**的回退。`is_local = true`：事务结束即失效，不会泄漏到连接池的下一条语句。
        sqlx::query("SELECT set_config('dbplatform.owner_epoch_reset_reason', $1, true)")
            .bind(reason.trim())
            .execute(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        sqlx::query("SELECT set_config('dbplatform.owner_epoch_floor', $1, true)")
            .bind(storage_epoch.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        sqlx::query(
            "UPDATE databases SET owner_epoch = $2::bigint, updated_at = now() WHERE id = $1::uuid",
        )
        .bind(db_uuid)
        .bind(target_epoch as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        sqlx::query(
            "INSERT INTO ownership_events
                 (database_id, from_worker_id, to_worker_id, from_epoch, to_epoch, reason)
             VALUES ($1::uuid, $2::text, $2::text, $3::bigint, $4::bigint, $5::text)",
        )
        .bind(db_uuid)
        .bind(owner)
        .bind(current_epoch as i64)
        .bind(target_epoch as i64)
        .bind(format!("epoch_reset: {}", reason.trim()))
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        tracing::warn!(
            database_id = %db_id,
            from_epoch = current_epoch,
            to_epoch = target_epoch,
            storage_epoch,
            reason = reason.trim(),
            "owner_epoch 被显式重置（显式 reset API，已审计）"
        );
        Ok(target_epoch)
    }

    /// Coalesce Wakeup（架构 §8）：同一 COLD DB 同时只允许一个 Start 动作。
    ///
    /// 单条 UPDATE 完成「检查 + 置位」，依赖 `wakeup_in_progress = FALSE AND state = 'COLD'`
    /// 作为原子条件；命中即 Leader，未命中再读取原因区分 Waiter / AlreadyRunning。
    ///
    /// 使用默认 wakeup 租约 [`DEFAULT_WAKEUP_LEASE`]：租约过期后其他调用方可以接手，
    /// 避免持锁者崩溃导致冷启动永久卡死（FIX-A）。
    #[tracing::instrument(skip(self), fields(database_id = %db_id))]
    pub async fn try_begin_wakeup(&self, db_id: DatabaseId) -> Result<WakeupDecision> {
        self.try_begin_wakeup_with_lease(db_id, DEFAULT_WAKEUP_LEASE)
            .await
    }

    /// 同 [`Catalog::try_begin_wakeup`]，wakeup 租约可覆盖。
    ///
    /// # 租约式抢占（FIX-A 修复）
    ///
    /// 旧实现的原子条件只有 `wakeup_in_progress = FALSE`，而唯一会清掉该标志的地方是
    /// `bump_ownership` —— 持有者若在那之前崩溃（Worker 掉线、进程被 kill），
    /// 标志就永久停在 TRUE，该 DB 的冷启动从此永久卡死（后续请求全部拿到 Waiter 等到超时）。
    ///
    /// 现在把条件放宽为「没人持锁 **或** 持锁者已超过租约」：抢到的调用方在同一条 UPDATE 里
    /// 刷新 `wakeup_started_at` 并继续被判定为 `Leader`。并发安全性不变：
    /// 这一条 UPDATE 仍然是唯一的仲裁点（行锁 + 条件重估），同一时刻只有一个事务能命中。
    ///
    /// 代价与边界：租约取得比最慢的冷启动略长（[`DEFAULT_WAKEUP_LEASE`]），
    /// 因此正常情况下不会误判；即便真的误判（启动慢到超过租约），后果也只是「多发起一次 Start 请求」——
    /// 真正阻止双 Owner 的是 `bump_ownership` 的 epoch 校验与 Storage Fencing（§15.2 / §11.3），
    /// wakeup 标志只是合并优化的加速器，不是所有权仲裁者。
    #[tracing::instrument(skip(self), fields(database_id = %db_id, lease_ms = millis(lease)))]
    pub async fn try_begin_wakeup_with_lease(
        &self,
        db_id: DatabaseId,
        lease: Duration,
    ) -> Result<WakeupDecision> {
        let db_uuid = id_to_uuid(&db_id)?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let claimed: Option<Uuid> = sqlx::query_scalar(
            "UPDATE databases
             SET wakeup_in_progress = TRUE, wakeup_started_at = now(), updated_at = now()
             WHERE id = $1::uuid
               AND state = 'COLD'
               AND (
                    wakeup_in_progress = FALSE
                    -- 脏行：标志已置位但没有时间戳，无法判断是否超时，按失效处理允许接手
                    OR wakeup_started_at IS NULL
                    -- 持锁者超过租约仍未推进到 bump_ownership：视为崩溃，允许接手
                    OR wakeup_started_at < now() - ($2::bigint * INTERVAL '1 millisecond')
               )
             RETURNING id",
        )
        .bind(db_uuid)
        .bind(millis(lease))
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let decision = if claimed.is_some() {
            WakeupDecision::Leader
        } else {
            let row: Option<(String, bool)> = sqlx::query_as(
                "SELECT state, wakeup_in_progress FROM databases WHERE id = $1::uuid",
            )
            .bind(db_uuid)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

            let (state, in_progress) = row
                .ok_or_else(|| catalog_error(CatalogError::DatabaseNotFound(db_id.to_string())))?;
            let state = LifecycleState::from_db_str(&state).map_err(|e| {
                platform_error(
                    ErrorCode::InternalError,
                    format!("database {db_id} 的 state 无法识别: {e}"),
                )
            })?;
            if in_progress && state == LifecycleState::Cold {
                WakeupDecision::Waiter
            } else {
                WakeupDecision::AlreadyRunning(state)
            }
        };

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(decision)
    }

    /// 冷启动结束（成功或失败）后必须释放 coalesce 标记。
    ///
    /// 幂等：无论是否仍是当前 Leader，调用都只是把本 DB 的标志清回 FALSE。
    /// 若 Leader 在调用本方法前崩溃，标志由 [`Catalog::reclaim_stale_wakeups`] 兜底回收。
    pub async fn end_wakeup(&self, db_id: DatabaseId) -> Result<()> {
        sqlx::query(
            "UPDATE databases
             SET wakeup_in_progress = FALSE, wakeup_started_at = NULL, updated_at = now()
             WHERE id = $1::uuid",
        )
        .bind(id_to_uuid(&db_id)?)
        .execute(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(())
    }

    /// Janitor：回收超时未推进的 `wakeup_in_progress`，返回被回收的 DB（**FIX-A 的修复**）。
    ///
    /// # 为什么需要它
    ///
    /// `try_begin_wakeup` 置位后，只有 `bump_ownership`（以及 `end_wakeup`）会清掉标志。
    /// 持锁者在 `bump_ownership` 之前崩溃时标志永久为 TRUE，而
    /// [`Catalog::clear_stale_ownership`] 的 WHERE 要求
    /// `state <> 'COLD' AND owner_worker_id IS NOT NULL` —— 被卡住的行恰好是
    /// `state = 'COLD'` 且 `owner_worker_id IS NULL`，永远扫不到，冷启动就此永久卡死。
    ///
    /// 因此本函数**不受 owner 非空约束**、也不要求非 COLD：它只认「标志 + 超时」两个事实。
    /// 它不改 `state` / `owner_epoch`，也不写 `ownership_events` —— 这里没有发生所有权变更，
    /// 只是把「谁都没在启动」这个事实写回 Catalog。`older_than` 是「距上次置位多久算超时」，
    /// 生产上用 [`DEFAULT_WAKEUP_LEASE`]（与 [`Catalog::try_begin_wakeup_with_lease`] 同一口径）。
    ///
    /// 已软删除（`deleted_at IS NOT NULL`）的 DB 不在扫描范围内：它们既不参与冷启动，
    /// 也不会因为标志被卡住而阻塞别人。
    #[tracing::instrument(skip(self), fields(stale_for_ms = millis(older_than)))]
    pub async fn reclaim_stale_wakeups(&self, older_than: Duration) -> Result<Vec<DatabaseId>> {
        let rows: Vec<Uuid> = sqlx::query_scalar(
            "UPDATE databases
             SET wakeup_in_progress = FALSE, wakeup_started_at = NULL, updated_at = now()
             WHERE wakeup_in_progress = TRUE
               AND deleted_at IS NULL
               AND (
                    -- 脏行：标志置位但没有时间戳，无法判断持有者是否还活着，按失效回收
                    wakeup_started_at IS NULL
                    OR wakeup_started_at < now() - ($1::bigint * INTERVAL '1 millisecond')
               )
             RETURNING id",
        )
        .bind(millis(older_than))
        .fetch_all(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        rows.into_iter()
            .map(|db| {
                crate::pg::decode_uuid_id::<DatabaseId>(db, "id").map_err(|e| {
                    platform_error(ErrorCode::InternalError, format!("decode database id: {e}"))
                })
            })
            .collect()
    }

    /// Route Cache 全量 reconcile 数据源。
    ///
    /// 过滤条件（FIX-C）：
    /// - `owner_worker_id IS NOT NULL AND deleted_at IS NULL`；
    /// - **租约未过期**：`lease_expires_at IS NULL OR lease_expires_at > now()`。
    ///   下发一个租约已过期的 owner，等于让 Server 把请求路由到一个 Catalog 已经认为
    ///   失去所有权的进程上：请求会在 Worker 侧被 `EPOCH_MISMATCH` 拒绝，再触发一轮
    ///   Route Refresh 抖动。过期租约的 DB 会由 `clear_stale_ownership` 收回并重新 placement。
    ///
    /// 注意**不按 state 过滤**：STARTING（Transparent Wake 需要它作为等待目标）、
    /// DRAINING / STOPPING（Server 要停止向它发新流量）、FAILED（Server 要触发 refresh）
    /// 都必须保留在快照里。
    pub async fn list_routing_entries(&self) -> Result<Vec<RoutingEntry>> {
        let rows: Vec<RoutingRow> = sqlx::query_as(
            "SELECT id, owner_worker_id, owner_epoch, state, lease_expires_at FROM databases
             WHERE owner_worker_id IS NOT NULL
               AND deleted_at IS NULL
               AND (lease_expires_at IS NULL OR lease_expires_at > now())",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        rows.into_iter()
            .map(|(db, worker, epoch, state, lease_expires_at)| {
                Ok(RoutingEntry {
                    database_id: crate::pg::decode_uuid_id::<DatabaseId>(db, "id").map_err(
                        |e| platform_error(ErrorCode::InternalError, format!("decode id: {e}")),
                    )?,
                    owner_worker_id: crate::pg::decode_text_id::<WorkerId>(
                        &worker,
                        "owner_worker_id",
                    )
                    .map_err(|e| {
                        platform_error(ErrorCode::InternalError, format!("decode worker id: {e}"))
                    })?,
                    owner_epoch: u64::try_from(epoch).unwrap_or(0),
                    state: LifecycleState::from_db_str(&state).map_err(|e| {
                        platform_error(
                            ErrorCode::InternalError,
                            format!("routing entry 的 state 无法识别: {e}"),
                        )
                    })?,
                    lease_expires_at,
                })
            })
            .collect()
    }

    /// 审计读取：ownership 变更历史（Split Brain 事后校验）。
    pub async fn list_ownership_events(
        &self,
        db_id: DatabaseId,
        limit: i64,
    ) -> Result<Vec<OwnershipEvent>> {
        let rows: Vec<OwnershipEventRow> = sqlx::query_as(
                "SELECT id, database_id, from_worker_id, to_worker_id, from_epoch, to_epoch, reason, created_at
                 FROM ownership_events WHERE database_id = $1::uuid
                 ORDER BY created_at DESC LIMIT $2::bigint",
            )
            .bind(id_to_uuid(&db_id)?)
            .bind(limit.clamp(1, 1000))
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        rows.into_iter()
            .map(
                |(id, database_id, from, to, from_epoch, to_epoch, reason, created_at)| {
                    Ok(OwnershipEvent {
                        id,
                        database_id: crate::pg::decode_uuid_id::<DatabaseId>(
                            database_id,
                            "database_id",
                        )
                        .map_err(|e| {
                            platform_error(ErrorCode::InternalError, format!("decode id: {e}"))
                        })?,
                        from_worker_id: crate::pg::decode_opt_text_id::<WorkerId>(
                            from.as_deref(),
                            "from_worker_id",
                        )
                        .map_err(|e| {
                            platform_error(
                                ErrorCode::InternalError,
                                format!("decode worker id: {e}"),
                            )
                        })?,
                        to_worker_id: crate::pg::decode_opt_text_id::<WorkerId>(
                            to.as_deref(),
                            "to_worker_id",
                        )
                        .map_err(|e| {
                            platform_error(
                                ErrorCode::InternalError,
                                format!("decode worker id: {e}"),
                            )
                        })?,
                        from_epoch: u64::try_from(from_epoch).unwrap_or(0),
                        to_epoch: u64::try_from(to_epoch).unwrap_or(0),
                        reason,
                        created_at,
                    })
                },
            )
            .collect()
    }
}

/// ownership_events 的 `query_as` 行类型（列顺序必须与 SELECT 一致）。
type OwnershipEventRow = (
    i64,
    Uuid,
    Option<String>,
    Option<String>,
    i64,
    i64,
    String,
    DateTime<Utc>,
);

/// ownership_events 行（审计读模型）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipEvent {
    pub id: i64,
    pub database_id: DatabaseId,
    pub from_worker_id: Option<WorkerId>,
    pub to_worker_id: Option<WorkerId>,
    pub from_epoch: u64,
    pub to_epoch: u64,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

// ------------------------------------------------------------------ SQL 组装

/// 构造 list_databases 的 SQL 与参数（纯函数，便于单测）。
pub(crate) fn build_database_list_query(
    filter: &DatabaseFilter,
) -> Result<(String, Vec<SqlParam>)> {
    let mut b = SqlBuilder::new();

    if let Some(tenant) = &filter.tenant_id {
        let n = b.next_placeholder();
        b.push(
            format!("tenant_id = ${n}::uuid"),
            Some(SqlParam::Uuid(id_to_uuid(tenant)?)),
        );
    }
    if let Some(worker) = &filter.worker_id {
        let n = b.next_placeholder();
        b.push(
            format!("owner_worker_id = ${n}::text"),
            Some(SqlParam::Text(worker.to_string())),
        );
    }
    if let Some(state) = &filter.state {
        let n = b.next_placeholder();
        b.push(
            format!("state = ${n}::text"),
            Some(SqlParam::Text(state.to_db_str().to_string())),
        );
    }
    if let Some(prefix) = &filter.name_prefix {
        let n = b.next_placeholder();
        b.push(
            format!("name LIKE ${n}::text"),
            Some(SqlParam::Text(like_prefix_pattern(prefix))),
        );
    }
    if !filter.include_deleted {
        b.push("deleted_at IS NULL", None);
    }

    let where_clause = b.where_clause();
    let mut params = b.params().to_vec();

    // LIMIT/OFFSET 同样走占位符，值不拼进 SQL 文本
    let limit = filter.limit.unwrap_or(100).clamp(1, 1000);
    let offset = filter.offset.unwrap_or(0).max(0);
    let limit_n = params.len() + 1;
    params.push(SqlParam::Int(limit));
    let offset_n = params.len() + 1;
    params.push(SqlParam::Int(offset));

    let sql = format!(
        "SELECT {DATABASE_COLUMNS} FROM databases{where_clause} \
         ORDER BY created_at DESC LIMIT ${limit_n}::bigint OFFSET ${offset_n}::bigint"
    );

    Ok((sql, params))
}

/// 名称前缀 -> LIKE 模式，转义 LIKE 元字符，避免用户输入变成通配符。
pub(crate) fn like_prefix_pattern(prefix: &str) -> String {
    let escaped = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("{escaped}%")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_tenant_is_the_builtin_tenant() {
        assert_eq!(default_tenant_id().to_string(), DEFAULT_TENANT_UUID);
    }

    #[test]
    fn list_query_without_filters_only_excludes_deleted() {
        let filter = DatabaseFilter::default();
        let (sql, params) = build_database_list_query(&filter).unwrap();
        assert!(sql.contains("WHERE deleted_at IS NULL"));
        assert!(sql.contains("ORDER BY created_at DESC"));
        assert!(sql.contains("LIMIT $1::bigint"));
        assert!(sql.contains("OFFSET $2::bigint"));
        assert_eq!(params, vec![SqlParam::Int(100), SqlParam::Int(0)]);
    }

    #[test]
    fn list_query_builds_ordered_placeholders_for_all_filters() {
        let tenant: TenantId = "00000000-0000-0000-0000-000000000001".parse().unwrap();
        let worker: WorkerId = "worker-a".parse().unwrap();
        let filter = DatabaseFilter {
            tenant_id: Some(tenant),
            worker_id: Some(worker),
            state: Some(LifecycleState::Hot),
            name_prefix: Some("app".into()),
            include_deleted: true,
            limit: Some(10),
            offset: Some(20),
        };
        let (sql, params) = build_database_list_query(&filter).unwrap();

        assert!(sql.contains("tenant_id = $1::uuid"));
        assert!(sql.contains("owner_worker_id = $2::text"));
        assert!(sql.contains("state = $3::text"));
        assert!(sql.contains("name LIKE $4::text"));
        assert!(!sql.contains("deleted_at IS NULL"));
        assert!(sql.contains("LIMIT $5::bigint"));
        assert!(sql.contains("OFFSET $6::bigint"));
        assert_eq!(params.len(), 6);
        assert_eq!(params[1], SqlParam::Text("worker-a".into()));
        assert_eq!(params[2], SqlParam::Text("HOT".into()));
        assert_eq!(params[3], SqlParam::Text("app%".into()));
        assert_eq!(params[4], SqlParam::Int(10));
        assert_eq!(params[5], SqlParam::Int(20));
    }

    #[test]
    fn list_query_clamps_limit_and_offset() {
        let filter = DatabaseFilter {
            limit: Some(100_000),
            offset: Some(-5),
            ..Default::default()
        };
        let (_, params) = build_database_list_query(&filter).unwrap();
        assert_eq!(params[0], SqlParam::Int(1000));
        assert_eq!(params[1], SqlParam::Int(0));

        let filter = DatabaseFilter {
            limit: Some(0),
            ..Default::default()
        };
        let (_, params) = build_database_list_query(&filter).unwrap();
        assert_eq!(params[0], SqlParam::Int(1));
    }

    #[test]
    fn like_pattern_escapes_metacharacters() {
        assert_eq!(like_prefix_pattern("app"), "app%");
        assert_eq!(like_prefix_pattern("a%b_c"), "a\\%b\\_c%");
        assert_eq!(like_prefix_pattern("a\\b"), "a\\\\b%");
    }

    #[test]
    fn wakeup_decision_helpers() {
        assert!(WakeupDecision::Leader.is_leader());
        assert!(!WakeupDecision::Leader.should_wait());
        assert!(WakeupDecision::Waiter.should_wait());
        assert!(!WakeupDecision::AlreadyRunning(LifecycleState::Hot).should_wait());
        assert!(WakeupDecision::AlreadyRunning(LifecycleState::Starting).should_wait());
        assert!(!WakeupDecision::AlreadyRunning(LifecycleState::Warm).should_wait());
    }

    /// FIX-A：wakeup 租约必须是「秒级、有限」的具体值，
    /// 否则 try_begin_wakeup / reclaim_stale_wakeups 的接手口径无法对齐。
    #[test]
    fn wakeup_lease_is_bounded_and_shared_by_takeover_paths() {
        assert_eq!(DEFAULT_WAKEUP_LEASE, Duration::from_secs(15));
        // 既要覆盖最慢的冷启动（§16 P99 <= 1s 的量级），又要短到不会长时间堵住冷启动
        assert!(DEFAULT_WAKEUP_LEASE >= Duration::from_secs(5));
        assert!(DEFAULT_WAKEUP_LEASE <= Duration::from_secs(60));
    }

    /// FIX-D：纯函数校验入口必须与 §7 冻结的状态机一致。
    #[test]
    fn lifecycle_validation_follows_frozen_state_machine() {
        use LifecycleState as S;

        // 合法边
        for (from, to) in [
            (S::Cold, S::Starting),
            (S::Starting, S::Warm),
            (S::Starting, S::Failed),
            (S::Warm, S::Hot),
            (S::Warm, S::Draining),
            (S::Warm, S::Stopping),
            (S::Warm, S::Cold),
            (S::Hot, S::Warm),
            (S::Draining, S::Stopping),
            (S::Stopping, S::Cold),
            (S::Stopping, S::Failed),
            (S::Failed, S::Starting),
            (S::Failed, S::Cold),
        ] {
            assert!(
                validate_lifecycle_transition(from, to).is_ok(),
                "{from} -> {to} 应当是合法转换"
            );
        }

        // 同态重复写入按幂等更新放行（domain 明确「同一状态不算转换」）
        for state in S::ALL {
            assert!(validate_lifecycle_transition(*state, *state).is_ok());
        }

        // 非法边：跳过 STARTING、HOT 直接回 COLD、COLD 直接 WARM 等
        for (from, to) in [
            (S::Cold, S::Warm),
            (S::Cold, S::Hot),
            (S::Cold, S::Stopping),
            (S::Starting, S::Cold),
            (S::Starting, S::Hot),
            (S::Hot, S::Cold),
            (S::Draining, S::Warm),
            (S::Failed, S::Warm),
        ] {
            let err = validate_lifecycle_transition(from, to).unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidArgument);
            // 错误信息必须能直接看出 from/to，否则排障时无法定位是谁写错了状态机
            assert!(err.message.contains(from.to_db_str()), "{}", err.message);
            assert!(err.message.contains(to.to_db_str()), "{}", err.message);
        }
    }

    /// FIX-D：ownership 接管的前驱白名单 —— 只允许「进程已失去」的态进入 STARTING。
    #[test]
    fn ownership_takeover_allows_only_process_lost_predecessors() {
        use LifecycleState as S;

        for allowed in [S::Cold, S::Starting, S::Warm, S::Stopping, S::Failed] {
            assert!(can_take_ownership(allowed), "{allowed} 应当允许被接管");
        }
        // HOT / DRAINING 下旧 Owner 可能仍在写数据，直接接管会制造双进程
        assert!(!can_take_ownership(S::Hot));
        assert!(!can_take_ownership(S::Draining));
    }

    #[test]
    fn default_create_params_keep_tenant_optional() {
        let p = CreateDatabaseParams::new("app");
        assert_eq!(p.name, "app");
        assert!(p.tenant_id.is_none());
        assert!(p.id.is_none());
    }
}
