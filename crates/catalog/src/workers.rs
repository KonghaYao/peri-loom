//! Worker inventory：注册 / 心跳 / 心跳计数 / 状态标记。
//!
//! 心跳是 Worker Plane 与 Catalog 之间唯一的权威状态同步点：Server 借此感知 Worker
//! 存活与本地 DB 注册表，并回传 draining / 全量拉取指令（架构 §4.3、§12.3）。

use std::time::Duration;

use domain::error::{ErrorCode, Result};
use domain::ids::{DatabaseId, WorkerId};
use domain::lifecycle::{LifecycleState, WorkerState};
use domain::records::WorkerRecord;
use domain::resources::WorkerResourceUsage;
use domain::wal::OwnerEpoch;

use crate::databases::DEFAULT_OWNER_LEASE;
use crate::error::{
    catalog_error, map_sqlx_error, platform_error, CatalogError, ConflictAs, NotFoundAs,
};
use crate::pg::{invalid_argument, millis, saturating_i64, WORKER_COLUMNS};
use crate::Catalog;

/// 心跳间隔基线（架构 §16「场景：Worker 故障」：Worker Heartbeat **1 s / 次**）。
///
/// 这个值不只是文档：控制面的 miss 判定周期必须与它对齐，
/// 否则「连续 3 次 miss」对应的墙钟时间会与 §16 的 P95 <= 4 s 检测目标脱节。
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// 心跳超时基线：超过该时间没有心跳即计入 missed_heartbeats。
///
/// §16 要求「连续 **3 次** Heartbeat Miss 后进入 Suspect / Unavailable」，
/// 且 Worker Failure Detection **P95 <= 4 s**：1 s 间隔下超时取 3 s，
/// 恰好覆盖 3 个心跳周期，检测上界 = 超时 3 s + 一轮检测 1 s = 4 s。
/// （旧值 5 s / 15 s 会把检测延迟推到 20 s 量级，与验收基线差 5 倍。）
pub const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);

/// inventory 缺失多少个心跳周期后才允许回收 ownership（架构 §12.2 / §16）。
///
/// 为什么需要「连续 N 次」而不是一轮就判死：心跳载荷是**某一时刻**的本地注册表快照，
/// Server 可能在上一个快照生成之后刚刚下达 `StartDatabase`，而 Worker 要等进程 READY
/// 才写入注册表（见 db-worker 的 supervisor）。若一轮缺失就回收，会把「正在启动、
/// 马上就会 READY」的库判死，制造无谓的所有权抖动。
///
/// 取 3 与 §16 的 Worker 故障判定同口径（连续 3 次 Heartbeat Miss）：1 s 心跳下
/// 对应墙钟时间 `3 * HEARTBEAT_INTERVAL = 3 s`，远小于 §16 要求的
/// 「Control Plane 恢复后 Catalog / Worker State 收敛 <= 30 s」。
pub const MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM: u32 = 3;

/// 回收原因（写进 `ownership_events.reason`，供事后审计与排障检索）。
pub mod reclaim_reason {
    /// Worker 心跳上报的完整 inventory 中已连续缺失该 DB（Worker 自己移除了它）。
    pub const INVENTORY_MISSING: &str = "inventory_missing";
    /// 同上，且本次心跳的 `inventory_version` 低于 Catalog 已记录值 —— Worker 进程
    /// 重启会让本地版本号归零，这是「本地状态已丢失」的直接证据。
    pub const INVENTORY_RESET: &str = "inventory_reset";
}

/// inventory 缺失判定的墙钟窗口（[`MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM`] 个心跳周期）。
#[must_use]
pub fn inventory_missing_grace() -> Duration {
    HEARTBEAT_INTERVAL * MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM
}

// ------------------------------------------------------------------ 参数 / 结果

#[derive(Debug, Clone)]
pub struct UpsertWorkerParams {
    pub id: WorkerId,
    pub endpoint: String,
    pub control_endpoint: Option<String>,
    pub data_endpoint: Option<String>,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub version: Option<String>,
    pub cpu_milli_total: Option<i64>,
    pub memory_mib_total: Option<i64>,
    pub fd_total: Option<i64>,
    pub disk_mib_total: Option<i64>,
    pub process_slots_total: Option<i64>,
    pub iops_total: Option<i64>,
    pub reserved_for_failover: Option<bool>,
    pub labels: Option<serde_json::Value>,
}

impl UpsertWorkerParams {
    pub fn new(id: WorkerId, endpoint: impl Into<String>) -> Self {
        Self {
            id,
            endpoint: endpoint.into(),
            control_endpoint: None,
            data_endpoint: None,
            region: None,
            zone: None,
            version: None,
            cpu_milli_total: None,
            memory_mib_total: None,
            fd_total: None,
            disk_mib_total: None,
            process_slots_total: None,
            iops_total: None,
            reserved_for_failover: None,
            labels: None,
        }
    }
}

/// Worker 本地 DB 注册表摘要（与 proto `LocalDatabaseState` 对应）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDatabaseState {
    pub database_id: DatabaseId,
    pub state: LifecycleState,
    pub owner_epoch: OwnerEpoch,
    pub pid: Option<i64>,
}

/// 心跳回执：Server 对 Worker 的指令。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeartbeatOutcome {
    /// Catalog 记录的 inventory_version 比 Worker 上报的更新 -> 要求全量上报
    pub request_full_inventory: bool,
    /// 该 Worker 不应再接新 placement（架构 §12.3）
    pub draining: bool,
    /// 明确授权该 Worker **重新接受**新 Placement（架构 §12.3 的 EMPTY 回归路径）。
    ///
    /// 判据：Catalog 侧已排空（`EMPTY`：排空完成，没有「必须停用」的理由）、
    /// Worker 自报状态也是 `EMPTY`（确实已无本地 DB），且本次 inventory 为空。
    /// 与 [`RecordHeartbeatOutcome::draining`] 可以同时为 true：那时 `draining` 表达的是
    /// 「现在还不是 ACTIVE」，本字段表达的是「允许你回到 ACTIVE」。DRAINING 期间恒为 false。
    pub accept_new_placement: bool,
    /// 期望的 ownership 快照版本，Worker 据此察觉自己被重新 placement
    pub catalog_version: i64,
    /// 本次心跳对账回收的 ownership（Worker 本地已不存在的 DB），供调用方审计与失效路由。
    pub reclaimed: Vec<ReclaimedOwnership>,
}

/// 一次因「Worker inventory 缺失」而被回收的 ownership（审计用）。
///
/// `from_epoch -> to_epoch` 是 epoch 推进区间：回收即 fencing 掉旧进程，
/// 后续 `Start` 会从 `to_epoch + 1` 开始，旧 Owner 的写入必然被存储层拒绝（§11.3）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimedOwnership {
    /// 被回收的 DB。
    pub database_id: DatabaseId,
    /// 被 fences 掉的旧 Owner。
    pub worker_id: WorkerId,
    /// 回收前的 epoch（已被 fences）。
    pub from_epoch: u64,
    /// 回收后的 epoch（写入 Catalog 的新值）。
    pub to_epoch: u64,
    /// 回收原因（`ownership_events.reason`）。
    pub reason: &'static str,
}

/// domain `WorkerResourceUsage` -> workers 表列。字段名/类型映射集中在此处。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UsageColumns {
    pub cpu_milli_used: i64,
    pub cpu_milli_total: i64,
    pub memory_mib_used: i64,
    pub memory_mib_total: i64,
    pub fd_used: i64,
    pub fd_total: i64,
    pub disk_mib_used: i64,
    pub disk_mib_total: i64,
    pub iops_used: i64,
    pub iops_total: i64,
    pub process_slots_used: i64,
    pub process_slots_total: i64,
}

impl UsageColumns {
    pub(crate) fn from_usage(usage: &WorkerResourceUsage) -> Self {
        Self {
            cpu_milli_used: saturating_i64(usage.cpu_milli_used),
            cpu_milli_total: saturating_i64(usage.cpu_milli_total),
            memory_mib_used: saturating_i64(usage.memory_mib_used),
            memory_mib_total: saturating_i64(usage.memory_mib_total),
            fd_used: saturating_i64(usage.fd_used),
            fd_total: saturating_i64(usage.fd_total),
            disk_mib_used: saturating_i64(usage.disk_mib_used),
            disk_mib_total: saturating_i64(usage.disk_mib_total),
            iops_used: saturating_i64(usage.iops_used),
            iops_total: saturating_i64(usage.iops_total),
            // workers 表用 process_slots_* 表达进程维，domain 用 db_process_*，此处对齐
            process_slots_used: saturating_i64(usage.db_process_count),
            process_slots_total: saturating_i64(usage.db_process_limit),
        }
    }
}

// ------------------------------------------------------------------ Worker

impl Catalog {
    /// 注册或更新 Worker（Worker 重启后重复注册必须幂等）。
    ///
    /// 注意：冲突时不覆盖 state —— DRAINING / EMPTY 是控制面下达的意图，
    /// 不能被 Worker 自身的重新注册清掉（架构 §12.3）。
    #[tracing::instrument(skip(self, params), fields(worker_id = %params.id))]
    pub async fn upsert_worker(&self, params: UpsertWorkerParams) -> Result<WorkerRecord> {
        if params.endpoint.trim().is_empty() {
            return Err(invalid_argument("worker endpoint 不能为空"));
        }
        let sql = format!(
            "INSERT INTO workers (
                 id, endpoint, control_endpoint, data_endpoint, region, zone, version,
                 cpu_milli_total, memory_mib_total, fd_total, disk_mib_total, process_slots_total,
                 iops_total, reserved_for_failover, labels
             ) VALUES (
                 $1::text, $2::text, $3::text, $4::text,
                 COALESCE($5::text, 'default'), COALESCE($6::text, 'default'), COALESCE($7::text, ''),
                 COALESCE($8::bigint, 0), COALESCE($9::bigint, 0), COALESCE($10::bigint, 0),
                 COALESCE($11::bigint, 0), COALESCE($12::bigint, 0), COALESCE($13::bigint, 0),
                 COALESCE($14::boolean, FALSE), COALESCE($15::jsonb, '{{}}'::jsonb)
             )
             ON CONFLICT (id) DO UPDATE SET
                 endpoint = EXCLUDED.endpoint,
                 control_endpoint = EXCLUDED.control_endpoint,
                 data_endpoint = EXCLUDED.data_endpoint,
                 region = EXCLUDED.region,
                 zone = EXCLUDED.zone,
                 version = EXCLUDED.version,
                 cpu_milli_total = EXCLUDED.cpu_milli_total,
                 memory_mib_total = EXCLUDED.memory_mib_total,
                 fd_total = EXCLUDED.fd_total,
                 disk_mib_total = EXCLUDED.disk_mib_total,
                 process_slots_total = EXCLUDED.process_slots_total,
                 iops_total = EXCLUDED.iops_total,
                 reserved_for_failover = EXCLUDED.reserved_for_failover,
                 labels = EXCLUDED.labels,
                 updated_at = now()
             RETURNING {WORKER_COLUMNS}"
        );

        let row = sqlx::query_as::<_, crate::pg::WorkerRow>(&sql)
            .bind(params.id.to_string())
            .bind(params.endpoint.as_str())
            .bind(params.control_endpoint.as_deref())
            .bind(params.data_endpoint.as_deref())
            .bind(params.region.as_deref())
            .bind(params.zone.as_deref())
            .bind(params.version.as_deref())
            .bind(params.cpu_milli_total)
            .bind(params.memory_mib_total)
            .bind(params.fd_total)
            .bind(params.disk_mib_total)
            .bind(params.process_slots_total)
            .bind(params.iops_total)
            .bind(params.reserved_for_failover)
            .bind(params.labels.clone())
            .fetch_one(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;

        Ok(row.0)
    }

    /// 记录心跳：重置 missed_heartbeats、更新资源占用、按需 reconcile 本地 DB 状态，
    /// 并对账「Catalog 有 ownership 但 Worker 本地没有」的 DB（架构 §12.2 / §16）。
    ///
    /// 四步写在同一个事务内：
    /// 1) 更新 Worker 自身计数与用量并回读指令；
    /// 2) 用 (owner_worker_id, owner_epoch) 作为条件 reconcile Worker 上报的本地 DB 状态，
    ///    旧 Owner（epoch 已过期）无法通过心跳篡改 Catalog；
    /// 3) 对账缺失 inventory 的 ownership（见 [`Catalog::reclaim_missing_inventory_in_tx`]）；
    /// 4) 一起提交，避免出现「心跳计数已清零但 DB 状态未同步」的中间态。
    ///
    /// `reported_state` 是 Worker 自报的本地状态（`WorkerInfo.state`），只在一处参与判定：
    /// **EMPTY -> ACTIVE 的重新接纳**（架构 §12.3）。DRAINING 仍然是控制面意图，
    /// 任何心跳都改不动它；而 EMPTY 表示排空已完成，Worker 收到 Server 的重新接纳授权
    /// 并自行回到 ACTIVE 之后，本函数才会把它写回 ACTIVE（见 [`RecordHeartbeatOutcome::accept_new_placement`]）。
    #[tracing::instrument(skip(self, usage, dbs), fields(worker_id = %worker_id))]
    pub async fn record_heartbeat(
        &self,
        worker_id: WorkerId,
        usage: &WorkerResourceUsage,
        inventory_version: i64,
        dbs: &[LocalDatabaseState],
        reported_state: WorkerState,
    ) -> Result<RecordHeartbeatOutcome> {
        let usage = UsageColumns::from_usage(usage);
        let worker_key = worker_id.to_string();
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;

        // 先把 Worker 行锁住并读出**本次上报之前**的 inventory_version：
        // - 行锁让同一 Worker 的并发心跳串行化，对账不会是「两个人各回收一半」；
        // - 旧值用于判断本次上报是否**回退**（进程重启后本地版本号归零）—— 这是
        //   「本地状态已丢失」的直接证据，只影响审计原因，不改变回收条件。
        let recorded_inventory_version: Option<i64> = sqlx::query_scalar(
            "SELECT inventory_version FROM workers WHERE id = $1::text FOR UPDATE",
        )
        .bind(&worker_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;
        let Some(recorded_inventory_version) = recorded_inventory_version else {
            return Err(catalog_error(CatalogError::WorkerNotFound(
                worker_key.clone(),
            )));
        };

        let outcome: Option<(String, bool, i64)> = sqlx::query_as(
            "UPDATE workers SET
                 cpu_milli_used = $2::bigint, cpu_milli_total = $3::bigint,
                 memory_mib_used = $4::bigint, memory_mib_total = $5::bigint,
                 fd_used = $6::bigint, fd_total = $7::bigint,
                 disk_mib_used = $8::bigint, disk_mib_total = $9::bigint,
                 iops_used = $10::bigint, iops_total = $11::bigint,
                 process_slots_used = $12::bigint, process_slots_total = $13::bigint,
                 inventory_version = GREATEST(inventory_version, $14::bigint),
                 missed_heartbeats = 0,
                 last_heartbeat_at = now(),
                 -- 状态只在三种情况下被心跳推进：
                 --   DRAINING：控制面意图，粘性（哨兵语义：排空未完成不得接新 Placement）；
                 --   EMPTY + 自报 ACTIVE：排空已完成且 Worker 已被授权并已就绪 -> 重新纳入 Placement；
                 --   其余（含 SUSPECT / UNAVAILABLE / ACTIVE）：心跳即证明进程活着，回到 ACTIVE。
                 -- 注意 CASE 的分支顺序：EMPTY 的两个分支必须在 ELSE 之前。
                 state = CASE
                     WHEN state = 'DRAINING' THEN state
                     WHEN state = 'EMPTY' AND $15::text = 'ACTIVE' THEN 'ACTIVE'
                     WHEN state = 'EMPTY' THEN state
                     ELSE 'ACTIVE' END,
                 updated_at = now()
             WHERE id = $1::text
             RETURNING state,
                       (inventory_version > $14::bigint) AS request_full_inventory,
                       (SELECT version FROM catalog_version WHERE id = 1) AS catalog_version",
        )
        .bind(&worker_key)
        .bind(usage.cpu_milli_used)
        .bind(usage.cpu_milli_total)
        .bind(usage.memory_mib_used)
        .bind(usage.memory_mib_total)
        .bind(usage.fd_used)
        .bind(usage.fd_total)
        .bind(usage.disk_mib_used)
        .bind(usage.disk_mib_total)
        .bind(usage.iops_used)
        .bind(usage.iops_total)
        .bind(usage.process_slots_used)
        .bind(usage.process_slots_total)
        .bind(inventory_version)
        // $15：Worker 自报的本地状态，只用于 EMPTY -> ACTIVE 的重新接纳判定
        .bind(reported_state.to_db_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;

        let (state, request_full_inventory, catalog_version) = outcome
            .ok_or_else(|| catalog_error(CatalogError::WorkerNotFound(worker_key.clone())))?;

        for db in dbs {
            // 仅当 Catalog 记录的 owner 与本 Worker、epoch 一致时才接受其状态上报。
            //
            // 这一条 UPDATE 同时是 ownership 租约的**唯一续约点**：没有它，
            // `bump_ownership` 发出的租约到期后无人续约，`clear_stale_ownership`
            // 会把**健康且正在服务**的 DB 一并回收（每 TTL + grace 一次），
            // DB 被反复踢回 COLD，epoch 空转、请求被迫重新冷启动。
            // 续约只发生在 (owner_worker_id, owner_epoch) 精确匹配时：
            // 已被取代的旧 Owner 无法借心跳给自己的租约续命。
            //
            // 已软删除的行（`deleted_at IS NOT NULL`）同样排除在外：`soft_delete_database`
            // 只置 `deleted_at` + `state = 'STOPPING'`，**不清** owner / 租约，所以 Worker 仍以
            // 当前 epoch 上报时这里照样会命中。那样租约被无限续期，只认「租约超时」的
            // `clear_stale_ownership` 永远等不到它过期，这一行就会带着 owner + 有效租约
            // 长期残留（且 `state` 会被上报值覆盖掉 STOPPING）。排除后租约自然过期，
            // 仍由 `clear_stale_ownership` 兜底回收。
            let updated: Option<uuid::Uuid> = sqlx::query_scalar(
                "UPDATE databases SET
                     state = $3::text,
                     lease_expires_at = now() + ($5::bigint * INTERVAL '1 millisecond'),
                     updated_at = now()
                 WHERE id = $1::uuid AND owner_worker_id = $2::text AND owner_epoch = $4::bigint
                   AND deleted_at IS NULL
                 RETURNING id",
            )
            .bind(crate::pg::id_to_uuid(&db.database_id)?)
            .bind(&worker_key)
            .bind(db.state.to_db_str())
            .bind(db.epoch_i64())
            .bind(millis(DEFAULT_OWNER_LEASE))
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;

            if updated.is_none() {
                tracing::warn!(
                    worker_id = %worker_id,
                    database_id = %db.database_id,
                    reported_state = db.state.to_db_str(),
                    "心跳上报的本地 DB 状态与 Catalog ownership 不一致，已忽略（fencing）"
                );
            }
        }

        // 反方向对账：Catalog 说「这个 DB 归你」，而 Worker 的完整 inventory 里根本没有它。
        // 不做这一步，Worker 重启后 DB 会永久卡在「Job 说 WARM、Owner 说 NOT_OWNER」，
        // 只能靠人工清库（架构 §12.2 要求控制面自行收敛）。
        let reclaim_reason = if inventory_version < recorded_inventory_version {
            reclaim_reason::INVENTORY_RESET
        } else {
            reclaim_reason::INVENTORY_MISSING
        };
        let reclaimed = Self::reclaim_missing_inventory_in_tx(
            &mut tx,
            &worker_id,
            &worker_key,
            dbs,
            reclaim_reason,
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;

        let worker_state = WorkerState::from_db_str(&state).map_err(|e| {
            platform_error(
                ErrorCode::InternalError,
                format!("worker {worker_id} 的 state 无法识别: {e}"),
            )
        })?;

        Ok(RecordHeartbeatOutcome {
            request_full_inventory,
            draining: matches!(worker_state, WorkerState::Draining | WorkerState::Empty),
            accept_new_placement: matches!(worker_state, WorkerState::Empty)
                && matches!(reported_state, WorkerState::Empty)
                && dbs.is_empty(),
            catalog_version,
            reclaimed,
        })
    }

    /// 对账：把「Catalog 记为本 Worker 所有、但 Worker 的完整 inventory 里不存在」的
    /// DB 收回为 COLD 并 fencing 掉旧 epoch（架构 §12.2 / §16）。
    ///
    /// 判定条件（全部满足才回收，宁可晚一点也不误杀正在跑的库）：
    ///
    /// 1. Catalog 侧 `owner_worker_id` 就是本 Worker，状态属于 `WARM` / `HOT` / `STARTING`，
    ///    且未软删 —— 只有「本应由该 Worker 提供进程」的 DB 才在讨论范围内；
    /// 2. 本次心跳上报的 inventory 里**没有以当前 Owner epoch 认领它**的条目。Worker 每轮
    ///    上报的都是本地注册表的**全量**快照（见 db-worker::heartbeat 的说明），但注册表
    ///    **有意保留**已停止 DB 的 COLD 条目（`on_process_exit`：「保留 epoch 记录，
    ///    所有权不变」），所以判存在必须带上 epoch —— 只比 id 会让任何停止过的 DB
    ///    永久留在 inventory 里，对账再也回收不掉它；
    /// 3. 缺失已持续 >= [`MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM`] 个心跳周期。
    ///    锚点用 `databases.updated_at`：它由本函数上方「按 (owner, epoch) 精确匹配」的
    ///    心跳回写刷新，因此等价于「最后一次被 Worker 以**当前 epoch** 确认为存在」的时刻；
    /// 4. `STARTING` 额外要求 ownership 租约已过期：启动流程可能仍在进行（此时 Worker 尚未
    ///    以新 epoch 认领），所以「inventory 缺失」在启动窗口内是正常的；租约由心跳续期，
    ///    真正在推进的启动不会被误杀。
    ///
    /// 幂等性：条件不满足时 0 行；并发心跳因 Worker 行锁而串行，后到的事务会重新评估
    /// 条件（此时 owner 已被清空）而不再命中，因此不会写出重复审计。
    async fn reclaim_missing_inventory_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        worker_id: &WorkerId,
        worker_key: &str,
        dbs: &[LocalDatabaseState],
        reason: &'static str,
    ) -> Result<Vec<ReclaimedOwnership>> {
        // 存在性判据是 (id, epoch) 而不是单独的 id。
        //
        // Worker 的本地注册表**有意**保留已停止 DB 的条目：正常停止路径把状态转成 COLD
        // 并注明「保留 epoch 记录，所有权不变」（见 db-worker 的 supervisor
        // `on_process_exit`）。因此「id 出现在 inventory 里」只证明 Worker **见过**它，
        // 不证明 Worker 此刻还在为它提供服务 —— 只有携带的 epoch 与 Catalog 当前
        // `owner_epoch` 一致才作数。
        //
        // 用单独 id 判存在会漏掉一整类卡死：启动被中断（StartDatabase 超时/被取消）后
        // Catalog 停在 STARTING 且 epoch 已推进，而 Worker 侧只剩一个旧 epoch 的 COLD 条目，
        // 于是对账永远认为「它还在」，只能等 clear_stale_ownership 按租约兜底。
        let reported_ids: Vec<String> = dbs.iter().map(|db| db.database_id.to_string()).collect();
        let reported_epochs: Vec<i64> = dbs.iter().map(LocalDatabaseState::epoch_i64).collect();

        let rows: Vec<(uuid::Uuid, i64, i64)> = sqlx::query_as(
            "UPDATE databases AS d SET
                 owner_worker_id = NULL,
                 owner_epoch = d.owner_epoch + 1,
                 lease_expires_at = NULL,
                 state = 'COLD',
                 updated_at = now()
             WHERE d.owner_worker_id = $1::text
               AND d.deleted_at IS NULL
               AND d.state IN ('WARM', 'HOT', 'STARTING')
               AND d.updated_at < now() - ($4::bigint * INTERVAL '1 millisecond')
               AND (d.state <> 'STARTING'
                    OR d.lease_expires_at IS NULL
                    OR d.lease_expires_at < now())
               AND NOT EXISTS (
                    SELECT 1
                    FROM unnest($2::text[], $3::bigint[]) AS reported(id, epoch)
                    WHERE reported.id = d.id::text
                      AND reported.epoch = d.owner_epoch
               )
             RETURNING d.id, d.owner_epoch - 1, d.owner_epoch",
        )
        .bind(worker_key)
        .bind(&reported_ids)
        .bind(&reported_epochs)
        .bind(millis(inventory_missing_grace()))
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

        let mut reclaimed = Vec::with_capacity(rows.len());
        for (db_uuid, from_epoch, to_epoch) in rows {
            sqlx::query(
                "INSERT INTO ownership_events
                     (database_id, from_worker_id, to_worker_id, from_epoch, to_epoch, reason)
                 VALUES ($1::uuid, $2::text, NULL, $3::bigint, $4::bigint, $5::text)",
            )
            .bind(db_uuid)
            .bind(worker_key)
            .bind(from_epoch)
            .bind(to_epoch)
            .bind(reason)
            .execute(&mut **tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;

            tracing::warn!(
                worker_id = %worker_id,
                database_id = %db_uuid,
                from_epoch,
                to_epoch,
                reason,
                "Worker inventory 中已不存在该 DB，回收 ownership 并置 COLD（架构 §12.2）"
            );

            let database_id =
                crate::pg::decode_uuid_id::<DatabaseId>(db_uuid, "id").map_err(|e| {
                    platform_error(ErrorCode::InternalError, format!("decode database id: {e}"))
                })?;
            reclaimed.push(ReclaimedOwnership {
                database_id,
                worker_id: worker_id.clone(),
                from_epoch: u64::try_from(from_epoch).unwrap_or(0),
                to_epoch: u64::try_from(to_epoch).unwrap_or(0),
                reason,
            });
        }

        Ok(reclaimed)
    }

    /// 对超时未心跳的 Worker 累加 missed_heartbeats（默认超时 [`DEFAULT_HEARTBEAT_TIMEOUT`]）。
    ///
    /// `excluded_workers` 通常是本轮刚处理过心跳的 Worker，避免「刚收到心跳就被判 miss」。
    pub async fn mark_missed_heartbeats(
        &self,
        excluded_workers: &[WorkerId],
    ) -> Result<Vec<(WorkerId, i32)>> {
        self.mark_missed_heartbeats_with_timeout(excluded_workers, DEFAULT_HEARTBEAT_TIMEOUT)
            .await
    }

    /// 同 [`Catalog::mark_missed_heartbeats`]，超时阈值可覆盖。
    #[tracing::instrument(skip(self, excluded_workers), fields(excluded = excluded_workers.len(), timeout_ms = millis(timeout)))]
    pub async fn mark_missed_heartbeats_with_timeout(
        &self,
        excluded_workers: &[WorkerId],
        timeout: Duration,
    ) -> Result<Vec<(WorkerId, i32)>> {
        let excluded: Vec<String> = excluded_workers.iter().map(|w| w.to_string()).collect();
        let rows: Vec<(String, i32)> = sqlx::query_as(
            "UPDATE workers
             SET missed_heartbeats = missed_heartbeats + 1, updated_at = now()
             WHERE COALESCE(last_heartbeat_at, created_at)
                       < now() - ($1::bigint * INTERVAL '1 millisecond')
               AND NOT (id = ANY($2::text[]))
             RETURNING id, missed_heartbeats",
        )
        .bind(millis(timeout))
        .bind(excluded)
        .fetch_all(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;

        rows.into_iter()
            .map(|(id, missed)| {
                Ok((
                    crate::pg::decode_text_id::<WorkerId>(&id, "id").map_err(|e| {
                        platform_error(ErrorCode::InternalError, format!("decode worker id: {e}"))
                    })?,
                    missed,
                ))
            })
            .collect()
    }

    /// 设置 Worker 状态（ACTIVE / SUSPECT / DRAINING / EMPTY / UNAVAILABLE）。
    #[tracing::instrument(skip(self), fields(worker_id = %worker_id, state = state.to_db_str()))]
    pub async fn mark_worker_state(
        &self,
        worker_id: WorkerId,
        state: WorkerState,
    ) -> Result<WorkerRecord> {
        let sql = format!(
            "UPDATE workers SET state = $2::text, updated_at = now()
             WHERE id = $1::text RETURNING {WORKER_COLUMNS}"
        );
        let row = sqlx::query_as::<_, crate::pg::WorkerRow>(&sql)
            .bind(worker_id.to_string())
            .bind(state.to_db_str())
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?
            .ok_or_else(|| catalog_error(CatalogError::WorkerNotFound(worker_id.to_string())))?;
        Ok(row.0)
    }

    pub async fn list_workers(&self) -> Result<Vec<WorkerRecord>> {
        let sql = format!("SELECT {WORKER_COLUMNS} FROM workers ORDER BY id");
        let rows = sqlx::query_as::<_, crate::pg::WorkerRow>(&sql)
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    pub async fn get_worker(&self, worker_id: WorkerId) -> Result<WorkerRecord> {
        let sql = format!("SELECT {WORKER_COLUMNS} FROM workers WHERE id = $1::text");
        let row = sqlx::query_as::<_, crate::pg::WorkerRow>(&sql)
            .bind(worker_id.to_string())
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Worker, ConflictAs::Database))?
            .ok_or_else(|| catalog_error(CatalogError::WorkerNotFound(worker_id.to_string())))?;
        Ok(row.0)
    }
}

impl LocalDatabaseState {
    /// epoch 在 SQL 侧是 bigint，这里统一做一次转换。
    pub(crate) fn epoch_i64(&self) -> i64 {
        i64::try_from(self.owner_epoch.get()).unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage() -> WorkerResourceUsage {
        WorkerResourceUsage {
            cpu_milli_used: 1_000,
            cpu_milli_total: 8_000,
            memory_mib_used: 2_048,
            memory_mib_total: 16_384,
            fd_used: 100,
            fd_total: 65_536,
            disk_mib_used: 5_120,
            disk_mib_total: 102_400,
            iops_used: 200,
            iops_total: 10_000,
            db_process_count: 7,
            db_process_limit: 256,
        }
    }

    #[test]
    fn usage_is_mapped_to_worker_columns() {
        let c = UsageColumns::from_usage(&usage());
        assert_eq!(c.cpu_milli_used, 1_000);
        assert_eq!(c.cpu_milli_total, 8_000);
        assert_eq!(c.memory_mib_used, 2_048);
        assert_eq!(c.memory_mib_total, 16_384);
        assert_eq!(c.fd_used, 100);
        assert_eq!(c.fd_total, 65_536);
        assert_eq!(c.disk_mib_used, 5_120);
        assert_eq!(c.disk_mib_total, 102_400);
        assert_eq!(c.iops_used, 200);
        assert_eq!(c.iops_total, 10_000);
        assert_eq!(c.process_slots_used, 7);
        assert_eq!(c.process_slots_total, 256);
    }

    #[test]
    fn usage_saturates_instead_of_wrapping() {
        let mut u = usage();
        u.cpu_milli_used = u64::MAX;
        assert_eq!(UsageColumns::from_usage(&u).cpu_milli_used, i64::MAX);
    }

    /// inventory 对账窗口必须与 §16 的收敛目标自洽：
    /// 「连续 N 次心跳缺失」的墙钟时间要远小于「Catalog / Worker State 收敛 <= 30 s」。
    #[test]
    fn inventory_reclaim_window_fits_convergence_budget() {
        assert_eq!(MISSING_INVENTORY_HEARTBEATS_BEFORE_RECLAIM, 3);
        assert_eq!(inventory_missing_grace(), Duration::from_secs(3));
        // 判定（3s）+ 回收后一次冷启动（§16 P99 <= 1s）必须留在 30s 预算内，
        // 且对比「一轮就判死」的差别是「不误杀刚下达 Start 的库」。
        assert!(
            inventory_missing_grace() + HEARTBEAT_INTERVAL < Duration::from_secs(30),
            "对账窗口 {:?} 必须远小于 §16 的 30s 收敛目标",
            inventory_missing_grace()
        );
    }

    /// 默认参数必须与 §16 验收基线自洽：
    /// 1 s / 次心跳 + 连续 3 次 miss 判定 Suspect/Unavailable，且检测 P95 <= 4 s。
    #[test]
    fn heartbeat_defaults_match_acceptance_baseline() {
        // §16：Worker Heartbeat 1 s / 次
        assert_eq!(HEARTBEAT_INTERVAL, Duration::from_secs(1));
        // §16：连续 3 次 Heartbeat Miss
        assert_eq!(DEFAULT_HEARTBEAT_TIMEOUT, HEARTBEAT_INTERVAL * 3);
        assert_eq!(DEFAULT_HEARTBEAT_TIMEOUT, Duration::from_secs(3));
        // §16：Worker Failure Detection P95 <= 4 s。
        // 上界 = 超时（3 个心跳周期）+ 一轮检测（1 个心跳周期），再宽就达不到验收目标。
        assert!(
            DEFAULT_HEARTBEAT_TIMEOUT + HEARTBEAT_INTERVAL <= Duration::from_secs(4),
            "心跳检测上界必须 <= 4s，当前为 {:?}",
            DEFAULT_HEARTBEAT_TIMEOUT + HEARTBEAT_INTERVAL
        );
    }

    #[test]
    fn local_database_state_reports_epoch_as_bigint() {
        let db = LocalDatabaseState {
            database_id: DatabaseId::new_v7(),
            state: LifecycleState::Warm,
            owner_epoch: OwnerEpoch::new(42),
            pid: Some(1234),
        };
        assert_eq!(db.epoch_i64(), 42);
    }
}
