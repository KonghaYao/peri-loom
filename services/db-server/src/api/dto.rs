//! HTTP 出口的请求 / 响应模型（架构 §17.4：Management REST JSON + Data HTTP API）。
//!
//! 两条约定：
//! 1. **DTO 与领域类型分离**。领域结构（`DatabaseRecord` 等）带一堆内部字段与
//!    `Option`，直接序列化会把 Catalog 的物理形态泄成对外契约；DTO 只暴露契约里
//!    承诺的字段，并在这里集中做转换。
//! 2. **ID 一律用字符串**。`domain::ids` 的 newtype 虽然 `serde(transparent)`，
//!    但它们没有实现 `utoipa::ToSchema`；对外统一成 `String` 也避免 OpenAPI 里
//!    出现平台内部类型名。

use chrono::{DateTime, Utc};
use domain::ids::{DatabaseId, OperationId, WorkerId};
use domain::lifecycle::{LifecycleState, WorkerState};
use domain::records::{DatabaseRecord, JobRecord, OperationRecord, SnapshotRecord, WorkerRecord};
use domain::value::{ColumnMeta, SqlValue};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ==================================================================== 通用

/// 分页信封。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Page<T> {
    /// 本页条目。
    pub items: Vec<T>,
    /// 请求的 limit（回显，便于客户端确认服务端是否夹取过值）。
    pub limit: i64,
    /// 请求的 offset。
    pub offset: i64,
}

/// 分页请求参数（`?limit=&offset=`）。
#[derive(Debug, Clone, Copy, Default, Deserialize, ToSchema)]
pub struct PageParams {
    /// 每页条数，缺省 50，夹取到 1..=500。
    #[serde(default)]
    pub limit: Option<i64>,
    /// 偏移量，缺省 0。
    #[serde(default)]
    pub offset: Option<i64>,
}

/// 默认分页大小。
pub const DEFAULT_PAGE_LIMIT: i64 = 50;
/// 单页上限：防止一次把整张表读进内存。
pub const MAX_PAGE_LIMIT: i64 = 500;

impl PageParams {
    /// 由显式字段构造。
    ///
    /// 为什么不用 `#[serde(flatten)]`：axum 的 `Query` 使用 serde_urlencoded，
    /// 它把 query string 当作“字符串到字符串”的映射，遇到 flatten 会走
    /// 内容缓冲路径，`Option<i64>` 这类非字符串字段会直接反序列化失败
    /// （表现为 "invalid type: string \"5\", expected i64"）。
    /// 因此查询参数一律**显式列出**字段，不用 flatten。
    #[must_use]
    pub fn from_parts(limit: Option<i64>, offset: Option<i64>) -> Self {
        Self { limit, offset }
    }

    /// 归一化后的 (limit, offset)。
    #[must_use]
    pub fn normalized(&self) -> (i64, i64) {
        (
            self.limit
                .unwrap_or(DEFAULT_PAGE_LIMIT)
                .clamp(1, MAX_PAGE_LIMIT),
            self.offset.unwrap_or(0).max(0),
        )
    }
}

/// 长操作受理响应（架构 §17.4 冻结：`202 { operation_id, state }`）。
///
/// 同时派生 `Deserialize`：幂等重放要把首次写下的响应体读回来（api::mod 的
/// `submit_long_operation` 直接反序列化它）。
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OperationAccepted {
    /// 操作 ID；`null` 表示本次请求是**幂等无操作**（目标状态已达成，没有产生新操作）。
    pub operation_id: Option<String>,
    /// 操作状态（PENDING / RUNNING / SUCCEEDED / FAILED / CANCELLED），
    /// 无操作时为目标资源当前状态。
    pub state: String,
    /// 关联的数据库（若适用）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_id: Option<String>,
    /// 是否命中了幂等重放（相同 `Idempotency-Key` 的重复提交）。
    pub replayed: bool,
}

// ==================================================================== 数据库

/// 创建数据库请求。
#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct CreateDatabaseRequest {
    /// 数据库名（同租户内唯一）。
    pub name: String,
    /// 租户；缺省用默认租户。
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// CPU（milli-core），缺省 1000。
    #[serde(default)]
    pub cpu_milli: Option<u64>,
    /// 内存（MiB），缺省 256。
    #[serde(default)]
    pub memory_mib: Option<u64>,
    /// 文件描述符上限，缺省 4096。
    #[serde(default)]
    pub fd_limit: Option<u64>,
    /// 本地磁盘配额（MiB）。
    #[serde(default)]
    pub disk_mib: Option<u64>,
    /// IOPS 提示上限。
    #[serde(default)]
    pub iops_limit: Option<u64>,
    /// 调度优先级（越小越优先）。
    #[serde(default)]
    pub priority: Option<i32>,
    /// 是否允许被生命周期回收驱逐（默认 true）。
    #[serde(default)]
    pub evictable: Option<bool>,
    /// 存储区域。
    #[serde(default)]
    pub storage_region: Option<String>,
    /// Panel 自定义标签。
    #[serde(default)]
    pub labels: Option<serde_json::Value>,
}

/// 资源预算视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BudgetView {
    /// CPU（milli-core）。
    pub cpu_milli: u64,
    /// 内存（MiB）。
    pub memory_mib: u64,
    /// 文件描述符上限。
    pub fd_limit: u64,
    /// 本地磁盘（MiB）。
    pub disk_mib: u64,
    /// IOPS 上限。
    pub iops_limit: u64,
}

/// 数据库视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DatabaseView {
    /// 数据库 ID。
    pub id: String,
    /// 租户 ID。
    pub tenant_id: String,
    /// 名称。
    pub name: String,
    /// 生命周期状态（COLD / STARTING / WARM / HOT / DRAINING / STOPPING / FAILED）。
    pub state: String,
    /// 当前 Owner Worker。
    pub owner_worker_id: Option<String>,
    /// Owner Epoch（fencing 依据；每次接管单调递增）。
    pub owner_epoch: Option<u64>,
    /// ownership 租约到期时间。
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// 是否有冷启动正在进行（内部协调位；对外只作为可观测信息，不代表可重试语义）。
    pub wakeup_in_progress: bool,
    /// 存储区域。
    pub storage_region: String,
    /// 存储前缀。
    pub storage_prefix: String,
    /// 最近一次快照 ID。
    pub last_snapshot_id: Option<String>,
    /// 最近一次快照的 LSN。
    pub last_snapshot_lsn: Option<u64>,
    /// 资源预算。
    pub budget: BudgetView,
    /// 调度优先级。
    pub priority: i32,
    /// 是否可被驱逐。
    pub evictable: bool,
    /// 引擎版本。
    pub engine_version: String,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
    /// 软删除时间（列表接口默认不返回已删除项）。
    pub deleted_at: Option<DateTime<Utc>>,
}

impl From<&DatabaseRecord> for DatabaseView {
    fn from(record: &DatabaseRecord) -> Self {
        Self {
            id: record.id.to_string(),
            tenant_id: record.tenant_id.to_string(),
            name: record.name.clone(),
            state: record.state.to_db_str().to_string(),
            owner_worker_id: record.owner_worker_id.as_ref().map(ToString::to_string),
            owner_epoch: Some(record.owner_epoch.get()),
            lease_expires_at: record.lease_expires_at,
            wakeup_in_progress: record.wakeup_in_progress,
            storage_region: record.storage_region.clone(),
            storage_prefix: record.storage_prefix.clone(),
            last_snapshot_id: record.last_snapshot_id.as_ref().map(ToString::to_string),
            last_snapshot_lsn: record.last_snapshot_lsn.map(|lsn| lsn.get()),
            budget: BudgetView {
                cpu_milli: record.cpu_milli,
                memory_mib: record.memory_mib,
                fd_limit: record.fd_limit,
                disk_mib: record.disk_mib,
                iops_limit: record.iops_limit,
            },
            priority: record.priority,
            evictable: record.evictable,
            engine_version: record.engine_version.clone(),
            created_at: record.created_at,
            updated_at: record.updated_at,
            deleted_at: record.deleted_at,
        }
    }
}

/// 数据库列表查询参数。
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct DatabaseListParams {
    /// 租户过滤。
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 生命周期状态过滤。
    #[serde(default)]
    pub state: Option<String>,
    /// 名称前缀过滤。
    #[serde(default)]
    pub name_prefix: Option<String>,
    /// 是否包含已软删除的库。
    #[serde(default)]
    pub include_deleted: Option<bool>,
    /// Worker 过滤。
    #[serde(default)]
    pub worker_id: Option<String>,
    /// 分页。
    /// 每页条数（缺省 50，夹取 1..=500）。
    #[serde(default)]
    pub limit: Option<i64>,
    /// 偏移量（缺省 0）。
    #[serde(default)]
    pub offset: Option<i64>,
}

impl DatabaseListParams {
    /// 归一化分页参数（显式字段版本，替代原先不可用的 flatten）。
    #[must_use]
    pub fn page(&self) -> PageParams {
        PageParams::from_parts(self.limit, self.offset)
    }
}

/// 迁移到指定（或自动选择）Worker。
#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct MoveDatabaseRequest {
    /// 目标 Worker；缺省由 Scheduler 自动选择。
    #[serde(default)]
    pub target_worker_id: Option<String>,
}

/// 恢复请求。
#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct RestoreDatabaseRequest {
    /// 指定快照；缺省使用最近一个 AVAILABLE 快照。
    #[serde(default)]
    pub snapshot_id: Option<String>,
}

// ==================================================================== 操作

/// 长操作视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct OperationView {
    /// 操作 ID。
    pub id: String,
    /// 操作类型（CREATE_DB / START_DB / ...）。
    pub kind: String,
    /// 状态（PENDING / RUNNING / SUCCEEDED / FAILED / CANCELLED）。
    pub state: String,
    /// 关联数据库。
    pub database_id: Option<String>,
    /// 关联 Worker。
    pub worker_id: Option<String>,
    /// 关联租户。
    pub tenant_id: Option<String>,
    /// 发起人。
    pub requested_by: Option<String>,
    /// 幂等键。
    pub idempotency_key: Option<String>,
    /// 进度 0..=100。
    pub progress: i16,
    /// 失败错误码。
    pub error_code: Option<String>,
    /// 失败信息。
    pub error_message: Option<String>,
    /// 结果负载（例如 create 返回 database_id）。
    pub result: serde_json::Value,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
    /// 完成时间。
    pub finished_at: Option<DateTime<Utc>>,
}

impl From<&OperationRecord> for OperationView {
    fn from(record: &OperationRecord) -> Self {
        Self {
            id: record.id.to_string(),
            kind: record.kind.clone(),
            state: record.state.clone(),
            database_id: record.database_id.as_ref().map(ToString::to_string),
            worker_id: record.worker_id.as_ref().map(ToString::to_string),
            tenant_id: record.tenant_id.as_ref().map(ToString::to_string),
            requested_by: record.requested_by.as_ref().map(ToString::to_string),
            idempotency_key: record.idempotency_key.clone(),
            progress: record.progress,
            error_code: record.error_code.as_ref().map(|c| c.as_str().to_string()),
            error_message: record.error_message.clone(),
            result: record.result.clone(),
            created_at: record.created_at,
            updated_at: record.updated_at,
            finished_at: record.finished_at,
        }
    }
}

/// 后台任务视图（运维排障用；job 是操作队列的物理载体）。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct JobView {
    /// 任务 ID。
    pub id: String,
    /// 任务类型。
    pub kind: String,
    /// 状态（READY / LEASED / DONE / FAILED / CANCELLED）。
    pub state: String,
    /// 优先级。
    pub priority: i32,
    /// 计划执行时间。
    pub run_after: DateTime<Utc>,
    /// 租约持有者。
    pub lease_owner: Option<String>,
    /// 尝试次数。
    pub attempts: i32,
    /// 最大尝试次数。
    pub max_attempts: i32,
    /// 最近一次错误。
    pub last_error: Option<String>,
    /// 负载。
    pub payload: serde_json::Value,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 完成时间。
    pub finished_at: Option<DateTime<Utc>>,
}

impl From<&JobRecord> for JobView {
    fn from(record: &JobRecord) -> Self {
        Self {
            id: record.id.to_string(),
            kind: record.kind.clone(),
            state: record.state.clone(),
            priority: record.priority,
            run_after: record.run_after,
            lease_owner: record.lease_owner.clone(),
            attempts: record.attempts,
            max_attempts: record.max_attempts,
            last_error: record.last_error.clone(),
            payload: record.payload.clone(),
            created_at: record.created_at,
            finished_at: record.finished_at,
        }
    }
}

// ==================================================================== Worker

/// Worker 资源用量视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UsageView {
    /// CPU（milli-core）。
    pub cpu_milli_used: u64,
    /// 内存（MiB）。
    pub memory_mib_used: u64,
    /// 文件描述符。
    pub fd_used: u64,
    /// 磁盘（MiB）。
    pub disk_mib_used: u64,
    /// 进程位。
    pub process_slots_used: u64,
    /// IOPS。
    pub iops_used: u64,
}

/// Worker 容量视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CapacityView {
    /// CPU（milli-core）。
    pub cpu_milli_total: u64,
    /// 内存（MiB）。
    pub memory_mib_total: u64,
    /// 文件描述符。
    pub fd_total: u64,
    /// 磁盘（MiB）。
    pub disk_mib_total: u64,
    /// 进程位。
    pub process_slots_total: u64,
    /// IOPS。
    pub iops_total: u64,
}

/// Worker 视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct WorkerView {
    /// Worker ID。
    pub id: String,
    /// 通用 endpoint。
    pub endpoint: String,
    /// Control Path endpoint。
    pub control_endpoint: Option<String>,
    /// Data Path endpoint。
    pub data_endpoint: Option<String>,
    /// 状态（ACTIVE / DRAINING / EMPTY / SUSPECT / UNAVAILABLE ...）。
    pub state: String,
    /// 区域。
    pub region: String,
    /// 可用区。
    pub zone: String,
    /// 版本。
    pub version: String,
    /// 用量。
    pub usage: UsageView,
    /// 容量。
    pub capacity: CapacityView,
    /// 最近心跳时间。
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// 连续丢失心跳次数。
    pub missed_heartbeats: i32,
    /// 是否为 failover 预留（不接普通放置）。
    pub reserved_for_failover: bool,
    /// 运行中的数据库（仅详情接口填充）。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub running_databases: Vec<DatabaseView>,
}

impl WorkerView {
    /// 由 Worker 记录构造；`running_databases` 由调用方按需填充。
    #[must_use]
    pub fn from_record(record: &WorkerRecord, running_databases: Vec<DatabaseView>) -> Self {
        Self {
            id: record.id.to_string(),
            endpoint: record.endpoint.clone(),
            control_endpoint: record.control_endpoint.clone(),
            data_endpoint: record.data_endpoint.clone(),
            state: record.state.to_db_str().to_string(),
            region: record.region.clone(),
            zone: record.zone.clone(),
            version: record.version.clone(),
            usage: UsageView {
                cpu_milli_used: record.cpu_milli_used,
                memory_mib_used: record.memory_mib_used,
                fd_used: record.fd_used,
                disk_mib_used: record.disk_mib_used,
                process_slots_used: record.process_slots_used,
                iops_used: record.iops_used,
            },
            capacity: CapacityView {
                cpu_milli_total: record.cpu_milli_total,
                memory_mib_total: record.memory_mib_total,
                fd_total: record.fd_total,
                disk_mib_total: record.disk_mib_total,
                process_slots_total: record.process_slots_total,
                iops_total: record.iops_total,
            },
            last_heartbeat_at: record.last_heartbeat_at,
            missed_heartbeats: record.missed_heartbeats,
            reserved_for_failover: record.reserved_for_failover,
            running_databases,
        }
    }
}

/// 排空 Worker 请求。
#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct DrainWorkerRequest {
    /// 是否连 COLD / WARM 的库一起停掉（false 表示只迁移在服务的库）。
    #[serde(default)]
    pub stop_cold_and_warm: Option<bool>,
}

// ==================================================================== 快照 / 备份 / 审计

/// 快照视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SnapshotView {
    /// 快照 ID。
    pub id: String,
    /// 所属数据库。
    pub database_id: String,
    /// 快照起点的 WAL LSN。
    pub base_lsn: Option<u64>,
    /// 校验和。
    pub checksum: String,
    /// 大小（字节）。
    pub size_bytes: u64,
    /// 对象存储 key。
    pub object_key: String,
    /// 压缩算法。
    pub compression: String,
    /// 生成快照时的 Owner Epoch。
    pub owner_epoch: Option<u64>,
    /// 引擎版本。
    pub engine_version: String,
    /// 状态（PENDING / AVAILABLE / CORRUPTED / DELETED）。
    pub state: String,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 校验通过时间。
    pub verified_at: Option<DateTime<Utc>>,
}

impl From<&SnapshotRecord> for SnapshotView {
    fn from(record: &SnapshotRecord) -> Self {
        Self {
            id: record.id.to_string(),
            database_id: record.database_id.to_string(),
            base_lsn: Some(record.base_lsn.get()),
            checksum: record.checksum.clone(),
            size_bytes: record.size_bytes,
            object_key: record.object_key.clone(),
            compression: record.compression.clone(),
            owner_epoch: Some(record.owner_epoch.get()),
            engine_version: record.engine_version.clone(),
            state: record.state.clone(),
            created_at: record.created_at,
            verified_at: record.verified_at,
        }
    }
}

/// 审计日志视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AuditView {
    /// 自增 ID。
    pub id: i64,
    /// 操作者用户 ID。
    pub actor_id: Option<String>,
    /// 操作者名称快照。
    pub actor_name: String,
    /// 租户。
    pub tenant_id: Option<String>,
    /// 数据库。
    pub database_id: Option<String>,
    /// 动作（例如 `db.start`）。
    pub action: String,
    /// 目标类型。
    pub target_type: String,
    /// 目标 ID。
    pub target_id: String,
    /// 结果（SUCCESS / FAILURE）。
    pub result: String,
    /// 失败错误码。
    pub error_code: Option<String>,
    /// 来源 IP。
    pub source_ip: Option<String>,
    /// 请求 ID。
    pub request_id: Option<String>,
    /// 附加信息。
    pub detail: serde_json::Value,
    /// 发生时间。
    pub created_at: DateTime<Utc>,
}

// ==================================================================== Token

/// 创建 API Token 请求。
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateTokenRequest {
    /// Token 名称（用途说明）。
    pub name: String,
    /// 绑定的唯一数据库。
    pub database_id: String,
    /// 权限子集；省略时默认为数据库读写权限。显式空数组无效。
    #[serde(default)]
    pub permissions: Option<Vec<String>>,
    /// 已有有效 token 时原子吊销旧 token 并签发新 token。
    #[serde(default)]
    pub rotate: bool,
    /// 过期时间；缺省不过期。
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    /// 限定租户。
    #[serde(default)]
    pub tenant_id: Option<String>,
}

/// 新建 Token 的响应：**明文只在这里出现一次**。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TokenCreated {
    /// Token ID。
    pub id: String,
    /// 名称。
    pub name: String,
    /// 绑定的唯一数据库。
    pub database_id: String,
    /// 明文 token（仅本次返回；服务端只保存哈希）。
    pub token: String,
    /// 权限集合。
    pub permissions: Vec<String>,
    /// 过期时间。
    pub expires_at: Option<DateTime<Utc>>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

/// Token 视图（不含明文 / 哈希）。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TokenView {
    /// Token ID。
    pub id: String,
    /// 名称。
    pub name: String,
    /// 绑定的唯一数据库。
    pub database_id: Option<String>,
    /// 权限集合。
    pub permissions: Vec<String>,
    /// 绑定的租户。
    pub tenant_id: Option<String>,
    /// 过期时间。
    pub expires_at: Option<DateTime<Utc>>,
    /// 最近使用时间。
    pub last_used_at: Option<DateTime<Utc>>,
    /// 吊销时间。
    pub revoked_at: Option<DateTime<Utc>>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

// ==================================================================== Panel

/// 创建 Saved Query 请求。
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateSavedQueryRequest {
    /// 名称。
    pub name: String,
    /// SQL 文本。
    pub sql: String,
    /// 关联数据库。
    #[serde(default)]
    pub database_id: Option<String>,
    /// 描述。
    #[serde(default)]
    pub description: Option<String>,
    /// 标签。
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Saved Query 视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SavedQueryView {
    /// ID。
    pub id: String,
    /// 所属用户。
    pub user_id: String,
    /// 关联数据库。
    pub database_id: Option<String>,
    /// 名称。
    pub name: String,
    /// SQL 文本。
    pub sql: String,
    /// 描述。
    pub description: Option<String>,
    /// 标签。
    pub tags: Vec<String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

/// Panel 偏好视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PreferenceView {
    /// 偏好键。
    pub key: String,
    /// 偏好值（任意 JSON）。
    pub value: serde_json::Value,
}

/// 写入 Panel 偏好。
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PutPreferenceRequest {
    /// 偏好值（任意 JSON）。
    pub value: serde_json::Value,
}

/// 慢查询视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SlowQueryView {
    /// 自增 ID。
    pub id: i64,
    /// 数据库。
    pub database_id: String,
    /// Worker。
    pub worker_id: Option<String>,
    /// 会话。
    pub session_id: Option<String>,
    /// SQL 指纹。
    pub fingerprint: String,
    /// SQL 文本。
    pub sql_text: String,
    /// 耗时（微秒）。
    pub duration_micros: i64,
    /// 返回行数。
    pub rows_returned: i64,
    /// 错误码。
    pub error_code: Option<String>,
    /// 发生时间。
    pub created_at: DateTime<Utc>,
}

// ---- Panel / 观测视图的领域类型转换 ----
//
// Catalog 的记录类型（审计、Token、Saved SQL、慢查询）在这里集中转换：DTO 不该
// 直接派生自 Catalog 结构，否则一次内部列改动就会漏成对外契约变更。

/// 从 `permissions` JSONB 解析权限集合（非数组 / 非字符串项一律忽略）。
#[must_use]
pub fn permissions_from_json(value: &serde_json::Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

impl From<&catalog::AuditLogRecord> for AuditView {
    fn from(record: &catalog::AuditLogRecord) -> Self {
        Self {
            id: record.id,
            actor_id: record.actor_id.as_ref().map(ToString::to_string),
            actor_name: record.actor_name.clone(),
            tenant_id: record.tenant_id.as_ref().map(ToString::to_string),
            database_id: record.database_id.as_ref().map(ToString::to_string),
            action: record.action.clone(),
            target_type: record.target_type.clone(),
            target_id: record.target_id.clone(),
            result: record.result.clone(),
            error_code: record.error_code.clone(),
            source_ip: record.source_ip.clone(),
            request_id: record.request_id.clone(),
            detail: record.detail.clone(),
            created_at: record.created_at,
        }
    }
}

impl From<&catalog::ApiTokenRecord> for TokenView {
    fn from(record: &catalog::ApiTokenRecord) -> Self {
        Self {
            id: record.id.to_string(),
            name: record.name.clone(),
            permissions: permissions_from_json(&record.permissions),
            tenant_id: record.tenant_id.as_ref().map(ToString::to_string),
            database_id: record.database_id.as_ref().map(ToString::to_string),
            expires_at: record.expires_at,
            last_used_at: record.last_used_at,
            revoked_at: record.revoked_at,
            created_at: record.created_at,
        }
    }
}

impl From<&catalog::SavedQueryRecord> for SavedQueryView {
    fn from(record: &catalog::SavedQueryRecord) -> Self {
        Self {
            id: record.id.to_string(),
            user_id: record.user_id.to_string(),
            database_id: record.database_id.as_ref().map(ToString::to_string),
            name: record.name.clone(),
            sql: record.sql.clone(),
            // Catalog 里 description 非空串默认；对外统一成「没有就是 null」。
            description: Some(record.description.clone()).filter(|desc| !desc.trim().is_empty()),
            tags: record.tags.clone(),
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

impl From<&catalog::SlowQueryRecord> for SlowQueryView {
    fn from(record: &catalog::SlowQueryRecord) -> Self {
        Self {
            id: record.id,
            database_id: record.database_id.to_string(),
            worker_id: record.worker_id.as_ref().map(ToString::to_string),
            session_id: record.session_id.clone(),
            fingerprint: record.fingerprint.clone(),
            sql_text: record.sql_text.clone(),
            duration_micros: record.duration_micros,
            rows_returned: record.rows_returned,
            error_code: record.error_code.clone(),
            created_at: record.created_at,
        }
    }
}

// ==================================================================== Data API

/// 单次查询请求。
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct QueryRequest {
    /// SQL 文本（单语句）。
    pub sql: String,
    /// 绑定参数（JSON 标量；`{"$blob":"<base64>"}` 表示二进制）。
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

/// 批量语句。
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct BatchStatementDto {
    /// SQL 文本。
    pub sql: String,
    /// 绑定参数。
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

/// 批量执行请求。
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct BatchRequest {
    /// 语句列表。
    pub statements: Vec<BatchStatementDto>,
    /// 是否整批包一个事务（默认 true：任一失败整体回滚）。
    #[serde(default)]
    pub atomic: Option<bool>,
}

/// 列元数据视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ColumnView {
    /// 列名。
    pub name: String,
    /// 类型名。
    pub type_name: String,
    /// 是否可空。
    pub nullable: bool,
}

impl From<&ColumnMeta> for ColumnView {
    fn from(column: &ColumnMeta) -> Self {
        Self {
            name: column.name.clone(),
            type_name: column.type_name.clone(),
            nullable: column.nullable,
        }
    }
}

/// 结果集视图。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ResultSetView {
    /// 列元数据（行值是**数组**：SQL 允许同名列，对象形式会静默丢列）。
    pub columns: Vec<ColumnView>,
    /// 行数据。
    pub rows: Vec<Vec<serde_json::Value>>,
    /// 受影响行数。
    pub affected_rows: u64,
    /// 是否被截断。
    pub truncated: bool,
}

impl ResultSetView {
    /// 由结果集构造（值编码与 NDJSON 完全一致，避免两种模式语义不同）。
    #[must_use]
    pub fn from_result_set(result: &domain::value::ResultSet) -> Self {
        Self {
            columns: result.columns.iter().map(ColumnView::from).collect(),
            rows: result
                .rows
                .iter()
                .map(|row| row.iter().map(crate::ndjson::encode_value).collect())
                .collect(),
            affected_rows: result.affected_rows,
            truncated: result.truncated,
        }
    }
}

/// 单次查询响应（结果集未超过内联上限时）。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueryResponse {
    /// 列元数据。
    pub columns: Vec<ColumnView>,
    /// 行数据。
    pub rows: Vec<Vec<serde_json::Value>>,
    /// 受影响行数。
    pub affected_rows: u64,
    /// 是否被截断。
    pub truncated: bool,
    /// 本次执行 durable 的 WAL LSN。
    pub wal_lsn: Option<u64>,
    /// Worker 侧执行耗时（微秒）。
    pub elapsed_micros: u64,
    /// 请求 ID（与响应头 `x-request-id` 一致）。
    pub request_id: String,
}

/// 批量执行响应。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BatchResponse {
    /// 每条语句的结果集。
    pub results: Vec<ResultSetView>,
    /// durable WAL LSN。
    pub wal_lsn: Option<u64>,
    /// 执行耗时（微秒）。
    pub elapsed_micros: u64,
    /// 请求 ID。
    pub request_id: String,
}

/// 打开会话响应。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SessionOpened {
    /// 会话 ID。
    pub session_id: String,
    /// 会话 pin 住的 Worker。
    pub worker_id: Option<String>,
    /// 数据库。
    pub database_id: String,
    /// 会话过期时间（Unix 毫秒）。
    pub expires_at_unix_ms: u64,
    /// 请求 ID。
    pub request_id: String,
}

/// 关闭会话响应。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SessionClosed {
    /// 已关闭的会话 ID。
    pub session_id: String,
    /// 是否在本地注册表中找到并移除（`false` 表示会话已过期 / 已失效）。
    pub known: bool,
}

// ==================================================================== 转换辅助

/// Worker 状态字符串（供过滤参数解析）。
#[must_use]
pub fn worker_state_str(state: WorkerState) -> String {
    state.to_db_str().to_string()
}

/// DB 状态字符串。
#[must_use]
pub fn lifecycle_state_str(state: LifecycleState) -> String {
    state.to_db_str().to_string()
}

/// 解析数据库 ID。
///
/// # Errors
/// 路径参数不是合法 UUID 时返回 `INVALID_ARGUMENT`（而不是 404：语法错误与不存在
/// 是两类问题）。
pub fn parse_database_id(raw: &str) -> Result<DatabaseId, domain::error::PlatformError> {
    raw.parse::<DatabaseId>().map_err(|err| {
        domain::error::PlatformError::new(
            domain::error::ErrorCode::InvalidArgument,
            format!("database id '{raw}' 不是合法 UUID: {err}"),
        )
    })
}

/// 解析操作 ID。
///
/// # Errors
/// 同上。
pub fn parse_operation_id(raw: &str) -> Result<OperationId, domain::error::PlatformError> {
    raw.parse::<OperationId>().map_err(|err| {
        domain::error::PlatformError::new(
            domain::error::ErrorCode::InvalidArgument,
            format!("operation id '{raw}' 不是合法 UUID: {err}"),
        )
    })
}

/// 解析 Worker ID（字符串语义，只校验非空）。
///
/// # Errors
/// 空字符串返回 `INVALID_ARGUMENT`。
pub fn parse_worker_id(raw: &str) -> Result<WorkerId, domain::error::PlatformError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(domain::error::PlatformError::new(
            domain::error::ErrorCode::InvalidArgument,
            "worker id 不能为空",
        ));
    }
    Ok(WorkerId::new(trimmed))
}

/// 把 JSON 标量转成 proto Value。
///
/// 支持：`null` / 整数 / 浮点 / 字符串 / `{"$blob":"<base64>"}`。
/// 其他形态（数组、普通对象）一律拒绝：SQL 绑定参数没有嵌套结构，
/// 静默字符串化会让客户端以为绑定了数组。
///
/// # Errors
/// 不支持的 JSON 形态或非法 base64 时返回 `INVALID_ARGUMENT`。
pub fn sql_value_from_json(value: &serde_json::Value) -> Result<SqlValue, String> {
    match value {
        serde_json::Value::Null => Ok(SqlValue::Null),
        serde_json::Value::Bool(b) => Ok(SqlValue::Integer(i64::from(*b))),
        serde_json::Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                Ok(SqlValue::Integer(int))
            } else if let Some(float) = number.as_f64() {
                Ok(SqlValue::Real(float))
            } else {
                Err(format!("数字 {number} 既不是 int64 也不是 f64"))
            }
        }
        serde_json::Value::String(text) => Ok(SqlValue::Text(text.clone())),
        serde_json::Value::Object(map) => {
            if map.len() == 1 {
                if let Some(serde_json::Value::String(encoded)) = map.get("$blob") {
                    use base64::Engine;
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .map_err(|err| format!("$blob 不是合法 base64: {err}"))?;
                    return Ok(SqlValue::Blob(bytes));
                }
            }
            Err("绑定参数不支持对象（二进制请用 {\"$blob\":\"<base64>\"}）".to_string())
        }
        serde_json::Value::Array(_) => {
            Err("绑定参数不支持数组（SQL 绑定参数只接受标量）".to_string())
        }
    }
}

/// 批量把 JSON 参数转成 proto Value。
///
/// # Errors
/// 任一参数非法即整体拒绝（避免「部分绑定」产生的隐蔽语义）。
pub fn sql_values_from_json(
    params: &[serde_json::Value],
) -> Result<Vec<protocol::data::Value>, String> {
    params
        .iter()
        .enumerate()
        .map(|(index, value)| {
            sql_value_from_json(value)
                .map(protocol::data::Value::from)
                .map_err(|err| format!("params[{index}]: {err}"))
        })
        .collect()
}

// ------------------------------------------------------------------ 认证

/// 本地登录请求。
#[derive(Debug, Clone, serde::Deserialize, utoipa::ToSchema)]
pub struct LoginRequest {
    /// 用户名。
    pub username: String,
    /// 明文密码（只在此请求体内出现，绝不落库、绝不回显）。
    pub password: String,
}

/// 登录响应。字段名与前端 `web/src/api/types.ts` 的 `LoginResponse` 对齐。
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct LoginResponse {
    /// 平台自签 JWT（HS256），与 OIDC token 走同一条校验路径。
    pub access_token: String,
    /// 固定为 `Bearer`。
    pub token_type: String,
    /// 有效期（秒）。
    pub expires_in: u64,
}

/// 当前认证主体视图（Panel 用它确认 token 是否仍然有效）。
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct Viewer {
    /// 用户 ID。
    pub user_id: String,
    /// 用户名。
    pub username: String,
    /// 展示名。
    pub display_name: String,
    /// 是否超级管理员。
    pub is_superuser: bool,
    /// 生效权限集合。
    pub permissions: Vec<String>,
    /// 默认租户。
    pub tenant_id: Option<String>,
}

impl DatabaseView {
    pub fn for_deployment(record: &domain::records::DatabaseRecord, remote: bool) -> Self {
        let mut view = Self::from(record);
        if !remote {
            view.owner_epoch = None;
        }
        view
    }
}
impl SnapshotView {
    pub fn for_deployment(record: &domain::records::SnapshotRecord, remote: bool) -> Self {
        let mut view = Self::from(record);
        if !remote {
            view.owner_epoch = None;
            view.base_lsn = None;
        }
        view
    }
}
