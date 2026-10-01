//! Catalog 记录结构（与 `migrations/0001_init.sql` 的列一一对应）。
//!
//! 这些结构是 **纯数据** 形态，不依赖 sqlx：Catalog / Panel 侧负责把行映射进来或写出去，
//! 领域层只提供类型安全与语义辅助（预算换算、容量/占用换算、重试判定等）。
//!
//! 约定：
//! - 字段名与 SQL 列名一致（`snake_case`），便于直接对照 schema 与 SQLx 映射。
//! - 时间为 `chrono::DateTime<Utc>`；可空列用 `Option`。
//! - `owner_epoch` / `base_lsn` 使用 [`OwnerEpoch`] / [`Lsn`]，避免把 epoch 与普通整数混用。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::ErrorCode;
use crate::ids::{DatabaseId, JobId, OperationId, SnapshotId, TenantId, UserId, WorkerId};
use crate::lifecycle::{LifecycleState, WorkerState};
use crate::resources::{ResourceBudget, WorkerCapacity, WorkerResourceUsage};
use crate::wal::{Lsn, OwnerEpoch};

/// `databases` 表记录。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatabaseRecord {
    /// 主键。
    pub id: DatabaseId,
    /// 所属租户。
    pub tenant_id: TenantId,
    /// 租户内唯一名。
    pub name: String,
    /// 生命周期状态（COLD / STARTING / WARM / HOT / DRAINING / STOPPING / FAILED）。
    pub state: LifecycleState,
    /// 当前 Owner Worker；COLD 时为 `None`。
    pub owner_worker_id: Option<WorkerId>,
    /// 单调递增 Owner Epoch（fencing 依据）。
    pub owner_epoch: OwnerEpoch,
    /// 租约到期时间；Owner 必须在此之前续约。
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// 冷启动合并：同一 COLD DB 只允许一个 Start 动作。
    pub wakeup_in_progress: bool,
    /// 本次 wakeup 的开始时间。
    pub wakeup_started_at: Option<DateTime<Utc>>,
    /// 存储区域（逻辑位置）。
    pub storage_region: String,
    /// 存储前缀。
    pub storage_prefix: String,
    /// 最近一次恢复基线快照。
    pub last_snapshot_id: Option<SnapshotId>,
    /// 最近一次恢复基线 LSN。
    pub last_snapshot_lsn: Option<Lsn>,
    /// 请求 CPU 预算（milli-core）。
    pub cpu_milli: u64,
    /// 请求内存预算（MiB）。
    pub memory_mib: u64,
    /// FD 上限。
    pub fd_limit: u64,
    /// 本地磁盘预算（MiB）。
    pub disk_mib: u64,
    /// IOPS 提示上限。
    pub iops_limit: u64,
    /// 优先级（数值越小越重要）。
    pub priority: i32,
    /// 是否可被回收（HOT / WARM 回收策略）。
    pub evictable: bool,
    /// DB engine 版本。
    pub engine_version: String,
    /// schema 版本。
    pub schema_version: i32,
    /// 亲和性 Worker。
    pub affinity_worker_id: Option<WorkerId>,
    /// 反亲和性 Worker。
    pub anti_affinity_worker_id: Option<WorkerId>,
    /// 标签（`labels JSONB`）。
    pub labels: serde_json::Value,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
    /// 软删除时间。
    pub deleted_at: Option<DateTime<Utc>>,
}

impl DatabaseRecord {
    /// 该 DB 启动所需资源预算。
    ///
    /// `process_slots` 恒为 1：一个 DB = 一个独立 DB Process（架构 §1.2），
    /// 因此它天然参与 Worker 的进程位 hard limit。
    #[must_use]
    pub const fn resource_budget(&self) -> ResourceBudget {
        ResourceBudget {
            cpu_milli: self.cpu_milli,
            memory_mib: self.memory_mib,
            file_descriptors: self.fd_limit,
            disk_mib: self.disk_mib,
            process_slots: 1,
            iops: self.iops_limit,
        }
    }

    /// 是否可被回收（可驱逐且不是 HOT 服务态）。
    #[must_use]
    pub fn is_evictable(&self) -> bool {
        self.evictable && !matches!(self.state, LifecycleState::Hot)
    }

    /// 是否有有效 Owner。
    #[must_use]
    pub fn has_owner(&self) -> bool {
        self.owner_worker_id.is_some()
    }

    /// 是否已软删除。
    #[must_use]
    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }
}

/// `workers` 表记录（Worker inventory）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerRecord {
    /// 主键（TEXT）。
    pub id: WorkerId,
    /// 对外 endpoint。
    pub endpoint: String,
    /// Control Path gRPC endpoint。
    pub control_endpoint: Option<String>,
    /// Data Path gRPC endpoint。
    pub data_endpoint: Option<String>,
    /// Worker 状态（ACTIVE / SUSPECT / DRAINING / EMPTY / UNAVAILABLE）。
    pub state: WorkerState,
    /// 区域。
    pub region: String,
    /// 可用区。
    pub zone: String,
    /// Worker 版本。
    pub version: String,
    /// CPU 总量（milli-core）。
    pub cpu_milli_total: u64,
    /// 内存总量（MiB）。
    pub memory_mib_total: u64,
    /// FD 总量。
    pub fd_total: u64,
    /// 磁盘总量（MiB）。
    pub disk_mib_total: u64,
    /// 进程位总量。
    pub process_slots_total: u64,
    /// IOPS 总量。
    pub iops_total: u64,
    /// 最近上报的已用 CPU。
    pub cpu_milli_used: u64,
    /// 最近上报的已用内存。
    pub memory_mib_used: u64,
    /// 最近上报的已用 FD。
    pub fd_used: u64,
    /// 最近上报的已用磁盘。
    pub disk_mib_used: u64,
    /// 最近上报的已用进程位。
    pub process_slots_used: u64,
    /// 最近上报的已用 IOPS。
    pub iops_used: u64,
    /// 最近心跳时间。
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// 连续 heartbeat miss 计数（>= 3 判 Suspect / Unavailable）。
    pub missed_heartbeats: i32,
    /// inventory 版本（用于 reconcile）。
    pub inventory_version: i64,
    /// 是否为 failover reserve 保留（不做普通 placement）。
    pub reserved_for_failover: bool,
    /// 标签（`labels JSONB`）。
    pub labels: serde_json::Value,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

impl WorkerRecord {
    /// 容量总量。
    #[must_use]
    pub const fn capacity(&self) -> WorkerCapacity {
        WorkerCapacity {
            cpu_milli: self.cpu_milli_total,
            memory_mib: self.memory_mib_total,
            file_descriptors: self.fd_total,
            disk_mib: self.disk_mib_total,
            process_slots: self.process_slots_total,
            iops: self.iops_total,
        }
    }

    /// 最近上报的占用快照。
    ///
    /// `workers` 表用 `process_slots_*` 表达进程维，proto 用 `db_process_*`，此处完成对齐。
    #[must_use]
    pub const fn usage(&self) -> WorkerResourceUsage {
        WorkerResourceUsage {
            cpu_milli_used: self.cpu_milli_used,
            cpu_milli_total: self.cpu_milli_total,
            memory_mib_used: self.memory_mib_used,
            memory_mib_total: self.memory_mib_total,
            fd_used: self.fd_used,
            fd_total: self.fd_total,
            disk_mib_used: self.disk_mib_used,
            disk_mib_total: self.disk_mib_total,
            iops_used: self.iops_used,
            iops_total: self.iops_total,
            db_process_count: self.process_slots_used,
            db_process_limit: self.process_slots_total,
        }
    }

    /// 是否可参与普通 placement（状态允许且未被保留作 failover）。
    #[must_use]
    pub const fn accepts_new_placement(&self) -> bool {
        self.state.accepts_new_placement() && !self.reserved_for_failover
    }
}

/// `operations` 表记录（长操作：202 + operation_id 语义）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationRecord {
    /// 主键。
    pub id: OperationId,
    /// 操作类型（如 `CREATE_DB` / `MOVE_DB` / `DRAIN_WORKER`）。
    pub kind: String,
    /// 状态（PENDING / RUNNING / SUCCEEDED / FAILED / CANCELLED）。
    pub state: String,
    /// 关联数据库。
    pub database_id: Option<DatabaseId>,
    /// 关联 Worker。
    pub worker_id: Option<WorkerId>,
    /// 关联租户。
    pub tenant_id: Option<TenantId>,
    /// 发起人。
    pub requested_by: Option<UserId>,
    /// 幂等键（同 key 只允许产生一个 Operation）。
    pub idempotency_key: Option<String>,
    /// 进度 0..=100。
    pub progress: i16,
    /// 失败错误码。
    pub error_code: Option<ErrorCode>,
    /// 失败信息。
    pub error_message: Option<String>,
    /// 结果（`result JSONB`）。
    pub result: serde_json::Value,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
    /// 结束时间。
    pub finished_at: Option<DateTime<Utc>>,
}

impl OperationRecord {
    /// 是否为终态。
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self.state.as_str(), "SUCCEEDED" | "FAILED" | "CANCELLED")
    }

    /// 进度归一化到 `0.0..=1.0`。
    #[must_use]
    pub fn progress_ratio(&self) -> f64 {
        f64::from(self.progress.clamp(0, 100)) / 100.0
    }
}

/// `snapshots` 表记录（Object Storage 快照元数据，架构 §11.4）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    /// 主键（TEXT）。
    pub id: SnapshotId,
    /// 所属数据库。
    pub database_id: DatabaseId,
    /// 快照基线 LSN：恢复时从此 LSN 之后 replay Remote WAL。
    pub base_lsn: Lsn,
    /// 校验和。
    pub checksum: String,
    /// 大小（字节）。
    pub size_bytes: u64,
    /// 对象存储 key。
    pub object_key: String,
    /// 压缩算法（默认 zstd）。
    pub compression: String,
    /// 快照时的 Owner Epoch。
    pub owner_epoch: OwnerEpoch,
    /// engine 版本。
    pub engine_version: String,
    /// schema 版本。
    pub schema_version: i32,
    /// 状态（PENDING / AVAILABLE / CORRUPTED / DELETED）。
    pub state: String,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 校验完成时间。
    pub verified_at: Option<DateTime<Utc>>,
}

impl SnapshotRecord {
    /// 是否可用作恢复基线。
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.state == "AVAILABLE"
    }
}

/// `jobs` 表记录（PostgreSQL Job Table + lease / idempotency）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    /// 主键。
    pub id: JobId,
    /// 任务类型。
    pub kind: String,
    /// 任务负载（`payload JSONB`）。
    pub payload: serde_json::Value,
    /// 状态（READY / LEASED / DONE / FAILED / CANCELLED）。
    pub state: String,
    /// 优先级（数值越小越先执行）。
    pub priority: i32,
    /// 最早可执行时间。
    pub run_after: DateTime<Utc>,
    /// 当前租约持有者。
    pub lease_owner: Option<String>,
    /// 租约到期时间。
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// 已尝试次数。
    pub attempts: i32,
    /// 最大尝试次数。
    pub max_attempts: i32,
    /// 最近一次错误。
    pub last_error: Option<String>,
    /// 幂等键。
    pub idempotency_key: Option<String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
    /// 结束时间。
    pub finished_at: Option<DateTime<Utc>>,
}

impl JobRecord {
    /// 是否还有重试机会。
    #[must_use]
    pub const fn can_retry(&self) -> bool {
        self.attempts < self.max_attempts
    }

    /// 是否可被当前时刻领取（READY 且已到 run_after）。
    #[must_use]
    pub fn is_runnable_at(&self, now: DateTime<Utc>) -> bool {
        self.state == "READY" && self.run_after <= now
    }

    /// 租约是否已过期（无租约视为已过期）。
    #[must_use]
    pub fn is_lease_expired_at(&self, now: DateTime<Utc>) -> bool {
        match self.lease_expires_at {
            Some(expires_at) => expires_at <= now,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn database_record() -> DatabaseRecord {
        DatabaseRecord {
            id: DatabaseId::new_v7(),
            tenant_id: TenantId::new_v7(),
            name: "app-db".to_string(),
            state: LifecycleState::Warm,
            owner_worker_id: Some(WorkerId::from("worker-01")),
            owner_epoch: OwnerEpoch::new(834),
            lease_expires_at: Some(ts(1_700_000_100)),
            wakeup_in_progress: false,
            wakeup_started_at: None,
            storage_region: "default".to_string(),
            storage_prefix: "tenants/t1".to_string(),
            last_snapshot_id: Some(SnapshotId::new_v7()),
            last_snapshot_lsn: Some(Lsn::new(4096)),
            cpu_milli: 500,
            memory_mib: 256,
            fd_limit: 512,
            disk_mib: 1024,
            iops_limit: 2000,
            priority: 100,
            evictable: true,
            engine_version: "0.8.1".to_string(),
            schema_version: 3,
            affinity_worker_id: None,
            anti_affinity_worker_id: None,
            labels: serde_json::json!({"env": "prod"}),
            created_at: ts(1_700_000_000),
            updated_at: ts(1_700_000_000),
            deleted_at: None,
        }
    }

    fn worker_record() -> WorkerRecord {
        WorkerRecord {
            id: WorkerId::from("worker-01"),
            endpoint: "http://worker-01:8080".to_string(),
            control_endpoint: Some("http://worker-01:9000".to_string()),
            data_endpoint: None,
            state: WorkerState::Active,
            region: "default".to_string(),
            zone: "default".to_string(),
            version: "0.1.0".to_string(),
            cpu_milli_total: 8000,
            memory_mib_total: 16384,
            fd_total: 4096,
            disk_mib_total: 102400,
            process_slots_total: 128,
            iops_total: 20000,
            cpu_milli_used: 4000,
            memory_mib_used: 4096,
            fd_used: 1024,
            disk_mib_used: 10240,
            process_slots_used: 12,
            iops_used: 6000,
            last_heartbeat_at: Some(ts(1_700_000_000)),
            missed_heartbeats: 0,
            inventory_version: 7,
            reserved_for_failover: false,
            labels: serde_json::json!({}),
            created_at: ts(1_699_000_000),
            updated_at: ts(1_700_000_000),
        }
    }

    #[test]
    fn database_budget_includes_one_process_slot() {
        let record = database_record();
        let budget = record.resource_budget();
        assert_eq!(budget.cpu_milli, 500);
        assert_eq!(budget.memory_mib, 256);
        assert_eq!(budget.file_descriptors, 512);
        assert_eq!(budget.disk_mib, 1024);
        assert_eq!(budget.iops, 2000);
        assert_eq!(budget.process_slots, 1, "一个 DB = 一个进程");
        assert!(record.has_owner());
        assert!(!record.is_deleted());
        assert!(record.is_evictable());

        let mut hot = record.clone();
        hot.state = LifecycleState::Hot;
        assert!(!hot.is_evictable(), "HOT 尽量保留");
    }

    #[test]
    fn worker_capacity_and_usage_map_sql_columns() {
        let record = worker_record();
        let capacity = record.capacity();
        assert_eq!(capacity.cpu_milli, 8000);
        assert_eq!(capacity.process_slots, 128);

        let usage = record.usage();
        assert_eq!(usage.cpu_milli_used, 4000);
        assert_eq!(usage.db_process_count, 12);
        assert_eq!(usage.db_process_limit, 128);
        assert_eq!(usage.used().process_slots, 12);
        assert_eq!(usage.capacity(), capacity);
        assert!((usage.utilization().cpu - 0.5).abs() < f64::EPSILON);
        assert!(record.accepts_new_placement());

        let mut reserved = record.clone();
        reserved.reserved_for_failover = true;
        assert!(
            !reserved.accepts_new_placement(),
            "failover reserve 不参与普通 placement"
        );

        let mut suspect = record;
        suspect.state = WorkerState::Suspect;
        assert!(!suspect.accepts_new_placement());
    }

    #[test]
    fn operation_record_helpers() {
        let record = OperationRecord {
            id: OperationId::new_v7(),
            kind: "MOVE_DB".to_string(),
            state: "RUNNING".to_string(),
            database_id: Some(DatabaseId::new_v7()),
            worker_id: Some(WorkerId::from("worker-02")),
            tenant_id: None,
            requested_by: None,
            idempotency_key: Some("idem-1".to_string()),
            progress: 40,
            error_code: None,
            error_message: None,
            result: serde_json::json!({}),
            created_at: ts(1_700_000_000),
            updated_at: ts(1_700_000_010),
            finished_at: None,
        };
        assert!(!record.is_terminal());
        assert!((record.progress_ratio() - 0.4).abs() < f64::EPSILON);

        let mut failed = record.clone();
        failed.state = "FAILED".to_string();
        failed.error_code = Some(ErrorCode::WorkerDraining);
        failed.progress = 100;
        assert!(failed.is_terminal());
        assert_eq!(failed.progress_ratio(), 1.0);
        assert_eq!(
            failed.error_code.map(|c| c.as_str()),
            Some("WORKER_DRAINING")
        );
    }

    #[test]
    fn snapshot_and_job_records() {
        let snapshot = SnapshotRecord {
            id: SnapshotId::new_v7(),
            database_id: DatabaseId::new_v7(),
            base_lsn: Lsn::new(8192),
            checksum: "crc32:deadbeef".to_string(),
            size_bytes: 1024,
            object_key: "snapshots/db1/1.zst".to_string(),
            compression: "zstd".to_string(),
            owner_epoch: OwnerEpoch::new(834),
            engine_version: "0.8.1".to_string(),
            schema_version: 1,
            state: "AVAILABLE".to_string(),
            created_at: ts(1_700_000_000),
            verified_at: Some(ts(1_700_000_010)),
        };
        assert!(snapshot.is_available());

        let job = JobRecord {
            id: JobId::new_v7(),
            kind: "BACKUP_DB".to_string(),
            payload: serde_json::json!({"database_id": "db1"}),
            state: "READY".to_string(),
            priority: 100,
            run_after: ts(1_700_000_000),
            lease_owner: None,
            lease_expires_at: None,
            attempts: 2,
            max_attempts: 5,
            last_error: Some("timeout".to_string()),
            idempotency_key: None,
            created_at: ts(1_699_999_000),
            updated_at: ts(1_700_000_000),
            finished_at: None,
        };
        assert!(job.can_retry());
        assert!(job.is_runnable_at(ts(1_700_000_001)));
        assert!(!job.is_runnable_at(ts(1_699_999_999)));
        assert!(job.is_lease_expired_at(ts(1_700_000_001)));

        let mut exhausted = job.clone();
        exhausted.attempts = 5;
        assert!(!exhausted.can_retry());

        let mut leased = job;
        leased.state = "LEASED".to_string();
        leased.lease_expires_at = Some(ts(1_700_000_030));
        assert!(!leased.is_runnable_at(ts(1_700_000_001)));
        assert!(!leased.is_lease_expired_at(ts(1_700_000_001)));
        assert!(leased.is_lease_expired_at(ts(1_700_000_031)));
    }

    #[test]
    fn records_round_trip_through_json() {
        let record = database_record();
        let json = serde_json::to_string(&record).unwrap();
        let back: DatabaseRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back, record);

        // 状态与 epoch/LSN 在 JSON 中是可读的稳定表示
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["state"], serde_json::json!("WARM"));
        assert_eq!(value["owner_epoch"], serde_json::json!(834));
        assert_eq!(value["last_snapshot_lsn"], serde_json::json!(4096));

        let worker = worker_record();
        let back: WorkerRecord =
            serde_json::from_str(&serde_json::to_string(&worker).unwrap()).unwrap();
        assert_eq!(back, worker);
    }

    #[test]
    fn database_record_accepts_state_written_by_sql() {
        // SQL CHECK 允许的取值必须能被解析（防止 Catalog 写回时状态丢失）
        for state in [
            "COLD", "STARTING", "WARM", "HOT", "DRAINING", "STOPPING", "FAILED",
        ] {
            let parsed = LifecycleState::from_db_str(state).unwrap();
            assert_eq!(parsed.to_db_str(), state);
        }
    }
}
