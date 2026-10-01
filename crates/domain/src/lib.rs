//! # domain —— DB Platform 领域模型层
//!
//! 本 crate 是整个平台的类型基座：ID、错误码、生命周期状态机、资源预算、
//! 冻结策略常量、WAL epoch / LSN、Session / Transaction 语义、结果值与 Catalog 记录。
//!
//! 硬性约束（架构 §15 / §17）：
//! - **零 I/O、零 async**：只允许纯计算与类型定义，任何网络 / 文件 / 时钟副作用都不得出现在这里。
//! - **契约单一来源**：错误码字符串与 proto `platform.common.v1` 一致；
//!   生命周期与 Worker 状态的 DB 字符串与 `migrations/0001_init.sql` 的 CHECK 取值一致。
//!   上述一致性由本 crate 的单元测试直接解析 proto / SQL 源文件校验（编译期 `include_str!`）。
//! - 水位常量属于冻结语义（§9 / §15.5），实现不得调整数值。

#![forbid(unsafe_code)]

pub mod error;
pub mod ids;
pub mod lifecycle;
pub mod policy;
pub mod records;
pub mod resources;
pub mod session;
pub mod time;
pub mod value;
pub mod wal;

pub use error::{ErrorCode, PlatformError, Result, UnknownEnumValue};
pub use ids::{
    DatabaseId, JobId, OperationId, RequestId, SessionId, SnapshotId, TenantId, TokenId,
    TransactionId, UserId, WorkerId,
};
pub use lifecycle::{LifecycleState, WorkerState};
pub use policy::{
    failover_reserve_required, failover_reserve_required_homogeneous, placement_gate,
    FailoverReserve, PlacementGate, EMERGENCY, MAX_DB_PROCESS_PER_WORKER_DEFAULT,
    PACKING_TARGET_MAX, PACKING_TARGET_MIN, STOP_NEW_PLACEMENT,
};
pub use records::{DatabaseRecord, JobRecord, OperationRecord, SnapshotRecord, WorkerRecord};
pub use resources::{ResourceBudget, Utilization, WorkerCapacity, WorkerResourceUsage};
pub use session::{
    SessionState, TransactionState, SESSION_IDLE_TIMEOUT, SESSION_IDLE_TIMEOUT_SECS,
    TRANSACTION_MAX_LIFETIME, TRANSACTION_MAX_LIFETIME_SECS,
};
pub use value::{ColumnMeta, ResultSet, SqlValue};
pub use wal::{Lsn, OwnerEpoch};

/// 契约测试用：解析 proto / SQL 源文件，避免文档与代码漂移（仅测试编译）。
#[cfg(test)]
pub(crate) mod contract_src {
    /// proto 公共契约源文件。
    pub const COMMON_PROTO: &str = include_str!("../../../proto/platform/common.proto");
    /// Catalog 初始 schema 源文件。
    pub const INIT_SQL: &str = include_str!("../../../migrations/0001_init.sql");

    /// 解析 proto 中某个 enum 的 `(名称, 数值)` 列表。
    pub fn proto_enum_values(src: &str, enum_name: &str) -> Vec<(String, i32)> {
        let header = format!("enum {enum_name} {{");
        let mut inside = false;
        let mut out = Vec::new();
        for raw in src.lines() {
            let line = raw.trim();
            if !inside {
                if line.starts_with(&header) {
                    inside = true;
                }
                continue;
            }
            if line.starts_with('}') {
                break;
            }
            if line.is_empty() || line.starts_with("//") {
                continue;
            }
            // 形如 `COLD = 1;`
            let Some((name, rest)) = line.split_once('=') else {
                continue;
            };
            let Ok(value) = rest.trim().trim_end_matches(';').trim().parse::<i32>() else {
                continue;
            };
            out.push((name.trim().to_string(), value));
        }
        assert!(!out.is_empty(), "proto enum {enum_name} 解析为空");
        out
    }

    /// 解析 `CREATE TABLE <table>` 中 `CHECK (<column> IN ('A','B',...))` 的取值集合。
    pub fn sql_check_values(src: &str, table: &str, column: &str) -> Vec<String> {
        let header = format!("CREATE TABLE {table} (");
        let block = src
            .split(&header)
            .nth(1)
            .unwrap_or_else(|| panic!("未找到表 {table}"));
        let block = block.split("\n);").next().unwrap_or(block);
        let flat = block.replace('\n', " ");
        let needle = format!("CHECK ({column} IN (");
        let start = flat
            .find(&needle)
            .unwrap_or_else(|| panic!("表 {table} 未找到 {column} 的 CHECK 约束"));
        let rest = &flat[start + needle.len()..];
        let end = rest.find("))").expect("CHECK 约束缺少右括号");
        rest[..end]
            .split(',')
            .map(|v| v.trim().trim_matches('\'').trim().to_string())
            .collect()
    }
}
