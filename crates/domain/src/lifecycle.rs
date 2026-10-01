//! 生命周期状态机（架构 §7 / §15）。
//!
//! 两个状态机都以 **字符串** 为权威持久化形式，取值必须与 `migrations/0001_init.sql`
//! 的 CHECK 约束完全一致：
//! - `databases.state`：COLD / STARTING / WARM / HOT / DRAINING / STOPPING / FAILED
//! - `workers.state`：ACTIVE / SUSPECT / DRAINING / EMPTY / UNAVAILABLE
//!
//! 一致性由 `tests` 中的 `include_str!` 契约测试直接解析 migration 源文件保证。

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::UnknownEnumValue;

/// DB 生命周期状态（一个 DB = 一个进程，进程按需存在）。
///
/// ```text
/// COLD -> STARTING -> WARM -> HOT -> WARM -> (DRAINING) -> COLD
///              \-> FAILED -> STARTING / COLD
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleState {
    /// 无进程、无资源占用；数据持久化在 Local NVMe + Object Storage。
    Cold,
    /// 正在做 Ownership Check / 存储准备 / 启动进程 / 打开 DB / 注册路由。
    Starting,
    /// 进程就绪，可立即处理请求。
    Warm,
    /// 持续活跃，保留优先级更高，尽量不回收。
    Hot,
    /// 正在被驱逐或迁移：不再接受新 placement，处理中的请求收敛后释放。
    Draining,
    /// 正在停止进程（写回 / 关闭 WAL / 释放资源）。
    Stopping,
    /// 失败态（启动失败 / 进程崩溃 / 恢复失败），等待重试或清理回 COLD。
    Failed,
}

impl LifecycleState {
    /// 全部状态（与 migration CHECK 取值顺序一致）。
    pub const ALL: &'static [LifecycleState] = &[
        LifecycleState::Cold,
        LifecycleState::Starting,
        LifecycleState::Warm,
        LifecycleState::Hot,
        LifecycleState::Draining,
        LifecycleState::Stopping,
        LifecycleState::Failed,
    ];

    /// DB 字符串（写 Catalog / 读 Catalog 用，必须与 CHECK 约束一致）。
    #[must_use]
    pub const fn to_db_str(&self) -> &'static str {
        match self {
            LifecycleState::Cold => "COLD",
            LifecycleState::Starting => "STARTING",
            LifecycleState::Warm => "WARM",
            LifecycleState::Hot => "HOT",
            LifecycleState::Draining => "DRAINING",
            LifecycleState::Stopping => "STOPPING",
            LifecycleState::Failed => "FAILED",
        }
    }

    /// 解析 DB 字符串（大小写不敏感）；未知取值返回错误，由调用方决定降级策略。
    pub fn from_db_str(value: &str) -> Result<Self, UnknownEnumValue> {
        let upper = value.trim().to_ascii_uppercase();
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.to_db_str() == upper)
            .ok_or_else(|| UnknownEnumValue::new("LifecycleState", value))
    }

    /// 宽松解析：未知取值落到 [`LifecycleState::Cold`]（保守：视为无进程，需要重新拉起）。
    #[must_use]
    pub fn from_db_str_lossy(value: &str) -> Self {
        Self::from_db_str(value).unwrap_or(LifecycleState::Cold)
    }

    /// proto 枚举数值（`platform.common.v1.LifecycleState`）。
    #[must_use]
    pub const fn to_proto_i32(&self) -> i32 {
        match self {
            LifecycleState::Cold => 1,
            LifecycleState::Starting => 2,
            LifecycleState::Warm => 3,
            LifecycleState::Hot => 4,
            LifecycleState::Draining => 5,
            LifecycleState::Stopping => 6,
            LifecycleState::Failed => 7,
        }
    }

    /// 解析 proto 数值；未知值落到 [`LifecycleState::Cold`]（保守），绝不 panic。
    #[must_use]
    pub fn from_proto_i32(value: i32) -> Self {
        Self::try_from_proto_i32(value).unwrap_or(LifecycleState::Cold)
    }

    /// 解析 proto 数值，未知值返回 `None`。
    #[must_use]
    pub fn try_from_proto_i32(value: i32) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.to_proto_i32() == value)
    }

    /// 状态机是否允许该转换（架构 §7 冻结）。
    ///
    /// 未列出的边一律非法；**同一状态不算转换**（幂等更新请直接比较相等），
    /// 调用方据此拒绝非法的 Catalog 更新。
    #[must_use]
    pub const fn can_transition_to(&self, next: LifecycleState) -> bool {
        use LifecycleState as S;
        match self {
            // 冷库被唤醒或预热。
            S::Cold => matches!(next, S::Starting),
            // 启动成功进入可服务态，否则失败。
            S::Starting => matches!(next, S::Warm | S::Failed),
            // WARM 可升温、被驱逐（DRAINING）、直接停止或回落到无进程态。
            S::Warm => matches!(next, S::Hot | S::Draining | S::Stopping | S::Cold),
            // HOT 只允许降温或回收，不允许直接 COLD。
            S::Hot => matches!(next, S::Warm | S::Draining | S::Stopping),
            // 架构中 “Draining -> Empty” 在 DB 生命周期里即 COLD（无进程 = 空）。
            S::Draining => matches!(next, S::Stopping | S::Cold),
            // 停止完成后回到 COLD，或停止失败进入 FAILED。
            S::Stopping => matches!(next, S::Cold | S::Failed),
            // 失败后允许重试启动或清理回 COLD。
            S::Failed => matches!(next, S::Starting | S::Cold),
        }
    }

    /// 是否可立即处理请求（WARM / HOT）。
    #[must_use]
    pub const fn is_serving(&self) -> bool {
        matches!(self, LifecycleState::Warm | LifecycleState::Hot)
    }

    /// 是否占用 Worker 进程 / 资源位。
    ///
    /// FAILED 保守计为占用：崩溃后的进程可能尚未回收，准入判断宁可拒绝也不超卖。
    #[must_use]
    pub const fn occupies_process(&self) -> bool {
        !matches!(self, LifecycleState::Cold)
    }

    /// 是否需要发起启动（COLD 唤醒 / FAILED 重试）。
    #[must_use]
    pub const fn needs_start(&self) -> bool {
        matches!(self, LifecycleState::Cold | LifecycleState::Failed)
    }
}

impl fmt::Display for LifecycleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_db_str())
    }
}

impl FromStr for LifecycleState {
    type Err = UnknownEnumValue;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_db_str(s)
    }
}

/// Worker 状态机（架构 §7 / §16）。
///
/// ```text
/// ACTIVE -> SUSPECT -> UNAVAILABLE -> ACTIVE(重新注册)
/// ACTIVE -> DRAINING -> EMPTY -> ACTIVE(重新接纳)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerState {
    /// 正常，可接受新 placement。
    Active,
    /// 心跳连续 miss（>= 3）但尚未判定死亡：停止新 placement，保留已有 DB。
    Suspect,
    /// 正在排空：不再接受新 DB，已有 DB 逐步迁移 / 停止。
    Draining,
    /// 已排空（无 DB 进程），可重新接纳或退出。
    Empty,
    /// 不可达 / 已判定故障。
    Unavailable,
}

impl WorkerState {
    /// 全部状态（与 migration CHECK 取值顺序一致）。
    pub const ALL: &'static [WorkerState] = &[
        WorkerState::Active,
        WorkerState::Suspect,
        WorkerState::Draining,
        WorkerState::Empty,
        WorkerState::Unavailable,
    ];

    /// DB 字符串（必须与 `workers.state` CHECK 约束一致）。
    #[must_use]
    pub const fn to_db_str(&self) -> &'static str {
        match self {
            WorkerState::Active => "ACTIVE",
            WorkerState::Suspect => "SUSPECT",
            WorkerState::Draining => "DRAINING",
            WorkerState::Empty => "EMPTY",
            WorkerState::Unavailable => "UNAVAILABLE",
        }
    }

    /// 解析 DB 字符串（大小写不敏感）。
    pub fn from_db_str(value: &str) -> Result<Self, UnknownEnumValue> {
        let upper = value.trim().to_ascii_uppercase();
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.to_db_str() == upper)
            .ok_or_else(|| UnknownEnumValue::new("WorkerState", value))
    }

    /// 宽松解析：未知取值落到 [`WorkerState::Unavailable`]（保守：不路由、不 placement）。
    #[must_use]
    pub fn from_db_str_lossy(value: &str) -> Self {
        Self::from_db_str(value).unwrap_or(WorkerState::Unavailable)
    }

    /// proto 枚举数值（`platform.common.v1.WorkerState`，值名带 `WORKER_STATE_` 前缀）。
    #[must_use]
    pub const fn to_proto_i32(&self) -> i32 {
        match self {
            WorkerState::Active => 1,
            WorkerState::Suspect => 2,
            WorkerState::Draining => 3,
            WorkerState::Empty => 4,
            WorkerState::Unavailable => 5,
        }
    }

    /// 解析 proto 数值；未知值落到 [`WorkerState::Unavailable`]（保守），绝不 panic。
    #[must_use]
    pub fn from_proto_i32(value: i32) -> Self {
        Self::try_from_proto_i32(value).unwrap_or(WorkerState::Unavailable)
    }

    /// 解析 proto 数值，未知值返回 `None`。
    #[must_use]
    pub fn try_from_proto_i32(value: i32) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.to_proto_i32() == value)
    }

    /// 状态机是否允许该转换（架构 §7 冻结）。
    ///
    /// 同一状态不算转换；被判定 UNAVAILABLE 的 Worker 只有重新注册（-> ACTIVE）才回到集群。
    #[must_use]
    pub const fn can_transition_to(&self, next: WorkerState) -> bool {
        use WorkerState as S;
        match self {
            S::Active => matches!(next, S::Suspect | S::Draining | S::Unavailable),
            // 心跳恢复可回到 ACTIVE；继续丢心跳则判死；运维也可直接 drain。
            S::Suspect => matches!(next, S::Active | S::Unavailable | S::Draining),
            // 排空完成进入 EMPTY；排空途中失联则 UNAVAILABLE。
            S::Draining => matches!(next, S::Empty | S::Unavailable),
            // 已排空的 Worker 可重新接纳新 DB，或直接下线。
            S::Empty => matches!(next, S::Active | S::Unavailable),
            // 故障 Worker 只能通过重新注册恢复。
            S::Unavailable => matches!(next, S::Active),
        }
    }

    /// 是否允许新 placement（只有 ACTIVE）。
    #[must_use]
    pub const fn accepts_new_placement(&self) -> bool {
        matches!(self, WorkerState::Active)
    }

    /// 是否仍可承载既有 DB 的流量（ACTIVE / SUSPECT，排空与故障态不再保证）。
    #[must_use]
    pub const fn serves_traffic(&self) -> bool {
        matches!(self, WorkerState::Active | WorkerState::Suspect)
    }

    /// 是否已被判定不可用。
    #[must_use]
    pub const fn is_unavailable(&self) -> bool {
        matches!(self, WorkerState::Unavailable)
    }
}

impl fmt::Display for WorkerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_db_str())
    }
}

impl FromStr for WorkerState {
    type Err = UnknownEnumValue;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_db_str(s)
    }
}

// serde 以 DB 字符串为线格式（Catalog / HTTP / proto 一致）；反序列化宽松降级。
impl Serialize for LifecycleState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.to_db_str())
    }
}

impl<'de> Deserialize<'de> for LifecycleState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(LifecycleState::from_db_str_lossy(&raw))
    }
}

impl Serialize for WorkerState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.to_db_str())
    }
}

impl<'de> Deserialize<'de> for WorkerState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(WorkerState::from_db_str_lossy(&raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract_src::{proto_enum_values, sql_check_values, COMMON_PROTO, INIT_SQL};

    /// 架构 §7 冻结的合法边；未列出的边必须为 false。
    const LEGAL_DB_EDGES: &[(LifecycleState, &[LifecycleState])] = &[
        (LifecycleState::Cold, &[LifecycleState::Starting]),
        (
            LifecycleState::Starting,
            &[LifecycleState::Warm, LifecycleState::Failed],
        ),
        (
            LifecycleState::Warm,
            &[
                LifecycleState::Hot,
                LifecycleState::Draining,
                LifecycleState::Stopping,
                LifecycleState::Cold,
            ],
        ),
        (
            LifecycleState::Hot,
            &[
                LifecycleState::Warm,
                LifecycleState::Draining,
                LifecycleState::Stopping,
            ],
        ),
        (
            LifecycleState::Draining,
            &[LifecycleState::Stopping, LifecycleState::Cold],
        ),
        (
            LifecycleState::Stopping,
            &[LifecycleState::Cold, LifecycleState::Failed],
        ),
        (
            LifecycleState::Failed,
            &[LifecycleState::Starting, LifecycleState::Cold],
        ),
    ];

    #[test]
    fn all_legal_db_transitions_are_allowed() {
        for (from, targets) in LEGAL_DB_EDGES {
            for target in *targets {
                assert!(
                    from.can_transition_to(*target),
                    "{from} -> {target} 应当合法"
                );
            }
        }
    }

    #[test]
    fn all_unlisted_db_transitions_are_rejected() {
        for from in LifecycleState::ALL {
            let allowed: &[LifecycleState] = LEGAL_DB_EDGES
                .iter()
                .find(|(s, _)| s == from)
                .map(|(_, targets)| *targets)
                .expect("每个状态都必须出现在冻结边表中");
            for to in LifecycleState::ALL {
                let expected = allowed.contains(to);
                assert_eq!(
                    from.can_transition_to(*to),
                    expected,
                    "{from} -> {to} 判定与架构 §7 不一致"
                );
            }
        }
    }

    #[test]
    fn boundary_transitions() {
        // 边界：COLD 只能 STARTING，不能直接 HOT（必须经过 Starting/Warm）。
        assert!(!LifecycleState::Cold.can_transition_to(LifecycleState::Hot));
        assert!(!LifecycleState::Cold.can_transition_to(LifecycleState::Cold));
        assert!(LifecycleState::Cold.can_transition_to(LifecycleState::Starting));
        // 边界：HOT 不能直接回 COLD，必须先降温或停止。
        assert!(!LifecycleState::Hot.can_transition_to(LifecycleState::Cold));
        // 边界：STARTING 不能被打断成 STOPPING。
        assert!(!LifecycleState::Starting.can_transition_to(LifecycleState::Stopping));
        // 边界：排空完成后即 COLD（架构的 Draining -> Empty 在 DB 语义下就是无进程）。
        assert!(LifecycleState::Draining.can_transition_to(LifecycleState::Cold));
        // 边界：FAILED 可重试启动，也可清理回 COLD。
        assert!(LifecycleState::Failed.can_transition_to(LifecycleState::Starting));
        assert!(!LifecycleState::Failed.can_transition_to(LifecycleState::Warm));
    }

    #[test]
    fn db_state_helpers() {
        assert!(LifecycleState::Warm.is_serving());
        assert!(LifecycleState::Hot.is_serving());
        assert!(!LifecycleState::Cold.is_serving());
        assert!(!LifecycleState::Starting.is_serving());

        assert!(!LifecycleState::Cold.occupies_process());
        for state in LifecycleState::ALL
            .iter()
            .filter(|s| **s != LifecycleState::Cold)
        {
            assert!(state.occupies_process(), "{state} 应占用进程位");
        }
        assert!(LifecycleState::Failed.occupies_process());
        assert!(LifecycleState::Cold.needs_start());
        assert!(LifecycleState::Failed.needs_start());
        assert!(!LifecycleState::Hot.needs_start());
    }

    #[test]
    fn all_legal_worker_transitions_are_allowed() {
        let legal: &[(WorkerState, &[WorkerState])] = &[
            (
                WorkerState::Active,
                &[
                    WorkerState::Suspect,
                    WorkerState::Draining,
                    WorkerState::Unavailable,
                ],
            ),
            (
                WorkerState::Suspect,
                &[
                    WorkerState::Active,
                    WorkerState::Unavailable,
                    WorkerState::Draining,
                ],
            ),
            (
                WorkerState::Draining,
                &[WorkerState::Empty, WorkerState::Unavailable],
            ),
            (
                WorkerState::Empty,
                &[WorkerState::Active, WorkerState::Unavailable],
            ),
            (WorkerState::Unavailable, &[WorkerState::Active]),
        ];
        for (from, targets) in legal {
            for to in WorkerState::ALL {
                assert_eq!(
                    from.can_transition_to(*to),
                    targets.contains(to),
                    "{from} -> {to} 判定与架构 §7 Worker 状态机不一致"
                );
            }
        }
    }

    #[test]
    fn worker_state_helpers() {
        assert!(WorkerState::Active.accepts_new_placement());
        assert!(!WorkerState::Suspect.accepts_new_placement());
        assert!(!WorkerState::Draining.accepts_new_placement());
        assert!(WorkerState::Suspect.serves_traffic());
        assert!(!WorkerState::Empty.serves_traffic());
        assert!(WorkerState::Unavailable.is_unavailable());
    }

    #[test]
    fn db_strings_match_migration_check_constraints() {
        let db_states = sql_check_values(INIT_SQL, "databases", "state");
        let ours: Vec<String> = LifecycleState::ALL
            .iter()
            .map(|s| s.to_db_str().to_string())
            .collect();
        assert_eq!(
            db_states, ours,
            "databases.state CHECK 取值与 LifecycleState 不一致"
        );

        let worker_states = sql_check_values(INIT_SQL, "workers", "state");
        let ours: Vec<String> = WorkerState::ALL
            .iter()
            .map(|s| s.to_db_str().to_string())
            .collect();
        assert_eq!(
            worker_states, ours,
            "workers.state CHECK 取值与 WorkerState 不一致"
        );
    }

    #[test]
    fn proto_values_match() {
        let proto = proto_enum_values(COMMON_PROTO, "LifecycleState");
        for (name, value) in &proto {
            if name == "LIFECYCLE_STATE_UNSPECIFIED" {
                assert_eq!(*value, 0);
                continue;
            }
            let state = LifecycleState::from_db_str(name)
                .unwrap_or_else(|_| panic!("proto {name} 在 Rust 侧缺失"));
            assert_eq!(state.to_proto_i32(), *value, "{name} 数值不一致");
        }
        assert_eq!(LifecycleState::ALL.len(), proto.len() - 1);

        let proto = proto_enum_values(COMMON_PROTO, "WorkerState");
        for (name, value) in &proto {
            if name == "WORKER_STATE_UNSPECIFIED" {
                assert_eq!(*value, 0);
                continue;
            }
            let bare = name.trim_start_matches("WORKER_STATE_");
            let state = WorkerState::from_db_str(bare)
                .unwrap_or_else(|_| panic!("proto {name} 在 Rust 侧缺失"));
            assert_eq!(state.to_proto_i32(), *value, "{name} 数值不一致");
        }
        assert_eq!(WorkerState::ALL.len(), proto.len() - 1);
    }

    #[test]
    fn parse_and_lossy_fallback() {
        assert_eq!(
            LifecycleState::from_db_str("warm").unwrap(),
            LifecycleState::Warm
        );
        assert!(LifecycleState::from_db_str("WAKING").is_err());
        assert_eq!(
            LifecycleState::from_db_str_lossy("WAKING"),
            LifecycleState::Cold
        );
        assert_eq!(
            WorkerState::from_str("suspECT").unwrap(),
            WorkerState::Suspect
        );
        assert_eq!(
            WorkerState::from_db_str_lossy("BROKEN"),
            WorkerState::Unavailable
        );
        assert_eq!(LifecycleState::from_proto_i32(-7), LifecycleState::Cold);
        assert_eq!(WorkerState::from_proto_i32(99), WorkerState::Unavailable);
    }

    #[test]
    fn serde_uses_db_strings() {
        assert_eq!(
            serde_json::to_string(&LifecycleState::Hot).unwrap(),
            "\"HOT\""
        );
        assert_eq!(
            serde_json::to_string(&WorkerState::Empty).unwrap(),
            "\"EMPTY\""
        );
        let parsed: LifecycleState = serde_json::from_str("\"STARTING\"").unwrap();
        assert_eq!(parsed, LifecycleState::Starting);
        let parsed: WorkerState = serde_json::from_str("\"garbage\"").unwrap();
        assert_eq!(parsed, WorkerState::Unavailable);
    }
}
