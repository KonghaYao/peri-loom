//! 公共数据模型（domain 类型、无 proto 类型泄漏）。

use std::time::Duration;

use bytes::Bytes;
use domain::{DatabaseId, Lsn};

/// 一次 WAL append 请求。
///
/// 注意 `append_id` 与 `start_lsn` 一起构成服务端的幂等键：**同一次业务写入的重试必须
/// 复用同一个 `append_id`**，否则「请求已提交但 ACK 丢失」时会重复写入 WAL。
#[derive(Clone, Debug)]
pub struct AppendWalRequest {
    /// 目标数据库。
    pub database_id: DatabaseId,
    /// 写 Owner 的 epoch；服务端用它做 storage-level fencing。
    pub owner_epoch: u64,
    /// 本批次第一个字节对应的 LSN（exclusive 末端由服务端返回）。
    pub start_lsn: Lsn,
    /// 对应 proto `AppendRequest.wal_file_offset`：本地 WAL 文件内的写入偏移，
    /// failover 恢复时按「同偏移回放」重建，必须随字节一起持久化。
    pub file_offset: u64,
    /// 对应 proto `AppendRequest.reset_wal`：本批次是否开启新一代本地 WAL。
    pub reset_wal: bool,
    /// WAL 原始字节。
    pub bytes: Bytes,
    /// 本批次是否包含 commit frame（允许服务端观测 commit 点）。
    pub contains_commit_frame: bool,
    /// 幂等键；**重试同一批次时必须保持不变**。空字符串会被拒绝。
    pub append_id: String,
}

/// 一次成功 append 的结果。
#[derive(Clone, Debug)]
pub struct AppendOutcome {
    /// 已 durable 的末端 LSN（exclusive）。
    pub durable_lsn: Lsn,
    /// 参与 quorum 确认的副本列表。
    pub acked_replicas: Vec<String>,
    /// 客户端观测到的本批次总耗时（含重试与退避）。
    pub latency: Duration,
    /// 幂等命中：该 append 在服务端之前已经提交过。
    pub deduplicated: bool,
}

/// `read_range` 返回的一段 WAL 数据。
#[derive(Clone, Debug)]
pub struct WalSegment {
    /// 该段起始 LSN。
    pub start_lsn: Lsn,
    /// 回放时要写入本地 WAL 文件的偏移。
    pub file_offset: u64,
    /// 该段是否开启新一代 WAL（回放前需先截断本地 WAL）。
    pub reset_wal: bool,
    /// 该段字节。
    pub data: Bytes,
}

/// 某个 DB 的 WAL 状态快照。
#[derive(Clone, Debug)]
pub struct WalStatus {
    /// 该 DB 是否已有 WAL 数据。
    pub has_data: bool,
    /// 现存第一个 LSN。
    pub first_lsn: Lsn,
    /// 现存末端 LSN（exclusive）。
    pub last_lsn: Lsn,
    /// 服务端记录的 owner epoch。
    pub owner_epoch: u64,
}

/// WAL 组健康状态（`health` 的聚合结果）。
#[derive(Clone, Debug)]
pub struct WalHealth {
    /// 该 WAL 组当前是否可接受写入（能联系到 leader 且其自评为健康）。
    pub healthy: bool,
    /// 应答节点是否就是 leader。
    pub is_leader: bool,
    /// Raft term。
    pub term: u64,
    /// 应答节点认为的 leader（node id；未知时为空串）。
    pub leader_id: String,
    /// 应答节点的 node id。
    pub node_id: String,
}
