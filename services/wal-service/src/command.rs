//! Raft 日志命令定义（状态机的唯一输入）与 raft-engine 持久化键。
//!
//! 为什么命令用 prost 手写结构体而不是新建 .proto：
//! 平台 gRPC 契约只覆盖 `platform.wal.v1.RemoteWal`；Raft 日志是 **WAL Service 内部**
//! 的复制载荷，把它塞进对外 proto 会让内部实现细节成为跨服务契约（例如未来换
//! 复制协议就要改 proto）。而 prost 支持在 Rust 结构体上直接 derive `Message`，
//! 因此这里用「手写结构体 + prost 编解码」拿到紧凑二进制与 0 额外依赖，
//! 同时不触碰 `proto/*.proto`（禁止修改）。
//!
//! 命令必须**确定性**：状态机在 3 个副本上独立 apply，任何非确定性字段
//! （时间戳、随机数、本地状态）都不得进入命令，否则副本状态会发散。

use prost::Message as _;

/// HardState 在 raft-engine KV 中的键（按 Raft Group 分区，键名不得以 `__` 开头 ——
/// raft-engine 保留该前缀）。
pub const HARD_STATE_KEY: &[u8] = b"wal-hard-state";

/// ConfState 在 raft-engine KV 中的键。
pub const CONF_STATE_KEY: &[u8] = b"wal-conf-state";

/// 状态机快照在 raft-engine KV 中的键。
pub const SM_SNAPSHOT_KEY: &[u8] = b"wal-state-machine";

/// 状态机快照的最小写入间隔（apply 次数）。
///
/// 快照的唯一目的是缩短重启后的 replay 长度（raft 日志本身不丢，见 storage.rs），
/// 因此不需要每条命令都写：4096 条命令一次足够把启动 replay 控制在秒级。
pub const SM_SNAPSHOT_APPLY_INTERVAL: u64 = 4096;

/// 一条 Raft 日志命令。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct WalCommand {
    /// 命令体（oneof）。
    #[prost(oneof = "wal_command::Kind", tags = "1, 2, 3")]
    pub kind: Option<wal_command::Kind>,
}

/// 命令体枚举。
pub mod wal_command {
    use super::{AppendCommand, SetEpochCommand, TrimCommand};

    /// 状态机支持的命令。
    #[derive(Clone, PartialEq, Eq, prost::Oneof)]
    pub enum Kind {
        /// 设置 / 提升 owner epoch（fencing 的核心，必须严格单调）。
        #[prost(message, tag = "1")]
        SetEpoch(SetEpochCommand),
        /// 追加 WAL 字节（成功 = quorum durable）。
        #[prost(message, tag = "2")]
        Append(AppendCommand),
        /// 截断 before_lsn 之前的 WAL（快照已覆盖）。
        #[prost(message, tag = "3")]
        Trim(TrimCommand),
    }
}

/// `SetOwnerEpoch` 命令。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SetEpochCommand {
    /// 目标 DB。
    #[prost(string, tag = "1")]
    pub database_id: String,
    /// 新 epoch，必须严格大于已记录值。
    #[prost(uint64, tag = "2")]
    pub owner_epoch: u64,
    /// 新 Owner Worker（诊断用；不参与 fencing 判定，判定只看 epoch）。
    #[prost(string, tag = "3")]
    pub worker_id: String,
    /// 变更原因（诊断用，例如 `failover` / `move`）。
    #[prost(string, tag = "4")]
    pub reason: String,
}

/// `Append` 命令。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct AppendCommand {
    /// 目标 DB。
    #[prost(string, tag = "1")]
    pub database_id: String,
    /// 写入者 epoch；`< 已记录 epoch` 一律拒绝（Storage-level Fencing）。
    #[prost(uint64, tag = "2")]
    pub owner_epoch: u64,
    /// 本批次第一个字节对应的 LSN。
    #[prost(uint64, tag = "3")]
    pub start_lsn: u64,
    /// 该批次首字节在本地 WAL 文件中的偏移（failover 时按同偏移回放）。
    #[prost(uint64, tag = "4")]
    pub wal_file_offset: u64,
    /// 是否开启新一代 WAL（回放前需先截断本地 WAL 文件）。
    #[prost(bool, tag = "5")]
    pub reset_wal: bool,
    /// 本批次的 WAL 字节。
    #[prost(bytes = "vec", tag = "6")]
    pub bytes: Vec<u8>,
    /// 客户端幂等键。
    #[prost(string, tag = "7")]
    pub append_id: String,
    /// 本批次是否包含 commit frame（仅记录，供上层观测 commit 点）。
    #[prost(bool, tag = "8")]
    pub contains_commit_frame: bool,
}

/// `TrimBeforeLsn` 命令。
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct TrimCommand {
    /// 目标 DB。
    #[prost(string, tag = "1")]
    pub database_id: String,
    /// 截断水位：`lsn` 之前的 WAL 数据可丢弃。
    #[prost(uint64, tag = "2")]
    pub before_lsn: u64,
    /// 覆盖该区间的 snapshot_id（防止误截断；状态机只在非空时落库）。
    #[prost(string, tag = "3")]
    pub snapshot_id: String,
}

impl WalCommand {
    /// 设置 owner epoch。
    pub fn set_epoch(command: SetEpochCommand) -> Self {
        Self {
            kind: Some(wal_command::Kind::SetEpoch(command)),
        }
    }

    /// 追加 WAL。
    pub fn append(command: AppendCommand) -> Self {
        Self {
            kind: Some(wal_command::Kind::Append(command)),
        }
    }

    /// 截断 WAL。
    pub fn trim(command: TrimCommand) -> Self {
        Self {
            kind: Some(wal_command::Kind::Trim(command)),
        }
    }

    /// 命令类型名（用于日志与指标标签，避免打印整个 payload）。
    pub const fn kind_name(&self) -> &'static str {
        match self.kind {
            Some(wal_command::Kind::SetEpoch(_)) => "set_epoch",
            Some(wal_command::Kind::Append(_)) => "append",
            Some(wal_command::Kind::Trim(_)) => "trim",
            None => "empty",
        }
    }

    /// 编码为 Raft 日志条目 payload。
    pub fn encode(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    /// 从 Raft 日志条目 payload 解码。
    pub fn decode(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        // 显式走 trait 方法：固有方法名与 trait 方法同名，直接 Self::decode 会无限递归
        <Self as prost::Message>::decode(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_command_roundtrip_preserves_bytes() {
        let command = WalCommand::append(AppendCommand {
            database_id: "db-1".into(),
            owner_epoch: 7,
            start_lsn: 4096,
            wal_file_offset: 128,
            reset_wal: true,
            bytes: vec![0, 1, 2, 250, 251, 252],
            append_id: "ap-1".into(),
            contains_commit_frame: true,
        });
        let encoded = command.encode();
        let decoded = WalCommand::decode(&encoded).unwrap();
        assert_eq!(decoded, command);
        assert_eq!(decoded.kind_name(), "append");
        // 编码必须是确定性的：副本之间必须得到完全相同的字节
        assert_eq!(encoded, command.encode());
    }

    #[test]
    fn set_epoch_and_trim_roundtrip() {
        let set_epoch = WalCommand::set_epoch(SetEpochCommand {
            database_id: "db-1".into(),
            owner_epoch: 835,
            worker_id: "worker-2".into(),
            reason: "failover".into(),
        });
        assert_eq!(
            WalCommand::decode(&set_epoch.encode()).unwrap(),
            set_epoch,
            "SetEpoch 命令必须可无损往返"
        );

        let trim = WalCommand::trim(TrimCommand {
            database_id: "db-1".into(),
            before_lsn: 8192,
            snapshot_id: "snap-1".into(),
        });
        assert_eq!(WalCommand::decode(&trim.encode()).unwrap(), trim);
    }

    #[test]
    fn invalid_payload_is_rejected_not_panicking() {
        // 日志损坏必须返回错误而不是 panic：Raft 日志可能包含旧版本写入的载荷
        assert!(WalCommand::decode(&[0xff, 0xff, 0xff]).is_err());
        assert!(WalCommand::decode(&[]).is_ok(), "空载荷解码为无命令");
        assert_eq!(WalCommand::decode(&[]).unwrap().kind_name(), "empty");
    }

    #[test]
    fn command_payload_is_compact() {
        // 命令头部开销必须远小于数据本体，否则复制成本会被元数据吃掉
        let command = WalCommand::append(AppendCommand {
            database_id: "db-1".into(),
            owner_epoch: 1,
            start_lsn: 0,
            wal_file_offset: 0,
            reset_wal: false,
            bytes: vec![7u8; 1024 * 1024],
            append_id: String::new(),
            contains_commit_frame: false,
        });
        let encoded = command.encode();
        assert!(
            encoded.len() < 1024 * 1024 + 64,
            "命令编码长度 {} 超出预期的元数据开销",
            encoded.len()
        );
    }

    #[test]
    fn storage_keys_avoid_raft_engine_reserved_prefix() {
        // raft-engine 把 `__` 前缀保留给内部键，业务键不得使用
        for key in [HARD_STATE_KEY, CONF_STATE_KEY, SM_SNAPSHOT_KEY] {
            assert!(
                !key.starts_with(b"__"),
                "键 {key:?} 使用了 raft-engine 保留前缀"
            );
        }
    }
}
