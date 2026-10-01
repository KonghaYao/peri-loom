//! 启动配置：CLI / 环境变量（架构 §17.14 与 docker-compose 的 `wal-env` 段）。
//!
//! 环境变量契约（已冻结，compose 与 .env.example 均按此命名）：
//!
//! ```text
//! WAL_NODE_ID            本节点在 Raft Group 中的 ID（u64，非 0）
//! WAL_SHARD_ID           WAL Shard 标识；一个 shard = 一个 Raft Group
//! WAL_LISTEN             客户端 gRPC 监听（RemoteWal service）
//! WAL_PEER_LISTEN        节点间 Raft 通信监听（自定义帧协议，见 peer.rs）
//! OPS_LISTEN             Ops HTTP 监听（/healthz、/readyz、/metrics）
//! WAL_DATA_DIR           raft-engine 日志目录（必须独立 NVMe，不与 Worker 混用）
//! WAL_CLUSTER            `1@host:port,2@host:port,3@host:port`，启动时的成员表
//! WAL_APPEND_TIMEOUT_MS  Append 等待 quorum durable 的上限
//! WAL_TRIM_INTERVAL_MS   状态机快照 / Trim 巡检周期
//! ```
//!
//! 单节点集群（`WAL_CLUSTER` 只含本节点，或留空）必须可用：开发机只起一个进程时，
//! 单节点 Raft 立即选出 leader，Append 语义退化为「本地 fsync 即 durable」，
//! 但**不改变**客户端看到的接口与错误语义。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::error::{WalError, WalResult};

/// 集群成员（`WAL_CLUSTER` 的一项）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMember {
    /// Raft 节点 ID。
    pub node_id: u64,
    /// 节点间通信地址（`host:port`，必须是本节点 WAL_PEER_LISTEN 可达地址）。
    pub endpoint: String,
}

/// wal-service 运行配置。
#[derive(Debug, Clone, Parser)]
#[command(
    name = "wal-service",
    about = "DB Platform Remote WAL Service（架构 §17.8：Rust + gRPC + raft-rs + raft-engine）",
    version
)]
pub struct WalConfig {
    /// 本节点 ID（必须出现在 WAL_CLUSTER 中）。
    #[arg(long = "node-id", env = "WAL_NODE_ID", default_value_t = 1)]
    pub node_id: u64,

    /// WAL Shard 标识；一个 shard 承载大量 DB（非一 DB 一 Raft Group）。
    #[arg(long = "shard-id", env = "WAL_SHARD_ID", default_value = "shard-0")]
    pub shard_id: String,

    /// 客户端 gRPC 监听地址。
    #[arg(long = "listen", env = "WAL_LISTEN", default_value = "0.0.0.0:9200")]
    pub listen: SocketAddr,

    /// 节点间 Raft 通信监听地址。
    #[arg(
        long = "peer-listen",
        env = "WAL_PEER_LISTEN",
        default_value = "0.0.0.0:9201"
    )]
    pub peer_listen: SocketAddr,

    /// Ops HTTP 监听地址（/healthz、/readyz、/metrics）。
    #[arg(
        long = "ops-listen",
        env = "OPS_LISTEN",
        default_value = "0.0.0.0:9300"
    )]
    pub ops_listen: SocketAddr,

    /// raft-engine 数据目录。
    #[arg(
        long = "data-dir",
        env = "WAL_DATA_DIR",
        default_value = "/var/lib/db-platform/wal"
    )]
    pub data_dir: PathBuf,

    /// 集群成员表：`1@host:port,2@host:port`。
    #[arg(long = "cluster", env = "WAL_CLUSTER", default_value = "")]
    pub cluster: String,

    /// Append 等待 quorum durable 的超时（毫秒）。
    #[arg(
        long = "append-timeout-ms",
        env = "WAL_APPEND_TIMEOUT_MS",
        default_value_t = 5000
    )]
    pub append_timeout_ms: u64,

    /// 状态机快照（写入 raft-engine KV）/ Trim 巡检周期（毫秒）。
    #[arg(
        long = "trim-interval-ms",
        env = "WAL_TRIM_INTERVAL_MS",
        default_value_t = 60_000
    )]
    pub trim_interval_ms: u64,

    /// Raft tick 周期（毫秒）。未在环境变量契约中固定，留默认值即可。
    #[arg(
        long = "tick-interval-ms",
        env = "WAL_TICK_INTERVAL_MS",
        default_value_t = 100
    )]
    pub tick_interval_ms: u64,

    /// Raft 选举超时（tick 数）。默认 10 tick = 1s。
    #[arg(
        long = "election-tick",
        env = "WAL_ELECTION_TICK",
        default_value_t = 10
    )]
    pub election_tick: usize,

    /// Raft 心跳周期（tick 数）。默认 3 tick = 300ms。
    #[arg(
        long = "heartbeat-tick",
        env = "WAL_HEARTBEAT_TICK",
        default_value_t = 3
    )]
    pub heartbeat_tick: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            shard_id: "shard-0".to_owned(),
            listen: "0.0.0.0:9200".parse().expect("默认监听地址必须合法"),
            peer_listen: "0.0.0.0:9201".parse().expect("默认监听地址必须合法"),
            ops_listen: "0.0.0.0:9300".parse().expect("默认监听地址必须合法"),
            data_dir: PathBuf::from("/var/lib/db-platform/wal"),
            cluster: String::new(),
            append_timeout_ms: 5000,
            trim_interval_ms: 60_000,
            tick_interval_ms: 100,
            election_tick: 10,
            heartbeat_tick: 3,
        }
    }
}

impl WalConfig {
    /// Append 等待超时。
    pub fn append_timeout(&self) -> Duration {
        Duration::from_millis(self.append_timeout_ms)
    }

    /// 状态机快照 / Trim 巡检周期（下限 1s，避免误配置把 CPU 打满）。
    pub fn trim_interval(&self) -> Duration {
        Duration::from_millis(self.trim_interval_ms.max(1000))
    }

    /// Raft tick 周期（下限 10ms）。
    pub fn tick_interval(&self) -> Duration {
        Duration::from_millis(self.tick_interval_ms.max(10))
    }

    /// 解析 `WAL_CLUSTER`；留空时退化为「单节点集群 = 本节点」，便于本地单进程验证。
    pub fn members(&self) -> WalResult<Vec<ClusterMember>> {
        if self.cluster.trim().is_empty() {
            return Ok(vec![ClusterMember {
                node_id: self.node_id,
                endpoint: self.peer_listen.to_string(),
            }]);
        }
        let mut members = Vec::new();
        let mut seen: BTreeMap<u64, ()> = BTreeMap::new();
        for raw in self.cluster.split(',') {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let (id, endpoint) = raw.split_once('@').ok_or_else(|| {
                WalError::InvalidArgument(format!(
                    "WAL_CLUSTER 项 `{raw}` 非法：期望 `node_id@host:port`"
                ))
            })?;
            let node_id: u64 = id.trim().parse().map_err(|_| {
                WalError::InvalidArgument(format!("WAL_CLUSTER 项 `{raw}` 的 node_id 非法"))
            })?;
            if node_id == 0 {
                return Err(WalError::InvalidArgument(
                    "node_id 0 是 raft 保留值（RawNode 会 panic），不允许出现在集群成员表中".into(),
                ));
            }
            let endpoint = endpoint.trim();
            if endpoint.is_empty() {
                return Err(WalError::InvalidArgument(format!(
                    "WAL_CLUSTER 项 `{raw}` 缺少 endpoint"
                )));
            }
            if seen.insert(node_id, ()).is_some() {
                return Err(WalError::InvalidArgument(format!(
                    "WAL_CLUSTER 中 node_id {node_id} 重复"
                )));
            }
            members.push(ClusterMember {
                node_id,
                endpoint: endpoint.to_owned(),
            });
        }

        if members.is_empty() {
            return Err(WalError::InvalidArgument(
                "WAL_CLUSTER 非空但不含任何合法成员".into(),
            ));
        }
        if !members.iter().any(|m| m.node_id == self.node_id) {
            // 允许启动但必须显式暴露：本节点不在成员表里会造成「永远选不出 leader」
            return Err(WalError::InvalidArgument(format!(
                "本节点 node_id={} 不在 WAL_CLUSTER 成员表中",
                self.node_id
            )));
        }
        Ok(members)
    }

    /// 启动期自检：把非法配置挡在起进程阶段，而不是运行时才炸。
    pub fn validate(&self) -> WalResult<()> {
        if self.node_id == 0 {
            return Err(WalError::InvalidArgument("WAL_NODE_ID 不能为 0".into()));
        }
        if self.shard_id.trim().is_empty() {
            return Err(WalError::InvalidArgument("WAL_SHARD_ID 不能为空".into()));
        }
        if self.listen.port() != 0 && self.listen == self.peer_listen {
            return Err(WalError::InvalidArgument(
                "WAL_LISTEN 与 WAL_PEER_LISTEN 不能相同：前者是 gRPC 契约端口，后者是 Raft 帧协议端口"
                    .into(),
            ));
        }
        // 端口为 0 表示「由内核分配」：此时地址相等不代表冲突（集成测试就依赖这一点），
        // 但**非 0** 端口之间不能撞车，否则第二个绑定的监听会直接失败。
        let mut bound: Vec<(&str, SocketAddr)> = Vec::new();
        for (name, addr) in [
            ("WAL_LISTEN", self.listen),
            ("WAL_PEER_LISTEN", self.peer_listen),
            ("OPS_LISTEN", self.ops_listen),
        ] {
            if addr.port() == 0 {
                continue;
            }
            if let Some((other, _)) = bound.iter().find(|(_, existing)| *existing == addr) {
                return Err(WalError::InvalidArgument(format!(
                    "{name} 与 {other} 不能使用同一地址 {addr}：三个监听必须彼此独立"
                )));
            }
            bound.push((name, addr));
        }
        if self.heartbeat_tick == 0 || self.election_tick <= self.heartbeat_tick {
            return Err(WalError::InvalidArgument(format!(
                "Raft tick 配置非法：heartbeat_tick={} 必须 >0 且 < election_tick={}",
                self.heartbeat_tick, self.election_tick
            )));
        }
        if self.append_timeout_ms == 0 {
            return Err(WalError::InvalidArgument(
                "WAL_APPEND_TIMEOUT_MS 不能为 0：没有超时的 Append 会永久挂住 gRPC 调用".into(),
            ));
        }
        self.members()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_cluster(cluster: &str) -> WalConfig {
        WalConfig {
            cluster: cluster.to_owned(),
            ..WalConfig::default()
        }
    }

    #[test]
    fn empty_cluster_falls_back_to_single_node() {
        let cfg = config_with_cluster("");
        let members = cfg.members().unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].node_id, 1);
        assert_eq!(members[0].endpoint, "0.0.0.0:9201");
        cfg.validate().unwrap();
    }

    #[test]
    fn three_node_cluster_parses() {
        let cfg = config_with_cluster("1@wal-1:9201, 2@wal-2:9201 ,3@wal-3:9201");
        let members = cfg.members().unwrap();
        assert_eq!(members.len(), 3);
        assert_eq!(members[2].node_id, 3);
        assert_eq!(members[2].endpoint, "wal-3:9201");
        cfg.validate().unwrap();
    }

    #[test]
    fn malformed_cluster_is_rejected() {
        assert!(config_with_cluster("1-wal-1:9201").members().is_err());
        assert!(config_with_cluster("0@wal-1:9201").members().is_err());
        assert!(config_with_cluster("1@wal-1:9201,1@wal-2:9201")
            .members()
            .is_err());
        // 本节点不在成员表：会让集群永远选不出 leader，必须启动即失败
        let cfg = WalConfig {
            node_id: 9,
            ..config_with_cluster("1@wal-1:9201,2@wal-2:9201")
        };
        assert!(cfg.members().is_err());
    }

    #[test]
    fn validate_rejects_same_listen_port() {
        let cfg = WalConfig {
            listen: "0.0.0.0:9200".parse().unwrap(),
            peer_listen: "0.0.0.0:9200".parse().unwrap(),
            ..WalConfig::default()
        };
        assert!(cfg.validate().is_err());
    }
}
