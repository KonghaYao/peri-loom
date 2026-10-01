//! db-runtime 的启动配置（命令行 / 环境变量）。
//!
//! 参数集合是 **Worker <-> DB Process 的冻结契约**：`WorkerConfig::runtime_args` 与
//! `WorkerConfig::runtime_env` 产出的每一项都必须在这里被接受。冲突时**命令行优先**，
//! 这是 clap 的既有语义：显式给出 `--x` 时不再读 `DB_RUNTIME_X`。
//!
//! 注意环境变量命名的两处历史包袱（必须兼容，不能"纠正"）：
//! - WAL 集群在 `runtime_env` 里叫 `WAL_CLUSTER`（不是 `DB_RUNTIME_WAL_CLUSTER`）；
//! - `--log-level` 只走命令行（Worker 把 `RUST_LOG` 的值塞进参数）。

use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use domain::session::{SESSION_IDLE_TIMEOUT, TRANSACTION_MAX_LIFETIME};

/// 默认 Workload 数据目录（与 Worker 的 `DEFAULT_DATA_DIR` 一致）。
pub const DEFAULT_DATA_DIR: &str = "/var/lib/db-platform";
/// 默认运行目录（UDS 所在，与 Worker 的 `DEFAULT_RUN_DIR` 一致）。
pub const DEFAULT_RUN_DIR: &str = "/run/db-platform";
/// Remote WAL 服务默认 gRPC 端口（与 Worker 的 `DEFAULT_WAL_GRPC_PORT` 一致）。
pub const DEFAULT_WAL_GRPC_PORT: u16 = 9200;
/// 请求未声明 deadline 时的兜底上限。
///
/// 为什么必须有：没有 deadline 的请求会把会话连接永久占住，一个卡死的语句就能让
/// 整个 DB 进程失去服务能力（架构 §15.3 的"绝不无限等"）。
pub const DEFAULT_REQUEST_DEADLINE: Duration = Duration::from_secs(30);
/// 握手等待 HelloAck 的上限。
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// 命令行参数（每项都可由 `DB_RUNTIME_*` 环境变量提供）。
#[derive(Debug, Clone, Parser)]
#[command(
    name = "db-runtime",
    version,
    about = "DB Platform DB Process：TursoDB 引擎宿主，仅通过 UDS 服务 Worker（架构 §1.2 / §5.3）"
)]
pub struct RuntimeConfig {
    /// 本节点 worker id。
    #[arg(long, env = "DB_RUNTIME_WORKER_ID", default_value = "worker-1")]
    pub worker_id: String,

    /// 本进程承载的数据库 id。
    #[arg(long, env = "DB_RUNTIME_DATABASE_ID")]
    pub database_id: String,

    /// 写 Owner epoch（fencing 身份，架构 §11.3）。
    #[arg(long, env = "DB_RUNTIME_OWNER_EPOCH", default_value_t = 0)]
    pub owner_epoch: u64,

    /// 数据库主文件路径。
    #[arg(long, env = "DB_RUNTIME_DB_PATH")]
    pub db_path: PathBuf,

    /// 本地 WAL 路径；缺省时按 `<db_path>-wal` 推导。
    #[arg(long, env = "DB_RUNTIME_WAL_PATH")]
    pub wal_path: Option<PathBuf>,

    /// UDS socket 路径（唯一对外接口，不监听任何 TCP 端口）。
    #[arg(long, env = "DB_RUNTIME_SOCKET_PATH")]
    pub socket_path: PathBuf,

    /// 数据目录。
    #[arg(long, env = "DB_RUNTIME_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,

    /// 运行目录。
    #[arg(long, env = "DB_RUNTIME_RUN_DIR", default_value = DEFAULT_RUN_DIR)]
    pub run_dir: PathBuf,

    /// CPU 配额（milli-core，仅用于日志与自检）。
    #[arg(long, env = "DB_RUNTIME_CPU_MILLI", default_value_t = 0)]
    pub cpu_milli: u64,

    /// 内存配额（MiB，仅用于日志与自检）。
    #[arg(long, env = "DB_RUNTIME_MEMORY_MIB", default_value_t = 0)]
    pub memory_mib: u64,

    /// 磁盘配额（MiB，仅用于日志与自检）。
    #[arg(long, env = "DB_RUNTIME_DISK_MIB", default_value_t = 0)]
    pub disk_mib: u64,

    /// 进程槽位（一个 DB = 一个进程；仅用于日志与自检）。
    #[arg(long, env = "DB_RUNTIME_PROCESS_SLOTS", default_value_t = 1)]
    pub process_slots: u32,

    /// 日志级别（Worker 把 `RUST_LOG` 的值传进来）。
    #[arg(long, env = "DB_RUNTIME_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Remote WAL 集群，形如 `1@wal-1:9201,2@wal-2:9201,3@wal-3:9201`。
    #[arg(long, env = "DB_RUNTIME_WAL_CLUSTER")]
    pub wal_cluster: Option<String>,
    /// Remote WAL **客户端** gRPC 端点（逗号分隔，如 wal-1:9200,wal-2:9200）。
    ///
    /// 为什么要与 `wal_cluster` 分开：后者的成员地址是**节点间 Raft 端口**（9201），
    /// 而平台调用 WAL 走的是客户端 gRPC 端口（9200）。从 raft 地址推导客户端地址会
    /// 连到错误端口，表现为 append 永远不返回，进而所有写请求都被 durability 门控拒绝。
    #[arg(long, env = "DB_RUNTIME_WAL_CLIENT_ENDPOINTS")]
    pub wal_client_endpoints: Option<String>,

    /// 只读实例：拒绝一切写语句。
    #[arg(long, env = "DB_RUNTIME_READ_ONLY", default_value_t = false)]
    pub read_only: bool,

    /// 快照 id（恢复来源；空 = 全新库）。
    #[arg(long, env = "DB_RUNTIME_SNAPSHOT_ID", default_value = "")]
    pub snapshot_id: String,

    /// 快照对应的 base LSN：Remote WAL 从这里开始回放。
    #[arg(long, env = "DB_RUNTIME_BASE_LSN", default_value_t = 0)]
    pub base_lsn: u64,
}

impl RuntimeConfig {
    /// 解析命令行 + 环境变量，并做一次启动前校验。
    pub fn from_args() -> Result<Self, ConfigError> {
        let mut config = Self::parse();
        // `WAL_CLUSTER` 兜底：Worker 的 runtime_env 用的就是这个名字。
        if config.wal_cluster.is_none() {
            config.wal_cluster = std::env::var("WAL_CLUSTER").ok();
        }
        if config.wal_client_endpoints.is_none() {
            config.wal_client_endpoints = std::env::var("WAL_CLIENT_ENDPOINTS").ok();
        }
        if config
            .wal_client_endpoints
            .as_deref()
            .is_some_and(|raw| raw.trim().is_empty())
        {
            config.wal_client_endpoints = None;
        }
        // 空字符串等价于"未配置"：容器编排常把未设置的变量注入成空串。
        if config
            .wal_cluster
            .as_deref()
            .is_some_and(|raw| raw.trim().is_empty())
        {
            config.wal_cluster = None;
        }
        config.validate()?;
        Ok(config)
    }

    /// 启动前校验：失败必须在打开引擎之前发生，避免半个进程已经跑起来。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.database_id.trim().is_empty() {
            return Err(ConfigError::Missing("--database-id"));
        }
        if self.socket_path.as_os_str().is_empty() {
            return Err(ConfigError::Missing("--socket-path"));
        }
        if self.db_path.as_os_str().is_empty() {
            return Err(ConfigError::Missing("--db-path"));
        }
        // `--db-path` 是主库**文件**，不是目录。指到目录上时引擎只会给出
        // `I/O error (open): is a directory`，Worker 侧看到的却是「子进程刚起来就退出」，
        // 因此这里在打开引擎之前就把原因说清楚。
        if self.db_path.is_dir() {
            return Err(ConfigError::Invalid(
                "--db-path 必须是数据库主文件，但该路径是一个目录（Worker 不应创建它）",
            ));
        }
        // 只读实例拿不到 snapshot 就没有可信起点，宁可在启动期失败。
        if self.read_only && self.base_lsn == 0 && self.snapshot_id.is_empty() {
            return Err(ConfigError::Invalid(
                "只读实例必须给出 --snapshot-id 或 --base-lsn（否则没有可服务的基线）",
            ));
        }
        Ok(())
    }

    /// 本地 WAL 路径：命令行优先，否则按 Turbo/SQLite 约定推导。
    #[must_use]
    pub fn resolved_wal_path(&self) -> PathBuf {
        self.wal_path
            .clone()
            .unwrap_or_else(|| engine_adapter::wal_path_for(&self.db_path))
    }

    /// 是否需要从 Remote WAL 恢复（failover 冷启动）。
    #[must_use]
    pub fn needs_recovery(&self) -> bool {
        self.base_lsn > 0 || !self.snapshot_id.is_empty()
    }

    /// 会话空闲超时（冻结默认 60s）。
    #[must_use]
    pub fn session_idle_timeout(&self) -> Duration {
        SESSION_IDLE_TIMEOUT
    }

    /// 单事务最大存活（冻结默认 30s）。
    #[must_use]
    pub fn transaction_max_lifetime(&self) -> Duration {
        TRANSACTION_MAX_LIFETIME
    }

    /// 由 `--wal-cluster` 推导 Remote WAL 端点（`1@host:port` -> `http://host:port`）。
    ///
    /// 与 Worker 的 `parse_wal_endpoints` 保持同一套规则：只做 scheme 补全与端口兜底，
    /// 不做 DNS 解析——解析失败要在第一次 append 时暴露，而不是启动期猜错端点。
    #[must_use]
    pub fn wal_endpoints(&self) -> Vec<String> {
        // 显式客户端端点优先：`wal_cluster` 里是节点间 Raft 端口，不能当客户端地址用。
        if let Some(raw) = self.wal_client_endpoints.as_deref() {
            let endpoints: Vec<String> = raw
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(normalize_client_endpoint)
                .collect();
            if !endpoints.is_empty() {
                return endpoints;
            }
        }
        self.wal_cluster
            .as_deref()
            .map(|cluster| {
                cluster
                    .split(',')
                    .filter_map(parse_cluster_member)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// DB pid 文件路径（当前仅用于日志定位，落盘在 run_dir 下）。
    #[must_use]
    pub fn log_identity(&self) -> String {
        format!(
            "{}@{}#{pid}",
            self.database_id,
            self.worker_id,
            pid = std::process::id()
        )
    }

    /// 数据库主文件所在目录（启动前确保存在）。
    #[must_use]
    pub fn db_dir(&self) -> &Path {
        self.db_path.parent().unwrap_or(Path::new("."))
    }
}

/// 把 `host:port` / `http://host:port` 归一成带 scheme 的端点。
fn normalize_client_endpoint(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_owned()
    } else {
        format!("http://{raw}")
    }
}

/// 单个集群成员 -> 端点。
fn parse_cluster_member(member: &str) -> Option<String> {
    let member = member.trim();
    if member.is_empty() {
        return None;
    }
    // 形如 `1@wal-1:9201`；没有 `@` 时整串就是地址（便于本地手工排障）。
    let address = member.split_once('@').map_or(member, |(_, addr)| addr);
    let address = address.trim();
    if address.is_empty() {
        return None;
    }
    if address.contains(':') {
        Some(format!("http://{address}"))
    } else {
        Some(format!("http://{address}:{DEFAULT_WAL_GRPC_PORT}"))
    }
}

/// 配置错误。
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// 必填项缺失。
    #[error("缺少必填参数：{0}")]
    Missing(&'static str),
    /// 取值不合法。
    #[error("参数不合法：{0}")]
    Invalid(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 集群成员解析：三种写法都要能落到同一个端点形态。
    #[test]
    fn parses_wal_cluster_members() {
        let config = RuntimeConfig {
            wal_cluster: Some("1@wal-1:9201, 2@wal-2:9201,wal-3".to_string()),
            wal_client_endpoints: None,
            ..sample()
        };
        assert_eq!(
            config.wal_endpoints(),
            vec![
                "http://wal-1:9201".to_string(),
                "http://wal-2:9201".to_string(),
                "http://wal-3:9200".to_string(),
            ]
        );
    }

    /// 未配置 WAL 集群时不能凭空造端点。
    #[test]
    fn empty_cluster_yields_no_endpoints() {
        let config = RuntimeConfig {
            wal_cluster: None,
            wal_client_endpoints: None,
            ..sample()
        };
        assert!(config.wal_endpoints().is_empty());
    }

    /// WAL 路径未显式给出时按 `<db>-wal` 推导。
    #[test]
    fn derives_wal_path_from_db_path() {
        let config = RuntimeConfig {
            db_path: PathBuf::from("/var/lib/platform/dbs/a.db"),
            wal_path: None,
            ..sample()
        };
        assert_eq!(
            config.resolved_wal_path(),
            PathBuf::from("/var/lib/platform/dbs/a.db-wal")
        );
    }

    /// `--db-path` 指到目录上必须在启动期就拒绝（引擎只会给出 "is a directory"）。
    #[test]
    fn rejects_db_path_that_is_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let config = RuntimeConfig {
            db_path: dir.path().to_path_buf(),
            ..sample()
        };
        assert!(config.validate().is_err());

        // 不存在的路径是合法输入：首次启动由引擎创建主库文件
        let config = RuntimeConfig {
            db_path: dir.path().join("new.db"),
            ..sample()
        };
        assert!(config.validate().is_ok());
    }

    fn sample() -> RuntimeConfig {
        RuntimeConfig {
            worker_id: "worker-1".to_string(),
            database_id: "db-1".to_string(),
            owner_epoch: 1,
            db_path: PathBuf::from("/tmp/a.db"),
            wal_path: None,
            socket_path: PathBuf::from("/tmp/a.sock"),
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            run_dir: PathBuf::from(DEFAULT_RUN_DIR),
            cpu_milli: 1000,
            memory_mib: 256,
            disk_mib: 1024,
            process_slots: 1,
            log_level: "info".to_string(),
            wal_cluster: None,
            wal_client_endpoints: None,
            read_only: false,
            snapshot_id: String::new(),
            base_lsn: 0,
        }
    }
}
