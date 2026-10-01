//! db-worker CLI / 环境变量配置（clap）。
//!
//! 约定（与 `.env.example` / `docker-compose.yml` 对齐，不得擅自改名）：
//! 每个参数都同时支持长选项与对应环境变量；环境变量是容器部署的主路径。
//!
//! 这里的取值会进一步传给 DB Process（见 [`WorkerConfig::runtime_args`]），
//! **该参数集合是 Worker 与 db-runtime 的冻结契约**，变更必须两侧同步。

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use domain::resources::WorkerCapacity;

use crate::error::{Result, WorkerError};
use crate::paths;

/// 默认 DB Runtime 二进制路径（镜像内固定位置）。
pub const DEFAULT_DB_RUNTIME_BIN: &str = "/usr/local/bin/db-runtime";
/// 默认 cgroup v2 根。
pub const DEFAULT_CGROUP_ROOT: &str = "/sys/fs/cgroup";
/// 默认 Workload 数据目录。
pub const DEFAULT_DATA_DIR: &str = "/var/lib/db-platform";
/// 默认运行目录（UDS 所在）。
pub const DEFAULT_RUN_DIR: &str = "/run/db-platform";
/// Remote WAL 服务默认 gRPC 端口（由 `WAL_CLUSTER` 推导端点时使用）。
pub const DEFAULT_WAL_GRPC_PORT: u16 = 9200;

/// 命令行参数。
#[derive(Debug, Clone, Parser)]
#[command(
    name = "db-worker",
    version,
    about = "DB Platform Worker：DB Process 宿主 + Data Dispatcher（架构 §5 / §17.6）"
)]
pub struct Cli {
    /// 本节点 worker id（Catalog 中唯一）。
    #[arg(long, env = "WORKER_ID", default_value = "worker-1")]
    pub worker_id: String,

    /// 区域标签（供调度做拓扑约束）。
    #[arg(long, env = "WORKER_REGION", default_value = "default")]
    pub region: String,

    /// 可用区标签。
    #[arg(long, env = "WORKER_ZONE", default_value = "default")]
    pub zone: String,

    /// 控制面 gRPC 监听地址（Server -> Worker：Start/Stop/Drain/Move）。
    #[arg(long, env = "WORKER_CONTROL_LISTEN", default_value = "0.0.0.0:9100")]
    pub control_listen: SocketAddr,

    /// 数据面 gRPC 监听地址（Server -> Worker：Execute/Stream/Session/Cancel）。
    #[arg(long, env = "WORKER_DATA_LISTEN", default_value = "0.0.0.0:9101")]
    pub data_listen: SocketAddr,

    /// Ops HTTP 监听地址（/healthz /readyz /metrics）。
    #[arg(long, env = "OPS_LISTEN", default_value = "0.0.0.0:9090")]
    pub ops_listen: SocketAddr,

    /// db-server 控制面地址（本进程作为 client 上报 Heartbeat）。
    #[arg(
        long,
        env = "SERVER_CONTROL_ENDPOINT",
        default_value = "http://127.0.0.1:8081"
    )]
    pub server_control_endpoint: String,

    /// 本 Worker 对外通告的地址（心跳上报；留空则用 control_listen）。
    #[arg(long, env = "WORKER_ADVERTISE_ENDPOINT", default_value = "")]
    pub advertise_endpoint: String,

    /// 对外通告的**数据面**地址（心跳上报；留空则用 advertise_endpoint 的 host + data_listen 端口）。
    ///
    /// 数据面与控制面是不同端口上的不同服务，必须分别上报，否则 Server 会把
    /// SQL 请求发到控制端口而拿到 Unimplemented。
    #[arg(long, env = "WORKER_ADVERTISE_DATA_ENDPOINT", default_value = "")]
    pub advertise_data_endpoint: String,

    /// 本地 NVMe 工作集根目录。
    #[arg(long, env = "WORKER_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,

    /// 运行目录（UDS socket 所在）。
    #[arg(long, env = "WORKER_RUN_DIR", default_value = DEFAULT_RUN_DIR)]
    pub run_dir: PathBuf,

    /// CPU 容量（milli-core，1000 = 1 vCPU）。
    #[arg(long, env = "WORKER_CPU_MILLI", default_value_t = 32000)]
    pub cpu_milli: u64,

    /// 内存容量（MiB）。
    #[arg(long, env = "WORKER_MEMORY_MIB", default_value_t = 131_072)]
    pub memory_mib: u64,

    /// 本地磁盘容量（MiB）。
    #[arg(long, env = "WORKER_DISK_MIB", default_value_t = 512_000)]
    pub disk_mib: u64,

    /// 进程位数量（容量口径）。
    #[arg(long, env = "WORKER_PROCESS_SLOTS", default_value_t = 2048)]
    pub process_slots: u64,

    /// IOPS 容量提示。
    #[arg(long, env = "WORKER_IOPS", default_value_t = 200_000)]
    pub iops: u64,

    /// DB 进程数硬上限（DB 数量只作 hard safety limit，架构 §9）。
    #[arg(long, env = "WORKER_MAX_DB_PROCESS", default_value_t = 256)]
    pub max_db_process: u64,

    /// 心跳周期（架构 §16：1s/次，连续 3 次 miss 判 Suspect）。
    #[arg(long, env = "HEARTBEAT_INTERVAL_MS", default_value_t = 1000)]
    pub heartbeat_interval_ms: u64,

    /// cgroup v2 根路径。容器内通常已挂载到 `/sys/fs/cgroup`。
    ///
    /// 同时接受 `CGROUP_ROOT`（compose）与 `WORKER_CGROUP_ROOT`（.env.example）两个环境变量。
    #[arg(long)]
    pub cgroup_root: Option<PathBuf>,

    /// 关闭 cgroup 限制（仅排障用；生产必须开启）。
    #[arg(long, env = "WORKER_CGROUP_DISABLED", default_value_t = false)]
    pub cgroup_disabled: bool,

    /// DB Runtime 二进制路径。
    #[arg(long, env = "DB_RUNTIME_BIN", default_value = DEFAULT_DB_RUNTIME_BIN)]
    pub db_runtime_bin: PathBuf,

    /// DB 进程 READY 等待上限（毫秒）。
    #[arg(long, env = "DB_RUNTIME_READY_TIMEOUT_MS", default_value_t = 5000)]
    pub runtime_ready_timeout_ms: u64,

    /// DB 进程优雅停止宽限（毫秒），超时后 SIGKILL。
    #[arg(long, env = "DB_RUNTIME_STOP_GRACE_MS", default_value_t = 5000)]
    pub runtime_stop_grace_ms: u64,

    /// DB 进程崩溃后的最大自动重启次数（架构 §12.1）。
    #[arg(long, env = "DB_RUNTIME_MAX_RESTARTS", default_value_t = 3)]
    pub runtime_max_restarts: u32,

    /// 首次重启退避（毫秒），后续按 2 倍递增（上限 1s）。
    #[arg(long, env = "DB_RUNTIME_RESTART_BACKOFF_MS", default_value_t = 100)]
    pub runtime_restart_backoff_ms: u64,

    /// Remote WAL Raft 成员（`id@endpoint`，逗号分隔），原样透传给 db-runtime。
    #[arg(long, env = "WAL_CLUSTER", default_value = "")]
    pub wal_cluster: String,

    /// Remote WAL gRPC 端点（逗号分隔）；留空则由 `WAL_CLUSTER` + 端口推导。
    #[arg(long, env = "WAL_ENDPOINTS", default_value = "")]
    pub wal_endpoints: String,

    /// 由 `WAL_CLUSTER` 推导 gRPC 端点时使用的端口。
    #[arg(long, env = "WAL_GRPC_PORT", default_value_t = DEFAULT_WAL_GRPC_PORT)]
    pub wal_grpc_port: u16,

    /// 单个 DB 的最大并发 UDS 连接数。
    #[arg(long, env = "WORKER_DB_MAX_CONNECTIONS", default_value_t = 4)]
    pub db_max_connections: usize,
}

/// 解析后的 Worker 配置。
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// 本节点 worker id。
    pub worker_id: String,
    /// 区域。
    pub region: String,
    /// 可用区。
    pub zone: String,
    /// 控制面监听地址。
    pub control_listen: SocketAddr,
    /// 数据面监听地址。
    pub data_listen: SocketAddr,
    /// Ops HTTP 监听地址。
    pub ops_listen: SocketAddr,
    /// db-server 控制面端点。
    pub server_control_endpoint: String,
    /// 对外通告地址（心跳上报）。
    pub advertise_endpoint: String,
    /// 对外通告的数据面地址（心跳上报）。
    pub advertise_data_endpoint: String,
    /// 数据目录。
    pub data_dir: PathBuf,
    /// 运行目录。
    pub run_dir: PathBuf,
    /// 资源容量。
    pub capacity: WorkerCapacity,
    /// DB 进程硬上限（生效值 = min(容量进程位, 本值)）。
    pub max_db_process: u64,
    /// 心跳周期。
    pub heartbeat_interval: Duration,
    /// cgroup 根。
    pub cgroup_root: PathBuf,
    /// 是否关闭 cgroup 限制。
    pub cgroup_disabled: bool,
    /// db-runtime 二进制。
    pub db_runtime_bin: PathBuf,
    /// READY 等待上限。
    pub runtime_ready_timeout: Duration,
    /// 优雅停止宽限。
    pub runtime_stop_grace: Duration,
    /// 最大自动重启次数。
    pub runtime_max_restarts: u32,
    /// 首次重启退避。
    pub runtime_restart_backoff: Duration,
    /// Remote WAL Raft 成员串（透传给 db-runtime）。
    pub wal_cluster: String,
    /// Remote WAL gRPC 端点（Worker 自己的 wal_client 使用）。
    pub wal_endpoints: Vec<String>,
    /// 单 DB 最大并发连接数。
    pub db_max_connections: usize,
    /// 本二进制版本（作为引擎版本的兜底值）。
    pub version: String,
}

impl WorkerConfig {
    /// 由 CLI 构造并校验配置。
    pub fn from_cli(cli: Cli) -> Result<Self> {
        if cli.worker_id.trim().is_empty() {
            return Err(WorkerError::Config("WORKER_ID 不能为空".into()));
        }
        if cli.heartbeat_interval_ms < 50 {
            // 过密的心跳会把 Server 打成轮询；50ms 是「比 1s 快一个数量级」的硬下限
            return Err(WorkerError::Config(format!(
                "HEARTBEAT_INTERVAL_MS={} 过小（必须 >= 50）",
                cli.heartbeat_interval_ms
            )));
        }
        if cli.runtime_ready_timeout_ms == 0 {
            return Err(WorkerError::Config(
                "DB_RUNTIME_READY_TIMEOUT_MS 不能为 0".into(),
            ));
        }
        if cli.db_max_connections == 0 {
            return Err(WorkerError::Config(
                "WORKER_DB_MAX_CONNECTIONS 不能为 0".into(),
            ));
        }

        let cgroup_root = resolve_cgroup_root(cli.cgroup_root);
        let capacity = WorkerCapacity::new(
            cli.cpu_milli,
            cli.memory_mib,
            fd_limit(),
            cli.disk_mib,
            cli.process_slots,
            cli.iops,
        );
        if capacity.is_zero() {
            return Err(WorkerError::Config(
                "Worker 容量六维全为 0：请检查 WORKER_CPU_MILLI / WORKER_MEMORY_MIB 等配置".into(),
            ));
        }

        let endpoints =
            parse_wal_endpoints(&cli.wal_endpoints, &cli.wal_cluster, cli.wal_grpc_port);
        let advertise_endpoint = if cli.advertise_endpoint.trim().is_empty() {
            cli.control_listen.to_string()
        } else {
            cli.advertise_endpoint.clone()
        };

        let config = Self {
            worker_id: cli.worker_id,
            region: cli.region,
            zone: cli.zone,
            control_listen: cli.control_listen,
            data_listen: cli.data_listen,
            ops_listen: cli.ops_listen,
            server_control_endpoint: cli.server_control_endpoint,
            advertise_endpoint,
            // 数据面端点：显式配置优先，否则留空由 WorkerInfo 按 host + data_listen 端口推导
            advertise_data_endpoint: cli.advertise_data_endpoint,
            data_dir: cli.data_dir,
            run_dir: cli.run_dir,
            capacity,
            max_db_process: cli.max_db_process,
            heartbeat_interval: Duration::from_millis(cli.heartbeat_interval_ms),
            cgroup_root,
            cgroup_disabled: cli.cgroup_disabled,
            db_runtime_bin: cli.db_runtime_bin,
            runtime_ready_timeout: Duration::from_millis(cli.runtime_ready_timeout_ms),
            runtime_stop_grace: Duration::from_millis(cli.runtime_stop_grace_ms),
            runtime_max_restarts: cli.runtime_max_restarts,
            runtime_restart_backoff: Duration::from_millis(cli.runtime_restart_backoff_ms),
            wal_cluster: cli.wal_cluster,
            wal_endpoints: endpoints,
            db_max_connections: cli.db_max_connections,
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        config.validate_paths()?;
        Ok(config)
    }

    /// 校验路径长度等硬约束（UDS 路径超长会在运行期才失败，必须提前发现）。
    pub fn validate_paths(&self) -> Result<()> {
        let probe = paths::socket_path(&self.run_dir, "00000000-0000-0000-0000-000000000000");
        if !paths::socket_path_fits(&probe) {
            return Err(WorkerError::Config(format!(
                "WORKER_RUN_DIR 过长，UDS 路径会超过内核上限（108 字节）：{}",
                probe.display()
            )));
        }
        Ok(())
    }

    /// 生效的 DB 进程数上限：容量进程位与 hard safety limit 取小。
    pub fn process_limit(&self) -> u64 {
        self.capacity.process_slots.min(self.max_db_process).max(1)
    }

    /// 某个 DB 的工作集根目录。
    pub fn db_dir(&self, db_id: &str) -> PathBuf {
        paths::db_dir(&self.data_dir, db_id)
    }

    /// 某个 DB 的 engine 数据目录（`--db-path`）。
    pub fn db_path(&self, db_id: &str) -> PathBuf {
        paths::db_path(&self.data_dir, db_id)
    }

    /// 某个 DB 的本地 WAL 文件（`--wal-path`）。
    pub fn wal_path(&self, db_id: &str) -> PathBuf {
        paths::wal_path(&self.data_dir, db_id)
    }

    /// 某个 DB 的 UDS 路径（`--socket-path`）。
    pub fn socket_path(&self, db_id: &str) -> PathBuf {
        paths::socket_path(&self.run_dir, db_id)
    }

    /// UDS socket 目录。
    pub fn socket_dir(&self) -> PathBuf {
        self.run_dir.join(paths::SOCKET_SUBDIR)
    }

    /// 数据目录与运行目录是否可写。
    pub fn ensure_dirs(&self) -> Result<()> {
        for dir in [&self.data_dir, &self.run_dir, &self.socket_dir()] {
            std::fs::create_dir_all(dir)
                .map_err(|err| WorkerError::io(format!("创建目录 {}", dir.display()), err))?;
        }
        Ok(())
    }

    /// 传给 DB Process 的参数（冻结契约，见模块文档与 REPORT）。
    pub fn runtime_args(
        &self,
        database_id: &str,
        owner_epoch: u64,
        budget: &domain::resources::ResourceBudget,
    ) -> Vec<String> {
        vec![
            "--worker-id".into(),
            self.worker_id.clone(),
            "--database-id".into(),
            database_id.to_string(),
            "--owner-epoch".into(),
            owner_epoch.to_string(),
            "--db-path".into(),
            self.db_path(database_id).display().to_string(),
            "--wal-path".into(),
            self.wal_path(database_id).display().to_string(),
            "--socket-path".into(),
            self.socket_path(database_id).display().to_string(),
            "--data-dir".into(),
            self.data_dir.display().to_string(),
            "--run-dir".into(),
            self.run_dir.display().to_string(),
            "--cpu-milli".into(),
            budget.cpu_milli.to_string(),
            "--memory-mib".into(),
            budget.memory_mib.to_string(),
            "--disk-mib".into(),
            budget.disk_mib.to_string(),
            "--process-slots".into(),
            budget.process_slots.max(1).to_string(),
            "--log-level".into(),
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        ]
        .into_iter()
        .chain(if self.wal_cluster.is_empty() {
            Vec::new()
        } else {
            vec!["--wal-cluster".into(), self.wal_cluster.clone()]
        })
        // 把已推导好的 **客户端** gRPC 端点透传给 db-runtime。
        //
        // 为什么必须显式传：`--wal-cluster` 里的成员地址是节点间 Raft 端口（9201），
        // db-runtime 若从它推导客户端地址就会连错端口，所有写请求都会因 append 超时
        // 被 durability 门控拒绝（表现为 CREATE TABLE 都失败）。
        .chain(if self.wal_endpoints.is_empty() {
            Vec::new()
        } else {
            vec![
                "--wal-client-endpoints".into(),
                self.wal_endpoints.join(","),
            ]
        })
        .collect()
    }

    /// 传给 DB Process 的环境变量（与命令行长选项一一对应，二者都可用）。
    ///
    /// 为什么同时给两种方式：命令行便于本地排障（直接手敲），环境变量便于容器编排与
    /// 避免超长 argv。二者冲突时 **命令行优先**（`DB_RUNTIME_*` 的约定由 db-runtime 决定）。
    pub fn runtime_env(
        &self,
        database_id: &str,
        owner_epoch: u64,
        budget: &domain::resources::ResourceBudget,
    ) -> Vec<(String, String)> {
        vec![
            ("DB_RUNTIME_WORKER_ID".into(), self.worker_id.clone()),
            ("DB_RUNTIME_DATABASE_ID".into(), database_id.to_string()),
            ("DB_RUNTIME_OWNER_EPOCH".into(), owner_epoch.to_string()),
            (
                "DB_RUNTIME_DB_PATH".into(),
                self.db_path(database_id).display().to_string(),
            ),
            (
                "DB_RUNTIME_WAL_PATH".into(),
                self.wal_path(database_id).display().to_string(),
            ),
            (
                "DB_RUNTIME_SOCKET_PATH".into(),
                self.socket_path(database_id).display().to_string(),
            ),
            (
                "DB_RUNTIME_DATA_DIR".into(),
                self.data_dir.display().to_string(),
            ),
            (
                "DB_RUNTIME_RUN_DIR".into(),
                self.run_dir.display().to_string(),
            ),
            ("DB_RUNTIME_CPU_MILLI".into(), budget.cpu_milli.to_string()),
            (
                "DB_RUNTIME_MEMORY_MIB".into(),
                budget.memory_mib.to_string(),
            ),
            ("DB_RUNTIME_DISK_MIB".into(), budget.disk_mib.to_string()),
            (
                "DB_RUNTIME_PROCESS_SLOTS".into(),
                budget.process_slots.max(1).to_string(),
            ),
            ("WAL_CLUSTER".into(), self.wal_cluster.clone()),
            ("WORKER_ID".into(), self.worker_id.clone()),
        ]
    }

    /// 某个 DB 的容器/日志标识用的短 id（日志可读性）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn short_db_id(db_id: &str) -> &str {
        db_id.get(..8).unwrap_or(db_id)
    }
}

/// cgroup 根解析：命令行 > `CGROUP_ROOT` > `WORKER_CGROUP_ROOT` > 默认值。
pub fn resolve_cgroup_root(cli_value: Option<PathBuf>) -> PathBuf {
    if let Some(value) = cli_value {
        return value;
    }
    for key in ["CGROUP_ROOT", "WORKER_CGROUP_ROOT"] {
        if let Ok(value) = std::env::var(key) {
            if !value.trim().is_empty() {
                return PathBuf::from(value);
            }
        }
    }
    PathBuf::from(DEFAULT_CGROUP_ROOT)
}

/// 解析 Remote WAL gRPC 端点。
///
/// 优先使用显式 `WAL_ENDPOINTS`；为空时从 `WAL_CLUSTER`（`id@host:peer_port`）推导，
/// 同 host 使用 `wal_grpc_port`（默认 9200）。推导是确定性的：只替换端口，不改 host。
pub fn parse_wal_endpoints(explicit: &str, cluster: &str, wal_grpc_port: u16) -> Vec<String> {
    let endpoints: Vec<String> = explicit
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| normalize_endpoint(item, wal_grpc_port))
        .collect();
    if !endpoints.is_empty() {
        return endpoints;
    }

    cluster
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .filter_map(|member| {
            // 形如 `1@wal-1:9201`
            let (_, address) = member.split_once('@')?;
            let host = address.rsplit_once(':').map(|(host, _)| host)?;
            Some(format!("http://{host}:{wal_grpc_port}"))
        })
        .collect()
}

/// 补全端点 scheme 并去掉尾部斜杠。
fn normalize_endpoint(raw: &str, wal_grpc_port: u16) -> String {
    let trimmed = raw.trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }
    if trimmed.contains(':') {
        return format!("http://{trimmed}");
    }
    format!("http://{trimmed}:{wal_grpc_port}")
}

/// 读取进程 FD 上限（用于容量口径）；取不到时回落到一个保守值。
pub fn fd_limit() -> u64 {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit 只写我们传入的结构体
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if rc != 0 {
        return 65_536;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        // 无限 -> 用一个足够大的常量，避免容量判断出现「无限」这种不可比较值
        return 1_048_576;
    }
    limit.rlim_cur
}

/// 便捷入口：解析进程参数（`main` 使用）。
pub fn parse_cli() -> Cli {
    Cli::parse()
}

/// 数据目录是否位于给定的父目录下（防御路径配置错误）。
#[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
pub fn is_within(parent: &Path, child: &Path) -> bool {
    child.starts_with(parent)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli_from(args: &[&str]) -> Cli {
        // 测试中不读环境变量：显式给出全部关键参数，避免并行测试互相污染
        Cli::try_parse_from(args).expect("CLI 解析")
    }

    #[test]
    fn defaults_match_env_example() {
        let cli = cli_from(&["db-worker"]);
        assert_eq!(cli.worker_id, "worker-1");
        assert_eq!(cli.control_listen.port(), 9100);
        assert_eq!(cli.data_listen.port(), 9101);
        assert_eq!(cli.ops_listen.port(), 9090);
        assert_eq!(cli.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
        assert_eq!(cli.run_dir, PathBuf::from(DEFAULT_RUN_DIR));
        assert_eq!(cli.heartbeat_interval_ms, 1000);
        assert_eq!(cli.db_runtime_bin, PathBuf::from(DEFAULT_DB_RUNTIME_BIN));
        assert_eq!(cli.max_db_process, 256);

        let config = WorkerConfig::from_cli(cli).unwrap();
        assert_eq!(config.capacity.cpu_milli, 32_000);
        assert_eq!(config.data_dir, PathBuf::from("/var/lib/db-platform"));
        assert_eq!(config.process_limit(), 256);
        assert!(config.wal_endpoints.is_empty(), "未配置 WAL 时不推导端点");
    }

    #[test]
    fn explicit_args_override_defaults() {
        let cli = cli_from(&[
            "db-worker",
            "--worker-id",
            "worker-7",
            "--control-listen",
            "127.0.0.1:19100",
            "--data-listen",
            "127.0.0.1:19101",
            "--ops-listen",
            "127.0.0.1:19090",
            "--data-dir",
            "/tmp/db-platform-data",
            "--run-dir",
            "/tmp/db-platform-run",
            "--cpu-milli",
            "4000",
            "--memory-mib",
            "8192",
            "--process-slots",
            "64",
            "--max-db-process",
            "32",
            "--heartbeat-interval-ms",
            "250",
            "--db-runtime-bin",
            "/bin/sleep",
            "--wal-cluster",
            "1@wal-1:9201,2@wal-2:9201",
        ]);
        let config = WorkerConfig::from_cli(cli).unwrap();
        assert_eq!(config.worker_id, "worker-7");
        assert_eq!(config.control_listen.to_string(), "127.0.0.1:19100");
        assert_eq!(config.capacity.cpu_milli, 4000);
        assert_eq!(config.capacity.memory_mib, 8192);
        assert_eq!(config.process_limit(), 32, "hard safety limit 生效");
        assert_eq!(config.heartbeat_interval, Duration::from_millis(250));
        assert_eq!(config.db_runtime_bin, PathBuf::from("/bin/sleep"));
        assert_eq!(
            config.wal_endpoints,
            vec![
                "http://wal-1:9200".to_string(),
                "http://wal-2:9200".to_string()
            ]
        );
        // 通告地址缺省等于控制面监听地址
        assert_eq!(config.advertise_endpoint, "127.0.0.1:19100");
    }

    #[test]
    fn invalid_configuration_is_rejected() {
        // 心跳过密
        let cli = cli_from(&["db-worker", "--heartbeat-interval-ms", "10"]);
        assert!(WorkerConfig::from_cli(cli).is_err());

        // 空 worker id
        let cli = cli_from(&["db-worker", "--worker-id", "  "]);
        assert!(WorkerConfig::from_cli(cli).is_err());

        // 连接数为 0
        let cli = cli_from(&["db-worker", "--db-max-connections", "0"]);
        assert!(WorkerConfig::from_cli(cli).is_err());

        // run_dir 过长会撑爆 UDS 路径上限
        let long = format!("/tmp/{}", "x".repeat(120));
        let cli = cli_from(&["db-worker", "--run-dir", &long]);
        assert!(WorkerConfig::from_cli(cli).is_err());
    }

    #[test]
    fn wal_endpoints_derivation() {
        // 显式配置优先，并补全 scheme
        assert_eq!(
            parse_wal_endpoints("wal-1:9200,http://wal-2:9200/", "1@ignore:1", 9200),
            vec![
                "http://wal-1:9200".to_string(),
                "http://wal-2:9200".to_string()
            ]
        );
        // 从 WAL_CLUSTER 推导（只换端口，不改 host）
        assert_eq!(
            parse_wal_endpoints("", "1@wal-1:9201,2@wal-2:9201", 9200),
            vec![
                "http://wal-1:9200".to_string(),
                "http://wal-2:9200".to_string()
            ]
        );
        // 非法成员被忽略，不 panic
        assert_eq!(
            parse_wal_endpoints("", "garbage,3@wal-3:9201", 9300),
            vec!["http://wal-3:9300".to_string()]
        );
        assert!(parse_wal_endpoints("", "", 9200).is_empty());
    }

    #[test]
    fn cgroup_root_resolution_prefers_cli() {
        assert_eq!(
            resolve_cgroup_root(Some(PathBuf::from("/custom/cgroup"))),
            PathBuf::from("/custom/cgroup")
        );
        // 未显式指定时落到默认值（测试进程一般未设置 CGROUP_ROOT）
        let resolved = resolve_cgroup_root(None);
        assert!(!resolved.as_os_str().is_empty());
    }

    #[test]
    fn runtime_args_and_env_are_consistent() {
        let cli = cli_from(&[
            "db-worker",
            "--worker-id",
            "worker-1",
            "--data-dir",
            "/tmp/data",
            "--run-dir",
            "/tmp/run",
            "--wal-cluster",
            "1@wal-1:9201",
            "--db-runtime-bin",
            "/bin/true",
        ]);
        let config = WorkerConfig::from_cli(cli).unwrap();
        let budget = domain::resources::ResourceBudget::new(1500, 512, 128, 2048, 1, 500);
        let args = config.runtime_args("db-1", 42, &budget);

        // 长选项必须成对出现
        assert_eq!(args.len() % 2, 0, "参数必须是 --key value 成对：{args:?}");
        for key in [
            "--worker-id",
            "--database-id",
            "--owner-epoch",
            "--db-path",
            "--wal-path",
            "--socket-path",
            "--data-dir",
            "--run-dir",
            "--cpu-milli",
            "--memory-mib",
            "--disk-mib",
            "--process-slots",
            "--log-level",
            "--wal-cluster",
        ] {
            assert!(args.iter().any(|a| a == key), "缺少参数 {key}");
        }
        let value_of = |key: &str| {
            let index = args.iter().position(|a| a == key).unwrap();
            args[index + 1].clone()
        };
        assert_eq!(value_of("--database-id"), "db-1");
        assert_eq!(value_of("--owner-epoch"), "42");
        assert_eq!(value_of("--db-path"), "/tmp/data/db-1/db");
        // WAL 必须是 <db_path>-wal：引擎自己按这个路径打开 WAL（见 paths 模块文档）
        assert_eq!(value_of("--wal-path"), "/tmp/data/db-1/db-wal");
        assert_eq!(value_of("--socket-path"), "/tmp/run/sockets/db-1.sock");
        assert_eq!(value_of("--cpu-milli"), "1500");

        let env = config.runtime_env("db-1", 42, &budget);
        let lookup = |key: &str| {
            env.iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        assert_eq!(lookup("DB_RUNTIME_DATABASE_ID"), "db-1");
        assert_eq!(lookup("DB_RUNTIME_OWNER_EPOCH"), "42");
        assert_eq!(
            lookup("DB_RUNTIME_SOCKET_PATH"),
            "/tmp/run/sockets/db-1.sock"
        );
        assert_eq!(lookup("DB_RUNTIME_WAL_PATH"), "/tmp/data/db-1/db-wal");
        assert_eq!(lookup("WAL_CLUSTER"), "1@wal-1:9201");
    }

    #[test]
    fn ensure_dirs_creates_layout() {
        let dir = tempfile::tempdir().unwrap();
        let cli = cli_from(&[
            "db-worker",
            "--data-dir",
            dir.path().join("data").to_str().unwrap(),
            "--run-dir",
            dir.path().join("run").to_str().unwrap(),
        ]);
        let config = WorkerConfig::from_cli(cli).unwrap();
        config.ensure_dirs().unwrap();
        assert!(dir.path().join("data").is_dir());
        assert!(dir.path().join("run/sockets").is_dir());
        assert!(is_within(dir.path(), &config.db_path("db-1")));
    }

    #[test]
    fn fd_limit_is_positive() {
        assert!(fd_limit() > 0);
    }

    #[test]
    fn short_db_id_is_bounded() {
        assert_eq!(WorkerConfig::short_db_id("0123456789abcdef"), "01234567");
        assert_eq!(WorkerConfig::short_db_id("abc"), "abc");
    }
}
