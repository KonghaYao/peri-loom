//! db-server 运行配置（架构 §17.4 / §17.14）。
//!
//! 约定：
//! - 所有取值来自环境变量，非法值一律**回落到默认值**并记录 WARN，而不是启动失败：
//!   控制面是平台唯一入口，因一个可选旋钮写错就拒绝启动的代价高于用默认值跑起来。
//!   例外是 [`ServerConfig::database_url`]（缺失直接失败，没有合理默认值）。
//! - secret 支持 `*_FILE`（挂载文件）优先、明文环境变量兜底，明文形式仅用于本地开发。

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use domain::error::ErrorCode;
use domain::error::{PlatformError, Result};

// ------------------------------------------------------------------ env key 常量

/// HTTP 出口监听地址（对外唯一入口）。
pub const ENV_SERVER_HTTP_LISTEN: &str = "SERVER_HTTP_LISTEN";
/// gRPC 监听地址：**只用于 Worker -> Server 的 Control Path**（Heartbeat / 状态上报）。
/// 架构 §17.4 明确「外部统一只暴露 HTTP」，因此该端口绝不能经公网发布。
pub const ENV_SERVER_GRPC_LISTEN: &str = "SERVER_GRPC_LISTEN";
/// 运维端口：与 HTTP 同进程的第二监听，只服务 `/healthz` `/readyz` `/metrics`。
pub const ENV_OPS_LISTEN: &str = "OPS_LISTEN";
/// PostgreSQL Catalog 连接串。
pub const ENV_DATABASE_URL: &str = "DATABASE_URL";
/// 组装 DATABASE_URL 用的分片变量。
///
/// 为什么需要它们：架构 §17.14 要求数据库密码经 Compose secrets / 挂载文件注入，
/// **不允许**把明文密码写进环境变量或镜像。密码来自文件时无法直接拼进
/// DATABASE_URL，因此这里支持「由分片 + 密码文件组装」这一条路径。
/// 优先级：DB_PASSWORD_FILE 存在 -> 用分片组装；否则直接用 DATABASE_URL。
pub const ENV_DB_HOST: &str = "DB_HOST";
pub const ENV_DB_PORT: &str = "DB_PORT";
pub const ENV_DB_NAME: &str = "DB_NAME";
pub const ENV_DB_USER: &str = "DB_USER";
pub const ENV_DB_PASSWORD_FILE: &str = "DB_PASSWORD_FILE";
/// Catalog 连接池上限。
pub const ENV_CATALOG_MAX_CONNECTIONS: &str = "CATALOG_MAX_CONNECTIONS";
/// Route Cache 兜底全量 reconcile 周期。
pub const ENV_ROUTE_RECONCILE_INTERVAL_MS: &str = "ROUTE_RECONCILE_INTERVAL_MS";
/// Route Cache 预分配容量（仅用于预分配与容量告警，不是淘汰上限）。
pub const ENV_ROUTE_CACHE_MAX_ENTRIES: &str = "ROUTE_CACHE_MAX_ENTRIES";
/// 透明 Wake 的等待上限（超过即返回 WAKEUP_TIMEOUT，不泄漏内部 WAKING 状态）。
pub const ENV_WAKEUP_TIMEOUT_MS: &str = "WAKEUP_TIMEOUT_MS";
/// 透明 Wake 的轮询间隔。
pub const ENV_WAKEUP_POLL_INTERVAL_MS: &str = "WAKEUP_POLL_INTERVAL_MS";
/// 显式会话空闲超时（默认 60s，架构 §15.3 冻结）。
pub const ENV_SESSION_IDLE_TIMEOUT_MS: &str = "SESSION_IDLE_TIMEOUT_MS";
/// 事务最大生命周期（默认 30s，架构 §15.3 冻结）。
pub const ENV_TRANSACTION_MAX_LIFETIME_MS: &str = "TRANSACTION_MAX_LIFETIME_MS";
/// 结果集内联返回上限（字节）；超过则改走 NDJSON streaming。
pub const ENV_INLINE_RESULT_LIMIT_BYTES: &str = "INLINE_RESULT_LIMIT_BYTES";
/// JWT HS256 密钥文件。
pub const ENV_JWT_SECRET_FILE: &str = "JWT_SECRET_FILE";
/// 平台自签 JWT 的有效期（秒）。
pub const ENV_JWT_TTL_SECONDS: &str = "JWT_TTL_SECONDS";
/// 默认有效期：1 小时（与 .env.example 一致）。
pub const DEFAULT_JWT_TTL_SECONDS: u64 = 3600;
/// JWT HS256 密钥（开发用明文形式）。
pub const ENV_JWT_SECRET: &str = "JWT_SECRET";
/// JWT issuer（校验 `iss`）。
pub const ENV_JWT_ISSUER: &str = "JWT_ISSUER";
/// 首次启动创建的管理员用户名。
pub const ENV_BOOTSTRAP_ADMIN_USER: &str = "BOOTSTRAP_ADMIN_USER";
/// 首次启动创建的管理员口令文件。
pub const ENV_BOOTSTRAP_ADMIN_PASSWORD_FILE: &str = "BOOTSTRAP_ADMIN_PASSWORD_FILE";
/// 首次启动创建的管理员口令（开发用明文形式）。
pub const ENV_BOOTSTRAP_ADMIN_PASSWORD: &str = "BOOTSTRAP_ADMIN_PASSWORD";
/// 对象存储 endpoint。
pub const ENV_S3_ENDPOINT: &str = "S3_ENDPOINT";
/// 对象存储 region。
pub const ENV_S3_REGION: &str = "S3_REGION";
/// 对象存储 bucket。
pub const ENV_S3_BUCKET: &str = "S3_BUCKET";
/// 对象存储访问密钥 ID 文件（docker secrets，最高优先级）。
pub const ENV_S3_ACCESS_KEY_ID_FILE: &str = "S3_ACCESS_KEY_ID_FILE";
/// 对象存储访问密钥 ID（开发用明文形式）。
pub const ENV_S3_ACCESS_KEY_ID: &str = "S3_ACCESS_KEY_ID";
/// 对象存储密钥文件（docker secrets，最高优先级）。
pub const ENV_S3_SECRET_ACCESS_KEY_FILE: &str = "S3_SECRET_ACCESS_KEY_FILE";
/// 对象存储密钥（开发用明文形式）。
pub const ENV_S3_SECRET_ACCESS_KEY: &str = "S3_SECRET_ACCESS_KEY";
/// Remote WAL 副本的 gRPC 端点（逗号分隔，显式形式）。
///
/// 控制面本身不写 WAL，但**必须能读存储层记录的 owner epoch**：Storage-level Fencing
/// 的权威在那里（架构 §11.3），Catalog 落后时只能向它对齐（见
/// `Catalog::align_ownership_epoch_with_storage`）。未配置时该项能力缺席（只记日志），
/// 不影响已运行 DB 的数据面。
pub const ENV_WAL_ENDPOINTS: &str = "WAL_ENDPOINTS";
/// Remote WAL 集群拓扑（`id@host:peer_port,...`），用于推导 gRPC 端点。
pub const ENV_WAL_CLUSTER: &str = "WAL_CLUSTER";
/// 由 [`ENV_WAL_CLUSTER`] 推导 gRPC 端点时使用的端口。
pub const ENV_WAL_GRPC_PORT: &str = "WAL_GRPC_PORT";

// ------------------------------------------------------------------ 默认值

/// 默认 HTTP 监听。
pub const DEFAULT_HTTP_LISTEN: &str = "0.0.0.0:8080";
/// 默认 gRPC（Worker Control）监听。
pub const DEFAULT_GRPC_LISTEN: &str = "0.0.0.0:8081";
/// 默认 reconcile 周期。
pub const DEFAULT_ROUTE_RECONCILE_INTERVAL_MS: u64 = 5_000;
/// 默认 Route Cache 预分配容量。
pub const DEFAULT_ROUTE_CACHE_MAX_ENTRIES: usize = 1_000_000;
/// 默认 Wake 等待上限。
pub const DEFAULT_WAKEUP_TIMEOUT_MS: u64 = 3_000;
/// 默认 Wake 轮询间隔。
pub const DEFAULT_WAKEUP_POLL_INTERVAL_MS: u64 = 20;
/// 默认 Catalog 连接池上限。
pub const DEFAULT_CATALOG_MAX_CONNECTIONS: u32 = 32;
/// 默认内联结果上限（256 KiB）。
pub const DEFAULT_INLINE_RESULT_LIMIT_BYTES: usize = 262_144;
/// 默认 JWT issuer。
pub const DEFAULT_JWT_ISSUER: &str = "db-platform";
/// Remote WAL 服务默认 gRPC 端口（由 `WAL_CLUSTER` 推导端点时使用，与 Worker 口径一致）。
pub const DEFAULT_WAL_GRPC_PORT: u16 = 9200;

/// 心跳监控周期：Worker 心跳 1s 一次，Server 侧检查频率与之对齐。
pub const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// 连续 miss 次数达到该值即判 UNAVAILABLE（架构 §16）。
pub const SUSPECT_MISS_THRESHOLD: i32 = 3;
/// ownership 租约 TTL：Owner 必须在此之前续约，否则被 GC 回收。
pub const OWNERSHIP_LEASE_TTL: Duration = Duration::from_secs(30);
/// 租约过期回收的判定余量（超过 TTL 这么多仍未续约才算过期）。
pub const OWNERSHIP_STALE_GRACE: Duration = Duration::from_secs(60);
/// ownership GC 周期。
pub const OWNERSHIP_GC_INTERVAL: Duration = Duration::from_secs(15);
/// 生命周期回收（WARM/空闲 DB eviction）周期。
pub const EVICTION_INTERVAL: Duration = Duration::from_secs(30);
/// WARM 且空闲超过该时长的 DB 允许被强制驱逐（架构 §7 生命周期回收）。
pub const WARM_IDLE_EVICTION_AFTER: Duration = Duration::from_secs(600);
/// 后台 Job Runner 的租约时长。
pub const JOB_LEASE_TTL: Duration = Duration::from_secs(60);
/// 后台 Job Runner 轮询周期。
pub const JOB_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// 与 Worker 建立 gRPC 连接的超时。
pub const WORKER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// 控制路径单次 RPC 的超时。
pub const WORKER_CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// 客户端断开后把 Cancel 送到 Worker 的时间预算（架构 §17.4：1s 内）。
pub const CANCEL_DISPATCH_TIMEOUT: Duration = Duration::from_millis(800);
/// 显式会话注册表的清理周期。
pub const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// 对象存储配置。
///
/// 说明：**db-server 不直接读写对象存储**。快照由 Worker 上传（架构 §17.9），
/// Server 只登记 Worker 回传的 `SnapshotMeta`。这里保留 S3_* 是为了：
/// 1) 部署侧能用同一份 env 注入所有服务；
/// 2) 启动日志里能确认「本次部署到底配没配对象存储」，避免快照静默失败时无从判断。
///
/// `is_configured()` 判定的是**快照真正能用**所需的完整集合
/// （endpoint + bucket + 凭据）：只配 endpoint + bucket 而缺凭据时，Worker 侧
/// `S3ObjectStore::from_env()` 会构造失败并让 `TriggerSnapshot` 返回
/// `STORAGE_UNAVAILABLE` —— 启动日志说「已配置」而接口说「没配」正是这样产生的。
/// 凭据是 secret，`Debug` 必须脱敏（见下方手写实现）。
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ObjectStoreConfig {
    /// S3 兼容 endpoint。
    pub endpoint: Option<String>,
    /// region。
    pub region: Option<String>,
    /// bucket。
    pub bucket: Option<String>,
    /// 访问密钥 ID（secret：不得进日志）。
    pub access_key_id: Option<String>,
    /// 访问密钥（secret：不得进日志）。
    pub secret_access_key: Option<String>,
}

impl std::fmt::Debug for ObjectStoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 凭据只以「是否设置」出现：Debug 可能被日志 / panic 消息带到任何地方。
        f.debug_struct("ObjectStoreConfig")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl ObjectStoreConfig {
    /// 是否配置了**可用**的对象存储（endpoint + bucket + 完整凭据）。
    ///
    /// 这与 Worker 侧 `S3ObjectStore::from_env()` 的必填项一致：两边口径一致，
    /// 「配置了」就等于「快照上传 / 冷启动恢复能用」。
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.endpoint.is_some()
            && self.bucket.is_some()
            && self.access_key_id.is_some()
            && self.secret_access_key.is_some()
    }

    /// 缺失项清单（用于启动日志精确定位「为什么快照不可用」）。
    #[must_use]
    pub fn missing_parts(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if self.endpoint.is_none() {
            missing.push(ENV_S3_ENDPOINT);
        }
        if self.bucket.is_none() {
            missing.push(ENV_S3_BUCKET);
        }
        if self.access_key_id.is_none() || self.secret_access_key.is_none() {
            missing.push("S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY");
        }
        missing
    }
}

/// db-server 配置。
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// 对外 HTTP 监听。
    pub http_listen: SocketAddr,
    /// Worker Control gRPC 监听（内部）。
    pub grpc_listen: SocketAddr,
    /// 运维监听（可选，第二个端口）。
    pub ops_listen: Option<SocketAddr>,
    /// Catalog 连接串。
    pub database_url: String,
    /// Catalog 连接池上限。
    pub catalog_max_connections: u32,
    /// Route Cache 全量 reconcile 周期。
    pub route_reconcile_interval: Duration,
    /// Route Cache 预分配容量。
    pub route_cache_max_entries: usize,
    /// 透明 Wake 等待上限。
    pub wakeup_timeout: Duration,
    /// 透明 Wake 轮询间隔。
    pub wakeup_poll_interval: Duration,
    /// 会话空闲超时（毫秒，转发给 Worker）。
    pub session_idle_timeout_ms: u32,
    /// 事务最大生命周期（毫秒，转发给 Worker）。
    pub transaction_max_lifetime_ms: u32,
    /// 内联结果上限（字节）。
    pub inline_result_limit_bytes: usize,
    /// JWT HS256 密钥；`None` 表示不启用 JWT 校验（仅 x-api-token 可用）。
    pub jwt_secret: Option<Vec<u8>>,
    /// JWT issuer。
    pub jwt_issuer: String,
    /// 自签 JWT 有效期（秒）。
    pub jwt_ttl_seconds: u64,
    /// 引导管理员用户名；与密码同时存在时才尝试创建。
    pub bootstrap_admin_user: Option<String>,
    /// 引导管理员口令（明文，仅在内存中用于计算 argon2 哈希）。
    pub bootstrap_admin_password: Option<String>,
    /// 对象存储配置（仅用于日志与运维可见性）。
    pub object_store: ObjectStoreConfig,
    /// Remote WAL 副本 gRPC 端点（见 [`Catalog::align_ownership_epoch_with_storage`] 的调用方）。
    ///
    /// 空表示「未配置 WAL」：控制面无法读取存储层 epoch，启动路径遇到 fencing 拒绝时
    /// 只能把原始错误返回给调用方（不猜、不无限重试）。
    ///
    /// [`Catalog::align_ownership_epoch_with_storage`]: catalog::Catalog::align_ownership_epoch_with_storage
    pub wal_endpoints: Vec<String>,
}

/// 解析 Catalog 连接串。
///
/// 两条路径，二选一：
/// 1. `DB_PASSWORD_FILE` 已设置 -> 由 `DB_HOST/DB_PORT/DB_NAME/DB_USER` + 文件里的密码组装。
///    这是 Compose / 生产路径：密码只存在于 secret 文件，环境变量与镜像里都没有明文。
/// 2. 否则 -> 直接使用 `DATABASE_URL`（本地开发便捷路径）。
///
/// 注意：**不能让两条路径同时声明密码**。曾经出现过「PostgreSQL 用 secret 文件里的
/// 随机密码初始化，而 db-server 用 .env 里的字面密码连接」导致认证必然失败的情况，
/// 因此这里以 DB_PASSWORD_FILE 为准，避免密码出现第二个事实源。
fn resolve_database_url() -> Result<String> {
    if let Some(path) = std::env::var_os(ENV_DB_PASSWORD_FILE) {
        let raw = std::fs::read_to_string(&path).map_err(|err| {
            PlatformError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "读取 {ENV_DB_PASSWORD_FILE}（{}）失败: {err}",
                    Path::new(&path).display()
                ),
            )
        })?;
        let password = raw.trim_end_matches(['\n', '\r']).to_owned();
        if password.is_empty() {
            return Err(PlatformError::new(
                ErrorCode::InvalidArgument,
                format!("{ENV_DB_PASSWORD_FILE} 内容为空（密码文件必须非空）"),
            ));
        }
        let host = std::env::var(ENV_DB_HOST).unwrap_or_else(|_| "postgres".to_owned());
        let port = std::env::var(ENV_DB_PORT).unwrap_or_else(|_| "5432".to_owned());
        let name = std::env::var(ENV_DB_NAME).unwrap_or_else(|_| "dbplatform".to_owned());
        let user = std::env::var(ENV_DB_USER).unwrap_or_else(|_| "dbplatform".to_owned());
        return Ok(format!(
            "postgres://{}:{}@{}:{}/{}",
            urlencode(&user),
            urlencode(&password),
            host,
            port,
            urlencode(&name),
        ));
    }

    std::env::var(ENV_DATABASE_URL).map_err(|_| {
        PlatformError::new(
            ErrorCode::InvalidArgument,
            format!(
                "{ENV_DATABASE_URL} 未设置，且 {ENV_DB_PASSWORD_FILE} 也未设置（db-server 需要 PostgreSQL Catalog）"
            ),
        )
    })
}

/// 最小 percent-encoding：只转义会破坏 URL 结构的字符。
///
/// 生成的 secret 通常是 base64（可能含 `+`、`/`、`=`），这些字符出现在 userinfo 段
/// 会改变 URL 语义，必须转义。
fn urlencode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

impl ServerConfig {
    /// 从环境变量构造。
    ///
    /// # Errors
    /// 仅当 `DATABASE_URL` 缺失或监听地址无法解析时返回 [`ErrorCode::InvalidArgument`]。
    pub fn from_env() -> Result<Self> {
        let database_url = resolve_database_url()?;
        let http_listen = parse_socket_addr(ENV_SERVER_HTTP_LISTEN, DEFAULT_HTTP_LISTEN)?;
        let grpc_listen = parse_socket_addr(ENV_SERVER_GRPC_LISTEN, DEFAULT_GRPC_LISTEN)?;
        Ok(Self {
            http_listen,
            grpc_listen,
            ops_listen: parse_optional_socket_addr(ENV_OPS_LISTEN),
            database_url,
            catalog_max_connections: env_u64(ENV_CATALOG_MAX_CONNECTIONS)
                .map(|v| v.clamp(1, 1024) as u32)
                .unwrap_or(DEFAULT_CATALOG_MAX_CONNECTIONS),
            route_reconcile_interval: Duration::from_millis(
                env_u64(ENV_ROUTE_RECONCILE_INTERVAL_MS)
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_ROUTE_RECONCILE_INTERVAL_MS),
            ),
            route_cache_max_entries: env_u64(ENV_ROUTE_CACHE_MAX_ENTRIES)
                .map(|v| v as usize)
                .unwrap_or(DEFAULT_ROUTE_CACHE_MAX_ENTRIES),
            wakeup_timeout: Duration::from_millis(
                env_u64(ENV_WAKEUP_TIMEOUT_MS)
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_WAKEUP_TIMEOUT_MS),
            ),
            wakeup_poll_interval: Duration::from_millis(
                env_u64(ENV_WAKEUP_POLL_INTERVAL_MS)
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_WAKEUP_POLL_INTERVAL_MS),
            ),
            session_idle_timeout_ms: env_u64(ENV_SESSION_IDLE_TIMEOUT_MS)
                .map(|v| v as u32)
                .unwrap_or(domain::session::SESSION_IDLE_TIMEOUT_SECS as u32 * 1000),
            transaction_max_lifetime_ms: env_u64(ENV_TRANSACTION_MAX_LIFETIME_MS)
                .map(|v| v as u32)
                .unwrap_or(domain::session::TRANSACTION_MAX_LIFETIME_SECS as u32 * 1000),
            inline_result_limit_bytes: env_u64(ENV_INLINE_RESULT_LIMIT_BYTES)
                .map(|v| v as usize)
                .unwrap_or(DEFAULT_INLINE_RESULT_LIMIT_BYTES),
            jwt_secret: read_secret(ENV_JWT_SECRET_FILE, ENV_JWT_SECRET).map(String::into_bytes),
            jwt_ttl_seconds: env_u64(ENV_JWT_TTL_SECONDS).unwrap_or(DEFAULT_JWT_TTL_SECONDS),
            jwt_issuer: env_string(ENV_JWT_ISSUER)
                .unwrap_or_else(|| DEFAULT_JWT_ISSUER.to_string()),
            bootstrap_admin_user: env_string(ENV_BOOTSTRAP_ADMIN_USER),
            bootstrap_admin_password: read_secret(
                ENV_BOOTSTRAP_ADMIN_PASSWORD_FILE,
                ENV_BOOTSTRAP_ADMIN_PASSWORD,
            ),
            object_store: ObjectStoreConfig {
                endpoint: env_string(ENV_S3_ENDPOINT),
                region: env_string(ENV_S3_REGION),
                bucket: env_string(ENV_S3_BUCKET),
                // 与 crates/objectstore 的 `read_var` 同口径：`*_FILE`（docker secrets）
                // 优先，明文环境变量兜底 —— 两个服务读同一份 env 必须得到同样的结论。
                access_key_id: read_secret(ENV_S3_ACCESS_KEY_ID_FILE, ENV_S3_ACCESS_KEY_ID),
                secret_access_key: read_secret(
                    ENV_S3_SECRET_ACCESS_KEY_FILE,
                    ENV_S3_SECRET_ACCESS_KEY,
                ),
            },
            wal_endpoints: parse_wal_endpoints(
                &env_string(ENV_WAL_ENDPOINTS).unwrap_or_default(),
                &env_string(ENV_WAL_CLUSTER).unwrap_or_default(),
                env_u64(ENV_WAL_GRPC_PORT)
                    .and_then(|v| u16::try_from(v).ok())
                    .unwrap_or(DEFAULT_WAL_GRPC_PORT),
            ),
        })
    }
}

/// 解析 Remote WAL gRPC 端点（与 db-worker 的 `parse_wal_endpoints` 同构，
/// 保证两个服务对同一份 `WAL_CLUSTER` 得出完全一致的端点列表）。
///
/// 优先使用显式 `WAL_ENDPOINTS`；为空时从 `WAL_CLUSTER`（`id@host:peer_port`）推导，
/// 只替换端口、不改 host。非法成员直接忽略（不 panic）：WAL 地址写错不该让
/// 控制面启动失败，只是失去「向存储层对齐 epoch」这条恢复路径。
#[must_use]
pub fn parse_wal_endpoints(explicit: &str, cluster: &str, wal_grpc_port: u16) -> Vec<String> {
    let endpoints: Vec<String> = explicit
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| normalize_wal_endpoint(item, wal_grpc_port))
        .collect();
    if !endpoints.is_empty() {
        return endpoints;
    }

    cluster
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .filter_map(|member| {
            let (_, address) = member.split_once('@')?;
            let host = address.rsplit_once(':').map(|(host, _)| host)?;
            Some(format!("http://{host}:{wal_grpc_port}"))
        })
        .collect()
}

/// 补全端点 scheme 并去掉尾部斜杠。
fn normalize_wal_endpoint(raw: &str, wal_grpc_port: u16) -> String {
    let trimmed = raw.trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_string();
    }
    if trimmed.contains(':') {
        return format!("http://{trimmed}");
    }
    format!("http://{trimmed}:{wal_grpc_port}")
}

// ------------------------------------------------------------------ env 解析辅助

/// 读取字符串环境变量，空白视为未设置。
#[must_use]
pub fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// 读取整数环境变量；解析失败按未设置处理并告警。
#[must_use]
pub fn env_u64(key: &str) -> Option<u64> {
    match std::env::var(key) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return None;
            }
            match trimmed.parse::<u64>() {
                Ok(value) => Some(value),
                Err(_) => {
                    tracing::warn!(key, value = trimmed, "环境变量不是合法整数，按未设置处理");
                    None
                }
            }
        }
        Err(_) => None,
    }
}

/// 读取 secret：优先 `*_FILE` 指向的文件（去掉行尾换行），其次明文环境变量。
///
/// 只做「读文件」不做「解析格式」：口令里允许出现任意字符。
#[must_use]
pub fn read_secret(file_key: &str, plain_key: &str) -> Option<String> {
    if let Some(path) = env_string(file_key) {
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let value = content.trim_end_matches(['\n', '\r']).to_string();
                if value.is_empty() {
                    tracing::warn!(path, "{file_key} 指向的文件为空，忽略");
                } else {
                    return Some(value);
                }
            }
            Err(err) => {
                tracing::warn!(path, error = %err, "{file_key} 指向的文件不可读，忽略");
            }
        }
    }
    if let Some(value) = env_string(plain_key) {
        return Some(value);
    }
    None
}

/// 解析监听地址；缺失时用默认值，非法时返回参数错误。
fn parse_socket_addr(key: &str, default: &str) -> Result<SocketAddr> {
    let raw = env_string(key).unwrap_or_else(|| default.to_string());
    raw.parse::<SocketAddr>().map_err(|err| {
        PlatformError::new(
            ErrorCode::InvalidArgument,
            format!("{key} 不是合法的监听地址 '{raw}': {err}"),
        )
    })
}

/// 解析可选监听地址；非法时告警并关闭该监听。
fn parse_optional_socket_addr(key: &str) -> Option<SocketAddr> {
    let raw = env_string(key)?;
    match raw.parse::<SocketAddr>() {
        Ok(addr) => Some(addr),
        Err(err) => {
            tracing::warn!(key, value = raw, error = %err, "运维监听地址非法，已禁用");
            None
        }
    }
}

/// 供 CLI 显示的配置摘要（**不含任何 secret**）。
#[must_use]
pub fn redacted_summary(config: &ServerConfig) -> serde_json::Value {
    serde_json::json!({
        "http_listen": config.http_listen.to_string(),
        "grpc_listen": config.grpc_listen.to_string(),
        "ops_listen": config.ops_listen.map(|a| a.to_string()),
        "catalog_max_connections": config.catalog_max_connections,
        "route_reconcile_interval_ms": config.route_reconcile_interval.as_millis(),
        "route_cache_max_entries": config.route_cache_max_entries,
        "wakeup_timeout_ms": config.wakeup_timeout.as_millis(),
        "wakeup_poll_interval_ms": config.wakeup_poll_interval.as_millis(),
        "session_idle_timeout_ms": config.session_idle_timeout_ms,
        "transaction_max_lifetime_ms": config.transaction_max_lifetime_ms,
        "inline_result_limit_bytes": config.inline_result_limit_bytes,
        "jwt_enabled": config.jwt_secret.is_some(),
        "jwt_issuer": config.jwt_issuer,
        "bootstrap_admin_user": config.bootstrap_admin_user,
        "bootstrap_admin_password_configured": config.bootstrap_admin_password.is_some(),
        "object_store_configured": config.object_store.is_configured(),
        "object_store_bucket": config.object_store.bucket,
        // 缺失项（仅变量名，不含取值）：让「日志说配置了、接口说没配」这类矛盾
        // 在启动日志里就能定位到具体少配了什么。
        "object_store_missing": config.object_store.missing_parts(),
        // 端点不是 secret（架构 §17.14 只要求凭据不入日志），启动日志里带上它才能
        // 判断「控制面是否有能力向存储层对齐 epoch」。
        "wal_endpoints": config.wal_endpoints,
    })
}

/// 默认配置（测试用；不读环境变量）。
#[must_use]
pub fn test_config() -> ServerConfig {
    ServerConfig {
        http_listen: "127.0.0.1:0".parse().expect("合法监听地址"),
        grpc_listen: "127.0.0.1:0".parse().expect("合法监听地址"),
        ops_listen: None,
        database_url: "postgres://localhost/dbplatform".to_string(),
        catalog_max_connections: 4,
        route_reconcile_interval: Duration::from_millis(100),
        route_cache_max_entries: 64,
        wakeup_timeout: Duration::from_millis(200),
        wakeup_poll_interval: Duration::from_millis(10),
        session_idle_timeout_ms: 60_000,
        transaction_max_lifetime_ms: 30_000,
        inline_result_limit_bytes: 1024,
        jwt_secret: None,
        jwt_issuer: DEFAULT_JWT_ISSUER.to_string(),
        jwt_ttl_seconds: DEFAULT_JWT_TTL_SECONDS,
        bootstrap_admin_user: None,
        bootstrap_admin_password: None,
        object_store: ObjectStoreConfig::default(),
        wal_endpoints: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_store_requires_endpoint_bucket_and_credentials() {
        let mut config = ObjectStoreConfig::default();
        assert!(!config.is_configured());
        config.endpoint = Some("http://s3:8333".into());
        assert!(!config.is_configured());
        config.bucket = Some("snapshots".into());
        // 缺凭据：endpoint + bucket 齐全，但 Worker 的 S3ObjectStore::from_env() 会失败，
        // 因此这里也必须判为「未配置」——「配置了」与「能用」是同一个判定。
        assert!(!config.is_configured());
        assert_eq!(
            config.missing_parts(),
            vec!["S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY"]
        );
        config.access_key_id = Some("minioadmin".into());
        // 只有一半凭据仍然不可用（objectstore 的 from_env 同样拒绝）
        assert!(!config.is_configured());
        config.secret_access_key = Some("minioadmin".into());
        assert!(config.is_configured());
        assert!(config.missing_parts().is_empty());
    }

    #[test]
    fn object_store_debug_redacts_credentials() {
        let config = ObjectStoreConfig {
            endpoint: Some("http://minio:9000".into()),
            region: None,
            bucket: Some("snapshots".into()),
            access_key_id: Some("minioadmin".into()),
            secret_access_key: Some("s3-crdb-secret".into()),
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("minioadmin"), "{rendered}");
        assert!(!rendered.contains("s3-crdb-secret"), "{rendered}");
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn redacted_summary_never_leaks_secrets() {
        let mut config = test_config();
        config.jwt_secret = Some(b"super-secret-jwt-key".to_vec());
        config.bootstrap_admin_password = Some("admin-password".to_string());
        let summary = redacted_summary(&config).to_string();
        assert!(!summary.contains("super-secret-jwt-key"));
        assert!(!summary.contains("admin-password"));
        assert!(summary.contains("\"jwt_enabled\":true"));
    }

    /// WAL 端点解析必须与 db-worker 的同名逻辑同构：两个服务在同一个 compose 里
    /// 读同一份 `WAL_CLUSTER`，解析结果不一致会让「向存储层对齐 epoch」这条恢复路径
    /// 打到不存在的地址上。
    #[test]
    fn wal_endpoints_follow_worker_parsing_rules() {
        // 显式配置优先，并补全 scheme / 去掉尾斜杠
        assert_eq!(
            parse_wal_endpoints("wal-1:9200,http://wal-2:9200/", "1@ignore:1", 9200),
            vec![
                "http://wal-1:9200".to_string(),
                "http://wal-2:9200".to_string()
            ]
        );
        // 从 WAL_CLUSTER 推导：只换端口，不改 host
        assert_eq!(
            parse_wal_endpoints("", "1@wal-1:9201,2@wal-2:9201", 9200),
            vec![
                "http://wal-1:9200".to_string(),
                "http://wal-2:9200".to_string()
            ]
        );
        // 非法成员被忽略而不是 panic（WAL 没配好不该让控制面起不来）
        assert_eq!(
            parse_wal_endpoints("", "garbage,3@wal-3:9201", 9300),
            vec!["http://wal-3:9300".to_string()]
        );
        // 两种来源都为空 -> 空列表（能力缺席，调用方降级）
        assert!(parse_wal_endpoints("", "", 9200).is_empty());
    }

    #[test]
    fn default_timeouts_match_frozen_architecture_values() {
        // 架构 §15.3 冻结：session 60s / transaction 30s
        let config = test_config();
        assert_eq!(config.session_idle_timeout_ms, 60_000);
        assert_eq!(config.transaction_max_lifetime_ms, 30_000);
    }
}
