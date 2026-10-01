//! 客户端配置。

use std::time::Duration;

/// Remote WAL 客户端配置。
///
/// `endpoints` 是**全部 WAL 副本**的 gRPC 地址（如 `http://wal-1:9200`），不是单个
/// leader 地址：客户端靠它完成 leader 轮换 —— 只配置 leader 的话，leader 切换后
/// 写路径会整体不可用。
#[derive(Clone, Debug)]
pub struct WalClientConfig {
    /// 全部 WAL 副本 gRPC 地址，如 `http://wal-1:9200`。
    pub endpoints: Vec<String>,
    /// 建链超时（每次新建 channel 生效；已缓存的 channel 不再受它约束）。
    pub connect_timeout: Duration,
    /// 单次 RPC 的超时（含 server-streaming 的单 chunk 等待）。
    pub request_timeout: Duration,
    /// 最大尝试次数（**含首次尝试**）；必须 >= 1。
    pub max_attempts: u32,
    /// 是否接受**配置列表之外**的 leader 提示端点（默认 `false`）。
    ///
    /// `false` 时只把「已知 leader」缓存/插队到配置给出的副本列表内的端点：leader 提示
    /// 是服务端应答里的一段自由文本，写路径不能因为一句话就去拨号一个运维没配置过的
    /// 地址（配置错误或应答被篡改时会把写入引到未授权端点）。列表外的提示会记 warning
    /// 并忽略，其余端点照常轮换。确实需要「提示端点不在本地配置里」（例如动态扩副本）
    /// 时再显式打开。
    pub allow_unlisted_leader_hint: bool,
}

impl Default for WalClientConfig {
    fn default() -> Self {
        Self {
            // 空列表是「未配置」而不是「无副本」：`WalClient::new` 会拒绝它，
            // 避免调用方在拿到一个永远失败的客户端后才在写路径上发现问题。
            endpoints: Vec::new(),
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(5),
            max_attempts: 4,
            allow_unlisted_leader_hint: false,
        }
    }
}
