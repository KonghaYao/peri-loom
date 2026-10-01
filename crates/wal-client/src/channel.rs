//! gRPC channel 缓存。
//!
//! tonic 的 `Channel` 内部是连接池 + 后台任务，`clone` 只复制句柄，因此按端点缓存
//! channel 既避免每次写都重新握手（WAL 在 commit 热路径上），又不会复制连接。
//! 缓存不做淘汰：副本数量是运维配置的小集合（通常 3 个），不会无限增长。

use std::collections::HashMap;
use std::time::Duration;

use parking_lot::RwLock;
use tonic::transport::{Channel, Endpoint};

use crate::error::AttemptFailure;

/// 端点 -> channel 的惰性缓存。
#[derive(Debug)]
pub(crate) struct ChannelPool {
    connect_timeout: Duration,
    channels: RwLock<HashMap<String, Channel>>,
}

impl ChannelPool {
    /// 构造空缓存。
    pub(crate) fn new(connect_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            channels: RwLock::new(HashMap::new()),
        }
    }

    /// 取（必要时建立）到某端点的 channel。
    ///
    /// 失败一律归为传输层失败：调用方据此换端点重试。
    pub(crate) async fn get(&self, endpoint: &str) -> std::result::Result<Channel, AttemptFailure> {
        if let Some(channel) = self.cached(endpoint) {
            return Ok(channel);
        }

        let parsed = Endpoint::from_shared(endpoint.to_owned())
            .map_err(|err| AttemptFailure::transport(endpoint, format!("端点地址非法：{err}")))?;
        let channel = parsed
            .connect_timeout(self.connect_timeout)
            // 请求级超时由每个 RPC 自己通过 `Request::set_timeout` 设置：
            // channel 级 timeout 会连 server-streaming 的整个流一起砍断，
            // 而流式读取需要按 chunk 单独限时（见 client.rs）。
            .connect()
            .await
            .map_err(|err| AttemptFailure::transport(endpoint, format!("建立连接失败：{err}")))?;

        // 并发场景下可能有两个任务同时建链，保留先写入的那个即可（都是等价句柄）
        let mut channels = self.channels.write();
        let channel = channels
            .entry(endpoint.to_owned())
            .or_insert(channel)
            .clone();
        Ok(channel)
    }

    /// 读缓存（同步、不跨 await 持锁）。
    fn cached(&self, endpoint: &str) -> Option<Channel> {
        self.channels.read().get(endpoint).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_endpoint_surfaces_as_transport_failure() {
        let pool = ChannelPool::new(Duration::from_millis(50));
        let failure = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(pool.get("not-a-uri"))
            .expect_err("非法地址必须失败");
        assert_eq!(failure.metric_reason(), "transport");
    }

    #[test]
    fn unreachable_endpoint_surfaces_as_transport_failure() {
        // 端口 1 是特权端口，本机不会有 WAL 副本监听；这里只验证「连不上 = 传输失败」，
        // 不依赖任何外部进程。
        let pool = ChannelPool::new(Duration::from_millis(200));
        let failure = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(pool.get("http://127.0.0.1:1"))
            .expect_err("不可达端点必须失败");
        assert_eq!(failure.metric_reason(), "transport");
    }
}
