//! Catalog 变更通知（LISTEN/NOTIFY）。
//!
//! 架构 §17.4：NOTIFY 只是 Route Cache 的**加速提示**，不是可靠消息通道。
//! 正确性由 `catalog_version` 全量 reconcile 保证，因此本模块的策略是：
//! - 断线自动重连并记录 tracing 事件；
//! - 丢弃无法解析的通知，不因单条脏数据中断整个 watch；
//! - 暴露 `last_version`，调用方发现版本跳跃时自行触发 reconcile。

use domain::error::{ErrorCode, Result};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgListener, PgNotification};
use sqlx::PgPool;

use crate::error::{map_sqlx_error, platform_error_retryable, ConflictAs, NotFoundAs};
use crate::Catalog;

/// 通知频道名（与 migrations 中 `pg_notify('catalog_changes', ...)` 一致）。
pub const CATALOG_CHANGES_CHANNEL: &str = "catalog_changes";

/// 重连的最大尝试次数（每次之间有退避），超过则把错误抛给调用方。
const RECONNECT_ATTEMPTS: u32 = 3;

/// 一条 Catalog 变更通知（由 DB trigger 产生）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogChange {
    pub version: i64,
    pub table: String,
    pub op: String,
    pub id: String,
}

impl CatalogChange {
    /// 解析 trigger 发出的 JSON 负载；格式不符返回 None（调用方丢弃该条即可）。
    pub fn parse(payload: &str) -> Option<Self> {
        serde_json::from_str::<CatalogChange>(payload)
            .ok()
            .filter(|c| !c.table.is_empty())
    }

    fn from_notification(notification: &PgNotification) -> Option<Self> {
        Self::parse(notification.payload())
    }
}

/// Catalog 变更订阅器。
pub struct CatalogWatcher {
    pool: PgPool,
    listener: PgListener,
    channel: String,
    last_version: Option<i64>,
    reconnects: u64,
}

impl CatalogWatcher {
    pub(crate) async fn connect(pool: &PgPool, channel: &str) -> Result<Self> {
        let mut listener = PgListener::connect_with(pool)
            .await
            .map_err(map_listener_error)?;
        listener.listen(channel).await.map_err(map_listener_error)?;

        Ok(Self {
            pool: pool.clone(),
            listener,
            channel: channel.to_string(),
            last_version: None,
            reconnects: 0,
        })
    }

    /// 等待下一条变更通知。断线会先自动重连；重连失败才返回错误。
    pub async fn recv(&mut self) -> Result<CatalogChange> {
        loop {
            match self.listener.recv().await {
                Ok(notification) => {
                    if let Some(change) = self.accept(&notification) {
                        return Ok(change);
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        channel = %self.channel,
                        error = %err,
                        reconnects = self.reconnects,
                        "catalog LISTEN 连接中断，尝试重连（期间的正确性由 catalog_version reconcile 保证）"
                    );
                    self.reconnect().await?;
                }
            }
        }
    }

    /// 非阻塞取一条通知（已就绪队列为空时返回 None）。
    pub async fn try_recv(&mut self) -> Result<Option<CatalogChange>> {
        match self.listener.try_recv().await {
            Ok(Some(notification)) => Ok(self.accept(&notification)),
            Ok(None) => Ok(None),
            Err(err) => {
                tracing::warn!(channel = %self.channel, error = %err, "catalog LISTEN 断线，重连");
                self.reconnect().await?;
                Ok(None)
            }
        }
    }

    /// 重新建立 LISTEN 连接（带退避重试）。
    pub async fn reconnect(&mut self) -> Result<()> {
        let mut last_error: Option<sqlx::Error> = None;
        for attempt in 1..=RECONNECT_ATTEMPTS {
            match PgListener::connect_with(&self.pool).await {
                Ok(mut listener) => match listener.listen(&self.channel).await {
                    Ok(()) => {
                        self.listener = listener;
                        self.reconnects += 1;
                        tracing::info!(
                            channel = %self.channel,
                            attempt,
                            reconnects = self.reconnects,
                            "catalog LISTEN 已重连"
                        );
                        return Ok(());
                    }
                    Err(err) => last_error = Some(err),
                },
                Err(err) => last_error = Some(err),
            }
            tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt))).await;
        }

        let err = last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown listener error".to_string());
        Err(platform_error_retryable(
            ErrorCode::InternalError,
            format!("catalog LISTEN 重连失败（已尝试 {RECONNECT_ATTEMPTS} 次）: {err}"),
        ))
    }

    /// 最近一次成功接收到的版本号；调用方据此判断是否需要全量 reconcile。
    pub fn last_version(&self) -> Option<i64> {
        self.last_version
    }

    /// 断线重连次数（观测用）。
    pub fn reconnect_count(&self) -> u64 {
        self.reconnects
    }

    fn accept(&mut self, notification: &PgNotification) -> Option<CatalogChange> {
        match CatalogChange::from_notification(notification) {
            Some(change) => {
                self.last_version = Some(change.version);
                Some(change)
            }
            None => {
                tracing::debug!(
                    channel = %self.channel,
                    payload = notification.payload(),
                    "忽略无法解析的 catalog 通知"
                );
                None
            }
        }
    }
}

fn map_listener_error(err: sqlx::Error) -> domain::error::PlatformError {
    let mapped = map_sqlx_error(err, NotFoundAs::Database, ConflictAs::Database);
    platform_error_retryable(mapped.code, mapped.message)
}

impl Catalog {
    /// 订阅 Catalog 变更。
    ///
    /// 推荐顺序（避免漏事件）：先 `watch_catalog_changes()`，再读一次
    /// `current_catalog_version()`，然后处理通知中 version 大于该值的变更。
    pub async fn watch_catalog_changes(&self) -> Result<CatalogWatcher> {
        CatalogWatcher::connect(self.pool(), CATALOG_CHANGES_CHANNEL).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_trigger_payload() {
        let payload = r#"{"version":42,"table":"databases","op":"UPDATE","id":"0189-abc"}"#;
        let change = CatalogChange::parse(payload).unwrap();
        assert_eq!(change.version, 42);
        assert_eq!(change.table, "databases");
        assert_eq!(change.op, "UPDATE");
        assert_eq!(change.id, "0189-abc");
    }

    #[test]
    fn rejects_malformed_payload() {
        assert!(CatalogChange::parse("not json").is_none());
        assert!(CatalogChange::parse(r#"{"version":1}"#).is_none());
        assert!(
            CatalogChange::parse(r#"{"version":1,"table":"","op":"INSERT","id":""}"#).is_none()
        );
    }

    #[test]
    fn channel_name_matches_migration_trigger() {
        assert_eq!(CATALOG_CHANGES_CHANNEL, "catalog_changes");
    }
}
