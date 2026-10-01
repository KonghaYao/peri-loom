//! 远程 WAL 写入的**注入点**。
//!
//! `PlatformDurableIO` 只需要「把一段 WAL 字节追加到 Remote WAL 并拿到 durable 确认」
//! 与「读取 Remote WAL 的末端 LSN」这两个能力。把它抽象成 trait 有两个明确目的：
//!
//! 1. **生产路径**：包住 [`wal_client::WalClient`]，因此端点轮换、有界重试、leader 缓存、
//!    `owner_epoch` fencing 全部沿用 wal-client 的既有语义，本 crate 不重新实现。
//! 2. **可测路径**：单测可以注入「必定失败」「必定成功但不推进 LSN」「延迟返回」的实现，
//!    从而直接验证 durability 契约（架构 §11.1）—— 这一点不可能用真实 gRPC 服务稳定覆盖。
//!
//! trait 的实现方**必须**保证：返回 `Ok` 就等于「已 quorum durable」。任何没拿到确认的情况
//! 都必须是 `Err`（`ErrorCode::WalNotDurable` / `WalNotLeader` / ...），不得伪装成功。

use std::sync::Arc;

use async_trait::async_trait;
use domain::error::{PlatformError, Result};
use domain::ids::DatabaseId;
use domain::wal::Lsn;
use wal_client::{AppendOutcome, AppendWalRequest, WalClient};

/// 远程 WAL 追加能力的抽象（生产实现见 [`WalClientAppender`]）。
#[async_trait]
pub trait RemoteWalAppender: Send + Sync + std::fmt::Debug {
    /// 追加一段 WAL 字节；`Ok` 即代表 quorum durable。
    async fn append(&self, request: AppendWalRequest) -> Result<AppendOutcome>;

    /// 读取 Remote WAL 的末端 LSN（`GetWalStatus.last_lsn`），用于进程重启后接续
    /// `durable_lsn`：重启后本地记账从 0 开始，若不接续，第一批 append 会带着重复的
    /// `start_lsn` 覆盖已 durable 区间（服务端必然拒绝）。
    ///
    /// 缺省实现返回 `Ok(None)`（= 本实现不提供远端末端，调用方保持本地记账不变），
    /// 只有测试替身与不接远端的本地实现会用到；生产实现必须给出真实末端。
    async fn last_lsn(&self, database_id: &DatabaseId) -> Result<Option<Lsn>> {
        let _ = database_id;
        Ok(None)
    }

    /// 供日志/诊断使用的实现名（低基数）。
    fn describe(&self) -> String {
        "remote-wal".to_string()
    }
}

/// 生产实现：直接用平台既有的 [`WalClient`]。
#[derive(Debug, Clone)]
pub struct WalClientAppender {
    client: Arc<WalClient>,
}

impl WalClientAppender {
    /// 包住一个已建立的客户端（客户端本身可廉价 clone，内部共享连接池）。
    #[must_use]
    pub fn new(client: Arc<WalClient>) -> Self {
        Self { client }
    }

    /// 返回被包装的客户端（诊断用）。
    #[must_use]
    pub fn client(&self) -> &Arc<WalClient> {
        &self.client
    }
}

#[async_trait]
impl RemoteWalAppender for WalClientAppender {
    async fn append(&self, request: AppendWalRequest) -> Result<AppendOutcome> {
        // 不在这里做任何「降级」或「吞错」：wal-client 的 Ok 是 quorum durable 的唯一凭据。
        self.client.append(request).await
    }

    async fn last_lsn(&self, database_id: &DatabaseId) -> Result<Option<Lsn>> {
        // `status` 自身已带端点轮换与重试；这里只取末端 LSN，不做任何本地推断。
        let status = self.client.status(database_id).await?;
        Ok(Some(status.last_lsn))
    }

    fn describe(&self) -> String {
        "wal-client".to_string()
    }
}

/// 便于在配置里直接传 `Arc<WalClient>`。
impl From<Arc<WalClient>> for WalClientAppender {
    fn from(client: Arc<WalClient>) -> Self {
        Self::new(client)
    }
}

/// 把 `PlatformError` 压成一行短描述，写入 durable IO 的 `last_error`。
///
/// `last_error` 会被日志与 `/health` 暴露，因此只保留错误码与消息，不带 detail，
/// 避免把可能的内部信息扩散出去。
pub(crate) fn describe_failure(err: &PlatformError) -> String {
    format!("[{}] {}", err.code.as_str(), err.message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::error::ErrorCode;
    use domain::{DatabaseId, Lsn};
    use std::time::Duration;

    /// 契约测试：注入的 fake 必须能表达「未 durable」与「durable」两种结果。
    #[derive(Debug)]
    struct FakeAppender {
        result: std::sync::Mutex<Option<Result<AppendOutcome>>>,
    }

    #[async_trait]
    impl RemoteWalAppender for FakeAppender {
        async fn append(&self, _request: AppendWalRequest) -> Result<AppendOutcome> {
            self.result
                .lock()
                .expect("测试用锁不得中毒")
                .take()
                .expect("每个 fake 只应被调用一次")
        }
    }

    #[tokio::test]
    async fn describe_failure_keeps_code_and_message() {
        let err = PlatformError::new(ErrorCode::WalNotDurable, "quorum 未确认");
        let text = describe_failure(&err);
        assert!(text.contains("WAL_NOT_DURABLE"), "实际：{text}");
        assert!(text.contains("quorum 未确认"), "实际：{text}");
    }

    #[tokio::test]
    async fn fake_appender_can_express_failure_without_network() {
        let fake = FakeAppender {
            result: std::sync::Mutex::new(Some(Err(PlatformError::new(
                ErrorCode::WalNotDurable,
                "注入失败",
            )))),
        };
        let err = fake
            .append(AppendWalRequest {
                database_id: DatabaseId::new_v7(),
                owner_epoch: 1,
                start_lsn: Lsn::ZERO,
                file_offset: 0,
                reset_wal: true,
                bytes: bytes::Bytes::from_static(b"x"),
                contains_commit_frame: true,
                append_id: "a".into(),
            })
            .await
            .expect_err("注入的失败必须原样返回");
        assert_eq!(err.code, ErrorCode::WalNotDurable);
        assert_eq!(fake.describe(), "remote-wal");
        assert!(Duration::from_millis(1).as_micros() > 0);
    }
}
