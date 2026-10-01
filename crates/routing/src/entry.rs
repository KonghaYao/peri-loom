//! 路由条目（架构 §4.2：`db_id -> worker_id -> worker_endpoint`）。

use domain::{DatabaseId, LifecycleState, WorkerId};
use serde::{Deserialize, Serialize};

/// 一条路由：DB 当前 Owner Worker 的地址与 ownership epoch。
///
/// `owner_epoch` 必须与 Catalog 中的 owner epoch 一致（§10）：
/// 它随请求发往 Worker，Worker 侧据此做 fencing，拒绝过期 epoch 的写入。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteEntry {
    /// 数据库 ID。
    pub database_id: DatabaseId,
    /// 当前 Owner Worker。
    pub worker_id: WorkerId,
    /// Worker 的对外 endpoint（Server -> Worker 的控制 / 数据通道地址）。
    pub worker_endpoint: String,
    /// Owner epoch（单调递增，禁止回退）。
    pub owner_epoch: u64,
    /// DB 生命周期状态（COLD / STARTING / WARM / HOT / …）。
    pub state: LifecycleState,
}

impl RouteEntry {
    /// 构造路由条目。
    #[must_use]
    pub fn new(
        database_id: DatabaseId,
        worker_id: WorkerId,
        worker_endpoint: impl Into<String>,
        owner_epoch: u64,
        state: LifecycleState,
    ) -> Self {
        Self {
            database_id,
            worker_id,
            worker_endpoint: worker_endpoint.into(),
            owner_epoch,
            state,
        }
    }

    /// endpoint 是否可用。
    ///
    /// 空 endpoint 无法建立连接，属于「尚未解析出 Worker 地址」的中间态，
    /// 不允许写入缓存（详见 [`crate::RouteCache::upsert`]）。
    #[must_use]
    pub fn has_endpoint(&self) -> bool {
        !self.worker_endpoint.trim().is_empty()
    }

    /// 是否处于可立即服务的状态（WARM / HOT）。
    ///
    /// `STARTING` 的路由依然是有效路由（用于 Transparent Wake 期间定位 Worker），
    /// 只是还需要等待 READY，因此调用方要分开判断「能不能路由」与「能不能直接执行」。
    #[must_use]
    pub fn is_serving(&self) -> bool {
        self.state.is_serving()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(endpoint: &str, state: LifecycleState) -> RouteEntry {
        RouteEntry::new(
            DatabaseId::new_v7(),
            WorkerId::new("worker-1"),
            endpoint,
            1,
            state,
        )
    }

    #[test]
    fn endpoint_and_serving_predicates() {
        assert!(entry("http://worker-1:9000", LifecycleState::Warm).has_endpoint());
        assert!(!entry("   ", LifecycleState::Warm).has_endpoint());
        assert!(!entry("", LifecycleState::Cold).has_endpoint());

        assert!(entry("e", LifecycleState::Warm).is_serving());
        assert!(entry("e", LifecycleState::Hot).is_serving());
        assert!(!entry("e", LifecycleState::Cold).is_serving());
        // STARTING 仍是有效路由（Transparent Wake 期间要用它找到 Worker）
        assert!(!entry("e", LifecycleState::Starting).is_serving());
    }
}
