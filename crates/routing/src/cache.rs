//! Server 本地 Route Cache（架构 §4.2 / §16 / §17.4）。
//!
//! 读路径：`DashMap` 分片读锁，无全局锁 —— 查询热路径只做一次哈希 + 分片读。
//! 写路径：单条更新走分片写锁；**全量快照 reconcile 用一把互斥锁串行化**，
//! 避免两个 reconcile 交错导致「A 的快照把 B 刚写入的路由删掉」。
//!
//! 再次强调（架构 §16）：这里**没有任何 TTL**。见 crate 文档。

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use domain::DatabaseId;
use observability::metrics as platform_metrics;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::entry::RouteEntry;

/// Route Cache 统计快照（供 metrics 暴露，判定 §16 的 Cache Hit Rate >= 99.9%）。
///
/// 字段全部是**单调累加**的计数或瞬时值；读取 `stats()` 本身不产生命中 / 未命中。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteCacheStats {
    /// 命中次数（仅 [`RouteCache::get`] 计数）。
    pub hits: u64,
    /// 未命中次数（仅 [`RouteCache::get`] 计数）。
    pub misses: u64,
    /// 当前条目数。
    pub entries: usize,
    /// 当前 Catalog 版本。
    pub catalog_version: i64,
    /// 失效事件数：显式 invalidate 删除 + 快照 reconcile 删除。
    pub invalidations: u64,
    /// 检测到的陈旧（stale）信号数，详见 [`RouteCache::stats`]。
    pub stale_detected: u64,
}

impl RouteCacheStats {
    /// 缓存命中率（`hits / (hits + misses)`）。
    ///
    /// 从未查询过时返回 1.0（未定义 ≠ 差），避免冷启动阶段误触发命中率告警。
    #[must_use]
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits.saturating_add(self.misses);
        if total == 0 {
            1.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Server 本地路由缓存。
///
/// 用 `Arc<RouteCache>` 在请求处理任务之间共享；内部无生命周期依赖，
/// 不持有任何连接，也不依赖 Control Plane 存活。
pub struct RouteCache {
    entries: DashMap<DatabaseId, RouteEntry>,
    catalog_version: AtomicI64,
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
    stale_detected: AtomicU64,
    max_entries: usize,
    /// 只被 [`RouteCache::apply_catalog_snapshot`] 获取：保证快照串行应用。
    reconcile: Mutex<()>,
}

impl fmt::Debug for RouteCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 不打印全部路由（可能是数十万条），只给规模与版本，便于日志安全使用。
        f.debug_struct("RouteCache")
            .field("entries", &self.len())
            .field("max_entries", &self.max_entries)
            .field("catalog_version", &self.catalog_version())
            .finish()
    }
}

impl RouteCache {
    /// 创建缓存。
    ///
    /// `max_entries` 只用于 DashMap 预分配与 [`RouteCache::is_over_capacity`] 告警，
    /// **不是**淘汰上限：Route 是正确性相关数据，缺一条就会让数据面走回源路径
    /// （甚至直接失败），因此不允许因为容量上限丢弃条目。传 0 表示不做预分配与告警。
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: DashMap::with_capacity(max_entries),
            catalog_version: AtomicI64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            stale_detected: AtomicU64::new(0),
            max_entries,
            reconcile: Mutex::new(()),
        }
    }

    /// 查询路由（热路径）。命中 / 未命中会计入 [`RouteCacheStats`] 并上报平台指标
    /// （`route_cache_hit_total` / `route_cache_miss_total`）。
    ///
    /// 未命中**不代表 DB 不存在**：调用方应回源 Catalog 并写回缓存
    /// （或触发冷启动），而不是直接向客户端报错。
    #[must_use]
    pub fn get(&self, database_id: &DatabaseId) -> Option<RouteEntry> {
        // 先 clone 出条目并释放分片读锁，再做计数与指标上报：
        // 观测调用不在热路径的锁竞争区里（RouteEntry 的 clone 本来就要做）。
        let found = self
            .entries
            .get(database_id)
            .map(|guard| guard.value().clone());
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            platform_metrics::record_route_cache_hit();
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            platform_metrics::record_route_cache_miss();
        }
        found
    }

    /// 写入 / 更新一条路由。
    ///
    /// 写入规则（fencing 安全）：
    /// - **epoch 倒退**的更新被拒绝（旧 Owner 不得复活）；
    /// - **同一 epoch 更换 owner** 被拒绝（换 owner 必须换 epoch，否则等于放行双 Owner）；
    /// - 同一 epoch 的同一 owner 更新 endpoint / state 是允许的
    ///   （Worker 重新注册地址、DB 从 WARM 变 HOT 都属于这种情况）；
    /// - endpoint 为空的路由被拒绝（无法建立连接，属于未解析完成的中间态）。
    ///
    /// 被拒绝的更新计入 [`RouteCacheStats::stale_detected`] 并上报
    /// `route_cache_stale_detected_total`。
    pub fn upsert(&self, entry: RouteEntry) {
        if !entry.has_endpoint() {
            tracing::warn!(
                database_id = %entry.database_id,
                worker_id = %entry.worker_id,
                "拒绝写入 endpoint 为空的路由"
            );
            return;
        }

        let database_id = entry.database_id;
        // 拒绝判定必须在锁内做，但日志与指标上报搬到**锁外**：分片写锁是热路径的
        // 竞争区，观测调用（尤其带 I/O 的日志）不得在里面做。
        // 元组为 (缓存 epoch, 传入 epoch, 原因)。
        let mut rejected: Option<(u64, u64, &'static str)> = None;
        // 用 entry API 全程持有分片写锁：读旧值与写新值之间不存在竞态。
        match self.entries.entry(database_id) {
            Entry::Occupied(mut occupied) => {
                let current = occupied.get();
                let stale = current.owner_epoch > entry.owner_epoch
                    || (current.owner_epoch == entry.owner_epoch
                        && current.worker_id != entry.worker_id);
                if stale {
                    let reason = if current.owner_epoch > entry.owner_epoch {
                        "owner epoch 倒退"
                    } else {
                        "同一 epoch 更换 owner"
                    };
                    rejected = Some((current.owner_epoch, entry.owner_epoch, reason));
                } else {
                    *occupied.get_mut() = entry;
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(entry);
            }
        }

        if let Some((cached_epoch, incoming_epoch, reason)) = rejected {
            tracing::warn!(
                database_id = %database_id,
                cached_epoch,
                incoming_epoch,
                reason,
                "拒绝陈旧的 route 更新"
            );
            self.stale_detected.fetch_add(1, Ordering::Relaxed);
            platform_metrics::record_route_cache_stale_detected(1);
        }
    }

    /// 显式失效一条路由（运维指令 / 已确认的 ownership 变更）。
    ///
    /// 只有**确实删除了条目**才计入 [`RouteCacheStats::invalidations`]，
    /// 避免重复失效把指标刷成噪音。
    pub fn invalidate(&self, database_id: &DatabaseId) {
        if self.entries.remove(database_id).is_some() {
            self.invalidations.fetch_add(1, Ordering::Relaxed);
            platform_metrics::record_route_cache_invalidations(1);
        }
    }

    /// 应用 Catalog 全量快照（version reconcile 的 correctness path）。
    ///
    /// 语义：
    /// - **只有 `catalog_version > 当前版本` 才生效**；版本相等或回退一律忽略
    ///   （回退计入 [`RouteCacheStats::stale_detected`]），杜绝旧快照覆盖新状态；
    /// - 快照中**已不存在**的条目会被删除（owner 被摘掉 / DB 被删除）；
    /// - 快照里 endpoint 为空的条目视为「调用方尚未解析出 Worker 地址」：
    ///   不覆盖、也不删除它的旧路由 —— Control Plane 抖动期间宁可继续用旧地址，
    ///   也不能让数据面无谓中断（§16）；
    /// - 应用过程串行化，快照内容与版本号对对读者可见的顺序为先内容后版本。
    pub fn apply_catalog_snapshot(&self, entries: Vec<RouteEntry>, catalog_version: i64) {
        // 串行化：两个 reconcile 并发时会互相删掉对方刚写入的条目。
        let _guard = self.reconcile.lock();

        let current_version = self.catalog_version();
        if catalog_version <= current_version {
            if catalog_version < current_version {
                self.stale_detected.fetch_add(1, Ordering::Relaxed);
                platform_metrics::record_route_cache_stale_detected(1);
                tracing::warn!(
                    current = current_version,
                    incoming = catalog_version,
                    "忽略版本回退的 catalog 快照"
                );
            }
            return;
        }

        let mut present: Vec<DatabaseId> = Vec::with_capacity(entries.len());
        for entry in entries {
            if !entry.has_endpoint() {
                // 保留旧路由，同时把它算作「仍在快照中」，避免下面的清理把它删掉。
                tracing::warn!(
                    database_id = %entry.database_id,
                    "catalog 快照中该 DB 的 worker endpoint 为空，保留现有路由"
                );
                present.push(entry.database_id);
                continue;
            }
            present.push(entry.database_id);
            self.upsert(entry);
        }

        // 清理：快照里没有的 DB 说明已无 owner（或已删除），必须移除。
        // ⚠️ 这是「事实驱动的失效」，不是 TTL。
        let before_removal = self.entries.len();
        self.entries
            .retain(|database_id, _| present.contains(database_id));
        let removed = before_removal.saturating_sub(self.entries.len());
        if removed > 0 {
            self.invalidations
                .fetch_add(removed as u64, Ordering::Relaxed);
            platform_metrics::record_route_cache_invalidations(removed as u64);
        }

        self.catalog_version
            .store(catalog_version, Ordering::Release);
        tracing::info!(
            catalog_version,
            entries = present.len(),
            removed,
            "应用 catalog 快照"
        );
    }

    /// 当前 Catalog 版本（0 表示从未应用过快照）。
    #[must_use]
    pub fn catalog_version(&self) -> i64 {
        self.catalog_version.load(Ordering::Acquire)
    }

    /// 统计快照。
    ///
    /// `stale_detected` 的口径（所有「缓存落后于事实」的信号，一次事件计一次）：
    /// - [`RouteCache::upsert`] 拒绝 epoch 倒退 / 同 epoch 换 owner；
    /// - [`RouteCache::validate_epoch`] 发现缓存 epoch 与调用方持有的 epoch 不一致；
    /// - [`RouteCache::apply_catalog_snapshot`] 收到版本回退的快照。
    #[must_use]
    pub fn stats(&self) -> RouteCacheStats {
        RouteCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: self.entries.len(),
            catalog_version: self.catalog_version(),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            stale_detected: self.stale_detected.load(Ordering::Relaxed),
        }
    }

    /// 当前条目数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 缓存是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// [`RouteCache::new`] 传入的软上限（0 表示未设置）。
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.max_entries
    }

    /// 是否已超过软上限（调用方据此告警 / 触发 Catalog reconcile，而不是丢条目）。
    #[must_use]
    pub fn is_over_capacity(&self) -> bool {
        self.max_entries != 0 && self.entries.len() > self.max_entries
    }

    /// 校验调用方持有的 epoch 是否仍是当前有效 epoch。
    ///
    /// - 返回 `true`：缓存中有该 DB，且 epoch 完全一致；
    /// - 返回 `false`：没有条目（需要回源 Catalog），或 epoch 不一致
    ///   （缓存陈旧或调用方陈旧，两种都必须刷新，绝不能继续按旧 epoch 写入）。
    ///
    /// 不一致时计入 [`RouteCacheStats::stale_detected`]；没有条目时**不**计入
    /// hits / misses（那是 [`RouteCache::get`] 的热路径指标）。
    #[must_use]
    pub fn validate_epoch(&self, database_id: &DatabaseId, epoch: u64) -> bool {
        // 先取出值再释放分片读锁：日志与计数不在持锁路径上做。
        let Some(cached_epoch) = self
            .entries
            .get(database_id)
            .map(|guard| guard.value().owner_epoch)
        else {
            return false;
        };
        if cached_epoch == epoch {
            return true;
        }
        tracing::warn!(
            database_id = %database_id,
            cached_epoch,
            requested_epoch = epoch,
            "route epoch 不一致：需要刷新路由"
        );
        self.stale_detected.fetch_add(1, Ordering::Relaxed);
        platform_metrics::record_route_cache_stale_detected(1);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{LifecycleState, WorkerId};
    use std::sync::Arc;
    use std::time::Duration;

    fn db() -> DatabaseId {
        DatabaseId::new_v7()
    }

    fn entry(database_id: DatabaseId, epoch: u64) -> RouteEntry {
        RouteEntry::new(
            database_id,
            WorkerId::new("worker-1"),
            "http://worker-1:9000",
            epoch,
            LifecycleState::Warm,
        )
    }

    fn entry_on(database_id: DatabaseId, worker: &str, epoch: u64) -> RouteEntry {
        RouteEntry::new(
            database_id,
            WorkerId::new(worker),
            format!("http://{worker}:9000"),
            epoch,
            LifecycleState::Warm,
        )
    }

    #[test]
    fn upsert_then_get_counts_hits_and_misses() {
        let cache = RouteCache::new(16);
        let id = db();
        cache.upsert(entry(id, 1));

        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        assert_eq!(cache.catalog_version(), 0, "未应用快照前版本为 0");

        let hit = cache.get(&id).expect("应命中");
        assert_eq!(hit.owner_epoch, 1);
        assert_eq!(hit.worker_endpoint, "http://worker-1:9000");
        assert!(hit.is_serving());

        assert!(cache.get(&db()).is_none());

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.invalidations, 0);
        assert_eq!(stats.stale_detected, 0);
        assert_eq!(stats.hit_rate(), 0.5);

        // stats() 自身不得影响命中率
        let again = cache.stats();
        assert_eq!(again.hits, 1);
        assert_eq!(again.misses, 1);
    }

    #[test]
    fn hit_rate_is_one_when_no_lookup_happened() {
        let cache = RouteCache::new(4);
        assert_eq!(cache.stats().hit_rate(), 1.0);
    }

    #[test]
    fn explicit_invalidation_removes_and_counts_once() {
        let cache = RouteCache::new(16);
        let id = db();
        cache.upsert(entry(id, 1));

        cache.invalidate(&id);
        assert!(cache.get(&id).is_none());
        assert_eq!(cache.stats().invalidations, 1);

        // 重复失效不刷指标
        cache.invalidate(&id);
        cache.invalidate(&db());
        assert_eq!(cache.stats().invalidations, 1);
    }

    #[test]
    fn there_is_no_ttl_so_entries_survive_time_passing() {
        // 架构 §16：Control Plane 不可用 30 分钟期间已缓存 Route 必须继续可用，
        // 因此缓存里不存在任何过期判定。这里用一个短睡眠证明「时间流逝不产生失效」，
        // 真正的证据是代码中不存在 TTL 字段 / 后台清理任务。
        let cache = RouteCache::new(16);
        let id = db();
        cache.upsert(entry(id, 7));

        std::thread::sleep(Duration::from_millis(80));

        let hit = cache.get(&id).expect("时间流逝不得让路由失效");
        assert_eq!(hit.owner_epoch, 7);
        assert_eq!(cache.stats().invalidations, 0);
        assert_eq!(cache.stats().stale_detected, 0);
    }

    #[test]
    fn catalog_version_rollback_and_duplicates_are_ignored() {
        let cache = RouteCache::new(16);
        let first = db();
        let second = db();

        cache.apply_catalog_snapshot(vec![entry(first, 1)], 5);
        assert_eq!(cache.catalog_version(), 5);
        assert!(cache.get(&first).is_some());

        // 版本回退：整体忽略（不能把 second 塞进缓存，也不能删掉 first）
        cache.apply_catalog_snapshot(vec![entry(second, 1)], 4);
        assert_eq!(cache.catalog_version(), 5);
        assert!(cache.get(&first).is_some(), "旧快照不得删除新条目");
        assert!(cache.get(&second).is_none(), "旧快照不得写入条目");
        assert_eq!(cache.stats().stale_detected, 1);

        // 版本相同：同样不生效
        cache.apply_catalog_snapshot(vec![entry(second, 1)], 5);
        assert!(cache.get(&second).is_none());
        assert_eq!(cache.stats().stale_detected, 1);

        // 版本前进：生效，且删除不在快照中的条目
        cache.apply_catalog_snapshot(vec![entry(second, 1)], 6);
        assert_eq!(cache.catalog_version(), 6);
        assert!(cache.get(&first).is_none(), "已不在快照中的条目必须删除");
        assert!(cache.get(&second).is_some());
        assert_eq!(cache.stats().invalidations, 1);
    }

    #[test]
    fn snapshot_keeps_routes_whose_endpoint_is_unresolved() {
        let cache = RouteCache::new(16);
        let id = db();
        cache.upsert(entry(id, 3));

        // Control Plane 尚未解析出 worker endpoint：保留旧路由，避免数据面中断
        let unresolved =
            RouteEntry::new(id, WorkerId::new("worker-2"), "", 4, LifecycleState::Warm);
        cache.apply_catalog_snapshot(vec![unresolved], 9);

        let kept = cache.get(&id).expect("旧路由必须保留");
        assert_eq!(kept.worker_id, WorkerId::new("worker-1"));
        assert_eq!(kept.owner_epoch, 3);
        assert_eq!(cache.catalog_version(), 9);
    }

    #[test]
    fn upsert_rejects_empty_endpoint() {
        let cache = RouteCache::new(4);
        let id = db();
        cache.upsert(RouteEntry::new(
            id,
            WorkerId::new("worker-1"),
            "   ",
            1,
            LifecycleState::Warm,
        ));
        assert!(cache.is_empty());
        assert!(cache.get(&id).is_none());
    }

    #[test]
    fn upsert_refuses_stale_epochs() {
        let cache = RouteCache::new(4);
        let id = db();
        cache.upsert(entry_on(id, "worker-1", 5));

        // epoch 倒退：拒绝（旧 Owner 不得复活）
        cache.upsert(entry_on(id, "worker-9", 4));
        assert_eq!(cache.get(&id).unwrap().worker_id, WorkerId::new("worker-1"));
        assert_eq!(cache.stats().stale_detected, 1);

        // 同 epoch 换 owner：拒绝（换 owner 必须换 epoch）
        cache.upsert(entry_on(id, "worker-2", 5));
        assert_eq!(cache.get(&id).unwrap().worker_id, WorkerId::new("worker-1"));
        assert_eq!(cache.stats().stale_detected, 2);

        // 同 epoch 同 owner 更新 endpoint / state：允许（Worker 地址会变）
        let mut refreshed = entry_on(id, "worker-1", 5);
        refreshed.worker_endpoint = "http://worker-1-new:9000".to_string();
        refreshed.state = LifecycleState::Hot;
        cache.upsert(refreshed);
        let current = cache.get(&id).unwrap();
        assert_eq!(current.worker_endpoint, "http://worker-1-new:9000");
        assert!(current.is_serving());
        assert_eq!(cache.stats().stale_detected, 2, "正常刷新不算陈旧");

        // epoch 前进：正常接管
        cache.upsert(entry_on(id, "worker-2", 6));
        assert_eq!(cache.get(&id).unwrap().worker_id, WorkerId::new("worker-2"));
        assert_eq!(cache.stats().stale_detected, 2);
    }

    #[test]
    fn validate_epoch_detects_stale_routes() {
        let cache = RouteCache::new(4);
        let id = db();
        cache.upsert(entry(id, 5));

        assert!(cache.validate_epoch(&id, 5));
        assert_eq!(cache.stats().stale_detected, 0);

        assert!(
            !cache.validate_epoch(&id, 6),
            "调用方 epoch 更新（缓存陈旧）"
        );
        assert!(
            !cache.validate_epoch(&id, 4),
            "调用方 epoch 更旧（请求陈旧）"
        );
        assert_eq!(cache.stats().stale_detected, 2);

        // 未缓存的 DB：返回 false，但不计入命中率指标
        assert!(!cache.validate_epoch(&db(), 1));
        let stats = cache.stats();
        assert_eq!(stats.stale_detected, 2);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn capacity_is_only_a_soft_alert_threshold() {
        let cache = RouteCache::new(2);
        assert_eq!(cache.capacity(), 2);
        assert!(!cache.is_over_capacity());

        for _ in 0..3 {
            cache.upsert(entry(db(), 1));
        }
        assert_eq!(cache.len(), 3);
        assert!(cache.is_over_capacity(), "超过软上限只告警，不丢条目");

        // 未设置软上限时永远不告警
        let unbounded = RouteCache::new(0);
        for _ in 0..100 {
            unbounded.upsert(entry(db(), 1));
        }
        assert!(!unbounded.is_over_capacity());
    }

    #[test]
    fn concurrent_readers_writers_and_snapshots_do_not_panic() {
        let cache = Arc::new(RouteCache::new(64));
        let ids: Vec<DatabaseId> = (0..32).map(|_| db()).collect();

        let mut handles = Vec::new();
        for worker in 0..8 {
            let cache = Arc::clone(&cache);
            let ids = ids.clone();
            handles.push(std::thread::spawn(move || {
                for round in 0..200u64 {
                    let id = ids[(round as usize + worker) % ids.len()];
                    match round % 5 {
                        0 => cache.upsert(entry_on(id, &format!("worker-{worker}"), round + 1)),
                        1 => {
                            let _ = cache.get(&id);
                        }
                        2 => cache.invalidate(&id),
                        3 => {
                            let _ = cache.validate_epoch(&id, round + 1);
                        }
                        _ => cache.apply_catalog_snapshot(
                            ids.iter()
                                .copied()
                                .map(|id| entry_on(id, "worker-reconcile", round + 1))
                                .collect(),
                            round as i64 + 1,
                        ),
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("并发访问不得 panic");
        }

        // 收敛性检查：最后应用的快照版本必须被记录下来，条目数量不得失控
        let stats = cache.stats();
        assert!(stats.catalog_version >= 1);
        assert_eq!(stats.entries, cache.len());
        assert!(cache.len() <= ids.len());
    }

    #[test]
    fn debug_output_stays_small() {
        let cache = RouteCache::new(8);
        cache.upsert(entry(db(), 1));
        let rendered = format!("{cache:?}");
        assert!(rendered.contains("entries"));
        assert!(
            !rendered.contains("worker-1:9000"),
            "不得把全部路由打进日志"
        );
    }
}
