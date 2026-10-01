//! 集成测试：架构 §16 的「Control Plane 故障」与「路由失效 / Stale Route」场景。
//!
//! 场景复现：
//! 1. Control Plane 正常，Server 用 Catalog 快照填满 Route Cache；
//! 2. Control Plane 全不可用（不再有任何快照 / 刷新），数据面持续打查询 ——
//!    已缓存 Route 必须继续可用，且命中率必须满足 >= 99.9%；
//! 3. Control Plane 恢复后 epoch 前进（Worker 更换），Server 侧由 Worker 的
//!    `NOT_OWNER` / `EPOCH_MISMATCH` 触发失效并重新拉取路由。

use domain::{DatabaseId, LifecycleState, WorkerId};
use routing::{RouteCache, RouteEntry};

fn entry(database_id: DatabaseId, worker: &str, epoch: u64) -> RouteEntry {
    RouteEntry::new(
        database_id,
        WorkerId::new(worker),
        format!("http://{worker}:9000"),
        epoch,
        LifecycleState::Warm,
    )
}

#[test]
fn control_plane_outage_does_not_expire_cached_routes() {
    let databases: Vec<DatabaseId> = (0..64).map(|_| DatabaseId::new_v7()).collect();
    let cache = RouteCache::new(databases.len());

    // 1) Control Plane 正常：应用首个快照
    let snapshot: Vec<RouteEntry> = databases
        .iter()
        .copied()
        .map(|id| entry(id, "worker-a", 1))
        .collect();
    cache.apply_catalog_snapshot(snapshot, 1);
    assert_eq!(cache.len(), databases.len());
    assert_eq!(cache.catalog_version(), 1);

    // 2) Control Plane 不可用：只有数据面在跑（反复查路由，没有任何 Catalog 交互）
    for _ in 0..1000 {
        for id in &databases {
            let route = cache
                .get(id)
                .expect("Control Plane 故障期间路由必须仍然可用");
            assert_eq!(route.worker_id, WorkerId::new("worker-a"));
            assert!(cache.validate_epoch(id, 1), "epoch 未变，必须继续有效");
        }
    }
    let stats = cache.stats();
    assert_eq!(stats.hits, 64_000);
    assert_eq!(stats.misses, 0);
    assert!(
        stats.hit_rate() >= 0.999,
        "命中率必须满足验收线，实际 {}",
        stats.hit_rate()
    );
    assert_eq!(stats.invalidations, 0, "不得有任何主动失效");
    assert_eq!(stats.stale_detected, 0, "epoch 未变，不应出现陈旧信号");
}

#[test]
fn stale_route_is_refreshed_after_worker_reports_not_owner() {
    let cache = RouteCache::new(16);
    let database_id = DatabaseId::new_v7();

    cache.apply_catalog_snapshot(vec![entry(database_id, "worker-a", 834)], 1);
    assert_eq!(cache.get(&database_id).unwrap().owner_epoch, 834);

    // Worker 返回 NOT_OWNER / EPOCH_MISMATCH：Server 发现手上的 epoch 已过期
    assert!(!cache.validate_epoch(&database_id, 835));
    assert_eq!(cache.stats().stale_detected, 1);

    // 显式失效 + 从 Catalog 重新拉取（epoch 前进）
    cache.invalidate(&database_id);
    assert!(cache.get(&database_id).is_none());
    cache.upsert(entry(database_id, "worker-b", 835));

    let refreshed = cache.get(&database_id).expect("刷新后必须可路由");
    assert_eq!(refreshed.worker_id, WorkerId::new("worker-b"));
    assert_eq!(refreshed.owner_epoch, 835);
    assert!(cache.validate_epoch(&database_id, 835));

    // 陈旧信号不因刷新而清零（供告警 / 审计使用），但也不再增长
    assert_eq!(cache.stats().stale_detected, 1);
    assert_eq!(cache.stats().invalidations, 1);
}

#[test]
fn catalog_reconcile_is_the_correctness_path_after_outage() {
    let cache = RouteCache::new(16);
    let kept = DatabaseId::new_v7();
    let moved = DatabaseId::new_v7();

    cache.apply_catalog_snapshot(
        vec![entry(kept, "worker-a", 1), entry(moved, "worker-a", 1)],
        10,
    );

    // Control Plane 恢复后：catalog 版本前进，ownership 发生变化
    cache.apply_catalog_snapshot(vec![entry(moved, "worker-b", 2)], 11);

    assert_eq!(cache.catalog_version(), 11);
    assert!(
        cache.get(&kept).is_none(),
        "快照中已无 owner 的 DB 必须从缓存移除"
    );
    let moved_route = cache.get(&moved).expect("ownership 变更必须同步");
    assert_eq!(moved_route.worker_id, WorkerId::new("worker-b"));
    assert_eq!(moved_route.owner_epoch, 2);
}
