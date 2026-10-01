//! 集成测试：架构 §9 / §15.5 冻结语义的端到端验证。
//!
//! 只使用 `scheduler` 的公共 API，模拟 Control Plane 的真实调用方式：
//! 从 Worker inventory 组装候选 -> 请求 placement -> 消费决策 / 错误码。

use domain::error::ErrorCode;
use domain::policy::{
    FailoverReserve, EMERGENCY, PACKING_TARGET_MAX, PACKING_TARGET_MIN, STOP_NEW_PLACEMENT,
};
use domain::{
    DatabaseId, ResourceBudget, WorkerCapacity, WorkerId, WorkerResourceUsage, WorkerState,
};
use scheduler::{AdmissionDecision, PlacementRequest, ScheduleError, Scheduler, WorkerCandidate};

/// 16000 MiB 内存的 Worker：让 memory 维成为唯一瓶颈，便于构造精确水位。
fn worker(id: &str, memory_used: u64) -> WorkerCandidate {
    let capacity = WorkerCapacity::new(64_000, 16_000, 4096, 102_400, 128, 20_000);
    WorkerCandidate {
        worker_id: WorkerId::new(id),
        state: WorkerState::Active,
        region: "cn-north-1".to_string(),
        zone: "cn-north-1a".to_string(),
        capacity,
        usage: WorkerResourceUsage::new(
            capacity,
            ResourceBudget::new(memory_used, memory_used, 100, 1_000, 4, 1_000),
        ),
        db_process_count: 4,
        respawning: false,
    }
}

/// 一个 DB 的资源画像：1 个进程位 + 指定的内存需求。
fn db(memory_mib: u64) -> ResourceBudget {
    ResourceBudget::new(100, memory_mib, 16, 64, 1, 20)
}

fn request(budget: ResourceBudget) -> PlacementRequest {
    PlacementRequest::new(DatabaseId::new_v7(), budget)
}

#[test]
fn 水位边界_80_90_取达到即生效() {
    let scheduler = Scheduler::new();

    // 放置后 0.79375 < 80%：放行
    assert_eq!(
        scheduler.admission_check(&worker("w1", 12_500), &db(200)),
        AdmissionDecision::Allow
    );
    // 放置后正好 0.80：停止新 placement
    let at_stop = scheduler.admission_check(&worker("w1", 12_600), &db(200));
    assert!(
        matches!(at_stop, AdmissionDecision::StopNewPlacement { .. }),
        "{at_stop:?}"
    );
    // 放置后正好 0.90：紧急保护
    let at_emergency = scheduler.admission_check(&worker("w1", 14_200), &db(200));
    assert!(at_emergency.is_emergency(), "{at_emergency:?}");

    // 与 domain 的冻结常量对齐（防止两处水位漂移）
    assert!((STOP_NEW_PLACEMENT - 0.80).abs() < f64::EPSILON);
    assert!((EMERGENCY - 0.90).abs() < f64::EPSILON);
}

#[test]
fn 打包目标区间_在三个候选中选中落在_070_075_的那个() {
    let scheduler = Scheduler::new();
    let cluster = vec![
        worker("w-low", 6_400),     // 放置后 0.4125
        worker("w-target", 11_200), // 放置后 0.7125 -> 目标区间
        worker("w-high", 12_000),   // 放置后 0.7625
    ];

    let decision = scheduler
        .select(&request(db(200)), &cluster)
        .expect("应选出候选");
    assert_eq!(decision.worker_id, WorkerId::new("w-target"));
    assert!(
        (PACKING_TARGET_MIN..=PACKING_TARGET_MAX).contains(&decision.utilization_after),
        "放置后利用率 {} 必须落在目标区间",
        decision.utilization_after
    );
}

#[test]
fn failover_reserve_不足时返回专用错误码() {
    let scheduler = Scheduler::new();
    let cluster = vec![worker("w1", 8_000), worker("w2", 8_000)];
    let mut req = request(db(200));
    // 两台各剩 6200 / 6400 MiB 的兜底能力，合计 12600
    req.reserved_for_failover_required = FailoverReserve::new(0, 12_601);

    let error = scheduler.select(&req, &cluster).expect_err("reserve 不足");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(matches!(
        error,
        ScheduleError::InsufficientFailoverReserve { .. }
    ));
    // 结构化错误体（HTTP 错误体可直接透出）
    let platform = error.to_platform_error();
    assert_eq!(platform.code.as_str(), "RESOURCE_EXHAUSTED");
    assert_eq!(
        platform
            .detail
            .and_then(|detail| detail["available_memory_mib"].as_u64()),
        Some(12_600)
    );
}

#[test]
fn anti_affinity_与_exclude_都生效() {
    let scheduler = Scheduler::new();
    let cluster = vec![worker("w-best", 11_200), worker("w-next", 10_000)];

    let mut req = request(db(200));
    req.anti_affinity_worker = Some(WorkerId::new("w-best"));
    assert_eq!(
        scheduler.select(&req, &cluster).unwrap().worker_id,
        WorkerId::new("w-next")
    );

    let mut req = request(db(200));
    req.exclude = vec![WorkerId::new("w-best"), WorkerId::new("w-next")];
    assert!(scheduler.select(&req, &cluster).is_err());
}

#[test]
fn emergency_时拒绝放置且绝不返回_90_以上的目标() {
    let scheduler = Scheduler::new();
    let cluster = vec![worker("w-hot", 15_200)]; // 0.95
    let error = scheduler
        .select(&request(db(100)), &cluster)
        .expect_err("紧急水位绝不放置");
    assert_eq!(error.code(), ErrorCode::AdmissionDenied);
    assert!(error.to_string().contains("紧急水位"), "{error}");
}

#[test]
fn worker_失效后的_failover_可以使用保留容量但不得进入_emergency() {
    let scheduler = Scheduler::new();

    // 失效接管目标：一台已经被压到 85% 的 Worker（普通 placement 早已停止）
    let busy = worker("w-busy", 13_600); // 放置 200 MiB 后 0.8625
    let cluster = vec![busy.clone()];

    let req = request(db(200));
    assert!(
        scheduler.select(&req, &cluster).is_err(),
        "普通 placement 不得越过 80% 停止线"
    );

    let decision = scheduler
        .select_for_failover(&req, &cluster)
        .expect("failover 允许使用被保留的容量");
    assert_eq!(decision.worker_id, WorkerId::new("w-busy"));
    assert!(decision.utilization_after < EMERGENCY);

    // 再往上一点就不行了：failover 同样不得进入 Emergency
    let too_hot = vec![worker("w-too-hot", 14_300)];
    assert!(scheduler.select_for_failover(&req, &too_hot).is_err());
}

#[test]
fn reserve_worker_只在_failover_路径使用() {
    let scheduler = Scheduler::new();
    let mut reserve_worker = worker("w-reserve", 1_600);
    reserve_worker.respawning = true;
    let cluster = vec![reserve_worker];

    let req = request(db(200));
    let error = scheduler
        .select(&req, &cluster)
        .expect_err("reserve 不参与普通 placement");
    assert_eq!(error.code(), ErrorCode::WorkerUnavailable);

    assert_eq!(
        scheduler
            .select_for_failover(&req, &cluster)
            .expect("reserve 正是给 failover 用的")
            .worker_id,
        WorkerId::new("w-reserve")
    );
}
