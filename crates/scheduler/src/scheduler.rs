//! Placement 主流程：结构性筛选 → 集群级 reserve → 单机准入 → 打包打分。
//!
//! 每一步的拒绝原因都会被保留下来，最终拼进错误消息 —— placement 失败在生产上必须
//! 能直接回答「为什么没有 Worker 可用」，否则运维只能猜。

use domain::policy::FailoverReserve;
use domain::ResourceBudget;

use crate::admission::{self, Verdict};
use crate::candidate::WorkerCandidate;
use crate::error::ScheduleError;
use crate::placement::{AdmissionDecision, PlacementDecision, PlacementRequest};
use crate::score::packing_score;

/// placement 路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// 普通 placement：受 80% 停止线约束，且必须保留 failover reserve。
    Normal,
    /// failover placement：允许吃 80% ~ 90% 这段被保留的容量，但仍不得进入 Emergency。
    Failover,
}

impl Mode {
    const fn as_str(self) -> &'static str {
        match self {
            Mode::Normal => "NORMAL",
            Mode::Failover => "FAILOVER",
        }
    }

    /// 是否允许使用 failover reserve（越过 80% 停止线）。
    const fn may_use_reserve(self) -> bool {
        matches!(self, Mode::Failover)
    }

    /// 是否允许选择正在重启 / 被保留作 reserve 的 Worker。
    const fn may_use_reserved_worker(self) -> bool {
        matches!(self, Mode::Failover)
    }
}

/// Resource Budget Packing 调度器（无状态，可自由 clone / 共享）。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Scheduler;

impl Scheduler {
    /// 构造调度器。
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// 准入判定：该 Worker 现在能否再启动一个 `budget` 规模的 DB（§9 `CanStart` + 水位）。
    ///
    /// 预算会先做与 [`Scheduler::select`] 相同的归一化（`process_slots` 至少 1），
    /// 保证两条路径对同一份输入给出相同的结论。
    ///
    /// 注意：状态（SUSPECT / DRAINING / …）与 `respawning` 不属于本函数的职责 ——
    /// 它们是 placement 的**候选筛选**条件，见 [`Scheduler::select`]。
    #[must_use]
    pub fn admission_check(
        &self,
        worker: &WorkerCandidate,
        budget: &ResourceBudget,
    ) -> AdmissionDecision {
        admission::to_admission_decision(&admission::verdict(worker, &normalize_budget(budget)))
    }

    /// 普通 placement：满足准入、80% 停止线与 failover reserve。
    ///
    /// # Errors
    /// - [`ScheduleError::InvalidRequest`]：请求本身非法；
    /// - [`ScheduleError::NoEligibleWorker`]：状态 / 排除列表 / region / zone / anti-affinity 过滤后无候选；
    /// - [`ScheduleError::InsufficientFailoverReserve`]：没有任何候选能在放置后保住集群 reserve；
    /// - [`ScheduleError::AdmissionDenied`]：候选都在资源、DB 数或水位上被拒。
    pub fn select(
        &self,
        request: &PlacementRequest,
        candidates: &[WorkerCandidate],
    ) -> Result<PlacementDecision, ScheduleError> {
        self.place(request, candidates, Mode::Normal)
    }

    /// failover placement：Worker 失效后为 DB 重新找家（§11.2 / §12.2）。
    ///
    /// 与 [`Scheduler::select`] 的差异（冻结语义）：
    /// - **允许使用被保留的容量**（80% ~ 90% 区间）—— reserve 本来就是给 failover 用的，
    ///   因此不再要求「放置后仍保留 reserve」；
    /// - **允许选择被保留 / 正在重启的 Worker**（它们就是 reserve 的载体）；
    /// - **仍然绝不进入 Emergency**：放置后资源压力 >= 90% 一律拒绝。
    ///
    /// # Errors
    /// 与 [`Scheduler::select`] 相同，但不会返回
    /// [`ScheduleError::InsufficientFailoverReserve`]（该路径豁免 reserve 约束）。
    pub fn select_for_failover(
        &self,
        request: &PlacementRequest,
        candidates: &[WorkerCandidate],
    ) -> Result<PlacementDecision, ScheduleError> {
        self.place(request, candidates, Mode::Failover)
    }

    fn place(
        &self,
        request: &PlacementRequest,
        candidates: &[WorkerCandidate],
        mode: Mode,
    ) -> Result<PlacementDecision, ScheduleError> {
        validate_request(request)?;
        let budget = normalize_budget(&request.budget);

        // 1) 结构性筛选：状态 / exclude / anti-affinity / region / zone / respawning
        let mut rejected: Vec<String> = Vec::with_capacity(candidates.len());
        let mut eligible: Vec<&WorkerCandidate> = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            match structural_rejection(candidate, request, mode) {
                Some(reason) => rejected.push(format!("{}({reason})", candidate.worker_id)),
                None => eligible.push(candidate),
            }
        }
        if eligible.is_empty() {
            return Err(ScheduleError::no_eligible_worker(summarize(
                &rejected,
                candidates.len(),
            )));
        }

        // 2) 集群级 failover reserve：单机装得下 ≠ 集群放得下。
        //    先判 reserve 再判单机准入，是因为 reserve 是「集群还能不能兜底」的约束，
        //    比「这台机器能不能装」更靠前（failover 路径豁免，见 Mode）。
        let mut admissible_pool: Vec<&WorkerCandidate> = Vec::with_capacity(eligible.len());
        if mode == Mode::Normal && request.requires_failover_reserve() {
            let required = request.reserved_for_failover_required;
            let mut best = FailoverReserve::default();
            let mut reserve_rejected: Vec<String> = Vec::new();
            for candidate in &eligible {
                let available = self.cluster_failover_usable(candidates, candidate, &budget);
                best = FailoverReserve::new(
                    best.cpu_milli.max(available.cpu_milli),
                    best.memory_mib.max(available.memory_mib),
                );
                if available.cpu_milli >= required.cpu_milli
                    && available.memory_mib >= required.memory_mib
                {
                    admissible_pool.push(candidate);
                } else {
                    reserve_rejected.push(format!(
                        "{}(放置后集群 failover 余量 cpu={} memory={})",
                        candidate.worker_id, available.cpu_milli, available.memory_mib
                    ));
                }
            }
            if admissible_pool.is_empty() {
                return Err(ScheduleError::InsufficientFailoverReserve {
                    required_cpu_milli: required.cpu_milli,
                    required_memory_mib: required.memory_mib,
                    available_cpu_milli: best.cpu_milli,
                    available_memory_mib: best.memory_mib,
                    reason: summarize(&reserve_rejected, eligible.len()),
                });
            }
        } else {
            admissible_pool = eligible;
        }

        // 3) 单机准入 + 打包打分
        let mut ranked: Vec<Ranked> = Vec::with_capacity(admissible_pool.len());
        let mut admission_rejected: Vec<String> = Vec::new();
        for candidate in admissible_pool {
            let verdict = admission::verdict(candidate, &budget);
            if !allows(&verdict, mode) {
                admission_rejected.push(format!(
                    "{}({})",
                    candidate.worker_id,
                    verdict.reason().unwrap_or_else(|| "拒绝".to_string())
                ));
                continue;
            }
            let pressure_after = candidate.pressure_after(&budget);
            ranked.push(Ranked {
                score: packing_score(pressure_after),
                pressure_after,
                candidate,
            });
        }
        if ranked.is_empty() {
            return Err(ScheduleError::admission_denied(summarize(
                &admission_rejected,
                candidates.len(),
            )));
        }

        // 4) 排序取最优：先比打包得分，再比放置后压力（越低越留有余量），最后用
        //    worker_id 兜底保证**确定性**（同样的输入必须给出同样的决策）。
        ranked.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then(a.pressure_after.total_cmp(&b.pressure_after))
                .then(a.candidate.worker_id.cmp(&b.candidate.worker_id))
        });

        // 亲和 Worker 是最高优先级的软偏好：可用就必须选它；不可用则退化为普通打分。
        let affinity = request.affinity_worker.as_ref();
        let chosen = affinity
            .and_then(|id| ranked.iter().find(|entry| &entry.candidate.worker_id == id))
            .unwrap_or(&ranked[0]);
        let affinity_hit = affinity.is_some_and(|id| &chosen.candidate.worker_id == id);

        let available = self.cluster_failover_usable(candidates, chosen.candidate, &budget);
        let reason = format!(
            "mode={}；priority={}；放置后资源压力 {:.4}（打包评分 {:.4}）；状态 {}；\
             集群 failover 余量 cpu={} memory={}；候选 {} 个{}",
            mode.as_str(),
            request.priority,
            chosen.pressure_after,
            chosen.score,
            chosen.candidate.state,
            available.cpu_milli,
            available.memory_mib,
            ranked.len(),
            if affinity_hit {
                "；命中 affinity"
            } else {
                ""
            },
        );

        Ok(PlacementDecision {
            worker_id: chosen.candidate.worker_id.clone(),
            score: chosen.score,
            utilization_after: chosen.pressure_after,
            reason,
        })
    }

    /// 集群 failover 可用容量：所有 `ACTIVE` 候选的 failover 余量之和，
    /// 其中 `target` 按「放置本次 DB 之后」的余量计算。
    ///
    /// 只统计 `ACTIVE`：SUSPECT / DRAINING / UNAVAILABLE 的 Worker 自身难保，
    /// 不能算作兜底能力。被保留 / 重启中的 Worker **计入** —— 它们就是 reserve 的载体。
    fn cluster_failover_usable(
        &self,
        candidates: &[WorkerCandidate],
        target: &WorkerCandidate,
        budget: &ResourceBudget,
    ) -> FailoverReserve {
        let mut total = FailoverReserve::default();
        for candidate in candidates {
            if !candidate.serves_failover() {
                continue;
            }
            let usable = if candidate.worker_id == target.worker_id {
                candidate.failover_usable_after(budget)
            } else {
                candidate.failover_usable()
            };
            total.cpu_milli = total.cpu_milli.saturating_add(usable.cpu_milli);
            total.memory_mib = total.memory_mib.saturating_add(usable.memory_mib);
        }
        total
    }
}

/// 候选的排序条目。
struct Ranked<'a> {
    score: f64,
    pressure_after: f64,
    candidate: &'a WorkerCandidate,
}

/// 该判定在给定路径下是否放行。
fn allows(verdict: &Verdict, mode: Mode) -> bool {
    match verdict {
        // failover 允许吃被保留的容量（80% ~ 90%），这正是 reserve 的用途
        Verdict::Waterline { .. } => mode.may_use_reserve(),
        other => other.allows(),
    }
}

/// 结构性筛选：返回拒绝原因（`None` 表示可继续参与 placement）。
fn structural_rejection(
    candidate: &WorkerCandidate,
    request: &PlacementRequest,
    mode: Mode,
) -> Option<String> {
    // 只有 ACTIVE 能作为目标：SUSPECT（可能已死）/ DRAINING（正在迁出）/ EMPTY / UNAVAILABLE
    if !candidate.serves_failover() {
        return Some(format!("状态 {} 不可作为 placement 目标", candidate.state));
    }
    if request.is_excluded(&candidate.worker_id) {
        return Some("在 exclude 列表中".to_string());
    }
    if request.anti_affinity_worker.as_ref() == Some(&candidate.worker_id) {
        return Some("命中 anti-affinity".to_string());
    }
    if let Some(region) = &request.region {
        if &candidate.region != region {
            return Some(format!("region {} 与请求 {region} 不符", candidate.region));
        }
    }
    if let Some(zone) = &request.zone {
        if &candidate.zone != zone {
            return Some(format!("zone {} 与请求 {zone} 不符", candidate.zone));
        }
    }
    if !mode.may_use_reserved_worker() && candidate.respawning {
        return Some("正在重启 / 保留作 failover reserve，不参与普通 placement".to_string());
    }
    None
}

/// 请求自检：明显的调用方 bug 直接拒绝，不要浪费一轮筛选。
fn validate_request(request: &PlacementRequest) -> Result<(), ScheduleError> {
    if request.budget.is_zero() {
        return Err(ScheduleError::invalid_request(
            "DB 资源预算全为 0：没有任何维度可以评估 placement",
        ));
    }
    if request.affinity_worker.is_some() && request.affinity_worker == request.anti_affinity_worker
    {
        return Err(ScheduleError::invalid_request(
            "affinity_worker 与 anti_affinity_worker 指向同一个 Worker",
        ));
    }
    Ok(())
}

/// 一个 DB = 一个进程（架构 §1.2）：预算里的 `process_slots` 至少为 1。
///
/// 调用方漏填时按 1 记账，避免用 0 绕过 DB count 的 hard safety limit。
fn normalize_budget(budget: &ResourceBudget) -> ResourceBudget {
    let mut normalized = *budget;
    normalized.process_slots = normalized.process_slots.max(1);
    normalized
}

/// 把拒绝原因压缩成一行（最多列 3 条，避免错误消息随集群规模爆炸）。
fn summarize(reasons: &[String], total: usize) -> String {
    if reasons.is_empty() {
        return format!("候选总数 {total}");
    }
    let head = reasons
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    format!("{head}（共 {total} 个候选）")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{db_budget, memory_candidate};
    use domain::{DatabaseId, WorkerId, WorkerState};
    use proptest::prelude::*;

    fn request(db_budget: ResourceBudget) -> PlacementRequest {
        PlacementRequest::new(DatabaseId::new_v7(), db_budget)
    }

    fn worker_ids(ids: &[&str]) -> Vec<WorkerId> {
        ids.iter().map(|id| WorkerId::new(*id)).collect()
    }

    #[test]
    fn packing_lands_inside_the_target_window() {
        // 三个候选只有「放置后落在 0.70 ~ 0.75」的那个应该被选中
        let scheduler = Scheduler::new();
        let cluster = vec![
            memory_candidate("w-under", 8_000),   // -> 0.5125
            memory_candidate("w-target", 11_200), // -> 0.7125
            memory_candidate("w-high", 12_000),   // -> 0.7625（越过区间但未到 80%）
        ];
        let decision = scheduler
            .select(&request(db_budget(200)), &cluster)
            .expect("应能选出候选");
        assert_eq!(decision.worker_id, WorkerId::new("w-target"));
        assert!(
            (0.70..=0.75).contains(&decision.utilization_after),
            "放置后必须落在目标区间，实际 {}",
            decision.utilization_after
        );
        assert!((decision.utilization_after - 0.7125).abs() < 1e-12);
        assert!(decision.score > 0.98, "目标中点附近应接近满分");
        assert!(decision.reason.contains("mode=NORMAL"));
    }

    #[test]
    fn packing_prefers_undershoot_over_crowding_the_watermark() {
        let scheduler = Scheduler::new();
        // 两座同等「距离」的候选：78%（上行）应输给 67%（下行）
        let cluster = vec![
            memory_candidate("w-crowded", 12_280), // +200 -> 0.78
            memory_candidate("w-roomy", 10_520),   // +200 -> 0.67
        ];
        let decision = scheduler
            .select(&request(db_budget(200)), &cluster)
            .expect("应能选出候选");
        assert_eq!(decision.worker_id, WorkerId::new("w-roomy"));
    }

    #[test]
    fn waterline_boundaries_hold_at_select_level() {
        let scheduler = Scheduler::new();

        // 放置后 0.79375：允许
        let ok = vec![memory_candidate("w1", 12_500)];
        assert!(scheduler.select(&request(db_budget(200)), &ok).is_ok());

        // 放置后正好 0.80：停止新 placement
        let stopped = vec![memory_candidate("w1", 12_600)];
        let error = scheduler
            .select(&request(db_budget(200)), &stopped)
            .expect_err("80% 必须停止新 placement");
        assert_eq!(error.code(), domain::error::ErrorCode::AdmissionDenied);
        assert!(error.to_string().contains("停止线"), "{error}");

        // 放置后正好 0.90：紧急保护
        let emergency = vec![memory_candidate("w1", 14_200)];
        let error = scheduler
            .select(&request(db_budget(200)), &emergency)
            .expect_err("90% 必须紧急保护");
        assert!(error.to_string().contains("紧急水位"), "{error}");
    }

    #[test]
    fn emergency_worker_is_never_selected_even_if_it_is_the_only_option() {
        let scheduler = Scheduler::new();
        let cluster = vec![memory_candidate("w-hot", 15_000)];
        let error = scheduler
            .select(&request(db_budget(100)), &cluster)
            .expect_err("紧急水位绝不放置");
        assert_eq!(error.code(), domain::error::ErrorCode::AdmissionDenied);
        let message = error.to_string();
        assert!(message.contains("w-hot"), "{message}");
        assert!(message.contains("紧急水位"), "{message}");
    }

    #[test]
    fn insufficient_failover_reserve_is_reported_as_such() {
        let scheduler = Scheduler::new();
        // 两台 16000 MiB 的 Worker，各用 8000：单机放置完全 OK，但集群兜底能力有限
        let cluster = vec![memory_candidate("w1", 8_000), memory_candidate("w2", 8_000)];
        let mut req = request(db_budget(200));
        // 放置后每台余量 14400-8200=6200 / 14400-8000=6400，合计 12600
        req.reserved_for_failover_required = FailoverReserve::new(0, 12_601);

        let error = scheduler
            .select(&req, &cluster)
            .expect_err("reserve 不足必须拒绝 placement");
        match error {
            ScheduleError::InsufficientFailoverReserve {
                required_memory_mib,
                available_memory_mib,
                ..
            } => {
                assert_eq!(required_memory_mib, 12_601);
                assert_eq!(available_memory_mib, 12_600);
            }
            other => panic!("期望 InsufficientFailoverReserve，实际 {other:?}"),
        }
    }

    #[test]
    fn failover_reserve_boundary_is_inclusive() {
        let scheduler = Scheduler::new();
        let cluster = vec![memory_candidate("w1", 8_000), memory_candidate("w2", 8_000)];
        let mut req = request(db_budget(200));
        req.reserved_for_failover_required = FailoverReserve::new(0, 12_600);
        assert!(
            scheduler.select(&req, &cluster).is_ok(),
            "恰好等于要求量应当放行（不得低于，而不是必须高于）"
        );
    }

    #[test]
    fn failover_may_use_the_reserved_capacity() {
        let scheduler = Scheduler::new();
        let cluster = vec![memory_candidate("w1", 8_000), memory_candidate("w2", 8_000)];
        let mut req = request(db_budget(200));
        req.reserved_for_failover_required = FailoverReserve::new(0, 100_000);

        assert!(scheduler.select(&req, &cluster).is_err());
        let decision = scheduler
            .select_for_failover(&req, &cluster)
            .expect("failover 允许使用被保留的容量");
        assert!(decision.reason.contains("mode=FAILOVER"));
    }

    #[test]
    fn failover_may_place_into_the_eighty_to_ninety_band() {
        let scheduler = Scheduler::new();
        // 放置后 0.87：普通 placement 拒绝，failover 允许
        let cluster = vec![memory_candidate("w1", 13_720)];
        let req = request(db_budget(200));
        assert!(scheduler.select(&req, &cluster).is_err());
        let decision = scheduler
            .select_for_failover(&req, &cluster)
            .expect("failover 可以使用 reserve 区间");
        assert!((decision.utilization_after - 0.87).abs() < 1e-12);
    }

    #[test]
    fn failover_still_refuses_emergency() {
        let scheduler = Scheduler::new();
        let cluster = vec![memory_candidate("w1", 14_300)]; // +200 -> 0.90625
        let error = scheduler
            .select_for_failover(&request(db_budget(200)), &cluster)
            .expect_err("failover 也不得进入 Emergency");
        assert!(error.to_string().contains("紧急水位"), "{error}");
    }

    #[test]
    fn failover_can_use_a_worker_reserved_for_the_reserve() {
        let scheduler = Scheduler::new();
        let mut reserved = memory_candidate("w-reserve", 1_600);
        reserved.respawning = true;
        let cluster = vec![reserved];

        let req = request(db_budget(200));
        // 普通 placement：reserve Worker 不参与
        let error = scheduler
            .select(&req, &cluster)
            .expect_err("reserve Worker 不做普通 placement");
        assert_eq!(error.code(), domain::error::ErrorCode::WorkerUnavailable);
        assert!(error.to_string().contains("reserve"), "{error}");

        // failover：正是它该出场的时候
        let decision = scheduler
            .select_for_failover(&req, &cluster)
            .expect("failover 应使用 reserve Worker");
        assert_eq!(decision.worker_id, WorkerId::new("w-reserve"));
    }

    #[test]
    fn anti_affinity_and_exclude_are_hard_filters() {
        let scheduler = Scheduler::new();
        let cluster = vec![
            memory_candidate("w-best", 11_200), // 打包最优
            memory_candidate("w-second", 10_000),
        ];
        let mut req = request(db_budget(200));
        req.anti_affinity_worker = Some(WorkerId::new("w-best"));
        let decision = scheduler.select(&req, &cluster).expect("仍有次优候选");
        assert_eq!(decision.worker_id, WorkerId::new("w-second"));
        assert_ne!(decision.worker_id, WorkerId::new("w-best"));

        // exclude 列表同样生效，且优先级不输 anti-affinity
        let mut req = request(db_budget(200));
        req.exclude = worker_ids(&["w-best"]);
        let decision = scheduler.select(&req, &cluster).expect("仍有次优候选");
        assert_eq!(decision.worker_id, WorkerId::new("w-second"));

        // 全部被排除：报 NoEligibleWorker 而不是随便挑一个
        let mut req = request(db_budget(200));
        req.exclude = worker_ids(&["w-best", "w-second"]);
        let error = scheduler.select(&req, &cluster).expect_err("全部排除");
        assert_eq!(error.code(), domain::error::ErrorCode::WorkerUnavailable);
        assert!(error.to_string().contains("exclude"), "{error}");
    }

    #[test]
    fn state_filter_excludes_non_active_workers() {
        let scheduler = Scheduler::new();
        for state in [
            WorkerState::Suspect,
            WorkerState::Draining,
            WorkerState::Empty,
            WorkerState::Unavailable,
        ] {
            let mut worker = memory_candidate("w1", 1_600);
            worker.state = state;
            let error = scheduler
                .select(&request(db_budget(200)), &[worker.clone()])
                .expect_err("非 ACTIVE 不得接收新 DB");
            assert!(error.to_string().contains(&state.to_string()), "{error}");

            // failover 同样不用不健康的 Worker（否则等于把 DB 送上第二次故障）
            assert!(scheduler
                .select_for_failover(&request(db_budget(200)), &[worker])
                .is_err());
        }
    }

    #[test]
    fn region_and_zone_are_hard_constraints() {
        let scheduler = Scheduler::new();
        let mut other_region = memory_candidate("w-other", 11_200);
        other_region.region = "r2".to_string();
        other_region.zone = "z9".to_string();
        let cluster = vec![memory_candidate("w-r1z1", 10_000), other_region];

        let mut req = request(db_budget(200));
        req.region = Some("r1".to_string());
        assert_eq!(
            scheduler.select(&req, &cluster).unwrap().worker_id,
            WorkerId::new("w-r1z1")
        );

        let mut req = request(db_budget(200));
        req.zone = Some("z9".to_string());
        assert_eq!(
            scheduler.select(&req, &cluster).unwrap().worker_id,
            WorkerId::new("w-other")
        );

        // 约束无法满足时明确失败，不做跨区兜底
        let mut req = request(db_budget(200));
        req.region = Some("r3".to_string());
        let error = scheduler
            .select(&req, &cluster)
            .expect_err("region 无法满足");
        assert!(error.to_string().contains("region"), "{error}");
    }

    #[test]
    fn affinity_wins_when_available_and_degrades_otherwise() {
        let scheduler = Scheduler::new();
        let cluster = vec![
            memory_candidate("w-packed", 11_200), // 打包最优
            memory_candidate("w-affinity", 9_000),
        ];

        let mut req = request(db_budget(200));
        req.affinity_worker = Some(WorkerId::new("w-affinity"));
        let decision = scheduler.select(&req, &cluster).expect("亲和候选可用");
        assert_eq!(decision.worker_id, WorkerId::new("w-affinity"));
        assert!(decision.reason.contains("affinity"), "{}", decision.reason);

        // 亲和候选不可用（在 exclude 里）时退化为普通打分
        let mut req = request(db_budget(200));
        req.affinity_worker = Some(WorkerId::new("w-affinity"));
        req.exclude = worker_ids(&["w-affinity"]);
        let decision = scheduler.select(&req, &cluster).expect("退化到普通打分");
        assert_eq!(decision.worker_id, WorkerId::new("w-packed"));
        assert!(!decision.reason.contains("命中 affinity"));
    }

    #[test]
    fn db_count_hard_limit_blocks_placement() {
        let scheduler = Scheduler::new();
        let mut full = memory_candidate("w-full", 1_600);
        full.capacity.process_slots = 4;
        full.usage.db_process_limit = 4;
        full.db_process_count = 4;
        full.usage.db_process_count = 4;

        let error = scheduler
            .select(&request(db_budget(200)), &[full])
            .expect_err("DB 数达 hard limit 必须拒绝");
        assert!(error.to_string().contains("hard safety limit"), "{error}");
    }

    #[test]
    fn request_validation_rejects_obvious_mistakes() {
        let scheduler = Scheduler::new();
        let cluster = vec![memory_candidate("w1", 1_600)];

        let error = scheduler
            .select(&request(ResourceBudget::ZERO), &cluster)
            .expect_err("零预算非法");
        assert_eq!(error.code(), domain::error::ErrorCode::InvalidArgument);

        let mut req = request(db_budget(200));
        req.affinity_worker = Some(WorkerId::new("w1"));
        req.anti_affinity_worker = Some(WorkerId::new("w1"));
        let error = scheduler.select(&req, &cluster).expect_err("自相矛盾");
        assert_eq!(error.code(), domain::error::ErrorCode::InvalidArgument);
    }

    #[test]
    fn missing_process_slots_are_counted_as_one() {
        let scheduler = Scheduler::new();
        let cluster = vec![memory_candidate("w1", 1_600)];
        let mut budget = db_budget(200);
        budget.process_slots = 0;
        let decision = scheduler
            .select(&request(budget), &cluster)
            .expect("漏填 process_slots 不应让 placement 失败");
        assert_eq!(decision.worker_id, WorkerId::new("w1"));
    }

    #[test]
    fn decision_is_deterministic_and_order_independent() {
        let scheduler = Scheduler::new();
        let forward = vec![
            memory_candidate("w-a", 8_000),
            memory_candidate("w-b", 8_000),
            memory_candidate("w-c", 8_000),
        ];
        let mut reversed = forward.clone();
        reversed.reverse();

        let req = request(db_budget(200));
        let first = scheduler.select(&req, &forward).unwrap();
        let second = scheduler.select(&req, &reversed).unwrap();
        assert_eq!(first, second, "候选顺序不得影响决策");
        // 三者评分完全相同时按 worker_id 兜底，结果稳定为字典序最小者
        assert_eq!(first.worker_id, WorkerId::new("w-a"));
    }

    #[test]
    fn empty_cluster_reports_no_eligible_worker() {
        let scheduler = Scheduler::new();
        let error = scheduler
            .select(&request(db_budget(200)), &[])
            .expect_err("空集群必须失败");
        assert_eq!(error.code(), domain::error::ErrorCode::WorkerUnavailable);
        assert!(error.to_string().contains("候选总数 0"), "{error}");
    }

    #[test]
    fn cluster_usable_counts_only_active_workers() {
        let scheduler = Scheduler::new();
        let mut suspect = memory_candidate("w-suspect", 1_000);
        suspect.state = WorkerState::Suspect;
        let active = memory_candidate("w-active", 8_000);
        let usable = scheduler.cluster_failover_usable(
            &[suspect, active.clone()],
            &active,
            &ResourceBudget::ZERO,
        );
        // 只剩一台 ACTIVE（16000*0.9=14400，用掉 8000）
        assert_eq!(usable.memory_mib, 6_400);
        assert_eq!(active.used().memory_mib, 8_000);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// 无论输入如何，返回的决策必须是结构性合法的目标，且绝不越过 Emergency。
        #[test]
        fn returned_decision_is_always_safe(
            usages in proptest::collection::vec(0u64..15_500, 1..6),
            excluded_mask in proptest::collection::vec(any::<bool>(), 1..6),
            needed in 1u64..2_000,
        ) {
            let scheduler = Scheduler::new();
            let cluster: Vec<WorkerCandidate> = usages
                .iter()
                .enumerate()
                .map(|(index, used)| memory_candidate(&format!("w{index}"), *used))
                .collect();
            let mut req = request(db_budget(needed));
            req.exclude = cluster
                .iter()
                .zip(excluded_mask.iter().cycle())
                .filter(|(_, excluded)| **excluded)
                .map(|(candidate, _)| candidate.worker_id.clone())
                .collect();
            req.anti_affinity_worker = cluster.first().map(|c| c.worker_id.clone());
            req.reserved_for_failover_required = FailoverReserve::new(0, 3_000);

            if let Ok(decision) = scheduler.select(&req, &cluster) {
                let chosen = cluster
                    .iter()
                    .find(|c| c.worker_id == decision.worker_id)
                    .expect("选中者必须来自候选集合");
                prop_assert!(chosen.serves_normal_placement());
                prop_assert!(!req.is_excluded(&decision.worker_id));
                prop_assert_ne!(Some(decision.worker_id.clone()), req.anti_affinity_worker.clone());
                prop_assert!(decision.utilization_after < 0.90);
                prop_assert!(decision.score >= 0.0 && decision.score <= 1.0);

                let after = scheduler.cluster_failover_usable(&cluster, chosen, &normalize_budget(&req.budget));
                prop_assert!(after.memory_mib >= req.reserved_for_failover_required.memory_mib);
            }

            // failover 只要成功，就必然没有越过 90%
            if let Ok(decision) = scheduler.select_for_failover(&req, &cluster) {
                prop_assert!(decision.utilization_after < 0.90);
            }
        }
    }
}
