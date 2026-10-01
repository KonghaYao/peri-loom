//! 打包评分。
//!
//! 目标（架构 §9 / §15.5）：在满足 SLO 的前提下**尽可能减少 Worker 数量**。
//! 因此打分不是「越空越好」，而是「放置后尽量落在 70% ~ 75% 目标区间」：
//!
//! ```text
//! 目标区间 0.70 ~ 0.75  ->  得分最高
//! 欠打包（< 0.70）      ->  距离 0.725 越远越低
//! 越过 0.75            ->  额外惩罚（越接近 80% / 90% 越危险）
//! ```

use domain::policy::{PACKING_TARGET_MAX, PACKING_TARGET_MIN};

use crate::watermark::{OVERSHOOT_WEIGHT, PACKING_TARGET_MID};

/// 对「放置后最大利用率」打分，取值 `0.0 ~ 1.0`。
///
/// - 距离目标中点 0.725 越近得分越高（0.725 处为 1.0）；
/// - 越过目标区间上沿 0.75 后额外按固定权重扣分（见 `watermark::OVERSHOOT_WEIGHT`），
///   保证「略微欠打包」始终优于「贴着 80% 水位」—— 水位线附近没有腾挪空间，
///   一旦突发流量就会触发 StopNewPlacement / Emergency。
/// - 非法输入（NaN / Inf）得 0 分：宁可退化为不确定选择，也不让 NaN 参与比较。
#[must_use]
pub fn packing_score(utilization_after: f64) -> f64 {
    if !utilization_after.is_finite() {
        return 0.0;
    }
    let distance = (utilization_after - PACKING_TARGET_MID).abs();
    let base = 1.0 - (distance / PACKING_TARGET_MID).min(1.0);
    let overshoot = (utilization_after - PACKING_TARGET_MAX).max(0.0);
    (base - overshoot * OVERSHOOT_WEIGHT).clamp(0.0, 1.0)
}

/// 利用率是否正好落在目标区间内。
#[must_use]
pub fn hits_packing_target(utilization_after: f64) -> bool {
    (PACKING_TARGET_MIN..=PACKING_TARGET_MAX).contains(&utilization_after)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_midpoint_scores_highest() {
        let best = packing_score(PACKING_TARGET_MID);
        assert!((best - 1.0).abs() < 1e-12);
        for value in [0.0, 0.3, 0.5, 0.69, 0.70, 0.75, 0.76, 0.80, 0.90, 1.2] {
            assert!(
                packing_score(value) < best,
                "{value} 不应超过目标中点 0.725 的得分"
            );
        }
    }

    #[test]
    fn inside_the_target_window_beats_outside() {
        for inside in [0.70, 0.72, 0.725, 0.74, 0.75] {
            for outside in [0.60, 0.65, 0.69, 0.76, 0.79] {
                assert!(
                    packing_score(inside) > packing_score(outside),
                    "目标区间内的 {inside} 必须优于区间外的 {outside}"
                );
            }
        }
    }

    #[test]
    fn undershooting_beats_crowding_the_watermark() {
        // 越过 0.75 的额外惩罚：宁可少装一点，也不要贴着 80% / 90%
        let slightly_under = packing_score(0.70 - 0.06); // 0.64
        let slightly_over = packing_score(0.75 + 0.06); // 0.81
        assert!(
            slightly_under > slightly_over,
            "欠打包 {slightly_under} 应优于贴水位 {slightly_over}"
        );
        assert!(packing_score(0.88) < packing_score(0.40));
    }

    #[test]
    fn score_is_bounded_and_defensive() {
        for value in [-1.0, 0.0, 0.725, 0.95, 1.0, 5.0, f64::NAN, f64::INFINITY] {
            let score = packing_score(value);
            assert!((0.0..=1.0).contains(&score), "{value} -> {score} 越界");
            assert!(score.is_finite());
        }
        assert_eq!(packing_score(f64::NAN), 0.0);
    }

    #[test]
    fn target_window_predicate_matches_domain_policy() {
        assert!(hits_packing_target(PACKING_TARGET_MIN));
        assert!(hits_packing_target(PACKING_TARGET_MAX));
        assert!(!hits_packing_target(PACKING_TARGET_MAX + 0.001));
        assert!(!hits_packing_target(PACKING_TARGET_MIN - 0.001));
    }
}
