//! 由 [`domain::policy`] 冻结水位派生的常量。
//!
//! 水位数值本身只允许定义在 `domain::policy`（契约单一来源）；本模块只做
//! 「浮点水位 -> 整数百分比」的派生，供整数运算路径（failover 余量计算）使用，
//! 避免在 crate 内散布 `.90` / `90` 这类魔数。

use domain::policy::{EMERGENCY, PACKING_TARGET_MAX, PACKING_TARGET_MIN, STOP_NEW_PLACEMENT};

/// Packing 目标区间中点：`0.70 ~ 0.75` 的中心（0.725）。
///
/// 打分对「放置后利用率与该点的距离」做惩罚，使落在目标区间内的候选得分最高。
pub const PACKING_TARGET_MID: f64 = (PACKING_TARGET_MIN + PACKING_TARGET_MAX) / 2.0;

/// 紧急保护水位的整数百分比（90），用于整数余量计算。
pub const EMERGENCY_PERCENT: u64 = (EMERGENCY * 100.0) as u64;

/// 停止新 placement 水位的整数百分比（80），用于整数余量计算。
pub const STOP_NEW_PLACEMENT_PERCENT: u64 = (STOP_NEW_PLACEMENT * 100.0) as u64;

/// 越过目标区间上沿的额外惩罚权重。
///
/// 存在意义：单纯的「与 0.725 的距离」惩罚会让 0.90 的候选比 0.50 的候选得分更高，
/// 从而把 DB 往水位线上堆。乘上该权重后，越过 0.75 越远扣分越狠，
/// 让「略微欠打包」始终优于「贴着 80% 水位」。
pub(crate) const OVERSHOOT_WEIGHT: f64 = 5.0;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_constants_track_frozen_watermarks() {
        // 整数百分比必须与冻结的浮点水位一致，否则 failover 余量会与水位判定漂移。
        assert_eq!(EMERGENCY_PERCENT, (EMERGENCY * 100.0) as u64);
        assert_eq!(
            STOP_NEW_PLACEMENT_PERCENT,
            (STOP_NEW_PLACEMENT * 100.0) as u64
        );
        assert!((PACKING_TARGET_MID - 0.725).abs() < f64::EPSILON);
        // 中点必须严格落在区间内部（用 const 断言，编译期即可发现漂移）
        const { assert!(PACKING_TARGET_MID > PACKING_TARGET_MIN) };
        const { assert!(PACKING_TARGET_MID < PACKING_TARGET_MAX) };
    }
}
