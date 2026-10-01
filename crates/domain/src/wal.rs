//! Ownership Epoch 与 WAL LSN（架构 §10 / §15.2）。
//!
//! `OwnerEpoch` 是单 DB 单写 Owner 的 fencing 核心：所有 Start / Write / Ownership
//! 动作都携带 epoch，Storage 与 Worker 拒绝任何**非最新** epoch 的写入。
//! 因此 epoch 必须严格单调，任何回退都等价于放行旧的写 Owner。

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Owner Epoch（单调递增，禁止回退）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct OwnerEpoch(u64);

impl OwnerEpoch {
    /// 初始 epoch：DB 从未有过 Owner。
    pub const ZERO: OwnerEpoch = OwnerEpoch(0);

    /// 由已知数值构造（从 Catalog 读回时使用）。
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// 数值。
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// 下一个 epoch。
    ///
    /// 到达 `u64::MAX` 后保持不回绕：回绕会让旧 Owner 的 epoch 重新“更新”，
    /// 破坏 fencing。真正耗尽应视为架构级故障，需人工介入。
    #[must_use]
    pub const fn next(self) -> OwnerEpoch {
        OwnerEpoch(self.0.saturating_add(1))
    }

    /// 下一个 epoch；已耗尽返回 `None`，便于上层显式告警而不是静默停滞。
    #[must_use]
    pub const fn checked_next(self) -> Option<OwnerEpoch> {
        match self.0.checked_add(1) {
            Some(value) => Some(OwnerEpoch(value)),
            None => None,
        }
    }

    /// 是否严格新于另一个 epoch（fencing 判定的唯一正确比较方式）。
    #[must_use]
    pub const fn is_newer_than(self, other: OwnerEpoch) -> bool {
        self.0 > other.0
    }

    /// 单调推进：仅当 `candidate` 更新时才前进，返回是否发生了推进。
    ///
    /// 用于“接收方本地 epoch 只能前进”的场景（拒绝回退更新）。
    pub fn advance_to(&mut self, candidate: OwnerEpoch) -> bool {
        if candidate.is_newer_than(*self) {
            self.0 = candidate.0;
            true
        } else {
            false
        }
    }

    /// 是否已耗尽（到达 `u64::MAX`）。
    #[must_use]
    pub const fn is_exhausted(self) -> bool {
        self.0 == u64::MAX
    }
}

impl fmt::Display for OwnerEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for OwnerEpoch {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.trim().parse()?))
    }
}

impl From<u64> for OwnerEpoch {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<OwnerEpoch> for u64 {
    fn from(value: OwnerEpoch) -> Self {
        value.0
    }
}

/// WAL Log Sequence Number（单调递增）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct Lsn(u64);

impl Lsn {
    /// 起始 LSN（日志为空）。
    pub const ZERO: Lsn = Lsn(0);

    /// 由已知数值构造。
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// 数值。
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// 饱和相加（用于偏移推进；溢出时停在 `u64::MAX` 而不是回绕）。
    #[must_use]
    pub const fn saturating_add(self, delta: u64) -> Lsn {
        Lsn(self.0.saturating_add(delta))
    }

    /// 检查相加是否溢出。
    #[must_use]
    pub const fn checked_add(self, delta: u64) -> Option<Lsn> {
        match self.0.checked_add(delta) {
            Some(value) => Some(Lsn(value)),
            None => None,
        }
    }

    /// 是否严格位于另一个 LSN 之前。
    #[must_use]
    pub const fn is_before(self, other: Lsn) -> bool {
        self.0 < other.0
    }

    /// 是否严格位于另一个 LSN 之后。
    #[must_use]
    pub const fn is_after(self, other: Lsn) -> bool {
        self.0 > other.0
    }

    /// 两点间距（`other` 早于 `self` 时返回 `self - other`）。
    #[must_use]
    pub const fn distance_from(self, other: Lsn) -> u64 {
        self.0.abs_diff(other.0)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Lsn {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.trim().parse()?))
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Lsn> for u64 {
    fn from(value: Lsn) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_next_is_monotonic() {
        let mut epoch = OwnerEpoch::ZERO;
        let mut seen = vec![epoch];
        for _ in 0..1000 {
            epoch = epoch.next();
            assert!(
                epoch.is_newer_than(*seen.last().unwrap()),
                "epoch 必须严格递增"
            );
            seen.push(epoch);
        }
        assert_eq!(epoch.get(), 1000);
    }

    #[test]
    fn epoch_never_goes_backwards() {
        assert!(!OwnerEpoch::new(5).is_newer_than(OwnerEpoch::new(5)));
        assert!(!OwnerEpoch::new(5).is_newer_than(OwnerEpoch::new(6)));

        let mut current = OwnerEpoch::new(834);
        // 旧的候选值不得推进本地 epoch（防 split-brain 回退）
        assert!(!current.advance_to(OwnerEpoch::new(834)));
        assert_eq!(current.get(), 834);
        assert!(!current.advance_to(OwnerEpoch::new(100)));
        assert_eq!(current.get(), 834);
        // 更新的候选值才推进
        assert!(current.advance_to(OwnerEpoch::new(835)));
        assert_eq!(current.get(), 835);
    }

    #[test]
    fn epoch_exhaustion_is_saturated_not_wrapped() {
        let max = OwnerEpoch::new(u64::MAX);
        assert_eq!(max.next(), max, "耗尽后不得回绕到 0");
        assert!(max.is_exhausted());
        assert_eq!(max.checked_next(), None);
        assert_eq!(OwnerEpoch::new(u64::MAX - 1).checked_next(), Some(max));
    }

    #[test]
    fn epoch_parse_and_serde() {
        assert_eq!("834".parse::<OwnerEpoch>().unwrap(), OwnerEpoch::new(834));
        assert!("abc".parse::<OwnerEpoch>().is_err());
        assert_eq!(OwnerEpoch::new(834).to_string(), "834");
        assert_eq!(serde_json::to_string(&OwnerEpoch::new(834)).unwrap(), "834");
        let back: OwnerEpoch = serde_json::from_str("835").unwrap();
        assert_eq!(back, OwnerEpoch::new(835));
    }

    #[test]
    fn lsn_arithmetic() {
        let lsn = Lsn::new(100);
        assert_eq!(lsn.saturating_add(50), Lsn::new(150));
        assert_eq!(lsn.checked_add(50), Some(Lsn::new(150)));
        assert_eq!(Lsn::new(u64::MAX).saturating_add(10), Lsn::new(u64::MAX));
        assert_eq!(Lsn::new(u64::MAX).checked_add(1), None);
        assert!(Lsn::new(1).is_before(lsn));
        assert!(lsn.is_after(Lsn::new(1)));
        assert_eq!(lsn.distance_from(Lsn::new(40)), 60);
        assert_eq!(lsn.distance_from(Lsn::new(140)), 40);
    }

    #[test]
    fn lsn_serde_and_display() {
        assert_eq!(Lsn::new(4096).to_string(), "4096");
        assert_eq!(serde_json::to_string(&Lsn::new(4096)).unwrap(), "4096");
        assert_eq!("4096".parse::<Lsn>().unwrap(), Lsn::new(4096));
    }
}
