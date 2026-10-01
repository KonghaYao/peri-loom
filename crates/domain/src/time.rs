//! 时间工具：Unix 毫秒 <-> `chrono::DateTime<Utc>` <-> `std::time::Instant`。
//!
//! 平台内部协议统一使用 **Unix 毫秒**（proto `uint64 deadline_unix_ms` /
//! `created_at_unix_ms`），Catalog 使用 `TIMESTAMPTZ`，本地调度使用单调时钟 `Instant`。
//! 所有转换都必须**无 panic**：非法/越界输入退化为可判定的兜底值。

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

/// `deadline_from_unix_ms` 允许的最大提前量（1 年）。
///
/// u64 毫秒最大可表示约 5.8 亿年，直接加到 `Instant` 会溢出 panic；
/// 钳制到 1 年既保持“远未来”的语义，又保证平台无关的安全性。
const MAX_DEADLINE_HORIZON: Duration = Duration::from_secs(365 * 24 * 3600);

/// 当前 Unix 毫秒时间戳。
#[must_use]
pub fn now_unix_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Unix 毫秒 -> UTC 时间；越界值退化为 `DateTime::<Utc>::MIN_UTC`，不 panic。
#[must_use]
pub fn unix_ms_to_datetime(unix_ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(unix_ms).unwrap_or(DateTime::<Utc>::MIN_UTC)
}

/// UTC 时间 -> Unix 毫秒。
#[must_use]
pub fn datetime_to_unix_ms(value: DateTime<Utc>) -> i64 {
    value.timestamp_millis()
}

/// proto deadline（Unix 毫秒，`0` 表示无 deadline）-> 单调时钟截止点。
///
/// - `0` -> `None`（proto 约定：无 deadline，调用方不得据此无限等待业务逻辑）
/// - 已经过期 -> `Some(Instant::now())`，让 `timeout` 立即触发而不是永远等下去
/// - 越界远未来 -> 钳制到 1 年，避免 `Instant` 溢出 panic
#[must_use]
pub fn deadline_from_unix_ms(deadline_unix_ms: u64) -> Option<Instant> {
    if deadline_unix_ms == 0 {
        return None;
    }
    let now_ms = now_unix_ms();
    if now_ms < 0 {
        // 系统时钟早于 Unix 纪元：无法计算剩余量，按最保守的“立即到期”处理
        return Some(Instant::now());
    }
    let now_ms = now_ms as u64;
    if deadline_unix_ms <= now_ms {
        return Some(Instant::now());
    }
    let delta = Duration::from_millis(deadline_unix_ms - now_ms).min(MAX_DEADLINE_HORIZON);
    Some(
        Instant::now()
            .checked_add(delta)
            .unwrap_or_else(Instant::now),
    )
}

/// 距离 deadline 还剩多少毫秒（已过期为 0）。
#[must_use]
pub fn remaining_millis(deadline_unix_ms: u64) -> u64 {
    if deadline_unix_ms == 0 {
        return 0;
    }
    deadline_unix_ms.saturating_sub(now_unix_ms().max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_populated() {
        // 2020-01-01 之后、2100 之前，足以证明时钟可用
        let now = now_unix_ms();
        assert!(now > 1_577_836_800_000);
        assert!(now < 4_102_444_800_000);
    }

    #[test]
    fn datetime_round_trip() {
        let dt = unix_ms_to_datetime(1_700_000_000_123);
        assert_eq!(dt.timestamp_millis(), 1_700_000_000_123);
        assert_eq!(datetime_to_unix_ms(dt), 1_700_000_000_123);
        assert_eq!(unix_ms_to_datetime(0).timestamp_millis(), 0);
        // 负数（Unix 纪元之前）同样是合法时间点
        assert_eq!(unix_ms_to_datetime(-1000).timestamp_millis(), -1000);
    }

    #[test]
    fn out_of_range_ms_does_not_panic() {
        // i64::MAX 毫秒远超 chrono 可表示范围
        let dt = unix_ms_to_datetime(i64::MAX);
        assert_eq!(dt, DateTime::<Utc>::MIN_UTC);
        assert_eq!(unix_ms_to_datetime(i64::MIN), DateTime::<Utc>::MIN_UTC);
    }

    #[test]
    fn deadline_zero_means_none() {
        assert_eq!(deadline_from_unix_ms(0), None);
    }

    #[test]
    fn past_deadline_is_already_expired() {
        let past = (now_unix_ms() - 5_000).max(1) as u64;
        let deadline = deadline_from_unix_ms(past).expect("过期 deadline 仍应返回可判定值");
        assert!(deadline <= Instant::now(), "过期 deadline 必须立即到期");
        assert_eq!(remaining_millis(past), 0);
    }

    #[test]
    fn future_deadline_is_in_the_future() {
        let future = (now_unix_ms() + 60_000) as u64;
        let deadline = deadline_from_unix_ms(future).expect("未来 deadline");
        let now = Instant::now();
        assert!(deadline > now);
        assert!(deadline <= now + Duration::from_secs(61));
        let remaining = remaining_millis(future);
        assert!(
            remaining > 59_000 && remaining <= 60_000,
            "剩余 {remaining}ms"
        );
    }

    #[test]
    fn astronomically_far_deadline_is_clamped_not_panicking() {
        let deadline = deadline_from_unix_ms(u64::MAX).expect("远未来 deadline");
        assert!(deadline > Instant::now());
    }
}
