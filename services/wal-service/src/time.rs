//! 时间工具：墙上时钟（Unix 毫秒）的唯一来源。
//!
//! 为什么单独成模块：proto 契约里的 `deadline_unix_ms` 是**绝对**时间戳（跨进程必须
//! 用绝对时间，节点之间时钟不同源），而进程内的等待必须用 tokio 的单调时钟
//! （墙上时钟被 NTP 回拨时不能让超时永不触发）。两种语义在代码里各自出现，
//! 必须只有一处负责把「现在」翻译成 Unix 毫秒，避免各处自行调用 `SystemTime::now()`
//! 造成语义漂移。

use std::time::{SystemTime, UNIX_EPOCH};

/// 当前 Unix 毫秒。
///
/// 系统时钟早于 1970（异常/容器时钟未同步）时返回 0，而不是让它回绕成一个巨大值：
/// 巨大值会让客户端 deadline 判据全部失效（等价于「永不超时」）。
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_plausible_unix_ms() {
        let now = now_unix_ms();
        // 2020-01-01 之后、2100 之前：只要时钟没坏就必然成立
        assert!(now > 1_577_836_800_000, "Unix 毫秒应远大于 2020 年");
        assert!(now < 4_102_444_800_000, "Unix 毫秒应远小于 2100 年");
    }

    #[test]
    fn now_is_monotonic_enough_for_deadline_math() {
        // 墙上时钟允许微小平移，但必须能支撑「deadline - now」的 saturating 减法
        let first = now_unix_ms();
        let second = now_unix_ms();
        assert!(
            second >= first,
            "两次读取不应倒退（倒退由调用方 saturating 兜底）"
        );
    }
}
