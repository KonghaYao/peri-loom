//! raft-rs 的日志桥：把 `slog` 记录转发进平台统一的 `tracing` 管线。
//!
//! 为什么需要这一层：raft-rs 只接受 `slog::Logger`（它内部用 slog 宏记录 term/index
//! 等关键状态），而平台冻结构建的是「tracing + tracing-subscriber」单一结构化日志管线
//! （架构 §17.10）。如果直接用 `RawNode::with_default_logger`，raft 的日志会另起一条
//! 输出路径（默认 logger 直接写 stderr、格式与字段名都和平台其它服务不一致），
//! 采集端就没法按同一套规则解析 raft 的选举/复制事件。
//!
//! 因此这里实现一个最小的 `slog::Drain`：
//! - 级别映射：slog 的五级一一对应 tracing 的同名宏；
//! - target 固定为 `raft`：便于用 `RUST_LOG=raft=debug` 单独打开 raft 诊断；
//! - 结构化 KV 先压成一行 `key=value ...` 再作为单个字段输出。这是有意的取舍：
//!   raft 的 KV 列表是运行时构造的（字段名不固定），逐字段展开需要 slog 的
//!   `OwnedKVList` 反射 API 才能拿到静态键名，收益不抵复杂度；压成一行文本
//!   仍然保留全部信息，且不会被 tracing 的字段名限制破坏语义。

use std::fmt;

use slog::{Drain, Key, Level, Record, KV};
use tracing::{debug, error, info, trace, warn};

/// 把 slog 的 KV 列表序列化成 `key=value` 文本。
#[derive(Default)]
struct KvText(String);

impl slog::Serializer for KvText {
    fn emit_arguments(&mut self, key: Key, value: &fmt::Arguments<'_>) -> slog::Result {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        // 键名直接来自 raft 的静态 KV（如 `index` / `term`），不需要转义。
        // `Key` 通过 Deref 到 `str`，这里显式解引用以避免调用到 `str::as_str`（未稳定）。
        // Key 通过 Deref 到 str；显式取 as_ref 避免解引用后又被自动重借用
        self.0.push_str(key.as_ref());
        self.0.push('=');
        self.0.push_str(&value.to_string());
        Ok(())
    }
}

/// 转发到 `tracing` 的 slog Drain。
///
/// 无状态：`slog::Logger` 会被 raft 内部长期持有并跨线程共享，Drain 必须是
/// `Send + Sync + 'static`，因此这里不保存任何可变状态。
pub struct TracingDrain;

impl Drain for TracingDrain {
    type Ok = ();
    /// 永不失败：日志写入不是业务路径，任何格式化问题都不应反向影响 raft。
    type Err = slog::Never;

    fn log(&self, record: &Record<'_>, values: &slog::OwnedKVList) -> Result<(), slog::Never> {
        // 序列化失败（自定义 KV 类型不支持）时退化为「无结构化字段」，
        // 日志主体仍然可读，绝不让 raft 因为一条日志而中断。
        let mut kv = KvText::default();
        let has_kv = values.serialize(record, &mut kv).is_ok() && !kv.0.is_empty();
        let message = record.msg();
        let kv_text = kv.0.as_str();

        // `message = %message` 而不是内联捕获：message 是 `fmt::Arguments`，
        // 显式字段写法在所有 tracing 版本上语义一致（内联捕获依赖版本行为）。
        match record.level() {
            Level::Critical | Level::Error if has_kv => {
                error!(target: "raft", kv = %kv_text, message = %message)
            }
            Level::Critical | Level::Error => error!(target: "raft", message = %message),
            Level::Warning if has_kv => {
                warn!(target: "raft", kv = %kv_text, message = %message)
            }
            Level::Warning => warn!(target: "raft", message = %message),
            Level::Info if has_kv => info!(target: "raft", kv = %kv_text, message = %message),
            Level::Info => info!(target: "raft", message = %message),
            Level::Debug if has_kv => debug!(target: "raft", kv = %kv_text, message = %message),
            Level::Debug => debug!(target: "raft", message = %message),
            Level::Trace if has_kv => trace!(target: "raft", kv = %kv_text, message = %message),
            Level::Trace => trace!(target: "raft", message = %message),
        }
        Ok(())
    }

    fn is_enabled(&self, level: Level) -> bool {
        // 先做一次粗过滤，避免为必然被 EnvFilter 丢弃的记录构造字符串。
        // 这里只按 tracing 的全局最大级别判断；EnvFilter 仍会做最终裁决。
        match level {
            Level::Critical | Level::Error => tracing::enabled!(tracing::Level::ERROR),
            Level::Warning => tracing::enabled!(tracing::Level::WARN),
            Level::Info => tracing::enabled!(tracing::Level::INFO),
            Level::Debug => tracing::enabled!(tracing::Level::DEBUG),
            Level::Trace => tracing::enabled!(tracing::Level::TRACE),
        }
    }
}

/// 构造 raft 专用 logger。
///
/// 固定 `raft.node_id` / `raft.shard_id` 两个 KV：一个进程只承载一个 WAL Shard
/// （见架构 §17.8 的 sharding 说明），把这两个值放在 logger 根上可以避免每条
/// raft 日志都要额外拼装上下文。
pub fn raft_logger(node_id: u64, shard_id: &str) -> slog::Logger {
    slog::Logger::root(
        TracingDrain,
        slog::o!(
            "raft.node_id" => node_id,
            "raft.shard_id" => shard_id.to_owned(),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    // emit_arguments 来自 slog::Serializer：测试直接验证格式化内容时需要该 trait
    use slog::Serializer as _;

    #[test]
    fn drain_accepts_records_without_panicking() {
        // Drain 必须对任意记录返回 Ok（Err 是 Never，不可达）
        let drain = TracingDrain;
        let logger = slog::Logger::root(drain, slog::o!("k" => 1u64));
        slog::info!(logger, "test message"; "index" => 7u64, "term" => 3u64);
        slog::debug!(logger, "debug message");
    }

    #[test]
    fn kv_text_serializes_static_keys() {
        let mut text = KvText::default();
        // 直接用 Serializer 接口验证格式化：这是 Drain 输出的核心内容
        text.emit_arguments(Key::from("index"), &format_args!("{}", 42u64))
            .unwrap();
        text.emit_arguments(Key::from("term"), &format_args!("{}", 7u64))
            .unwrap();
        assert_eq!(text.0, "index=42 term=7");
    }

    #[test]
    fn logger_root_includes_node_identity() {
        // 只为验证构造不 panic；KV 内容由 slog 自己持有
        let logger = raft_logger(3, "shard-0");
        slog::warn!(logger, "选举超时"; "node" => 3u64);
    }
}
