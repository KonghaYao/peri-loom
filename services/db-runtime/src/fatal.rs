//! durable IO fail-stop 的**进程退出**决策（架构 §11.1 / §12.1）。
//!
//! 问题：`PlatformDurableIO` 一旦判定本地 WAL 字节流不可信就会 fail-stop，此后本进程的
//! 每一次 WAL 写入都直接失败——进程活着，却再也写不进去。对外表现为一个"看似可用但不能写"
//! 的库（Server 一直回 `STORAGE_UNAVAILABLE`），而架构 §12.1 给这条路径的正确答案是
//! **DB Process 退出**，由 Worker 的崩溃检测 + 自动重启拉一个从 Remote WAL 重新建立
//! 一致状态的干净进程（本地 WAL 的解析游标与 durable 记账由新进程重新推导）。
//!
//! 为什么不是"就地恢复"：本地 WAL 的字节级对应关系已经不可信，本进程没有第二条播种机会
//! （见 `engine_adapter::PlatformDurableIO::seed_wal_stream` 的文档），继续运行不可能
//! 重建一致性。
//!
//! 本模块只做两件事：**判定**（`decide_exit`，纯函数，可单测）与**应用**（轮询 + 退出）。
//! 判定必须精确：误判会带来无谓的重启抖动，漏判则退回本文开头描述的僵尸状态。

use std::sync::Arc;
use std::time::Duration;

use engine_adapter::PlatformDurableIO;

use crate::host::{Host, HostState};

/// durable IO 状态的轮询周期。
///
/// 取值权衡：fail-stop 之后本进程已无任何价值，越早退出越早被重启（§12.1 的 P95 1s
/// 含退出检测，Worker 侧另有 pidfd/reaper 的退出检测）；而轮询本身只是读两把锁，
/// 100ms 的代价可以忽略。
pub const WATCH_INTERVAL: Duration = Duration::from_millis(100);

/// durable IO 的故障报告。
///
/// 这是 [`PlatformDurableIO`] 既有只读接口（`fenced()` / `last_error()`）的快照，
/// 抽成纯数据是为了让退出决策可以脱离 docker / 真实 WAL 集群单测。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DurableReport {
    /// WAL 流是否已被 fence（出现过 durability 失败，此后写入一律失败）。
    pub fenced: bool,
    /// 最近一次 durability 失败的原因（成功后不清空）。
    pub last_error: Option<String>,
}

impl DurableReport {
    /// 采集当前 durable IO 状态。
    #[must_use]
    pub fn capture(durable: &PlatformDurableIO) -> Self {
        Self {
            fenced: durable.fenced(),
            last_error: durable.last_error(),
        }
    }
}

/// 进程退出决策。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitDecision {
    /// 继续运行（没有失败，或失败属于可恢复类）。
    Continue,
    /// 立刻退出：durable IO 已 fail-stop，本进程不可能再成功提交一次 WAL 写入。
    Exit {
        /// 退出原因（进日志，带 db_id / epoch 一起打印）。
        reason: String,
    },
}

/// 不可恢复故障的签名（本地 WAL 字节流不再可信）。
///
/// 这些是 `engine-adapter` 写入 `last_error` / 引擎错误体的**稳定模板**，来源固定：
/// `durable_io.rs` 的 `on_stream_error` / `fail_resume`，以及 `wal_frames.rs` 的
/// `WalStreamError::Rollback`。之所以只能按文本匹配：`engine-adapter` 对外的
/// `DURABILITY_ERROR_LABELS` 只覆盖远程 append 一类错误，本地字节流违规没有对应的结构化标识。
///
/// **注意**：`platform: durable io stopped` 这个标签本身**不能**作为 fail-stop 判据——
/// 它同样被挂在可恢复的远程 append 超时上（例如
/// `platform: durable io stopped: 等待 remote WAL append 超过 3.25s 仍未完成`）。
const UNRECOVERABLE_SIGNATURES: [&str; 4] = [
    // on_stream_error：本地 WAL 字节流违规（内部含 WalStreamError 的完整 display）
    "本地 WAL 字节流违规",
    // 写入路径上终止本次写入的引擎错误文本："platform: durable io stopped: 本地 WAL 帧解析失败: ..."
    "本地 WAL 帧解析失败",
    // fail_resume：读不出本地 WAL 头 / magic 非法，无法在未知位置续写
    "本地 WAL 接续失败",
    // WalStreamError::Rollback 的判别文本：已提交字节不允许被覆盖
    "已提交字节不允许被覆盖",
];

/// 该失败原因是否属于「本地 WAL 字节流已不可信」的不可恢复 fail-stop。
///
/// 只有这一类才允许触发进程退出：其他 durability 失败（远程 append 超时 / 未拿到
/// quorum / 被拒）只说明这一批写入没有 durable，写请求照样必须失败，但进程仍有意义
/// （架构 §11.1 要求的是"不假成功"，不是"立刻重启"）。
#[must_use]
pub fn is_unrecoverable_fail_stop(reason: &str) -> bool {
    UNRECOVERABLE_SIGNATURES
        .iter()
        .any(|signature| reason.contains(signature))
}

/// 退出决策（纯函数）：根据 durable IO 的故障报告决定进程去留。
///
/// 两个条件同时成立才退出：①WAL 流已被 fence（保证 `last_error` 是「已经发生并阻止了写入」
/// 的故障，而不是未生效的瞬时记录）；②原因是不可恢复签名。剩余情况一律继续运行——
/// 写请求的失败语义由 `PlatformDurableIO` 与 Server 负责，退出决策不越权。
#[must_use]
pub fn decide_exit(report: &DurableReport) -> ExitDecision {
    if !report.fenced {
        return ExitDecision::Continue;
    }
    match report.last_error.as_deref() {
        Some(reason) if is_unrecoverable_fail_stop(reason) => ExitDecision::Exit {
            reason: reason.to_string(),
        },
        _ => ExitDecision::Continue,
    }
}

/// 启动 durable IO 监视器：轮询到 fail-stop 就让进程退出。
///
/// 用轮询而不是阻塞等待：`PlatformDurableIO` 的状态变更没有通知接口，而 fail-stop 可能
/// 由任意写路径（引擎写入 / 恢复接续 / 截断）触发，逐个路径挂钩子会漏。
pub fn spawn_watch(host: Arc<Host>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(WATCH_INTERVAL);
        loop {
            ticker.tick().await;
            // 已经开始停机（信号 / fencing / 父进程消失）：退出路径已经在跑，不重复触发。
            if host.state() == HostState::Draining {
                break;
            }
            let report = DurableReport::capture(&host.durable);
            if let ExitDecision::Exit { reason } = decide_exit(&report) {
                host.durable_fail_stop(&reason);
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(reason: &str) -> DurableReport {
        DurableReport {
            fenced: true,
            last_error: Some(reason.to_string()),
        }
    }

    fn assert_exit(reason: &str) {
        match decide_exit(&report(reason)) {
            // 退出原因必须原样带出（日志要能对上是哪条 fail-stop）。
            ExitDecision::Exit { reason: got } => assert_eq!(got, reason),
            ExitDecision::Continue => panic!("必须判定为退出：{reason}"),
        }
    }

    fn assert_continue(reason: &str) {
        assert_eq!(
            decide_exit(&report(reason)),
            ExitDecision::Continue,
            "可恢复失败不得触发退出：{reason}"
        );
    }

    /// 现场原样复现的 fail-stop 文本（验收脚本 durability 场景）：必须退出。
    #[test]
    fn wal_frame_parse_violation_exits() {
        assert_exit(
            "platform: durable io stopped: 本地 WAL 帧解析失败: 本地 WAL 回卷到 12392，\
             早于最后一个 commit 边界 16512：已提交字节不允许被覆盖",
        );
    }

    /// durable IO 自己记录的字节流违规（`last_error` 的形态）。测试里带上 db-wal 路径，
    /// 保证匹配的是诊断模板而不是某次运行的偏移量。
    #[test]
    fn recorded_stream_violation_exits() {
        assert_exit(
            "本地 WAL 字节流违规（/var/lib/db-platform/db-wal）: 本地 WAL 写入偏移不连续：\
             write_pos=12392，解析游标=16512（本地 WAL 字节流被破坏）",
        );
    }

    /// 接续失败（读不出 WAL 头 / magic 非法）同样不可恢复：换新进程重新播种是唯一出路。
    #[test]
    fn resume_failure_exits() {
        assert_exit(
            "本地 WAL 接续失败（/var/lib/db-platform/db-wal）: \
             本地 WAL 头非法（magic 或 page_size 不合法），无法在未知位置续写",
        );
    }

    /// 只有回卷判别文本（没有外层包装）也要能判定：叶子错误可能被别处直接上报。
    #[test]
    fn rollback_violation_leaf_exits() {
        assert_exit(
            "本地 WAL 回卷到 12392，早于最后一个 commit 边界 16512：已提交字节不允许被覆盖",
        );
    }

    /// 短暂的 append 超时是**可恢复**的：写请求必须失败（§11.1），但进程不该退出。
    #[test]
    fn transient_append_timeout_does_not_exit() {
        assert_continue("remote WAL append 超过 append_timeout(3s) 未返回");
        assert_continue("等待 remote WAL append 超过 3.25s 仍未完成（append 流水线可能已停摆）");
    }

    /// 回归：远程 append 失败同样带着 `platform: durable io stopped` 标签，
    /// **不能**因为看到标签就退出（这是现场日志里真实出现的一行）。
    #[test]
    fn durable_stopped_label_on_transient_failure_does_not_exit() {
        assert_continue(
            "platform: durable io stopped: 等待 remote WAL append 超过 3.25s 仍未完成\
             （append 流水线可能已停摆）",
        );
        assert_continue("platform: durable io stopped");
    }

    /// 未拿到 quorum / 被 Remote WAL 拒绝：写失败语义由 durable IO 负责，进程继续。
    #[test]
    fn remote_not_durable_failures_do_not_exit() {
        assert_continue("remote WAL append 失败: [WAL_NOT_DURABLE] 多数副本未确认");
        assert_continue("remote WAL append 失败: [WAL_APPEND_REJECTED] owner epoch 过期");
        assert_continue("读取 remote WAL 末端 LSN 超过 append_timeout(3s) 未返回");
        assert_continue("remote WAL 返回的 durable_lsn=100 未覆盖本批次 [100, 200)");
    }

    /// 没有失败时不得退出；有文本但流未被 fence 时也不退出（保守：只认已成事实的故障）。
    #[test]
    fn healthy_or_unfenced_reports_continue() {
        assert_eq!(
            decide_exit(&DurableReport::default()),
            ExitDecision::Continue
        );
        assert_eq!(
            decide_exit(&DurableReport {
                fenced: false,
                last_error: Some("本地 WAL 字节流违规：不该在未 fence 时触发退出".to_string()),
            }),
            ExitDecision::Continue
        );
    }

    /// 签名表与判定一致（防止有人只改表不改判定）。
    #[test]
    fn every_signature_triggers_exit() {
        for signature in UNRECOVERABLE_SIGNATURES {
            assert!(
                is_unrecoverable_fail_stop(signature),
                "签名必须被判定为不可恢复：{signature}"
            );
        }
    }
}
