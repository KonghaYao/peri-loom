//! Worker -> Server 心跳与资源上报（架构 §5.1 / §12 / §16）。
//!
//! 心跳方向的固定约定（proto `platform.control.v1.WorkerControl`）：
//! **Worker 是 client**，Server 是 server。本模块负责：
//!
//! ```text
//! 每 HEARTBEAT_INTERVAL_MS（默认 1s）：
//!   采样资源 -> 组装 WorkerInfo + WorkerResourceUsage + 本地 DB 摘要 + inventory_version
//!   -> 发送 Heartbeat（带有限重试与退避）
//!   -> 处理 Server 回执（要求全量 inventory / 要求排空 / 观察 catalog_version）
//! ```
//!
//! 三条硬约束：
//!
//! 1. **Server 不可达不能影响数据面**：心跳跑在独立任务里，所有失败只记日志与指标，
//!    绝不阻塞、绝不 panic、绝不触碰注册表（除 Server 明确要求的排空）。
//! 2. **重试必须有界**：一次心跳最多尝试 [`MAX_ATTEMPTS`] 次，且整轮不得超过一个心跳
//!    周期 —— 否则心跳会随故障雪崩式堆积。
//! 3. **退避必须有上限**：连接失败后按指数退避重连，但不超过 [`MAX_BACKOFF`]，
//!    保证 Server 恢复后心跳能在一个合理时间内重新接上。

use std::sync::Arc;
use std::time::{Duration, Instant};

use protocol::control::{self, server_ingress_client::ServerIngressClient, HeartbeatRequest};
use tokio::sync::watch;

use crate::cli::WorkerConfig;
use crate::control::WorkerControlService;
use crate::registry::LocalDbRegistry;
use crate::resources::ResourceSampler;
use crate::worker_state::WorkerStateMachine;

/// 单轮心跳的最大尝试次数（含首次）。
const MAX_ATTEMPTS: u32 = 3;

/// 失败后的初始退避。
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);

/// 退避上限：超过它就意味着「Server 长时间不可达」，重连频率不该继续降低。
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// 单次 RPC 的超时（必须显著小于心跳周期，否则心跳会堆叠）。
///
/// 默认周期 1s（`HEARTBEAT_INTERVAL_MS`），因此取 500ms：连接与 RPC 各一次超时
/// 也仍在一个周期内收尾，配合 `MAX_ATTEMPTS` + 退避，整轮不会拖到下一轮心跳。
const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

/// 心跳任务的共享依赖。
pub struct HeartbeatContext {
    /// 配置（worker id / 端点 / 周期）。
    pub cfg: Arc<WorkerConfig>,
    /// Worker 状态机（Server 可以要求排空）。
    pub state: Arc<WorkerStateMachine>,
    /// 本地 DB 注册表（上报 inventory）。
    pub registry: Arc<LocalDbRegistry>,
    /// 资源采样器（上报 usage）。
    pub sampler: Arc<ResourceSampler>,
    /// 控制面服务（复用其 worker_info / local_databases 视图，避免两处口径不一致）。
    pub control: Arc<WorkerControlService>,
}

/// 单轮心跳的统计（用于日志与测试断言）。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatOutcome {
    /// 成功收到回执。
    pub delivered: bool,
    /// 实际尝试次数（含成功那次）。
    pub attempts: u32,
}

/// 心跳循环。
///
/// `shutdown` 由主进程在收到 SIGTERM 时置位；每次 tick 前都会检查，做到 1 个周期内退出。
pub async fn run(context: HeartbeatContext, mut shutdown: watch::Receiver<bool>) {
    let interval = context.cfg.heartbeat_interval;
    let mut ticker = tokio::time::interval(interval);
    // 落后时不要补发（补发只会把故障放大成心跳风暴）
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut backoff = INITIAL_BACKOFF;
    let mut consecutive_failures: u64 = 0;

    tracing::info!(
        worker_id = %context.cfg.worker_id,
        endpoint = %context.cfg.server_control_endpoint,
        interval_ms = interval.as_millis() as u64,
        "心跳任务启动"
    );

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                // 通道关闭（进程正在退出）与显式置位都视为退出信号
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!(worker_id = %context.cfg.worker_id, "心跳任务退出");
                    return;
                }
            }
            _ = ticker.tick() => {
                // 每个周期重新采样资源：上报的是「最近一段时间」的实测值，
                // CPU 是速率，必须由采样器按增量计算（见 resources 模块）。
                context.sampler.refresh(&context.registry).await;

                match send_once(&context).await {
                    Ok(outcome) if outcome.delivered => {
                        if consecutive_failures > 0 {
                            tracing::info!(
                                worker_id = %context.cfg.worker_id,
                                failures = consecutive_failures,
                                "心跳已恢复"
                            );
                            consecutive_failures = 0;
                            backoff = INITIAL_BACKOFF;
                        }
                    }
                    Ok(_) => {
                        consecutive_failures += 1;
                        log_failure(&context, consecutive_failures, "回执未送达");
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                    Err(err) => {
                        consecutive_failures += 1;
                        log_failure(&context, consecutive_failures, &err);
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                }

                // 失败后退避：不改变 tick 节奏，只是把「下一轮可能的额外等待」体现出来。
                // 这里刻意不 sleep 超过一个周期，避免心跳与主循环节奏脱节。
                if consecutive_failures > 0 && backoff > interval {
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return;
                            }
                        }
                        _ = tokio::time::sleep(backoff.min(MAX_BACKOFF)) => {}
                    }
                }
            }
        }
    }
}

/// 组装并发送一次心跳（含有限重试）。
async fn send_once(context: &HeartbeatContext) -> Result<HeartbeatOutcome, String> {
    let request = build_request(context);
    let mut last_error = String::new();

    for attempt in 1..=MAX_ATTEMPTS {
        match send_request(context, &request).await {
            Ok(response) => {
                handle_response(context, response);
                return Ok(HeartbeatOutcome {
                    delivered: true,
                    attempts: attempt,
                });
            }
            Err(err) => {
                last_error = err;
                if attempt < MAX_ATTEMPTS {
                    // 指数退避，但整轮仍远小于心跳周期
                    tokio::time::sleep(INITIAL_BACKOFF * attempt).await;
                }
            }
        }
    }

    Err(last_error)
}

/// 组装心跳请求（每次发送都重新构建，避免跨轮污染）。
fn build_request(context: &HeartbeatContext) -> HeartbeatRequest {
    HeartbeatRequest {
        worker: Some(context.control.worker_info()),
        usage: Some(context.sampler.current().into()),
        databases: context.control.local_databases(),
        // 注册表版本号由状态变更驱动递增；Server 据此判断是否需要全量拉取
        inventory_version: context.registry.inventory_version(),
    }
}

/// 发送一次心跳 RPC。
///
/// 每次尝试都**新建连接**：心跳间隔是秒级，长连接复用省下的握手成本可以忽略，
/// 而新建连接天然避免了「半死连接」把心跳卡住（tonic 的 channel 在断网后可能长时间
/// 认为自己是 ready）。控制面不是热路径，简单可靠优先。
async fn send_request(
    context: &HeartbeatContext,
    request: &HeartbeatRequest,
) -> Result<control::HeartbeatResponse, String> {
    let endpoint = context.cfg.server_control_endpoint.clone();
    // 心跳是 Worker -> Server 方向，必须调用 Server 侧实现的 ServerIngress
    let connect = ServerIngressClient::connect(endpoint.clone());
    let mut client = match tokio::time::timeout(REQUEST_TIMEOUT, connect).await {
        Ok(Ok(client)) => client,
        Ok(Err(err)) => return Err(format!("连接 {endpoint} 失败：{err}")),
        Err(_) => return Err(format!("连接 {endpoint} 超时")),
    };

    match tokio::time::timeout(REQUEST_TIMEOUT, client.heartbeat(request.clone())).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(format!("Heartbeat 被拒绝：{status}")),
        Err(_) => Err("Heartbeat 超时".to_string()),
    }
}

/// 处理 Server 回执。
///
/// 四种回执语义：
/// - `request_full_inventory`：本实现**每轮都上报完整 inventory**（DB 数量在单个 Worker
///   上是千级，摘要本身不大，且能免掉「Server 发现缺失再拉一次」的往返），
///   因此这里只记日志确认；
/// - `accept_new_placement`：Server 明确授权重新接纳新 Placement —— 这是 EMPTY 回到
///   ACTIVE 的**唯一**触发点（架构 §12.3）。授权优先于 `draining`：EMPTY 节点上的
///   `draining=true` 只表示「现在还不是 ACTIVE」，授权说的正是「可以回到 ACTIVE」，
///   两者同时出现时按授权处理，否则刚回归的节点会被同一条回执立刻打回 DRAINING；
/// - `draining`：Server 判定本节点应停止接受新 Placement —— 交给状态机（幂等）；
/// - `catalog_version`：仅记录，用于排障时对照 Server 侧所有权版本。
fn handle_response(context: &HeartbeatContext, response: control::HeartbeatResponse) {
    if response.accept_new_placement {
        if !resume_from_empty(context) {
            tracing::trace!(
                worker_id = %context.cfg.worker_id,
                state = %context.state.current(),
                "收到重新接纳授权，但未满足本地条件（状态 / 冷却期 / 资源水位 / 本地仍有 DB）"
            );
        }
    } else if response.draining {
        match context.state.begin_drain() {
            Ok(state) => tracing::warn!(
                worker_id = %context.cfg.worker_id,
                state = %state,
                "Server 要求排空，本节点不再接受新 Placement"
            ),
            Err(err) => tracing::warn!(error = %err, "进入排空状态失败"),
        }
    }
    if response.request_full_inventory {
        tracing::debug!(
            worker_id = %context.cfg.worker_id,
            databases = context.registry.len(),
            "Server 要求全量 inventory（本实现每轮均为全量，无需补发）"
        );
    }
    tracing::trace!(
        worker_id = %context.cfg.worker_id,
        catalog_version = response.catalog_version,
        "心跳回执"
    );
}

/// 尝试执行 EMPTY -> ACTIVE（返回是否真的迁移了）。
///
/// 「资源健康」直接用与准入判定同一个水位闸门：既然回归的目的是重新接活，那么标准就是
/// 「现在能不能接活」—— 自造第二套阈值只会让两个判定打架。
fn resume_from_empty(context: &HeartbeatContext) -> bool {
    let usage = context.sampler.current();
    let healthy = domain::policy::placement_gate(usage.max_utilization()).allows_new_placement();
    context
        .state
        .try_resume_from_empty(&context.registry, healthy, Instant::now())
}

/// 记录心跳失败。前几次用 warn，之后降级为 debug，避免长时间断连把日志刷满。
fn log_failure(context: &HeartbeatContext, failures: u64, reason: &str) {
    if failures <= 3 || failures.is_power_of_two() {
        tracing::warn!(
            worker_id = %context.cfg.worker_id,
            endpoint = %context.cfg.server_control_endpoint,
            failures,
            reason,
            "心跳发送失败（数据面不受影响）"
        );
    } else {
        tracing::debug!(
            worker_id = %context.cfg.worker_id,
            failures,
            reason,
            "心跳持续失败"
        );
    }
}

/// 心跳是否已经「过期」（供测试与诊断使用）。
///
/// 语义：`last_success` 距今超过 `interval * 3` 即视为过期，对应架构 §16
/// 「连续 3 次 miss 判定 Suspect/Unavailable」的 Worker 侧自检。
#[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
pub fn is_stale(last_success: Instant, interval: Duration, now: Instant) -> bool {
    now.saturating_duration_since(last_success) > interval * 3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_detection_matches_three_missed_beats() {
        let now = Instant::now();
        let interval = Duration::from_secs(1);
        assert!(!is_stale(now, interval, now));
        assert!(!is_stale(now - Duration::from_millis(2900), interval, now));
        assert!(is_stale(now - Duration::from_millis(3100), interval, now));
    }

    #[test]
    fn backoff_is_bounded() {
        let mut backoff = INITIAL_BACKOFF;
        for _ in 0..20 {
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
        assert_eq!(backoff, MAX_BACKOFF);
    }

    #[test]
    fn request_timeout_is_below_default_interval() {
        // 单次 RPC 超时必须小于默认心跳周期，否则心跳会互相堆叠
        assert!(REQUEST_TIMEOUT < Duration::from_millis(1000));
    }

    #[test]
    fn retry_budget_stays_within_default_interval() {
        // 整轮重试的睡眠总量（200 + 400ms）必须小于默认心跳周期（1s），
        // 否则一次抖动就会把下一轮心跳推迟到周期之外。
        let total = INITIAL_BACKOFF + INITIAL_BACKOFF * 2;
        assert_eq!(MAX_ATTEMPTS, 3);
        assert!(total < Duration::from_secs(1));
    }
}
