//! 端点轮换与 leader 端点缓存（纯逻辑，无 IO，便于单测）。
//!
//! 为什么需要「轮换 + 缓存」而不是每次从第一个端点试起：
//! 非 leader 副本会稳定地返回 `WAL_NOT_LEADER`，从固定起点重试等于每次写都白打几次
//! 网络往返（WAL 在 commit 热路径上，白打的往返会直接抬高 P99）。因此：
//! - 任何时候只要某端点证明自己是 leader（append 成功）或服务端提示了 leader，
//!   就把它缓存下来作为下次的起点；
//! - 失败时从上次的**下一个**端点继续，避免固定顺序造成的重复试错；
//! - 上限始终由 `max_attempts` 约束，绝不无限重试。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

/// 全部 WAL 副本端点 + 当前已知 leader 缓存。
#[derive(Debug)]
pub(crate) struct EndpointSet {
    /// 配置端点（去重保序，顺序即运维给出的副本顺序）。
    endpoints: Vec<String>,
    /// 是否接受配置列表之外的 leader 提示（见 [`EndpointSet::accepts_hint`]）。
    allow_unlisted_hint: bool,
    /// 当前已知 leader 端点（服务端提示或上次 append 成功的目标）。
    leader: RwLock<Option<String>>,
    /// 轮换游标：下次调用的起点下标，避免总是从第一个端点试起。
    cursor: AtomicUsize,
}

impl EndpointSet {
    /// 构造；保留首次出现的顺序并去掉重复端点（重试同一个地址没有意义）。
    ///
    /// `allow_unlisted_hint = false`（默认）时，配置列表就是「可拨号端点」的闭集。
    pub(crate) fn new(endpoints: Vec<String>, allow_unlisted_hint: bool) -> Self {
        let mut deduped: Vec<String> = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            if !deduped.contains(&endpoint) {
                deduped.push(endpoint);
            }
        }
        Self {
            endpoints: deduped,
            allow_unlisted_hint,
            leader: RwLock::new(None),
            cursor: AtomicUsize::new(0),
        }
    }

    /// 配置端点（供 `WalClient::endpoints()` 暴露）。
    pub(crate) fn as_slice(&self) -> &[String] {
        &self.endpoints
    }

    /// 当前缓存的 leader 端点。
    pub(crate) fn leader(&self) -> Option<String> {
        self.leader.read().clone()
    }

    /// 该 leader 提示是否可以采纳（缓存为起点 / 插队到本次尝试）。
    ///
    /// 默认只在**配置列表内**采纳：配置列表是运维给出的「可拨号端点」闭集，而提示来自
    /// 服务端应答里的一段自由文本 —— 写路径不能因为一句话就去连一个没配置过的地址
    /// （配置错误、残留提示或被篡改的应答都会把写入引到未授权端点）。
    pub(crate) fn accepts_hint(&self, endpoint: &str) -> bool {
        if self.allow_unlisted_hint {
            return true;
        }
        self.endpoints
            .iter()
            .any(|configured| configured == endpoint)
    }

    /// 记录已知 leader（原子缓存更新）。
    ///
    /// 只允许把缓存改成「服务端明确提示的地址」或「刚刚证明自己成功的地址」，不做猜测。
    /// 返回是否采纳：[`EndpointSet::accepts_hint`] 拒绝的提示会被记 warning 并忽略，
    /// 调用方据此把它从本次尝试计划里剔掉（否则提示仍会被拨号，等于绕过该限制）。
    pub(crate) fn prefer(&self, endpoint: &str) -> bool {
        if endpoint.is_empty() {
            return false;
        }
        if !self.accepts_hint(endpoint) {
            tracing::warn!(
                hint = %endpoint,
                "忽略配置列表之外的 leader 提示（allow_unlisted_leader_hint=false）"
            );
            return false;
        }
        let mut leader = self.leader.write();
        if leader.as_deref() != Some(endpoint) {
            *leader = Some(endpoint.to_owned());
        }
        true
    }

    /// 遗忘某个 leader 缓存（该端点又报 `WAL_NOT_LEADER`，说明缓存已过期）。
    pub(crate) fn forget_leader(&self, endpoint: &str) {
        let mut leader = self.leader.write();
        if leader.as_deref() == Some(endpoint) {
            *leader = None;
        }
    }

    /// 生成本轮尝试计划。
    ///
    /// 顺序：已知 leader（若有）→ 从游标开始绕一圈的其余端点。重复端点只出现一次。
    pub(crate) fn plan(&self, max_attempts: u32) -> AttemptPlan {
        AttemptPlan::new(self.ordered(), max_attempts)
    }

    /// 生成本轮「覆盖全部端点各一次」的尝试计划（用于 `health` 这类诊断调用）。
    ///
    /// 诊断不能因为 `max_attempts` 小于副本数就漏报某个副本：健康检查的语义是
    /// 「整个 WAL 组是否可用」，不是「前 N 次尝试是否成功」。
    pub(crate) fn plan_all(&self) -> AttemptPlan {
        let ordered = self.ordered();
        let attempts = u32::try_from(ordered.len()).unwrap_or(u32::MAX);
        AttemptPlan::new(ordered, attempts.max(1))
    }

    /// 本轮结束后推进轮换游标：下次从「最后一个尝试过的配置端点」的下一个开始。
    pub(crate) fn finish(&self, plan: &AttemptPlan) {
        let n = self.endpoints.len();
        if n == 0 {
            return;
        }
        self.cursor
            .store(plan.next_cursor(&self.endpoints), Ordering::Relaxed);
    }

    /// 完整尝试顺序（leader 优先 + 游标旋转）。
    fn ordered(&self) -> Vec<String> {
        let n = self.endpoints.len();
        let mut ordered = Vec::with_capacity(n + 1);
        if let Some(leader) = self.leader() {
            ordered.push(leader);
        }
        if n > 0 {
            let start = self.cursor.load(Ordering::Relaxed) % n;
            for offset in 0..n {
                ordered.push(self.endpoints[(start + offset) % n].clone());
            }
        }
        ordered
    }
}

/// 一次调用内「下一个该试哪个端点」的状态机。
#[derive(Debug)]
pub(crate) struct AttemptPlan {
    /// 一轮完整顺序（去重后）。
    ordered: Vec<String>,
    /// 待尝试队列；leader 提示可插队到队首。
    queue: VecDeque<String>,
    /// 已经尝试过的端点（防止同一个提示重复插队）。
    tried: Vec<String>,
    /// 已尝试次数。
    attempts: u32,
    /// 尝试上限（含首次）。
    max_attempts: u32,
}

impl AttemptPlan {
    fn new(ordered: Vec<String>, max_attempts: u32) -> Self {
        let mut deduped: Vec<String> = Vec::with_capacity(ordered.len());
        for endpoint in ordered {
            if !endpoint.is_empty() && !deduped.contains(&endpoint) {
                deduped.push(endpoint);
            }
        }
        Self {
            queue: deduped.iter().cloned().collect(),
            ordered: deduped,
            tried: Vec::new(),
            attempts: 0,
            max_attempts: max_attempts.max(1),
        }
    }

    /// 取下一个要尝试的端点；已耗尽返回 `None`。
    pub(crate) fn next(&mut self) -> Option<String> {
        if self.attempts >= self.max_attempts {
            return None;
        }
        if self.queue.is_empty() {
            // 端点数量少于尝试上限时允许再绕一轮（例如单副本重连），次数仍受 max_attempts 约束
            if self.ordered.is_empty() {
                return None;
            }
            self.queue = self.ordered.iter().cloned().collect();
        }
        let endpoint = self.queue.pop_front()?;
        self.attempts += 1;
        self.tried.push(endpoint.clone());
        Some(endpoint)
    }

    /// 把 leader 提示插到队首（未尝试过才插入），返回是否插队成功。
    pub(crate) fn promote(&mut self, endpoint: &str) -> bool {
        if endpoint.is_empty()
            || self.attempts >= self.max_attempts
            || self.tried.iter().any(|tried| tried == endpoint)
            || self.queue.iter().any(|queued| queued == endpoint)
        {
            return false;
        }
        self.queue.push_front(endpoint.to_owned());
        true
    }

    /// 已尝试过的端点（按顺序，含重复）。
    pub(crate) fn tried(&self) -> &[String] {
        &self.tried
    }

    /// 已完成的尝试次数。
    pub(crate) fn attempts(&self) -> u32 {
        self.attempts
    }

    /// 下一次调用的轮换游标：最后一个尝试过的配置端点的下一个。
    fn next_cursor(&self, endpoints: &[String]) -> usize {
        let n = endpoints.len();
        if n == 0 {
            return 0;
        }
        for endpoint in self.tried.iter().rev() {
            if let Some(index) = endpoints.iter().position(|candidate| candidate == endpoint) {
                return (index + 1) % n;
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoints() -> EndpointSet {
        EndpointSet::new(
            vec![
                "http://wal-1:9200".to_owned(),
                "http://wal-2:9200".to_owned(),
                "http://wal-3:9200".to_owned(),
            ],
            false,
        )
    }

    fn drain(plan: &mut AttemptPlan) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(endpoint) = plan.next() {
            out.push(endpoint);
        }
        out
    }

    #[test]
    fn endpoint_set_dedupes_preserving_order() {
        let set = EndpointSet::new(
            vec![
                "http://wal-1:9200".to_owned(),
                "http://wal-2:9200".to_owned(),
                "http://wal-1:9200".to_owned(),
            ],
            false,
        );
        assert_eq!(set.as_slice(), ["http://wal-1:9200", "http://wal-2:9200"]);
    }

    #[test]
    fn plan_without_leader_rotates_in_config_order() {
        let set = endpoints();
        let mut plan = set.plan(3);
        assert_eq!(
            drain(&mut plan),
            [
                "http://wal-1:9200",
                "http://wal-2:9200",
                "http://wal-3:9200"
            ]
        );
    }

    #[test]
    fn leader_hint_is_tried_first() {
        let set = endpoints();
        set.prefer("http://wal-3:9200");
        let mut plan = set.plan(3);
        let order = drain(&mut plan);
        assert_eq!(order[0], "http://wal-3:9200");
        // 一轮内每个端点只出现一次（提示命中的副本不再重复）
        assert_eq!(order.len(), 3);
        assert!(order.contains(&"http://wal-1:9200".to_owned()));
        assert!(order.contains(&"http://wal-2:9200".to_owned()));
    }

    #[test]
    fn leader_hint_outside_configured_set_is_ignored_by_default() {
        // 默认配置：配置列表就是「可拨号端点」闭集。列表外的提示若被采纳，一次被篡改 /
        // 配错的应答就能把写路径引到运维没授权过的地址。
        let set = endpoints();
        assert!(!set.prefer("http://wal-9:9200"), "列表外的提示必须被拒绝");
        assert_eq!(set.leader(), None, "拒绝的提示不得进入 leader 缓存");

        let mut plan = set.plan(4);
        let order = drain(&mut plan);
        assert!(
            !order.contains(&"http://wal-9:9200".to_owned()),
            "被忽略的提示不得出现在尝试计划里：{order:?}"
        );
        assert_eq!(
            &order[..3],
            [
                "http://wal-1:9200",
                "http://wal-2:9200",
                "http://wal-3:9200"
            ],
            "忽略提示后仍按配置顺序轮换（尝试额度多于副本数时允许再绕一轮）"
        );
        assert!(!set.accepts_hint("http://wal-9:9200"));
        assert!(
            set.accepts_hint("http://wal-2:9200"),
            "列表内的提示照常采纳"
        );
    }

    #[test]
    fn leader_hint_outside_configured_set_is_tried_first_when_opted_in() {
        // 显式打开开关（动态扩副本 / 提示端点不在本地配置里）后才允许拨号
        let set = EndpointSet::new(
            vec![
                "http://wal-1:9200".to_owned(),
                "http://wal-2:9200".to_owned(),
                "http://wal-3:9200".to_owned(),
            ],
            true,
        );
        assert!(set.prefer("http://wal-9:9200"));
        let mut plan = set.plan(4);
        let order = drain(&mut plan);
        assert_eq!(order[0], "http://wal-9:9200");
        // 提示端点不在配置里时，其余端点仍然要全部轮到
        for expected in [
            "http://wal-1:9200",
            "http://wal-2:9200",
            "http://wal-3:9200",
        ] {
            assert!(order.contains(&expected.to_owned()), "{expected} 未尝试");
        }
    }

    #[test]
    fn plan_rotation_continues_after_previous_run() {
        let set = endpoints();
        let mut first = set.plan(2);
        assert_eq!(first.next().as_deref(), Some("http://wal-1:9200"));
        assert_eq!(first.next().as_deref(), Some("http://wal-2:9200"));
        set.finish(&first);

        // 下一次调用从 wal-2 之后开始，而不是又从 wal-1 试错
        let mut second = set.plan(2);
        assert_eq!(second.next().as_deref(), Some("http://wal-3:9200"));
        assert_eq!(second.next().as_deref(), Some("http://wal-1:9200"));
    }

    #[test]
    fn plan_respects_max_attempts_and_never_loops_forever() {
        let set = endpoints();
        let mut plan = set.plan(2);
        assert_eq!(drain(&mut plan).len(), 2);
        assert_eq!(plan.attempts(), 2);

        let mut plan = set.plan(1);
        assert_eq!(plan.next().as_deref(), Some("http://wal-1:9200"));
        assert_eq!(plan.next(), None, "超过上限后必须停止尝试");
    }

    #[test]
    fn plan_wraps_rounds_when_attempts_exceed_replicas() {
        let set = EndpointSet::new(vec!["http://wal-1:9200".to_owned()], false);
        let mut plan = set.plan(3);
        assert_eq!(
            drain(&mut plan),
            [
                "http://wal-1:9200",
                "http://wal-1:9200",
                "http://wal-1:9200"
            ]
        );
    }

    #[test]
    fn promote_inserts_new_hint_at_front_once() {
        let set = endpoints();
        let mut plan = set.plan(4);
        assert!(plan.promote("http://wal-9:9200"), "未出现过的提示应插队");
        assert_eq!(plan.next().as_deref(), Some("http://wal-9:9200"));
        assert!(!plan.promote("http://wal-9:9200"), "已尝试过不得重复插队");
        assert!(!plan.promote("http://wal-1:9200"), "队列里已有的不得重复");
        assert!(!plan.promote(""), "空提示不得插队");
    }

    #[test]
    fn promote_does_not_exceed_attempt_budget() {
        let set = endpoints();
        let mut plan = set.plan(1);
        assert_eq!(plan.next().as_deref(), Some("http://wal-1:9200"));
        assert!(
            !plan.promote("http://wal-9:9200"),
            "尝试额度已用尽，插队也必须被拒绝"
        );
        assert_eq!(plan.next(), None);
    }

    #[test]
    fn forget_leader_clears_stale_cache() {
        let set = endpoints();
        set.prefer("http://wal-2:9200");
        assert_eq!(set.leader().as_deref(), Some("http://wal-2:9200"));
        set.forget_leader("http://wal-3:9200");
        assert_eq!(
            set.leader().as_deref(),
            Some("http://wal-2:9200"),
            "遗忘其它端点不得影响缓存"
        );
        set.forget_leader("http://wal-2:9200");
        assert_eq!(set.leader(), None);
    }

    #[test]
    fn plan_all_covers_every_endpoint_even_beyond_max_attempts() {
        let set = endpoints();
        let mut plan = set.plan_all();
        let order = drain(&mut plan);
        assert_eq!(order.len(), 3, "健康检查必须覆盖每个副本");
    }

    #[test]
    fn plan_all_includes_leader_hint_then_all_replicas() {
        // 诊断路径同样受「只在配置列表内采纳提示」约束，这里用打开开关的集合验证
        // 「提示优先 + 其余副本仍然全部轮到」这一顺序语义
        let set = EndpointSet::new(
            vec![
                "http://wal-1:9200".to_owned(),
                "http://wal-2:9200".to_owned(),
                "http://wal-3:9200".to_owned(),
            ],
            true,
        );
        assert!(set.prefer("http://wal-9:9200"));
        let mut plan = set.plan_all();
        let order = drain(&mut plan);
        assert_eq!(order[0], "http://wal-9:9200");
        assert_eq!(order.len(), 4);
    }

    #[test]
    fn empty_endpoint_set_yields_empty_plan() {
        let set = EndpointSet::new(Vec::new(), false);
        let mut plan = set.plan(4);
        assert_eq!(plan.next(), None);
        set.finish(&plan);
        assert_eq!(set.as_slice().len(), 0);
    }
}
