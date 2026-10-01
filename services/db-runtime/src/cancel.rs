//! 在途请求的取消登记表（`CancelRequest` / `CancelNotice` -> 语句边界中断）。
//!
//! 为什么是 `AtomicBool` 而不是 `CancellationToken`：取消只在一个地方被观察
//! （语句边界），不需要唤醒等待者，也不需要传播到子任务。一个布尔标记足够，
//! 而且**不能**用它做"取消后回滚"之类的资源释放决策——释放一律走 Drop。
//!
//! Turso 引擎没有中断正在运行的语句的接口（`Statement::run_*` 一旦进入不可打断），
//! 因此本进程的保证是：**在语句边界尽快返回 `CANCELLED`**。
//! 具体地：①取到连接后、执行前检查一次；②执行返回后立刻检查一次（此时结果被丢弃，
//! 对 Dispatcher 表现为取消成功）。跨语句的批量通过 `atomic` 包装成显式事务，
//! 中途取消时事务回滚，不会留下半截写入。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

/// 单个请求的取消标记（可克隆，共享同一标记）。
#[derive(Debug, Clone, Default)]
pub struct CancelFlag {
    cancelled: Arc<AtomicBool>,
    reason: Arc<parking_lot::Mutex<Option<String>>>,
}

impl CancelFlag {
    /// 新标记（未取消）。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 置为已取消，并记录原因（首次原因保留，便于诊断）。
    pub fn cancel(&self, reason: impl Into<String>) {
        let mut slot = self.reason.lock();
        if slot.is_none() {
            *slot = Some(reason.into());
        }
        drop(slot);
        self.cancelled.store(true, Ordering::Release);
    }

    /// 是否已被取消。
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// 取消原因（未取消时为 `None`）。
    #[must_use]
    pub fn reason(&self) -> Option<String> {
        self.reason.lock().clone()
    }
}

/// 在途请求登记表。
///
/// 键是 `request_id`，值是标记 + 所属会话（会话级取消要按会话反查）。
#[derive(Debug, Default)]
pub struct CancelRegistry {
    entries: DashMap<String, Entry>,
}

#[derive(Debug)]
struct Entry {
    session_id: String,
    flag: CancelFlag,
}

impl CancelRegistry {
    /// 新建空登记表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记一个请求；返回的守卫在 Drop 时自动注销（任何返回路径都不会泄漏）。
    pub fn register(self: &Arc<Self>, request_id: &str, session_id: &str) -> CancelGuard {
        let flag = CancelFlag::new();
        if !request_id.is_empty() {
            self.entries.insert(
                request_id.to_string(),
                Entry {
                    session_id: session_id.to_string(),
                    flag: flag.clone(),
                },
            );
        }
        CancelGuard {
            registry: Arc::clone(self),
            request_id: request_id.to_string(),
        }
    }

    /// 标记标志位（供登记前的早期检查使用）。
    #[must_use]
    pub fn flag_for(&self, request_id: &str) -> Option<CancelFlag> {
        self.entries.get(request_id).map(|entry| entry.flag.clone())
    }

    /// 按 request_id 取消；返回是否命中了一个在途请求。
    pub fn cancel(&self, request_id: &str) -> bool {
        match self.entries.get(request_id) {
            Some(entry) => {
                entry
                    .flag
                    .cancel(format!("request {request_id} 被显式取消"));
                true
            }
            None => false,
        }
    }

    /// 取消某个会话上的全部在途请求；返回命中数量。
    pub fn cancel_session(&self, session_id: &str) -> usize {
        let mut hits = 0;
        for entry in self.entries.iter() {
            if entry.session_id == session_id {
                entry.flag.cancel(format!("会话 {session_id} 被取消"));
                hits += 1;
            }
        }
        hits
    }

    /// 取消全部在途请求（进程即将退出时使用）；返回命中数量。
    ///
    /// 退出路径不允许挂住调用方：把在途请求标记为取消后，语句边界会尽快返回 `CANCELLED`，
    /// 停机收尾也就不必等会话超时。
    pub fn cancel_all(&self, reason: &str) -> usize {
        let mut hits = 0;
        for entry in self.entries.iter() {
            entry.flag.cancel(reason.to_string());
            hits += 1;
        }
        hits
    }

    /// 当前在途请求数（健康检查与诊断用）。
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn unregister(&self, request_id: &str) {
        if !request_id.is_empty() {
            self.entries.remove(request_id);
        }
    }
}

/// 登记守卫：Drop 即注销。
#[derive(Debug)]
pub struct CancelGuard {
    registry: Arc<CancelRegistry>,
    request_id: String,
}

impl Drop for CancelGuard {
    fn drop(&mut self) {
        self.registry.unregister(&self.request_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_by_request_id() {
        let registry = Arc::new(CancelRegistry::new());
        let guard = registry.register("req-1", "sess-1");
        let flag = registry.flag_for("req-1").expect("已登记");
        assert!(!flag.is_cancelled());
        assert!(registry.cancel("req-1"));
        assert!(flag.is_cancelled());
        assert!(flag.reason().is_some());
        drop(guard);
        assert_eq!(registry.len(), 0, "守卫 Drop 后必须注销");
        assert!(!registry.cancel("req-1"), "注销后取消不应命中");
    }

    #[test]
    fn cancel_whole_session() {
        let registry = Arc::new(CancelRegistry::new());
        let _a = registry.register("req-a", "sess-1");
        let _b = registry.register("req-b", "sess-1");
        let _c = registry.register("req-c", "sess-2");
        assert_eq!(registry.cancel_session("sess-1"), 2);
        assert!(registry.flag_for("req-a").unwrap().is_cancelled());
        assert!(!registry.flag_for("req-c").unwrap().is_cancelled());
    }

    /// 空 request_id（单向通知）不登记，避免所有通知挤在同一个键上。
    #[test]
    fn skips_empty_request_id() {
        let registry = Arc::new(CancelRegistry::new());
        let _guard = registry.register("", "sess-1");
        assert_eq!(registry.len(), 0);
    }
}
