//! Commit 门：把「一次包含 commit frame 的本地写入」拆成**本地写**与**远程 append**
//! 两件事，并规定只有两者都成功才算完成。
//!
//! 这是架构 §11.1「Commit Success ⇒ Remote WAL Durable」在代码里的落点：
//!
//! ```text
//! WalFile::pwrite(commit frame)
//!        │
//!        ├── inner.pwrite(...)  ──► 本地 NVMe（工作集）
//!        │        └── 回调 complete_local()
//!        │
//!        └── RemoteWalAppender::append(...)  ──► Remote WAL（durability 边界）
//!                 └── 回调 complete_remote()
//!                        │
//!                        ▼
//!            两者都拿到结果后才 settle：
//!            成功 -> parent.complete(本地字节数)
//!            失败 -> parent.error(带平台错误标签的 IO 错误)
//! ```
//!
//! 三条规则不允许被优化掉：
//!
//! * 本地成功 + 远程未确认 ⇒ **不结算**（宁可在 `wait_for_completion` 里等，也不假成功）。
//! * 远程失败 ⇒ parent 失败；调用方拿到的必然是错误。
//! * 结算只发生一次（`Completion` 内部是 `OnceLock`，但状态机自己也要显式保证）。

use std::io::ErrorKind;
use std::sync::Arc;

use domain::error::ErrorCode;
use parking_lot::Mutex;
use turso_core::{Completion, CompletionError};

use crate::error::{LABEL_DURABLE_STOPPED, LABEL_WAL_NOT_DURABLE, LABEL_WAL_REJECTED};

/// 远程 append 的失败原因（保留平台错误码，便于映射回 `ErrorCode`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateFailure {
    /// 平台错误码。
    pub code: ErrorCode,
    /// 人类可读描述（进 `last_error` / 日志）。
    pub message: String,
}

impl GateFailure {
    /// 由平台错误构造。
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// 引擎侧看到的 `CompletionError` 标签：引擎只认识 `LimboError`，
    /// 平台错误码通过标签在 [`crate::error`] 里被还原。
    #[must_use]
    pub fn completion_label(&self) -> &'static str {
        match self.code {
            ErrorCode::WalAppendRejected
            | ErrorCode::EpochMismatch
            | ErrorCode::NotOwner
            | ErrorCode::IdempotencyConflict
            | ErrorCode::InvalidArgument
            | ErrorCode::PermissionDenied
            | ErrorCode::Unauthenticated => LABEL_WAL_REJECTED,
            ErrorCode::StorageUnavailable => LABEL_DURABLE_STOPPED,
            _ => LABEL_WAL_NOT_DURABLE,
        }
    }
}

/// 门的最终结果。
#[derive(Debug)]
pub struct GateOutcome {
    /// 本地写结果（成功时为写入字节数）。
    pub local: Option<Result<i32, CompletionError>>,
    /// 远程 append 结果（成功时为 durable LSN）。
    pub remote: Option<Result<u64, GateFailure>>,
    /// 最终失败原因；成功时为 `None`。
    pub failure: Option<GateFailure>,
    /// 诊断标签（db id + 偏移）。
    pub label: String,
}

impl GateOutcome {
    /// 是否成功（本地写成功且远程 append 已 durable）。
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failure.is_none()
    }
}

#[derive(Debug, Default)]
struct GateState {
    local: Option<Result<i32, CompletionError>>,
    remote: Option<Result<u64, GateFailure>>,
    settled: bool,
}

/// 一次 commit 写入的双边完成门。
#[derive(Debug)]
pub struct CommitGate {
    parent: Completion,
    state: Mutex<GateState>,
    label: String,
}

impl CommitGate {
    /// 用引擎传入的 completion 建立门。
    #[must_use]
    pub fn new(parent: Completion, label: String) -> Arc<Self> {
        Arc::new(Self {
            parent,
            state: Mutex::new(GateState::default()),
            label,
        })
    }

    /// 诊断标签。
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// 是否已结算。
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.state.lock().settled
    }

    /// 记录本地写结果。
    pub fn complete_local(&self, result: Result<i32, CompletionError>) -> Option<GateOutcome> {
        let mut state = self.state.lock();
        if state.local.is_none() {
            state.local = Some(result);
        }
        self.try_settle(&mut state)
    }

    /// 记录远程 append 结果。
    pub fn complete_remote(&self, result: Result<u64, GateFailure>) -> Option<GateOutcome> {
        let mut state = self.state.lock();
        if state.remote.is_none() {
            state.remote = Some(result);
        }
        self.try_settle(&mut state)
    }

    /// 外部原因（远程 append 超时、WAL 代被重置、IO 已 fail-stop）导致必然失败。
    ///
    /// 只在门尚未结算时生效；已经结算过的门不再受外部影响。
    pub fn fail(&self, failure: GateFailure) -> Option<GateOutcome> {
        let mut state = self.state.lock();
        if state.remote.is_none() {
            state.remote = Some(Err(failure));
        }
        self.try_settle(&mut state)
    }

    fn try_settle(&self, state: &mut GateState) -> Option<GateOutcome> {
        if state.settled {
            return None;
        }
        let local = state.local?;
        let failure = match &local {
            Err(err) => Some(GateFailure::new(
                ErrorCode::StorageUnavailable,
                format!("本地 WAL 写入失败：{err}"),
            )),
            Ok(_) => match &state.remote {
                // 本地成功后**必须**等远程确认：这里是 durability 契约的守门点
                None => return None,
                Some(Ok(_)) => None,
                Some(Err(failure)) => Some(failure.clone()),
            },
        };

        state.settled = true;
        let outcome = GateOutcome {
            local: Some(local),
            remote: state.remote.clone(),
            failure: failure.clone(),
            label: self.label.clone(),
        };

        match (failure, local) {
            (None, Ok(written)) => self.parent.complete(written),
            (Some(failure), _) => self.parent.error(CompletionError::IOError(
                ErrorKind::Other,
                failure.completion_label(),
            )),
            // 不可达：failure 为 None 时 local 一定是 Ok
            (None, Err(err)) => self.parent.error(err),
        }
        Some(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn gate() -> (Arc<CommitGate>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let parent = Completion::new_write(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        (CommitGate::new(parent, "test".into()), calls)
    }

    #[test]
    fn local_success_alone_never_settles() {
        let (gate, calls) = gate();
        assert!(gate.complete_local(Ok(4096)).is_none());
        assert!(!gate.is_settled(), "本地成功不得单独结算");
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let outcome = gate.complete_remote(Ok(8192)).expect("远程确认后必须结算");
        assert!(outcome.is_success());
        assert!(gate.is_settled());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "parent 只允许完成一次");
        assert!(!gate.parent.failed());
    }

    #[test]
    fn remote_failure_fails_parent_even_when_local_succeeded() {
        let (gate, calls) = gate();
        assert!(gate.complete_local(Ok(4096)).is_none());
        let outcome = gate
            .complete_remote(Err(GateFailure::new(
                ErrorCode::WalNotDurable,
                "quorum 未确认",
            )))
            .expect("远程失败必须立即结算");
        assert!(!outcome.is_success());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(gate.parent.failed(), "远程失败时 parent 必须是失败状态");
        assert_eq!(
            gate.parent.get_error(),
            Some(CompletionError::IOError(
                ErrorKind::Other,
                LABEL_WAL_NOT_DURABLE
            ))
        );
    }

    #[test]
    fn remote_success_then_local_failure_still_fails() {
        let (gate, _calls) = gate();
        assert!(gate.complete_remote(Ok(4096)).is_none());
        let outcome = gate
            .complete_local(Err(CompletionError::IOError(ErrorKind::Other, "pwrite")))
            .expect("本地失败必须立即结算");
        assert!(!outcome.is_success());
        assert!(gate.parent.failed());
    }

    #[test]
    fn timeout_failure_is_terminal_and_only_once() {
        let (gate, _calls) = gate();
        // 远程 append 只在本地写完成之后才会发生，因此本地结果先到。
        assert!(gate.complete_local(Ok(4096)).is_none());
        let first = gate.fail(GateFailure::new(ErrorCode::WalNotDurable, "append 超时"));
        assert!(first.is_some());
        assert!(gate
            .fail(GateFailure::new(ErrorCode::WalNotDurable, "第二次"))
            .is_none());
        // 结算之后到达的远程成功不得把失败翻回成功
        assert!(gate.complete_remote(Ok(1)).is_none());
        assert!(gate.parent.failed());
    }

    #[test]
    fn fencing_failure_uses_rejected_label() {
        let (gate, _calls) = gate();
        gate.complete_local(Ok(1));
        gate.complete_remote(Err(GateFailure::new(
            ErrorCode::WalAppendRejected,
            "epoch 过期",
        )));
        assert_eq!(
            gate.parent.get_error(),
            Some(CompletionError::IOError(
                ErrorKind::Other,
                LABEL_WAL_REJECTED
            ))
        );
    }
}
