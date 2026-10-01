//! 握手身份的 fencing 判定（架构 §11.3）。
//!
//! 规则只有一条：**本进程只承认自己 `owner_epoch` 对应的所有权**。
//! 一旦 Dispatcher 的 epoch 与本进程不一致（无论它更高还是更低），说明调度器已经
//! 把该库交给了别人（或本进程是暂停后被唤醒的僵尸），继续服务就会产生两个写 Owner
//! 同时写同一份 WAL —— 因此必须**立即退出**，由容器编排重新拉起正确的一代。
//!
//! 这里刻意做成纯函数：进程退出动作无法在单测里观察，但"什么情况下必须退出"必须可测。

use protocol::runtime_local as rt;

/// 握手判定结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceVerdict {
    /// 身份一致，可以继续服务。
    Accept,
    /// 身份不一致或对端明确拒绝：必须立即退出。
    Fenced {
        /// 退出原因（写日志与退出码上下文）。
        reason: String,
    },
}

/// 校验 Dispatcher 回的 `HelloAck`。
///
/// `dispatcher_epoch == 0` 表示对端没有声明 epoch（旧版本 / 本地排障），
/// 此时不做 epoch 比较，只认 `accepted` 字段——不能用 0 去和真实 epoch 比，
/// 否则会把每一个合法的握手都判成 fencing。
#[must_use]
pub fn verify_hello_ack(ack: &rt::HelloAck, owner_epoch: u64) -> FenceVerdict {
    if !ack.accepted {
        return FenceVerdict::Fenced {
            reason: if ack.reject_reason.is_empty() {
                "Dispatcher 拒绝了握手（未给出原因）".to_string()
            } else {
                format!("Dispatcher 拒绝了握手：{}", ack.reject_reason)
            },
        };
    }
    if ack.dispatcher_epoch != 0 && ack.dispatcher_epoch != owner_epoch {
        return FenceVerdict::Fenced {
            reason: format!(
                "epoch 不一致：dispatcher={} 本进程={owner_epoch}",
                ack.dispatcher_epoch
            ),
        };
    }
    FenceVerdict::Accept
}

/// 校验对端主动发来的 `Hello`（DB Process 主动握手时不会收到这种情况，但必须支持）。
#[must_use]
pub fn verify_peer_hello(hello: &rt::Hello, database_id: &str, owner_epoch: u64) -> FenceVerdict {
    if hello.database_id != database_id {
        return FenceVerdict::Fenced {
            reason: format!(
                "database_id 不一致：对端={} 本进程={database_id}",
                hello.database_id
            ),
        };
    }
    if hello.owner_epoch != owner_epoch {
        return FenceVerdict::Fenced {
            reason: format!(
                "epoch 不一致：对端={} 本进程={owner_epoch}",
                hello.owner_epoch
            ),
        };
    }
    FenceVerdict::Accept
}

#[cfg(test)]
mod tests {
    /// 测试辅助：judgement 是否放行。
    trait IsAccept {
        fn is_accept_for_test(&self) -> bool;
    }

    impl IsAccept for FenceVerdict {
        fn is_accept_for_test(&self) -> bool {
            matches!(self, FenceVerdict::Accept)
        }
    }

    use super::*;

    fn ack(accepted: bool, epoch: u64, reason: &str) -> rt::HelloAck {
        rt::HelloAck {
            accepted,
            worker_id: "worker-1".to_string(),
            dispatcher_epoch: epoch,
            reject_reason: reason.to_string(),
        }
    }

    #[test]
    fn accepts_matching_epoch() {
        assert!(verify_hello_ack(&ack(true, 7, ""), 7).is_accept_for_test());
    }

    /// epoch 为 0 = 对端未声明，不做比较。
    #[test]
    fn accepts_unspecified_dispatcher_epoch() {
        assert!(verify_hello_ack(&ack(true, 0, ""), 7).is_accept_for_test());
    }

    /// 被拒绝 -> 必须退出。
    #[test]
    fn rejects_when_not_accepted() {
        let verdict = verify_hello_ack(&ack(false, 7, "会话已被接管"), 7);
        assert!(!verdict.is_accept_for_test(), "accepted=false 必须 fencing");
        let FenceVerdict::Fenced { reason } = verdict else {
            unreachable!("已断言不是 Accept")
        };
        assert!(reason.contains("会话已被接管"), "{reason}");
    }

    /// 对端 epoch 更高（本进程已被取代）-> 必须退出。
    #[test]
    fn rejects_higher_epoch() {
        assert!(!verify_hello_ack(&ack(true, 8, ""), 7).is_accept_for_test());
    }

    /// 对端 epoch 更低（本进程持有的是更新的一代）同样退出：
    /// 双方对"谁是 Owner"没有共识时，唯一安全的动作是停下来让调度器裁决。
    #[test]
    fn rejects_lower_epoch() {
        assert!(!verify_hello_ack(&ack(true, 6, ""), 7).is_accept_for_test());
    }

    #[test]
    fn rejects_peer_hello_with_other_identity() {
        let hello = rt::Hello {
            database_id: "db-other".to_string(),
            owner_epoch: 7,
            ..Default::default()
        };
        assert!(!verify_peer_hello(&hello, "db-1", 7).is_accept_for_test());

        let hello = rt::Hello {
            database_id: "db-1".to_string(),
            owner_epoch: 9,
            ..Default::default()
        };
        assert!(!verify_peer_hello(&hello, "db-1", 7).is_accept_for_test());

        let hello = rt::Hello {
            database_id: "db-1".to_string(),
            owner_epoch: 7,
            ..Default::default()
        };
        assert!(verify_peer_hello(&hello, "db-1", 7).is_accept_for_test());
    }
}
