//! SetOwnerEpoch / Trim / Status / Health 契约测试。

mod mock;

use domain::{DatabaseId, ErrorCode, Lsn, WorkerId};
use mock::{client, dead_endpoint, spawn, EpochAction, TrimAction};
use protocol::common::ErrorCode as Proto;

#[tokio::test]
async fn set_owner_epoch_returns_applied_epoch() {
    let server = spawn("wal-1").await;
    server.mock.script_epoch(vec![EpochAction::Ok(836)]);

    let db = DatabaseId::new_v7();
    let worker = WorkerId::new("worker-17");
    let client = client(vec![server.endpoint.clone()], 2);

    let applied = client
        .set_owner_epoch(&db, 836, &worker)
        .await
        .expect("设置 epoch 成功");
    assert_eq!(applied, 836);

    let seen = server.mock.epochs();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].database_id, db.to_string());
    assert_eq!(seen[0].owner_epoch, 836);
    assert_eq!(seen[0].worker_id, "worker-17");
    assert!(seen[0].context.is_some());
}

#[tokio::test]
async fn set_owner_epoch_rejected_is_fencing_and_not_retried() {
    let server = spawn("wal-1").await;
    let other = spawn("wal-2").await;
    server.mock.script_epoch(vec![EpochAction::Err {
        code: Proto::EpochMismatch as i32,
        message: "epoch 回退被拒绝",
        detail_json: "",
        applied_epoch: 900,
    }]);

    let client = client(vec![server.endpoint.clone(), other.endpoint.clone()], 4);
    let error = client
        .set_owner_epoch(&DatabaseId::new_v7(), 836, &WorkerId::new("worker-17"))
        .await
        .expect_err("epoch 被拒绝必须失败");

    assert_eq!(error.code, ErrorCode::WalAppendRejected);
    assert_eq!(server.mock.epochs().len(), 1, "fencing 拒绝不得重试");
    assert_eq!(other.mock.epochs().len(), 0);

    let detail = error.detail.expect("detail");
    assert_eq!(detail["current_epoch"], serde_json::json!(900));
    assert_eq!(detail["requested_epoch"], serde_json::json!(836));
}

/// set_owner_epoch 的重试同样必须重新生成 deadline（与 append 一致的缺陷与修法）。
#[tokio::test]
async fn set_owner_epoch_retry_carries_fresh_deadline() {
    let server = spawn("wal-1").await;
    // 第一次：fence 未生效（applied_epoch 低于请求值）-> 换端点重试；mock 用同一个脚本
    // 的第二条命中成功
    server
        .mock
        .script_epoch(vec![EpochAction::Ok(700), EpochAction::Ok(836)]);

    let client = client(vec![server.endpoint.clone()], 2);
    let applied = client
        .set_owner_epoch(&DatabaseId::new_v7(), 836, &WorkerId::new("worker-17"))
        .await
        .expect("第二次尝试必须成功");
    assert_eq!(applied, 836);

    let seen = server.mock.epochs();
    assert_eq!(seen.len(), 2, "第一次 fence 未生效必须重试");
    assert!(
        seen[1].deadline_unix_ms > seen[0].deadline_unix_ms,
        "重试的 deadline ({}ms) 必须晚于首次 ({}ms)",
        seen[1].deadline_unix_ms,
        seen[0].deadline_unix_ms
    );
}

#[tokio::test]
async fn set_owner_epoch_fence_not_applied_is_retried_then_fails() {
    let server = spawn("wal-1").await;
    // 服务端回报的 applied_epoch 低于请求值：fence 没生效，绝不能告诉调用方「已接管」
    server.mock.script_epoch(vec![EpochAction::Ok(700)]);

    let client = client(vec![server.endpoint.clone()], 2);
    let error = client
        .set_owner_epoch(&DatabaseId::new_v7(), 836, &WorkerId::new("worker-17"))
        .await
        .expect_err("fence 未生效时必须失败");

    assert_eq!(error.code, ErrorCode::WalNotDurable);
    assert_eq!(server.mock.epochs().len(), 2, "必须重试到尝试上限");
}

#[tokio::test]
async fn set_owner_epoch_transport_failure_is_bounded() {
    let client = client(vec![dead_endpoint(), dead_endpoint()], 2);
    let error = client
        .set_owner_epoch(&DatabaseId::new_v7(), 1, &WorkerId::new("worker-1"))
        .await
        .expect_err("端点不可达必须失败");
    assert_eq!(error.code, ErrorCode::WalNotDurable);
}

#[tokio::test]
async fn trim_before_passes_snapshot_id() {
    let server = spawn("wal-1").await;
    let db = DatabaseId::new_v7();
    let client = client(vec![server.endpoint.clone()], 2);

    client
        .trim_before(&db, Lsn::new(4096), "snap-1")
        .await
        .expect("截断成功");

    let seen = server.mock.trims();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].lsn, 4096);
    assert_eq!(seen[0].snapshot_id, "snap-1");
}

#[tokio::test]
async fn trim_before_requires_snapshot_id_without_rpc() {
    let server = spawn("wal-1").await;
    let client = client(vec![server.endpoint.clone()], 1);

    let error = client
        .trim_before(&DatabaseId::new_v7(), Lsn::new(1), "   ")
        .await
        .expect_err("缺 snapshot_id 必须本地拦下");
    assert_eq!(error.code, ErrorCode::InvalidArgument);
    assert!(server.mock.trims().is_empty(), "参数不合法时不得发出 RPC");
}

#[tokio::test]
async fn trim_before_retries_on_not_leader() {
    let follower = spawn("wal-1").await;
    let leader = spawn("wal-2").await;
    follower.mock.script_trim(vec![TrimAction::Err {
        code: Proto::WalNotLeader as i32,
        message: "not leader",
    }]);

    let client = client(vec![follower.endpoint.clone(), leader.endpoint.clone()], 3);
    client
        .trim_before(&DatabaseId::new_v7(), Lsn::new(100), "snap-2")
        .await
        .expect("换端点后截断成功");

    assert_eq!(follower.mock.trims().len(), 1);
    assert_eq!(leader.mock.trims().len(), 1);
    assert_eq!(leader.mock.trims()[0].snapshot_id, "snap-2");
}

#[tokio::test]
async fn status_maps_proto_fields() {
    let server = spawn("wal-1").await;
    let client = client(vec![server.endpoint.clone()], 1);

    let status = client
        .status(&DatabaseId::new_v7())
        .await
        .expect("查询状态成功");
    assert!(status.has_data);
    assert_eq!(status.first_lsn, Lsn::new(10));
    assert_eq!(status.last_lsn, Lsn::new(20));
    assert_eq!(status.owner_epoch, 7);
}

#[tokio::test]
async fn status_reports_server_error_code() {
    let server = spawn("wal-1").await;
    let client = client(vec![server.endpoint.clone()], 2);
    // 默认 mock 返回成功；这里改写成错误码：DB 不存在属于终态，不得重试
    server
        .mock
        .set_status_error(Proto::DbNotFound as i32, "db 不存在");

    let error = client
        .status(&DatabaseId::new_v7())
        .await
        .expect_err("服务端报错必须失败");
    assert_eq!(error.code, ErrorCode::DbNotFound);
    assert_eq!(server.mock.status_calls(), 1, "终态错误不得重试");
}

#[tokio::test]
async fn health_prefers_leader_view() {
    let follower = spawn("wal-1").await;
    let leader = spawn("wal-2").await;
    follower.mock.set_health(false, true, "wal-2");
    leader.mock.set_health(true, true, "wal-2");

    let client = client(vec![follower.endpoint.clone(), leader.endpoint.clone()], 4);
    let health = client.health().await.expect("健康检查成功");

    assert!(health.healthy);
    assert!(health.is_leader);
    assert_eq!(health.leader_id, "wal-2");
    assert_eq!(health.node_id, "wal-2");
    assert_eq!(health.term, 5);
}

#[tokio::test]
async fn health_with_only_followers_reports_not_healthy() {
    let follower = spawn("wal-1").await;
    follower.mock.set_health(false, true, "wal-9");

    let client = client(vec![follower.endpoint.clone()], 1);
    let health = client.health().await.expect("副本可应答");

    assert!(
        !health.healthy,
        "没有 leader 时写入路径不可用，绝不能报 healthy"
    );
    assert!(!health.is_leader);
    assert_eq!(health.leader_id, "wal-9", "要保留副本给出的 leader 视图");
}

#[tokio::test]
async fn health_without_any_reachable_endpoint_returns_error() {
    let client = client(vec![dead_endpoint(), dead_endpoint()], 1);
    let error = client.health().await.expect_err("端点全挂时必须失败");
    assert_eq!(error.code, ErrorCode::WalNotDurable);
}

#[tokio::test]
async fn health_probes_every_endpoint_even_beyond_max_attempts() {
    let first = spawn("wal-1").await;
    let second = spawn("wal-2").await;
    first.mock.set_health(false, true, "wal-2");
    second.mock.set_health(true, true, "wal-2");

    // max_attempts=1，但健康检查必须覆盖到第二个端点才能找到 leader
    let client = client(vec![first.endpoint.clone(), second.endpoint.clone()], 1);
    let health = client.health().await.expect("health 成功");
    assert!(
        health.is_leader,
        "health 不应受 max_attempts 限制而漏掉 leader"
    );
}
