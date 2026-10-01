//! Append durability 契约测试（架构 §11.1 / §11.3）。
//!
//! 这里验证的是「客户端绝不允许把未 durable 的写当成成功」这一条硬约束，
//! 以及重试的幂等前提（append_id 在整轮重试中保持不变）。

mod mock;

use std::time::{Duration, Instant};

use domain::{DatabaseId, ErrorCode, Lsn};
use mock::{
    client, client_with_options, client_with_timeout, dead_endpoint, spawn, AppendAction, ReadStep,
};
use protocol::common::ErrorCode as Proto;
use wal_client::AppendWalRequest;

fn append_request(db: DatabaseId, start_lsn: u64, append_id: &str) -> AppendWalRequest {
    AppendWalRequest {
        database_id: db,
        owner_epoch: 835,
        start_lsn: Lsn::new(start_lsn),
        file_offset: 4096,
        reset_wal: false,
        bytes: bytes::Bytes::from_static(b"wal-frame-payload"),
        contains_commit_frame: true,
        append_id: append_id.to_owned(),
    }
}

#[tokio::test]
async fn append_success_means_quorum_durable() {
    let server = spawn("wal-1").await;
    let db = DatabaseId::new_v7();
    let client = client(vec![server.endpoint.clone()], 3);

    let outcome = client
        .append(append_request(db, 4096, "append-1"))
        .await
        .expect("mock 返回成功时必须 Ok");

    // durable_lsn 必须覆盖本批次的全部字节（start_lsn + len）
    assert_eq!(outcome.durable_lsn, Lsn::new(4096 + 17));
    assert_eq!(outcome.acked_replicas, ["wal-1", "wal-2"]);
    assert!(!outcome.deduplicated);
    assert!(outcome.latency >= Duration::ZERO);

    let seen = server.mock.appends();
    assert_eq!(seen.len(), 1, "成功路径只应打一次 RPC");
    assert_eq!(seen[0].append_id, "append-1");
    assert_eq!(seen[0].owner_epoch, 835);
    assert_eq!(seen[0].wal_file_offset, 4096);
    assert!(seen[0].contains_commit_frame);
    assert!(
        seen[0].context.is_some(),
        "内部 RPC 必须携带 RequestContext"
    );
}

#[tokio::test]
async fn append_retries_on_not_leader_and_keeps_append_id() {
    // wal-1 返回 NOT_LEADER 并提示 leader 是 wal-2；wal-3 是配置里的下一个端点（不可达）
    let follower = spawn("wal-1").await;
    let leader = spawn("wal-2").await;
    let dead = dead_endpoint();

    follower
        .mock
        .script_append(vec![AppendAction::err_with_detail(
            Proto::WalNotLeader as i32,
            "this node is not leader",
            format!("{{\"leader_endpoint\":\"{}\"}}", leader.endpoint),
        )]);

    let db = DatabaseId::new_v7();
    // 配置里只有 follower 与一个不可达端点：能成功只可能是因为用了 leader 提示，
    // 而提示端点不在配置列表内 —— 这正是 `allow_unlisted_leader_hint` 要显式打开的场景
    // （默认关闭时的行为见 `append_ignores_leader_hint_outside_configured_endpoints`）。
    let client = client_with_options(
        vec![follower.endpoint.clone(), dead],
        3,
        Duration::from_millis(500),
        true,
    );

    let outcome = client
        .append(append_request(db, 100, "append-42"))
        .await
        .expect("leader 提示必须被优先尝试");

    assert_eq!(outcome.durable_lsn, Lsn::new(117));

    let first = follower.mock.appends();
    let second = leader.mock.appends();
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    // 幂等前提：两次尝试必须是同一个 append_id
    assert_eq!(first[0].append_id, "append-42");
    assert_eq!(second[0].append_id, "append-42");
    assert_eq!(second[0].start_lsn, first[0].start_lsn);
    assert_eq!(second[0].wal_bytes, first[0].wal_bytes);
}

/// 列表外的 leader 提示默认必须被忽略：写路径不能因为应答里的一段文本就去拨号
/// 一个运维没有配置过的地址（覆盖 FIX-XII 的默认行为）。
#[tokio::test]
async fn append_ignores_leader_hint_outside_configured_endpoints() {
    let follower = spawn("wal-1").await;
    let healthy = spawn("wal-2").await;
    // 提示指向一个**没有**配置进客户端的端点
    let unlisted = spawn("wal-3").await;

    follower
        .mock
        .script_append(vec![AppendAction::err_with_detail(
            Proto::WalNotLeader as i32,
            "not leader",
            format!("{{\"leader_endpoint\":\"{}\"}}", unlisted.endpoint),
        )]);

    let client = client(vec![follower.endpoint.clone(), healthy.endpoint.clone()], 2);
    let outcome = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-hint"))
        .await
        .expect("忽略提示后仍应轮换到列表内的下一个端点");

    assert_eq!(outcome.durable_lsn, Lsn::new(17));
    assert_eq!(
        unlisted.mock.append_calls(),
        0,
        "默认不得拨号配置列表之外的提示端点"
    );
    assert_eq!(follower.mock.append_calls(), 1);
    assert_eq!(healthy.mock.append_calls(), 1);
}

#[tokio::test]
async fn append_without_hint_rotates_to_next_endpoint() {
    let first = spawn("wal-1").await;
    let second = spawn("wal-2").await;
    first.mock.script_append(vec![AppendAction::err(
        Proto::WalNotLeader as i32,
        "not leader",
    )]);

    let client = client(vec![first.endpoint.clone(), second.endpoint.clone()], 3);
    client
        .append(append_request(DatabaseId::new_v7(), 0, "append-rotate"))
        .await
        .expect("没有 leader 提示时必须轮换到下一个端点");

    assert_eq!(first.mock.append_calls(), 1);
    assert_eq!(second.mock.append_calls(), 1);
    assert_eq!(second.mock.appends()[0].append_id, "append-rotate");
}

#[tokio::test]
async fn append_all_not_leader_returns_wal_not_leader() {
    let first = spawn("wal-1").await;
    let second = spawn("wal-2").await;
    first.mock.script_append(vec![AppendAction::err(
        Proto::WalNotLeader as i32,
        "not leader",
    )]);
    second.mock.script_append(vec![AppendAction::err(
        Proto::WalNotLeader as i32,
        "not leader",
    )]);

    let client = client(vec![first.endpoint.clone(), second.endpoint.clone()], 2);
    let error = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-x"))
        .await
        .expect_err("全部端点都不是 leader 时必须失败");

    assert_eq!(error.code, ErrorCode::WalNotLeader);
    assert_eq!(first.mock.append_calls(), 1);
    assert_eq!(
        second.mock.append_calls(),
        1,
        "尝试次数必须受 max_attempts 限制"
    );
}

#[tokio::test]
async fn append_transport_failure_returns_wal_not_durable() {
    let client = client(vec![dead_endpoint(), dead_endpoint()], 2);
    let started = Instant::now();
    let error = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-down"))
        .await
        .expect_err("端点不可达时绝不能返回 Ok");

    assert_eq!(error.code, ErrorCode::WalNotDurable);
    // 重试带退避：至少一次退避（5ms）已被等待
    assert!(started.elapsed() >= Duration::from_millis(5));
}

#[tokio::test]
async fn append_timeout_returns_wal_not_durable() {
    let server = spawn("wal-1").await;
    server
        .mock
        .script_append(vec![AppendAction::Stall(Duration::from_secs(5))]);

    // 超时要远大于建链与 HTTP/2 握手耗时：否则在高负载下首个请求可能先因
    // connect_timeout 失败，测到的就不是「请求超时」这条路径（会变成 flaky）。
    let client = client_with_timeout(
        vec![server.endpoint.clone()],
        1,
        Duration::from_millis(1200),
    );
    let error = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-slow"))
        .await
        .expect_err("请求超时必须视为未 durable");

    assert_eq!(error.code, ErrorCode::WalNotDurable);
    assert!(error.message.contains("超时"), "{}", error.message);
}

#[tokio::test]
async fn append_epoch_mismatch_is_not_retried() {
    let server = spawn("wal-1").await;
    let other = spawn("wal-2").await;
    server
        .mock
        .script_append(vec![AppendAction::err_with_detail(
            Proto::EpochMismatch as i32,
            "epoch 已被取代",
            "{\"owner_epoch\":900}",
        )]);

    let client = client(vec![server.endpoint.clone(), other.endpoint.clone()], 4);
    let error = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-fenced"))
        .await
        .expect_err("fencing 拒绝必须失败");

    assert_eq!(error.code, ErrorCode::WalAppendRejected);
    assert!(!error.retryable);
    assert_eq!(
        server.mock.append_calls(),
        1,
        "fencing 拒绝不得重试（重试只会让旧 Owner 继续写）"
    );
    assert_eq!(other.mock.append_calls(), 0, "不得换端点重试");

    let detail = error.detail.expect("fencing detail 必须存在");
    // 统一后的字段名：current_epoch = 服务端权威值，requested_epoch = 本次请求携带值
    assert_eq!(detail["current_epoch"], serde_json::json!(900));
    assert_eq!(detail["requested_epoch"], serde_json::json!(835));
    assert_eq!(detail["server_code"], serde_json::json!("EPOCH_MISMATCH"));
}

#[tokio::test]
async fn append_rejected_by_wal_is_not_retried() {
    let server = spawn("wal-1").await;
    let other = spawn("wal-2").await;
    server.mock.script_append(vec![AppendAction::err(
        Proto::WalAppendRejected as i32,
        "fenced by newer owner",
    )]);

    let client = client(vec![server.endpoint.clone(), other.endpoint.clone()], 4);
    let error = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-rejected"))
        .await
        .expect_err("fencing 拒绝必须失败");

    assert_eq!(error.code, ErrorCode::WalAppendRejected);
    assert_eq!(server.mock.append_calls(), 1);
    assert_eq!(other.mock.append_calls(), 0);
    // 服务端没给 current epoch 时不得编造
    let detail = error.detail.expect("detail");
    assert_eq!(detail["current_epoch"], serde_json::Value::Null);
    assert_eq!(detail["requested_epoch"], serde_json::json!(835));
}

/// 一次重试链里的每次尝试都必须携带**重新生成**的 deadline。
///
/// 复用了首次构造的 deadline 时，重试（必然晚于首次若干毫秒）带上的是已经更接近
/// 甚至已经过期的截止时间；一旦首次尝试走满超时预算，后续尝试的 deadline 已过期，
/// 服务端算出 wait_budget=0 后立即失败 —— 明明 leader 健康却报 WAL_NOT_DURABLE。
#[tokio::test]
async fn append_retry_carries_fresh_deadline_per_attempt() {
    let first = spawn("wal-1").await;
    let second = spawn("wal-2").await;
    first.mock.script_append(vec![AppendAction::err(
        Proto::WalNotLeader as i32,
        "not leader",
    )]);

    let client = client(vec![first.endpoint.clone(), second.endpoint.clone()], 2);
    client
        .append(append_request(DatabaseId::new_v7(), 0, "append-deadline"))
        .await
        .expect("换端点后必须成功");

    let attempts = first.mock.appends();
    let retried = second.mock.appends();
    assert_eq!(attempts.len(), 1);
    assert_eq!(retried.len(), 1);

    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("时钟")
            .as_millis(),
    )
    .expect("ms");
    for request in [&attempts[0], &retried[0]] {
        assert!(
            request.deadline_unix_ms >= now,
            "deadline 必须落在未来：{} < {now}",
            request.deadline_unix_ms
        );
        assert!(
            request.context.is_some(),
            "内部 RPC 必须携带 RequestContext"
        );
    }
    assert!(
        retried[0].deadline_unix_ms > attempts[0].deadline_unix_ms,
        "第二次尝试的 deadline ({}ms) 必须晚于第一次 ({}ms)——重试要重新算 deadline",
        retried[0].deadline_unix_ms,
        attempts[0].deadline_unix_ms
    );
    // context 里的 deadline 与顶层字段必须来自同一个时刻（服务端取更紧的一个）
    let context_deadline = retried[0]
        .context
        .as_ref()
        .map(|context| context.deadline_unix_ms)
        .unwrap_or_default();
    assert!(
        context_deadline >= retried[0].deadline_unix_ms,
        "context.deadline ({context_deadline}ms) 不得早于顶层 deadline（同一次尝试内生成）"
    );
}

/// 幂等冲突是终态：服务端明确说明该 append_id 属于别的区间，换端点重试不会成功。
///
/// 错误码必须原样保留 IDEMPOTENCY_CONFLICT（不能折叠成 WAL_APPEND_REJECTED 的
/// fencing 语义），否则调用方会去重新获取 Owner，而真正的问题是幂等键被复用。
#[tokio::test]
async fn append_idempotency_conflict_is_terminal_and_not_retried() {
    let server = spawn("wal-1").await;
    let other = spawn("wal-2").await;
    server
        .mock
        .script_append(vec![AppendAction::err_with_detail(
            Proto::IdempotencyConflict as i32,
            "append_id 已被 start_lsn=0 使用",
            "{\"append_id\":\"append-dup\",\"recorded_start_lsn\":0,\"requested_start_lsn\":8}",
        )]);

    let client = client(vec![server.endpoint.clone(), other.endpoint.clone()], 4);
    let error = client
        .append(append_request(DatabaseId::new_v7(), 8, "append-dup"))
        .await
        .expect_err("幂等冲突必须失败");

    assert_eq!(error.code, ErrorCode::IdempotencyConflict);
    assert!(!error.retryable, "幂等冲突不得被上层自动重试");
    assert_eq!(server.mock.append_calls(), 1, "不得重试");
    assert_eq!(other.mock.append_calls(), 0, "不得换端点重试");
    let detail = error.detail.expect("detail 必须带冲突区间");
    assert_eq!(detail["recorded_start_lsn"], serde_json::json!(0));
    assert_eq!(detail["requested_start_lsn"], serde_json::json!(8));
}

/// NOT_OWNER 与 epoch 过期同属 fencing：换端点重试携带的还是同一个失效身份。
#[tokio::test]
async fn append_not_owner_is_terminal_and_not_retried() {
    let server = spawn("wal-1").await;
    let other = spawn("wal-2").await;
    server
        .mock
        .script_append(vec![AppendAction::err_with_detail(
            Proto::NotOwner as i32,
            "本节点不是该 DB 的 Owner",
            "{\"current_epoch\":900}",
        )]);

    let client = client(vec![server.endpoint.clone(), other.endpoint.clone()], 4);
    let error = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-not-owner"))
        .await
        .expect_err("NOT_OWNER 必须失败");

    assert_eq!(error.code, ErrorCode::WalAppendRejected);
    assert!(!error.retryable);
    assert_eq!(server.mock.append_calls(), 1, "NOT_OWNER 不得重试");
    assert_eq!(other.mock.append_calls(), 0, "NOT_OWNER 不得换端点重试");
    let detail = error.detail.expect("detail");
    assert_eq!(detail["server_code"], serde_json::json!("NOT_OWNER"));
    assert_eq!(detail["current_epoch"], serde_json::json!(900));
    assert_eq!(detail["requested_epoch"], serde_json::json!(835));
}

#[tokio::test]
async fn append_server_not_durable_maps_to_wal_not_durable() {
    let first = spawn("wal-1").await;
    let second = spawn("wal-2").await;
    // wal-service 在 raft 提案等待超时时返回 WAL_NOT_DURABLE
    first.mock.script_append(vec![AppendAction::err(
        Proto::WalNotDurable as i32,
        "append timeout",
    )]);
    second.mock.script_append(vec![AppendAction::err(
        Proto::WalNotDurable as i32,
        "append timeout",
    )]);

    let client = client(vec![first.endpoint.clone(), second.endpoint.clone()], 2);
    let error = client
        .append(append_request(DatabaseId::new_v7(), 0, "append-nd"))
        .await
        .expect_err("未 durable 绝不能返回 Ok");

    assert_eq!(error.code, ErrorCode::WalNotDurable);
    // 幂等键相同，换端点重试是安全的
    assert_eq!(first.mock.appends()[0].append_id, "append-nd");
    assert_eq!(second.mock.appends()[0].append_id, "append-nd");
}

#[tokio::test]
async fn append_short_durable_lsn_is_never_ok() {
    let server = spawn("wal-1").await;
    // 服务端声称成功，但 durable_lsn 没有覆盖本批次字节：属于未 durable
    server.mock.script_append(vec![AppendAction::Ok {
        durable_lsn: Some(4096),
        deduplicated: false,
        acked: vec!["wal-1".into()],
    }]);

    let client = client(vec![server.endpoint.clone()], 2);
    let error = client
        .append(append_request(DatabaseId::new_v7(), 4096, "append-short"))
        .await
        .expect_err("durable_lsn 未覆盖本批次时不得返回 Ok");

    assert_eq!(error.code, ErrorCode::WalNotDurable);
    assert_eq!(server.mock.append_calls(), 2, "必须重试到尝试上限");
}

#[tokio::test]
async fn append_deduplicated_result_still_means_durable() {
    let server = spawn("wal-1").await;
    server.mock.script_append(vec![AppendAction::Ok {
        durable_lsn: None,
        deduplicated: true,
        acked: vec!["wal-1".into(), "wal-2".into()],
    }]);

    let client = client(vec![server.endpoint.clone()], 1);
    let outcome = client
        .append(append_request(DatabaseId::new_v7(), 100, "append-dup"))
        .await
        .expect("幂等命中同样是已 durable");
    assert!(outcome.deduplicated);
    assert_eq!(outcome.durable_lsn, Lsn::new(117));
}

/// read_range 在 append 测试里只用于确认「mock 也能服务其它 RPC」，
/// 真正的流式契约在 `read_range_contract.rs`。
#[tokio::test]
async fn mock_streams_read_range_chunks() {
    let server = spawn("wal-1").await;
    // 流返回的字节必须恰好覆盖请求区间 [100, 101)：client 会校验覆盖完整性
    server.mock.script_read(vec![vec![
        ReadStep::chunk(100, 0, b"a"),
        ReadStep::Last { start_lsn: 101 },
    ]]);

    let client = client(vec![server.endpoint.clone()], 1);
    let segments = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(101))
        .await
        .expect("读取成功");
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].start_lsn, Lsn::new(100));
    assert!(!segments[0].reset_wal);
}
