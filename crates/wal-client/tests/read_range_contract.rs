//! ReadRange streaming 契约测试（架构 §11.2 failover replay）。

mod mock;

use domain::{DatabaseId, ErrorCode, Lsn};
use mock::{client, ReadStep};
use protocol::common::ErrorCode as Proto;
use tonic::Code;
use wal_client::AppendWalRequest;

/// 每个数据块 100 字节：客户端会校验「拼接长度 == to - from」，因此 mock 的流必须
/// 与请求区间自洽（长度对不上会被判为截断）。
const A100: [u8; 100] = [b'a'; 100];
const B100: [u8; 100] = [b'b'; 100];
const C100: [u8; 100] = [b'c'; 100];

#[tokio::test]
async fn read_range_consumes_stream_and_sorts_by_start_lsn() {
    let server = mock::spawn("wal-1").await;
    // 服务端乱序下发（异常实现的防御性测试）：客户端必须按 start_lsn 升序返回
    server.mock.script_read(vec![vec![
        ReadStep::Chunk {
            start_lsn: 300,
            file_offset: 300,
            reset_wal: true,
            data: &C100,
        },
        ReadStep::chunk(100, 100, &A100),
        ReadStep::chunk(200, 200, &B100),
        ReadStep::Last { start_lsn: 400 },
    ]]);

    let client = client(vec![server.endpoint.clone()], 1);
    let segments = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(400))
        .await
        .expect("读取成功");

    let lsns: Vec<u64> = segments.iter().map(|s| s.start_lsn.get()).collect();
    assert_eq!(lsns, [100, 200, 300]);
    assert_eq!(segments[0].file_offset, 100);
    assert_eq!(segments[2].file_offset, 300);
    assert!(
        segments[2].reset_wal,
        "reset_wal 必须原样透传（回放前要先截断本地 WAL）"
    );
    assert!(!segments[0].reset_wal);
    assert_eq!(segments[1].data, bytes::Bytes::from_static(&B100));
}

#[tokio::test]
async fn read_range_resumes_after_mid_stream_failure() {
    let broken = mock::spawn("wal-1").await;
    let healthy = mock::spawn("wal-2").await;

    // wal-1：先给出 100/200 两段，随后流中断
    broken.mock.script_read(vec![vec![
        ReadStep::chunk(100, 100, &A100),
        ReadStep::chunk(200, 200, &B100),
        ReadStep::Fail(Code::Unavailable, "replica restarted"),
    ]]);
    // wal-2：补齐剩余区间
    healthy.mock.script_read(vec![vec![
        ReadStep::chunk(200, 200, &B100),
        ReadStep::chunk(300, 300, &C100),
        ReadStep::Last { start_lsn: 400 },
    ]]);

    let client = client(vec![broken.endpoint.clone(), healthy.endpoint.clone()], 3);
    let segments = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(400))
        .await
        .expect("换端点续读后必须成功");

    let lsns: Vec<u64> = segments.iter().map(|s| s.start_lsn.get()).collect();
    assert_eq!(lsns, [100, 200, 300], "续读不得产生重复段");
    assert_eq!(segments[1].data, bytes::Bytes::from_static(&B100));
    assert_eq!(
        segments.iter().map(|s| s.data.len()).sum::<usize>(),
        300,
        "三段合起来必须恰好覆盖 [100, 400)"
    );

    let resumed = healthy.mock.reads();
    assert_eq!(resumed.len(), 1);
    assert_eq!(
        resumed[0].start_lsn, 200,
        "续读起点必须是最后一段的 start_lsn（重读并覆盖）"
    );
}

#[tokio::test]
async fn read_range_rotates_on_chunk_level_not_leader() {
    let follower = mock::spawn("wal-1").await;
    let leader = mock::spawn("wal-2").await;

    // chunk 里带回 in-band 错误（服务端在流中途才发现自己不是 leader），并给出 leader 提示
    follower.mock.script_read(vec![vec![ReadStep::ErrorChunk {
        code: Proto::WalNotLeader as i32,
        message: "not leader",
        detail_json: format!("{{\"leader_endpoint\":\"{}\"}}", leader.endpoint),
    }]]);
    leader.mock.script_read(vec![vec![
        ReadStep::chunk(100, 100, &A100),
        ReadStep::Last { start_lsn: 200 },
    ]]);

    let client = client(vec![follower.endpoint.clone(), leader.endpoint.clone()], 3);
    let segments = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(200))
        .await
        .expect("chunk 级 NOT_LEADER 也应换端点重试");
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].data.len(), 100, "必须恰好覆盖 [100, 200)");
    assert_eq!(leader.mock.read_calls(), 1);
}

#[tokio::test]
async fn read_range_all_endpoints_down_returns_wal_not_durable() {
    let client = client(vec![mock::dead_endpoint(), mock::dead_endpoint()], 2);
    let error = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(1), Lsn::new(2))
        .await
        .expect_err("端点全挂时必须失败");
    assert_eq!(error.code, ErrorCode::WalNotDurable);
}

/// 流「正常结束」但少给了数据：必须以失败告终，绝不能把不完整区间当成读成功。
///
/// 这是 read_range 最危险的失败模式 —— 调用方（failover replay / 冷启动恢复）会把返回的
/// 字节直接回放进本地 WAL，短读且不报错 = 静默丢数据（架构 §11.1 的 RPO 承诺被破坏）。
#[tokio::test]
async fn read_range_rejects_truncated_stream() {
    let server = mock::spawn("wal-1").await;
    // 请求 [100, 400) 共 300 字节，但流只给了 2 字节就结束（没有 last 标记）
    server.mock.script_read(vec![vec![
        ReadStep::chunk(100, 100, b"a"),
        ReadStep::chunk(101, 101, b"b"),
    ]]);

    let client = client(vec![server.endpoint.clone()], 2);
    let error = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(400))
        .await
        .expect_err("截断的流必须报错，而不是返回部分数据");

    assert_eq!(error.code, ErrorCode::WalNotDurable);
    assert!(
        error.message.contains("截断") || error.message.contains("不可信"),
        "错误消息必须体现「数据不完整」：{}",
        error.message
    );
    assert_eq!(
        server.mock.read_calls(),
        2,
        "覆盖校验失败算一次失败，必须重试到尝试上限"
    );
}

/// 有空洞的流（段之间不连续）同样必须被发现：只比总长度的检查会被空洞 + 重叠骗过。
#[tokio::test]
async fn read_range_rejects_stream_with_hole() {
    let server = mock::spawn("wal-1").await;
    // [100,101) + [105,108)：总长 4，但 [101,105) 缺失
    server.mock.script_read(vec![vec![
        ReadStep::chunk(100, 100, b"a"),
        ReadStep::chunk(105, 105, b"bbb"),
        ReadStep::Last { start_lsn: 108 },
    ]]);

    let client = client(vec![server.endpoint.clone()], 2);
    let error = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(104))
        .await
        .expect_err("有空洞的流必须报错");

    assert_eq!(error.code, ErrorCode::WalNotDurable);
    assert!(
        error.message.contains("不连续") || error.message.contains("不可信"),
        "错误消息必须体现「空洞」：{}",
        error.message
    );
}

/// 换端点重试时，覆盖校验针对的是**累积结果**：第二个端点补齐后必须成功。
#[tokio::test]
async fn read_range_recovers_when_next_endpoint_completes_the_range() {
    let truncated = mock::spawn("wal-1").await;
    let healthy = mock::spawn("wal-2").await;

    // wal-1：只给前 2 字节就结束（覆盖校验失败 -> 换端点）
    truncated.mock.script_read(vec![vec![
        ReadStep::chunk(100, 100, b"a"),
        ReadStep::chunk(101, 101, b"b"),
    ]]);
    // wal-2：从客户端续读起点（101 会被重读并覆盖）补齐到 104
    healthy.mock.script_read(vec![vec![
        ReadStep::chunk(101, 101, b"b"),
        ReadStep::chunk(102, 102, b"cc"),
        ReadStep::Last { start_lsn: 104 },
    ]]);

    let client = client(
        vec![truncated.endpoint.clone(), healthy.endpoint.clone()],
        3,
    );
    let segments = client
        .read_range(&DatabaseId::new_v7(), Lsn::new(100), Lsn::new(104))
        .await
        .expect("补齐后必须成功");
    let restored: Vec<u8> = segments
        .iter()
        .flat_map(|segment| segment.data.to_vec())
        .collect();
    assert_eq!(restored, b"abcc");
    assert_eq!(
        healthy.mock.reads()[0].start_lsn,
        101,
        "续读起点 = 已收到数据的末端（重读最后一段并覆盖）"
    );
}

/// 读路径与写路径共享同一个客户端实例（真实调用形态）。
#[tokio::test]
async fn client_reuses_leader_cache_across_calls() {
    let follower = mock::spawn("wal-1").await;
    let leader = mock::spawn("wal-2").await;
    follower
        .mock
        .script_append(vec![mock::AppendAction::err_with_detail(
            Proto::WalNotLeader as i32,
            "not leader",
            format!("{{\"leader_endpoint\":\"{}\"}}", leader.endpoint),
        )]);

    let client = mock::client(vec![follower.endpoint.clone(), leader.endpoint.clone()], 3);
    let db = DatabaseId::new_v7();
    client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: 1,
            start_lsn: Lsn::new(0),
            file_offset: 0,
            reset_wal: false,
            bytes: bytes::Bytes::from_static(b"x"),
            contains_commit_frame: true,
            append_id: "a-1".into(),
        })
        .await
        .expect("第一次 append 成功");

    // 第二次调用应当直接用缓存下来的 leader，不再打 follower
    client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: 1,
            start_lsn: Lsn::new(1),
            file_offset: 1,
            reset_wal: false,
            bytes: bytes::Bytes::from_static(b"y"),
            contains_commit_frame: true,
            append_id: "a-2".into(),
        })
        .await
        .expect("第二次 append 成功");

    assert_eq!(
        follower.mock.append_calls(),
        1,
        "leader 缓存生效后不应再向 follower 试错"
    );
    assert_eq!(leader.mock.append_calls(), 2);
}
