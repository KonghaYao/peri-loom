//! Remote WAL 端到端 durability 验证（架构 §11 / §16「Commit Durability / Remote WAL」）。
//!
//! 为什么必须有这一层测试：单元测试只验证了状态机的纯逻辑，而本服务最核心的承诺
//! ——「Append 返回成功 == 已 quorum durable」——只有在 **真实起进程 + 真实 gRPC +
//! 真实 raft 复制** 的链路上才成立。这里用一个单节点 Raft Group 覆盖该链路：
//!
//!   1. 写者确立（SetOwnerEpoch）-> Append -> ReadRange 能读回完全相同的字节；
//!   2. Storage-level Fencing：旧 owner_epoch 的 Append 必须被拒绝（架构 §11.3）；
//!   3. 幂等：同一 append_id 重复 Append 只生效一次，且不得重复推进 durable LSN；
//!   4. 覆盖已 durable 区间必须被拒绝（持久化数据不可被改写）。
//!
//! 单节点集群同样经过完整的 Raft 提案/提交路径，因此上面的语义与三副本部署一致。

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use domain::{DatabaseId, Lsn, WorkerId};
use wal_client::{AppendWalRequest, WalClient, WalClientConfig};

use crate::config::WalConfig;
use crate::runtime::{self, WalNode};

/// 启动一个单节点 wal-service；所有监听端口都用 0，由内核分配，避免测试间冲突。
async fn start_single_node(dir: &Path) -> WalNode {
    let config = Arc::new(WalConfig::parse_from([
        "wal-service",
        "--node-id",
        "1",
        "--shard-id",
        "shard-0",
        "--listen",
        "127.0.0.1:0",
        "--peer-listen",
        "127.0.0.1:0",
        "--ops-listen",
        "127.0.0.1:0",
        "--data-dir",
        dir.to_str().expect("临时目录必须是合法 UTF-8"),
        "--append-timeout-ms",
        "5000",
        // 缩短选举周期，避免测试等待默认的 1s 选举超时
        "--tick-interval-ms",
        "50",
        "--election-tick",
        "4",
        "--heartbeat-tick",
        "1",
    ]));
    runtime::start(config).await.expect("单节点 WAL 必须能启动")
}

fn connect(node: &WalNode) -> WalClient {
    WalClient::new(WalClientConfig {
        endpoints: vec![format!("http://{}", node.grpc_addr)],
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        max_attempts: 8,
        ..Default::default()
    })
    .expect("构造 WalClient")
}

/// 等待本节点选出 leader 并确立写者；返回生效的 epoch。
async fn establish_writer(client: &WalClient, db: &DatabaseId, epoch: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(20);
    let worker = WorkerId::new("worker-1");
    loop {
        match client.set_owner_epoch(db, epoch, &worker).await {
            Ok(applied) => return applied,
            Err(err) => {
                if Instant::now() >= deadline {
                    panic!("20s 内没能确立写者（leader 未选出？）：{err}");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_is_durable_readable_and_fenced() {
    let dir = tempfile::tempdir().expect("临时目录");
    let node = start_single_node(dir.path()).await;
    let client = connect(&node);
    let db = DatabaseId::new_v7();

    // ---- 1) 确立写者后 Append，返回值即 quorum durable 的承诺 ----
    let epoch = establish_writer(&client, &db, 1).await;
    assert_eq!(epoch, 1, "首次 SetOwnerEpoch 应生效 epoch 1");

    let payload = b"WAL-HEADER+fake-frame".to_vec();
    let first = client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: epoch,
            start_lsn: Lsn::new(0),
            file_offset: 0,
            reset_wal: true,
            bytes: bytes::Bytes::from(payload.clone()),
            contains_commit_frame: true,
            append_id: "append-1".into(),
        })
        .await
        .expect("Append 必须成功");
    assert!(!first.deduplicated, "首次 Append 不应被判为重复");
    assert_eq!(
        first.durable_lsn.get(),
        payload.len() as u64,
        "durable_lsn 必须等于 start_lsn + 本批次字节数"
    );
    assert!(
        !first.acked_replicas.is_empty(),
        "必须至少有一个副本确认（单节点下即自身）"
    );

    // ---- 2) ReadRange 必须读回完全一致的字节与偏移语义 ----
    let segments = client
        .read_range(&db, Lsn::new(0), first.durable_lsn)
        .await
        .expect("ReadRange 必须成功");
    let mut restored = Vec::new();
    let mut cursor = Lsn::new(0);
    for segment in &segments {
        assert_eq!(segment.start_lsn, cursor, "回放必须按 LSN 连续推进");
        cursor = Lsn::new(cursor.get() + segment.data.len() as u64);
        restored.extend_from_slice(&segment.data);
    }
    assert_eq!(restored, payload, "回放的字节必须与写入完全一致");
    assert_eq!(
        segments[0].file_offset, 0,
        "首个段的 file_offset 必须原样保留（恢复端据此定位写入偏移）"
    );
    assert!(segments[0].reset_wal, "reset_wal 语义必须随段持久化");

    // ---- 3) 幂等：同一 append_id + 同一 start_lsn 重复提交 ----
    let replay = client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: epoch,
            start_lsn: Lsn::new(0),
            file_offset: 0,
            reset_wal: true,
            bytes: bytes::Bytes::from(payload.clone()),
            contains_commit_frame: true,
            append_id: "append-1".into(),
        })
        .await
        .expect("重复 Append 不应报错");
    assert!(replay.deduplicated, "重复 Append 必须被识别为幂等命中");
    assert_eq!(
        replay.durable_lsn.get(),
        payload.len() as u64,
        "幂等命中不得重复推进 durable LSN"
    );

    // ---- 4) Storage-level Fencing：旧 epoch 的 Append 必须被拒 ----
    let epoch2 = establish_writer(&client, &db, 2).await;
    assert_eq!(epoch2, 2);

    let fenced = client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: epoch, // 故意使用已被取代的旧 epoch
            start_lsn: first.durable_lsn,
            file_offset: payload.len() as u64,
            reset_wal: false,
            bytes: bytes::Bytes::from_static(b"stale-owner-write"),
            contains_commit_frame: true,
            append_id: "append-stale".into(),
        })
        .await
        .expect_err("旧 owner_epoch 的 Append 必须被拒绝");
    assert_eq!(
        fenced.code,
        domain::ErrorCode::WalAppendRejected,
        "fencing 拒绝必须映射为 WalAppendRejected（错误语义正确率 100%）：{fenced}"
    );

    // ---- 5) 新 epoch 可以继续写，且被 fencing 拒绝的批次没有污染存储 ----
    let after = client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: epoch2,
            start_lsn: first.durable_lsn,
            file_offset: payload.len() as u64,
            reset_wal: false,
            bytes: bytes::Bytes::from_static(b"new-owner"),
            contains_commit_frame: true,
            append_id: "append-2".into(),
        })
        .await
        .expect("新 epoch 必须能继续写");
    assert_eq!(after.durable_lsn.get(), payload.len() as u64 + 9);

    let all = client
        .read_range(&db, Lsn::new(0), after.durable_lsn)
        .await
        .expect("ReadRange 必须成功");
    let total: usize = all.iter().map(|segment| segment.data.len()).sum();
    assert_eq!(
        total,
        payload.len() + 9,
        "被 fencing 拒绝的批次绝不能进入存储"
    );

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_append_id_for_same_range_is_rejected() {
    let dir = tempfile::tempdir().expect("临时目录");
    let node = start_single_node(dir.path()).await;
    let client = connect(&node);
    let db = DatabaseId::new_v7();
    let epoch = establish_writer(&client, &db, 1).await;

    client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: epoch,
            start_lsn: Lsn::new(0),
            file_offset: 0,
            reset_wal: true,
            bytes: bytes::Bytes::from_static(b"first-batch"),
            contains_commit_frame: true,
            append_id: "ap-a".into(),
        })
        .await
        .expect("首次写入");

    // 用不同的 append_id 覆盖同一段 LSN：必须被拒绝，否则已 durable 的数据会被改写
    let overwritten = client
        .append(AppendWalRequest {
            database_id: db,
            owner_epoch: epoch,
            start_lsn: Lsn::new(0),
            file_offset: 0,
            reset_wal: true,
            bytes: bytes::Bytes::from_static(b"overwrite!"),
            contains_commit_frame: true,
            append_id: "ap-b".into(),
        })
        .await
        .expect_err("覆盖已 durable 区间必须被拒绝");
    // 这里必须是 InvalidArgument 而不是 WalAppendRejected：
    //   - WalAppendRejected 表示「本写者已失去所有权（epoch 过期）」，客户端应重新获取 Owner；
    //   - 本场景是客户端自己算错了 LSN（发来了已被覆盖的区间），属于请求参数错误，
    //     重试或重新取 Owner 都不会让请求变合法 —— 语义必须区分，否则会误导客户端。
    assert_eq!(
        overwritten.code,
        domain::ErrorCode::InvalidArgument,
        "覆盖已 durable 区间必须映射为 InvalidArgument：{overwritten}"
    );

    node.shutdown().await;
}
