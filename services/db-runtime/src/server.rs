//! UDS 服务循环：监听 -> 握手（fencing）-> 逐帧分发 -> 优雅停机。
//!
//! 本进程**只**通过 Unix Domain Socket 服务，不监听任何 TCP 端口（架构 §1.2 / §15.6）。
//!
//! 并发与背压：
//! - 连接内**每帧一个任务**：同一连接上多条请求并行，互不排队；
//! - 写出全部 `await` socket（`FrameWriter` 上一把互斥量保证一帧不被切开），
//!   对端不读 -> 内核缓冲满 -> 本端写入挂起，背压一路贯通到引擎调用；
//! - 读侧以长度前缀为界，单帧上限由 `LocalFrameCodec` 强制（超限即断开，不按声明长度分配）。

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use protocol::framing::LocalFrameCodec;
use protocol::runtime_local as rt;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::Framed;

use crate::config::HANDSHAKE_TIMEOUT;
use crate::dispatch::{self, ConnectionCtx, FrameWriter};
use crate::frame;
use crate::host::{Host, HostState, EXIT_DURABLE_STOP, EXIT_FAILURE, EXIT_OK};

/// 会话 reaper 的扫描周期。
const REAPER_INTERVAL: Duration = Duration::from_millis(500);
/// 父进程存活检查周期。
const PARENT_CHECK_INTERVAL: Duration = Duration::from_secs(2);
/// 优雅停机的等待上限（含在途请求收尾与事务回滚）。
pub const DRAIN_GRACE: Duration = Duration::from_secs(5);
/// durable IO fail-stop 之后的收尾上限。
///
/// 比 [`DRAIN_GRACE`] 短得多：此时进程已经写不了任何东西，收尾只需要让在途请求以明确错误
/// 结束就够；拖长只会推迟 Worker 的自动重启（架构 §12.1 的目标是 1s 级恢复）。
const FAIL_STOP_DRAIN_GRACE: Duration = Duration::from_millis(250);
/// 连接循环的状态轮询周期（防止停机通知被漏掉后连接一直挂着）。
const CONNECTION_POLL: Duration = Duration::from_secs(1);

/// 运行服务直到停机；返回进程退出码。
pub async fn run(host: Arc<Host>) -> Result<i32> {
    let socket_path = host.config.socket_path.clone();
    let listener = bind_socket(&socket_path)?;
    tracing::info!(socket = %socket_path.display(), "DB Process 正在监听 UDS");

    spawn_reaper(Arc::clone(&host));
    spawn_parent_watch(Arc::clone(&host));
    // durable IO fail-stop 的看门狗：本地 WAL 字节流不可信 -> 本进程退出（架构 §12.1）。
    crate::fatal::spawn_watch(Arc::clone(&host));

    // 主循环：接受连接 + 三类停机触发（信号 / Shutdown 帧 / fencing）。
    let mut exit_code = EXIT_OK;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let host = Arc::clone(&host);
                        tokio::spawn(async move { serve_connection(host, stream).await });
                    }
                    Err(err) => {
                        // 单次 accept 失败（如 fd 耗尽）不应终止服务。
                        tracing::warn!(error = %err, "接受 UDS 连接失败");
                    }
                }
            }
            _ = host.shutdown.notified() => {
                exit_code = host.exit_code.load(Ordering::Acquire);
                break;
            }
            signal = wait_for_signal() => {
                tracing::info!(signal, "收到终止信号，开始优雅停机");
                host.begin_drain();
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if host.state() == HostState::Draining {
                    exit_code = host.exit_code.load(Ordering::Acquire);
                    break;
                }
            }
        }
    }

    let grace = if exit_code == EXIT_DURABLE_STOP {
        FAIL_STOP_DRAIN_GRACE
    } else {
        DRAIN_GRACE
    };
    host.drain(grace).await;
    // 摘掉 socket 文件：留下一个指向死进程的 sock 会让 Worker 的重连一直等到超时。
    if let Err(err) = std::fs::remove_file(&socket_path) {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(socket = %socket_path.display(), error = %err, "删除 socket 文件失败");
        }
    }
    Ok(exit_code)
}

/// 绑定 UDS：建目录、清理残留、收紧权限。
fn bind_socket(path: &std::path::Path) -> Result<UnixListener> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("创建 socket 目录失败：{}", dir.display()))?;
        }
    }
    // 上一代进程被 SIGKILL 时会留下 sock 文件；它不承载任何状态，直接删掉重建。
    // 但如果那个路径上是**普通文件**，说明配置错了（比如把 db 路径写成了 socket 路径），
    // 这种情况必须报错而不是删掉别人的文件。
    #[cfg(unix)]
    use std::os::unix::fs::FileTypeExt;
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            std::fs::remove_file(path)
                .with_context(|| format!("删除残留 socket 失败：{}", path.display()))?;
        }
        Ok(_) => {
            return Err(anyhow!(
                "socket 路径已被非 socket 文件占用：{}",
                path.display()
            ));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(anyhow!("检查 socket 路径失败：{}", path.display())).context(err);
        }
    }

    let listener =
        UnixListener::bind(path).with_context(|| format!("绑定 UDS 失败：{}", path.display()))?;
    // 只允许本用户连接：同机上其他租户不应能连进别人的 DB 进程。
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o600);
        std::fs::set_permissions(path, permissions)
            .with_context(|| format!("设置 socket 权限失败：{}", path.display()))?;
    }
    Ok(listener)
}

/// 处理一条连接：握手 -> 读循环。
async fn serve_connection(host: Arc<Host>, stream: UnixStream) {
    let peer = format!(
        "pid={}",
        stream
            .peer_cred()
            .map(|cred| cred.pid().unwrap_or_default())
            .unwrap_or_default()
    );
    let (read_half, write_half) = stream.into_split();
    let mut framed = Framed::new(read_half, LocalFrameCodec::new());
    let mut writer = FrameWriter::new(write_half);

    match handshake(&host, &mut framed, &mut writer).await {
        HandshakeOutcome::Accepted => {}
        HandshakeOutcome::Rejected(reason) => {
            tracing::warn!(peer = %peer, reason = %reason, "握手未完成，关闭连接");
            return;
        }
        HandshakeOutcome::Fenced(reason) => {
            // 所有权存疑：不是"关掉这条连接"，而是"本进程必须消失"。
            host.fenced(&reason);
            return;
        }
    }

    let host_for_ctx = Arc::clone(&host);
    let ctx = ConnectionCtx::new(host_for_ctx, writer, peer.clone());
    host.opened_connections.fetch_add(1, Ordering::AcqRel);
    tracing::info!(peer = %peer, "UDS 连接已建立（握手通过）");

    spawn_notice_forwarder(Arc::clone(&ctx));

    loop {
        if host.state() == HostState::Draining {
            tracing::info!(peer = %peer, "DB Process 正在停机，关闭连接");
            break;
        }
        match tokio::time::timeout(CONNECTION_POLL, framed.next()).await {
            Ok(Some(Ok(request))) => {
                let ctx = Arc::clone(&ctx);
                let in_flight = Arc::clone(&host.in_flight);
                in_flight.fetch_add(1, Ordering::AcqRel);
                // 每帧独立 spawn：同连接上的请求并行处理，互不排队。
                tokio::spawn(async move {
                    dispatch::dispatch(ctx, request).await;
                    in_flight.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Ok(Some(Err(err))) => {
                // 帧边界已经不可恢复（超长 / 非法 protobuf）：只能断开重来。
                tracing::warn!(peer = %peer, error = %err, "读帧失败，断开连接");
                break;
            }
            Ok(None) => {
                tracing::info!(peer = %peer, "对端关闭连接");
                break;
            }
            Err(_) => {
                // 只是这一轮没读到东西，回到循环顶部重新检查停机状态。
            }
        }
    }
}

/// 握手：本进程主动发 `Hello`，也接受对端先发 `Hello`。
/// 握手结果：**必须把对方发来的帧读干净**。
///
/// 双方都是「先发 Hello，再读对方的消息」（见本函数与 Worker 侧 `uds::handshake`），
/// 因此一条连接上会交换两条握手帧，方向各一条：
///
/// ```text
/// Worker:  Hello  ------------------->  DB Process
/// Worker:  HelloAck  <----------------  Hello
/// Worker:  HelloAck  ----------------->  DB Process  (回应它先发的 Hello)
/// Worker:  Hello  <-------------------  HelloAck      (回应我先发的 Hello)
/// ```
///
/// 只要有一侧「收到对方的 Hello 就返回」，对方回应**我发的**那条 HelloAck 就会留在读缓冲里，
/// 被新连接上的**第一个真实请求**当成响应吃掉 —— 而那时请求类型完全对不上，表现为
/// 「DB Process 返回了非预期的响应帧」这种与服务端毫无关系的错误。所以这里必须
/// 等到两条握手帧都读完才返回。
async fn handshake(
    host: &Arc<Host>,
    framed: &mut Framed<tokio::net::unix::OwnedReadHalf, LocalFrameCodec>,
    writer: &mut FrameWriter,
) -> HandshakeOutcome {
    let hello = host.hello();
    let request = rt::Frame {
        seq: 1,
        reply_to_seq: 0,
        request_id: String::new(),
        database_id: host.database_id_text().to_string(),
        owner_epoch: host.config.owner_epoch,
        session_id: String::new(),
        transaction_id: String::new(),
        deadline_unix_ms: 0,
        error: None,
        message: Some(rt::frame::Message::Hello(hello)),
    };
    if let Err(err) = writer.send(&request).await {
        return HandshakeOutcome::Rejected(format!("发送 Hello 失败：{err}"));
    }

    let deadline = tokio::time::Instant::now() + HANDSHAKE_TIMEOUT;
    // 已校验并应答过对端的 Hello（此时仍缺「对端回应我们 Hello 的 HelloAck」）。
    let mut peer_hello_seen = false;
    // 允许跳过少量非握手帧（对端可能先推一条通知）。
    for _ in 0..8 {
        let next = match tokio::time::timeout_at(deadline, framed.next()).await {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(err))) => {
                return HandshakeOutcome::Rejected(format!("握手期间读帧失败：{err}"));
            }
            Ok(None) => {
                return HandshakeOutcome::Rejected("对端在握手完成前关闭了连接".to_string());
            }
            Err(_) => {
                return HandshakeOutcome::Rejected(format!(
                    "等待 HelloAck 超时（{}ms）",
                    HANDSHAKE_TIMEOUT.as_millis()
                ));
            }
        };

        if let Some(err) = next.error.as_ref() {
            return HandshakeOutcome::Rejected(format!("对端拒绝握手：{}", err.message));
        }

        match next.message {
            Some(rt::frame::Message::HelloAck(ack)) => {
                // 对端回应的是**我们先发的那条 Hello**，握手到此才算收尾：收到即返回，
                // 不会再留下任何握手帧。
                return match crate::fencing::verify_hello_ack(&ack, host.config.owner_epoch) {
                    crate::fencing::FenceVerdict::Accept => {
                        tracing::info!(
                            worker_id = %ack.worker_id,
                            dispatcher_epoch = ack.dispatcher_epoch,
                            "握手完成"
                        );
                        HandshakeOutcome::Accepted
                    }
                    crate::fencing::FenceVerdict::Fenced { reason } => {
                        HandshakeOutcome::Fenced(reason)
                    }
                };
            }
            Some(rt::frame::Message::Hello(peer)) => {
                if peer_hello_seen {
                    // 重复的 Hello：已应答过，忽略即可（继续等 HelloAck）。
                    continue;
                }
                let verdict = crate::fencing::verify_peer_hello(
                    &peer,
                    host.database_id_text(),
                    host.config.owner_epoch,
                );
                let (accepted, reason) = match verdict {
                    crate::fencing::FenceVerdict::Accept => (true, String::new()),
                    crate::fencing::FenceVerdict::Fenced { reason } => (false, reason),
                };
                let ack = rt::Frame {
                    seq: 2,
                    reply_to_seq: next.seq,
                    request_id: next.request_id.clone(),
                    database_id: host.database_id_text().to_string(),
                    owner_epoch: host.config.owner_epoch,
                    session_id: String::new(),
                    transaction_id: String::new(),
                    deadline_unix_ms: 0,
                    error: None,
                    message: Some(rt::frame::Message::HelloAck(rt::HelloAck {
                        accepted,
                        worker_id: host.config.worker_id.clone(),
                        dispatcher_epoch: host.config.owner_epoch,
                        reject_reason: reason.clone(),
                    })),
                };
                if let Err(err) = writer.send(&ack).await {
                    return HandshakeOutcome::Rejected(format!("回复 HelloAck 失败：{err}"));
                }
                if !accepted {
                    return HandshakeOutcome::Fenced(reason);
                }
                // 不在这里返回：对端还会回应我们先发的 Hello，那条帧读掉之前不能让
                // 连接进入请求循环（否则它会成为下一个请求的"响应"）。
                peer_hello_seen = true;
            }
            _ => continue,
        }
    }
    HandshakeOutcome::Rejected("握手帧过多，始终没有收到 HelloAck".to_string())
}

/// 会话过期通知的转发任务（单向帧，不需要应答）。
fn spawn_notice_forwarder(ctx: Arc<ConnectionCtx>) {
    let mut receiver = ctx.host.notices.subscribe();
    let database_id = ctx.host.database_id_text().to_string();
    let owner_epoch = ctx.host.config.owner_epoch;
    tokio::spawn(async move {
        while let Ok((session_id, reason)) = receiver.recv().await {
            let notice = frame::session_expired_notice(
                &database_id,
                owner_epoch,
                &session_id,
                reason.as_str(),
            );
            if ctx.write(&notice).await.is_err() {
                // 连接已经没了（或通知积压导致写不出去）：结束转发，避免任务泄漏。
                break;
            }
        }
    });
}

/// 会话 reaper：超时会话摘除 + 回滚 + 广播通知。
fn spawn_reaper(host: Arc<Host>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(REAPER_INTERVAL);
        loop {
            ticker.tick().await;
            if host.state() == HostState::Draining {
                break;
            }
            let now_ms = crate::session::now_ms();
            for (session, reason) in host.sessions.reap(now_ms) {
                tracing::info!(
                    db_id = %host.database_id,
                    session_id = %session.id(),
                    reason = reason.as_str(),
                    "会话因超时被回收"
                );
                host.notify_expired(session.id(), reason);
                session.close_and_rollback().await;
            }
        }
    });
}

/// 父进程存活探测。
///
/// 为什么不直接依赖 pidfd：本进程只关心一件事——**父进程（Worker）死了就自杀**，
/// 否则它会作为一个没有归属的孤儿进程继续持有数据库文件与 Remote WAL 的写权限。
/// 读 `/proc/self/status` 的 `PPid` 不需要额外依赖，也能覆盖 pidfd 不可用的老内核。
fn spawn_parent_watch(host: Arc<Host>) {
    tokio::spawn(async move {
        let initial = parent_pid();
        let mut ticker = tokio::time::interval(PARENT_CHECK_INTERVAL);
        loop {
            ticker.tick().await;
            if host.state() == HostState::Draining {
                break;
            }
            let Some(current) = parent_pid() else {
                continue;
            };
            if Some(current) != initial {
                tracing::error!(
                    initial_ppid = ?initial,
                    current_ppid = current,
                    "父进程已消失（被重新挂载），为避免孤儿写库，本进程退出"
                );
                host.begin_drain();
                host.request_shutdown(EXIT_FAILURE);
                break;
            }
        }
    });
}

/// 读取 `PPid`（取不到时返回 `None`，此时不做任何动作）。
fn parent_pid() -> Option<u32> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("PPid:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

/// 等待 SIGTERM / SIGINT。
async fn wait_for_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(err) => {
            tracing::warn!(error = %err, "无法安装 SIGTERM 处理器");
            // 安装失败时永不返回，保证 select 分支不会空转。
            return std::future::pending().await;
        }
    };
    let mut interrupt = match signal(SignalKind::interrupt()) {
        Ok(stream) => stream,
        Err(err) => {
            tracing::warn!(error = %err, "无法安装 SIGINT 处理器");
            return std::future::pending().await;
        }
    };
    tokio::select! {
        _ = terminate.recv() => "SIGTERM",
        _ = interrupt.recv() => "SIGINT",
    }
}

/// 握手结果。
enum HandshakeOutcome {
    /// 握手通过。
    Accepted,
    /// 本次连接不可用（对端拒绝 / 超时），进程继续服务其他连接。
    Rejected(String),
    /// 所有权存疑，进程必须退出。
    Fenced(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn socket_bind_cleans_stale_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested/a.sock");
        let listener = bind_socket(&path).expect("首次绑定");
        drop(listener);
        // 残留文件（真实 socket 在关闭后仍在磁盘上）必须能被下一次绑定清掉。
        assert!(path.exists());
        let listener = bind_socket(&path).expect("二次绑定");
        drop(listener);
    }

    #[test]
    fn socket_bind_refuses_regular_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("not-a-socket");
        std::fs::write(&path, b"x").expect("写文件");
        assert!(
            bind_socket(&path).is_err(),
            "普通文件路径必须报错而不是删除"
        );
        assert!(path.exists(), "报错时不得动别人的文件");
    }
}
