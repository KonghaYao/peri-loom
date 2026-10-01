//! pidfd 封装：无 PID 复用竞态的进程跟踪与信号投递（架构 §17.6）。
//!
//! 为什么必须用 pidfd：`kill(pid, ...)` 的语义是「发给当前持有该 pid 的进程」。
//! 子进程退出后 PID 可能被内核立刻回收给别的进程，此时一条迟到的 SIGKILL 会打到
//! 无辜进程上。pidfd 是「指向具体进程实例」的句柄，进程退出后对其发信号只会得到
//! `ESRCH`，不会误伤。
//!
//! 本模块只做两件事：打开 pidfd、通过 pidfd 发信号 / 等待退出。内核不支持
//! （< 5.3）时上层退化到按 PID 发信号 —— 退化路径只损失防竞态能力，不影响功能。

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use tokio::io::unix::AsyncFd;

/// 需要投递的信号（只暴露平台用到的两种）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessSignal {
    /// 优雅终止（SIGTERM）：给引擎机会 flush / 关闭 WAL。
    Term,
    /// 立即终止（SIGKILL）：Kill DB / 停止超时后的兜底。
    Kill,
}

impl ProcessSignal {
    /// 原生信号编号。
    pub fn as_raw(self) -> i32 {
        match self {
            ProcessSignal::Term => libc::SIGTERM,
            ProcessSignal::Kill => libc::SIGKILL,
        }
    }

    /// 指标 / 日志标签。
    pub fn as_str(self) -> &'static str {
        match self {
            ProcessSignal::Term => "sigterm",
            ProcessSignal::Kill => "sigkill",
        }
    }
}

/// 指向具体进程实例的句柄。
#[derive(Debug)]
pub struct PidFd {
    fd: OwnedFd,
}

impl PidFd {
    /// 为 `pid` 打开 pidfd。内核不支持时返回错误，由调用方退化。
    pub fn open(pid: u32) -> io::Result<Self> {
        // SAFETY: syscall 参数为 (pid, flags=0)，返回值按 -1 判定错误；
        // 成功时返回的 fd 立即被 OwnedFd 接管，不会泄漏。
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0u32) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: raw 是本进程内新建的、未被他人持有的 fd
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(raw as i32) },
        })
    }

    /// 通过 pidfd 发信号。目标进程已退出时返回 `ESRCH`。
    pub fn send_signal(&self, signal: ProcessSignal) -> io::Result<()> {
        // SAFETY: fd 有效（OwnedFd 保证生命周期）；siginfo 传 NULL 表示默认信息。
        let raw = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.fd.as_raw_fd(),
                signal.as_raw(),
                std::ptr::null::<libc::siginfo_t>(),
                0u32,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// 等待进程退出（pidfd 变为可读）。
    ///
    /// 注意 pidfd **不可 read**：可读即代表进程已退出，因此这里用 `clear_ready()`
    /// 复位就绪状态而不是读走数据，避免 epoll 边沿触发的忙轮询。
    pub async fn wait_exit(self) -> io::Result<()> {
        let async_fd = AsyncFd::new(self.fd)?;
        let mut guard = async_fd.readable().await?;
        guard.clear_ready();
        Ok(())
    }

    /// 原生 fd（诊断用）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn as_raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }
}

/// 按 PID 发信号（pidfd 不可用时的退化路径，存在 PID 复用竞态）。
pub fn send_signal_by_pid(pid: i32, signal: ProcessSignal) -> io::Result<()> {
    // nix 的 kill 封装比自己写 syscall 更安全（参数校验 + errno 转换）
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::try_from(signal.as_raw())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?,
    )
    .map_err(|err| io::Error::from_raw_os_error(err as i32))
}

/// 进程是否仍然存在（`kill(pid, 0)` 语义）。
#[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
pub fn process_alive(pid: i32) -> bool {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn signal_numbers_match_libc() {
        assert_eq!(ProcessSignal::Term.as_raw(), libc::SIGTERM);
        assert_eq!(ProcessSignal::Kill.as_raw(), libc::SIGKILL);
        assert_eq!(ProcessSignal::Kill.as_str(), "sigkill");
    }

    #[tokio::test]
    async fn pidfd_detects_child_exit() {
        // 用 /bin/sleep 0.2 代替 db-runtime：只依赖本地进程，不依赖网络
        let mut child = tokio::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 0.2")
            .spawn()
            .expect("spawn sh");

        let pidfd = match PidFd::open(child.id().expect("child pid")) {
            Ok(fd) => fd,
            Err(err) => {
                // 老内核（< 5.3）没有 pidfd：退化路径由 supervisor 的 child.wait() 覆盖
                eprintln!("pidfd 不可用，跳过：{err}");
                let _ = child.wait().await;
                return;
            }
        };

        let started = std::time::Instant::now();
        pidfd.wait_exit().await.expect("等待退出");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "退出检测耗时过长：{:?}",
            started.elapsed()
        );
        let status = child.wait().await.expect("reap");
        assert!(status.success());
    }

    #[tokio::test]
    async fn pidfd_signal_kills_target() {
        let mut child = tokio::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 30")
            .spawn()
            .expect("spawn sh");
        let pid = child.id().expect("child pid") as i32;

        let pidfd = match PidFd::open(pid as u32) {
            Ok(fd) => fd,
            Err(err) => {
                eprintln!("pidfd 不可用，改用 PID 信号：{err}");
                send_signal_by_pid(pid, ProcessSignal::Kill).expect("kill by pid");
                let _ = child.wait().await;
                return;
            }
        };

        pidfd.send_signal(ProcessSignal::Kill).expect("pidfd kill");
        // 进程被 SIGKILL 后不可用 signal 判定存活，直接 wait
        let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .expect("等待被杀进程退出超时")
            .expect("wait");
        assert!(status.code().is_none(), "应死于信号：{status:?}");

        // 退出后再发信号只应得到 ESRCH，不会误伤其它进程
        let err = pidfd.send_signal(ProcessSignal::Kill).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ESRCH));
    }

    #[test]
    fn process_alive_reflects_reality() {
        assert!(process_alive(std::process::id() as i32));
        // PID 0 不对应任何用户进程，kill(0, 0) 语义为「进程组」，这里只验证不 panic
        let _ = process_alive(0);
        assert!(!process_alive(i32::MAX - 1));
    }

    #[test]
    fn signalling_unknown_pid_fails() {
        let err = send_signal_by_pid(i32::MAX - 1, ProcessSignal::Term).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ESRCH));
    }
}
