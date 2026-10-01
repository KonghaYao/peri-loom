//! 全局 telemetry 安装的集成测试。
//!
//! 为什么放在集成测试而不是单元测试里：`tracing` 的全局 subscriber 与 metrics 的全局
//! recorder 都是**不可撤销**的进程级单例，而 `cargo test` 在同一个测试二进制内并行执行
//! 用例。把它留在 `src/lib.rs` 的单元测试里，会和同进程的其它用例互相干扰
//! （谁先装谁赢），表现为偶发失败。
//!
//! 集成测试文件各自独立成进程，一个文件只放一个用例即可得到确定性结果。

use std::net::{Ipv4Addr, SocketAddr};

use observability::{init, TelemetryConfig, TelemetryError};

fn test_config() -> TelemetryConfig {
    let mut config = TelemetryConfig::new("observability-integration", "0.0.1");
    config.log_level = "error".to_owned();
    // 端口 0：绑定随机端口，避免与宿主机上其它进程冲突
    config.metrics_listen_addr = Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    config
}

#[test]
fn init_installs_once_and_rejects_repeats() {
    let first = init(test_config());
    assert!(first.is_ok(), "首次 init 应成功: {first:?}");
    let guard = first.unwrap();
    assert!(guard.config().metrics_enabled());
    assert!(!guard.otlp_active(), "未开启 otlp feature 时不应导出 trace");

    // 重复 init 必须安全返回错误而不是 panic（全局 subscriber 与 recorder 均已被占用），
    // 这是「进程内只允许初始化一次 telemetry」的契约。
    let second = init(test_config());
    assert!(
        matches!(second, Err(TelemetryError::AlreadyInitialized)),
        "重复 init 必须返回 AlreadyInitialized，实际: {second:?}"
    );

    guard.shutdown();
}
