//! 空闲卸载后的驻留与回收语义。
//!
//! 这些用例钉住的是「实例什么时候真的被丢弃」这一条链：
//!   - 加载失败的 open 必须把占位条目与会话计数一起收回（`WorkerRuntime::open` 的 Err 分支）；
//!   - `release`（句柄析构与 `make_handle` 失败路径共用）只把会话数减到 0 并打上 `idle_since`，
//!     条目仍是 `Loaded`，实例继续驻留；
//!   - 只有 idle sweep 才会把条目转成 `Retiring` 并最终释放实例。
//!
//! 任何一条被破坏，「重开是否要重付一次冷加载」的结论都会改变。

// `test_support` 是共用 helper，本文件只用其中的 fixture/identifier；
// 其余公开项（Gate 等）在这里没有调用点，属预期，不应触发 dead_code。
#[allow(dead_code)]
#[path = "../src/runtime/test_support.rs"]
mod support;

use anyhow::Result;
use peri_loom_v1::runtime::{DatabaseLifecycle, WorkerLimits};
use std::sync::atomic::Ordering;
use std::time::Duration;
use support::{fixture, identifier};

#[test]
fn failed_load_releases_the_slot_and_the_next_open_pays_a_full_load() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    let database = identifier("unload");

    provider.failures.store(1, Ordering::SeqCst);
    let failed = runtime.open(database.clone(), 1);
    assert!(failed.is_err(), "injected provider failure must fail the open");
    drop(failed);

    let status = runtime.status()?;
    assert_eq!(
        status.resident_count, 0,
        "a failed load must not leave a Loading entry behind"
    );
    assert_eq!(
        status.loading_databases, 0,
        "a failed load must not leave the database marked as loading"
    );
    assert_eq!(
        status.active_sessions, 0,
        "a failed load must not leak the session count"
    );

    // 条目已被收回，所以下一次打开必然重新走一遍 provider.open_store。
    let calls_before = provider.calls.load(Ordering::SeqCst);
    let handle = runtime.open(database.clone(), 1)?;
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls_before + 1,
        "the retry after a failed load must load again"
    );
    drop(handle);
    Ok(())
}

#[test]
fn release_keeps_the_instance_resident_and_only_idle_sweep_retires_it() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    let database = identifier("residency");

    let handle = runtime.open(database.clone(), 1)?;
    handle.execute("CREATE TABLE data(value INTEGER); INSERT INTO data VALUES(7)")?;
    let calls_after_load = provider.calls.load(Ordering::SeqCst);
    drop(handle);

    let status = runtime.status()?;
    assert_eq!(status.active_sessions, 0);
    assert_eq!(status.resident_count, 1, "the instance must stay resident");
    let entry = status
        .databases
        .iter()
        .find(|entry| entry.identifier == database)
        .expect("the database must still be reported");
    assert_eq!(
        entry.lifecycle,
        DatabaseLifecycle::Open,
        "release must leave the entry Loaded instead of retiring it"
    );
    assert!(
        entry.idle_ms.is_some(),
        "release must stamp idle_since so the sweep can see it"
    );

    // 驻留实例被复用：这次打开不需要再向 provider 要一次存储句柄。
    let reopened = runtime.open(database.clone(), 1)?;
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        calls_after_load,
        "reopening a resident instance must not load again"
    );
    assert_eq!(
        reopened.query("SELECT value FROM data")?[0][0].to_string(),
        "7"
    );
    drop(reopened);

    assert_eq!(runtime.evict_idle(Duration::ZERO)?, vec![database.clone()]);
    let status = runtime.status()?;
    assert_eq!(status.resident_count, 0, "the sweep must release the entry");
    assert_eq!(status.active_sessions, 0);
    assert_eq!(provider.calls.load(Ordering::SeqCst), calls_after_load);
    Ok(())
}
