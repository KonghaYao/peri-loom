#[path = "../src/runtime/test_support.rs"]
mod support;

use anyhow::Result;
use peri_loom_v1::{
    namespace::Namespace,
    runtime::{DatabaseLifecycle, WorkerLimits, WorkerRuntime},
};
use std::{
    sync::{atomic::Ordering, mpsc, Arc, Barrier},
    thread,
    time::Duration,
};
use support::{fixture, identifier, Gate};

#[test]
fn thousand_database_rotations_reopen_acked_wal_under_resident_cap() -> Result<()> {
    let (_, _, runtime) = fixture(WorkerLimits {
        max_open_databases: 2,
        ..WorkerLimits::default()
    })?;
    for index in 0..1000 {
        let id = identifier(&format!("rotation_{index}"));
        let handle = runtime.open(id.clone(), 1)?;
        handle.execute(&format!(
            "CREATE TABLE data(value INTEGER); INSERT INTO data VALUES({index})"
        ))?;
        drop(handle);
        runtime.evict_idle(Duration::ZERO)?;
        assert_eq!(runtime.status()?.resident_count, 0);
        let reopened = runtime.open(id, 1)?;
        assert_eq!(
            reopened.query("SELECT value FROM data")?[0][0].to_string(),
            index.to_string()
        );
        drop(reopened);
        runtime.evict_idle(Duration::ZERO)?;
    }
    Ok(())
}

#[test]
fn sessions_are_independent_and_final_drop_aborts_transaction() -> Result<()> {
    let (_, _, runtime) = fixture(WorkerLimits::default())?;
    let first = runtime.open(identifier("db"), 1)?;
    first.execute("CREATE TABLE data(value INTEGER); INSERT INTO data VALUES(1)")?;
    let second = runtime.open(identifier("db"), 1)?;
    first.execute("BEGIN; INSERT INTO data VALUES(2)")?;
    assert!(first.in_transaction()?);
    assert!(!second.in_transaction()?);
    assert_eq!(
        second.query("SELECT COUNT(*) FROM data")?[0][0].to_string(),
        "1"
    );
    assert!(first.checkpoint().is_err());
    assert!(first.compact_wal().is_err());
    drop(first);
    assert_eq!(
        second.query("SELECT COUNT(*) FROM data")?[0][0].to_string(),
        "1"
    );
    second.execute("INSERT INTO data VALUES(3)")?;
    assert_eq!(runtime.status()?.active_sessions, 1);
    Ok(())
}

#[test]
fn close_rejects_active_handles_and_unloads_idle_database() -> Result<()> {
    let (_, _, runtime) = fixture(WorkerLimits::default())?;
    let id = identifier("db");
    assert!(!runtime.close(&id)?);
    let handle = runtime.open(id.clone(), 4)?;
    assert!(runtime.close(&id).is_err());
    drop(handle);
    assert!(runtime.close(&id)?);
    assert!(!runtime.close(&id)?);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    Ok(())
}

#[test]
fn drain_stops_admission_and_unloads_only_after_final_session() -> Result<()> {
    let (_, _, runtime) = fixture(WorkerLimits::default())?;
    let id = identifier("db");
    let first = runtime.open(id.clone(), 1)?;
    let second = runtime.open(id.clone(), 1)?;
    assert!(runtime.drain(&id)?);
    assert!(runtime.open(id.clone(), 1).is_err());
    assert_eq!(
        runtime.status()?.databases[0].lifecycle,
        DatabaseLifecycle::Draining
    );
    first.execute("CREATE TABLE data(value INTEGER)")?;
    drop(first);
    assert_eq!(runtime.status()?.active_sessions, 1);
    second.execute("INSERT INTO data VALUES(8)")?;
    drop(second);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    assert!(!runtime.drain(&id)?);
    let reopened = runtime.open(id, 2)?;
    assert_eq!(
        reopened.query("SELECT value FROM data")?[0][0].to_string(),
        "8"
    );
    Ok(())
}

#[test]
fn idle_eviction_uses_elapsed_time_and_never_evicts_active_handles() -> Result<()> {
    let (_, _, runtime) = fixture(WorkerLimits::default())?;
    let active = runtime.open(identifier("active"), 1)?;
    let idle_id = identifier("idle");
    drop(runtime.open(idle_id.clone(), 1)?);
    assert!(runtime.evict_idle(Duration::from_secs(60))?.is_empty());
    thread::sleep(Duration::from_millis(15));
    assert_eq!(
        runtime.evict_idle(Duration::from_millis(10))?,
        vec![idle_id]
    );
    assert_eq!(runtime.status()?.active_sessions, 1);
    drop(active);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?.len(), 1);
    Ok(())
}

#[test]
fn session_limits_are_enforced_and_released_on_drop() -> Result<()> {
    let limits = WorkerLimits {
        max_total_sessions: 2,
        max_sessions_per_database: 1,
        ..WorkerLimits::default()
    };
    let (_, _, runtime) = fixture(limits)?;
    let first = runtime.open(identifier("first"), 1)?;
    assert!(runtime.open(identifier("first"), 1).is_err());
    let second = runtime.open(identifier("second"), 1)?;
    assert!(runtime.open(identifier("third"), 1).is_err());
    drop(first);
    let third = runtime.open(identifier("third"), 1)?;
    assert_eq!(runtime.status()?.active_sessions, 2);
    drop(second);
    drop(third);
    assert_eq!(runtime.status()?.active_sessions, 0);
    Ok(())
}

#[test]
fn epoch_mismatch_rejects_loaded_and_loading_instances() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    let handle = runtime.open(identifier("loaded"), 7)?;
    assert!(runtime.open(identifier("loaded"), 8).is_err());
    drop(handle);
    assert!(runtime.open(identifier("loaded"), 8).is_err());
    let gate = Arc::new(Gate::default());
    *provider.slow.lock().unwrap() = Some(("loading".into(), gate.clone()));
    let other = runtime.clone();
    let task = thread::spawn(move || {
        drop(other.open(identifier("loading"), 7).unwrap());
    });
    gate.entered();
    assert!(runtime.open(identifier("loading"), 8).is_err());
    gate.release();
    task.join().unwrap();
    Ok(())
}

#[test]
fn loading_reservation_enforces_database_and_global_session_capacity() -> Result<()> {
    for limits in [
        WorkerLimits {
            max_open_databases: 1,
            ..WorkerLimits::default()
        },
        WorkerLimits {
            max_total_sessions: 1,
            ..WorkerLimits::default()
        },
    ] {
        let (_, provider, runtime) = fixture(limits)?;
        let gate = Arc::new(Gate::default());
        *provider.slow.lock().unwrap() = Some(("slow".into(), gate.clone()));
        let other = runtime.clone();
        let task = thread::spawn(move || {
            drop(other.open(identifier("slow"), 1).unwrap());
        });
        gate.entered();
        let status = runtime.status()?;
        assert_eq!(status.loading_databases, 1);
        assert_eq!(status.active_sessions, 1);
        assert!(status.databases[0].resources.is_none());
        assert!(runtime.open(identifier("other"), 1).is_err());
        gate.release();
        task.join().unwrap();
    }
    Ok(())
}

#[test]
fn concurrent_same_key_opens_coalesce_one_engine() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    let start = Arc::new(Barrier::new(9));
    let opened = Arc::new(Barrier::new(9));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let runtime = runtime.clone();
        let start = start.clone();
        let opened = opened.clone();
        tasks.push(thread::spawn(move || {
            start.wait();
            let handle = runtime.open(identifier("shared"), 1);
            // The result is carried past the barrier: unwrapping before it would leave the
            // other participants waiting for a thread that already panicked.
            opened.wait();
            drop(handle.unwrap());
        }));
    }
    start.wait();
    opened.wait();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.status()?.loaded_databases, 1);
    assert_eq!(runtime.status()?.active_sessions, 0);
    Ok(())
}

#[test]
fn failed_open_rolls_back_capacity_and_allows_retry() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits {
        max_open_databases: 1,
        max_total_sessions: 1,
        ..WorkerLimits::default()
    })?;
    provider.failures.store(1, Ordering::SeqCst);
    assert!(runtime.open(identifier("db"), 1).is_err());
    let status = runtime.status()?;
    assert_eq!(
        status.loaded_databases + status.loading_databases + status.active_sessions,
        0
    );
    drop(runtime.open(identifier("retry"), 1)?);
    Ok(())
}

#[test]
fn slow_loading_database_does_not_block_other_database_or_status() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    let gate = Arc::new(Gate::default());
    *provider.slow.lock().unwrap() = Some(("slow".into(), gate.clone()));
    let other = runtime.clone();
    let task = thread::spawn(move || {
        drop(other.open(identifier("slow"), 1).unwrap());
    });
    gate.entered();
    let (sent, received) = mpsc::channel();
    let other = runtime.clone();
    let fast = thread::spawn(move || {
        let handle = other.open(identifier("fast"), 1).unwrap();
        handle.execute("CREATE TABLE data(value INTEGER)").unwrap();
        sent.send(other.status().unwrap().loaded_databases).unwrap();
    });
    let outcome = received.recv_timeout(Duration::from_secs(10));
    gate.release();
    task.join().unwrap();
    fast.join().unwrap();
    assert_eq!(outcome.unwrap(), 1);
    Ok(())
}

#[test]
fn draining_loading_instance_cancels_publication_and_releases_capacity() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    let gate = Arc::new(Gate::default());
    *provider.slow.lock().unwrap() = Some(("slow".into(), gate.clone()));
    let other = runtime.clone();
    let task = thread::spawn(move || assert!(other.open(identifier("slow"), 1).is_err()));
    gate.entered();
    assert!(runtime.drain(&identifier("slow"))?);
    assert!(runtime.close(&identifier("slow")).is_err());
    assert!(runtime.open(identifier("slow"), 1).is_err());
    gate.release();
    task.join().unwrap();
    assert_eq!(runtime.status()?.loading_databases, 0);
    assert_eq!(runtime.status()?.active_sessions, 0);
    Ok(())
}

#[test]
fn invalid_identifiers_fail_before_provider_and_cache_paths_do_not_alias() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    let mut forged = identifier("valid");
    forged.namespace.tenant = "../escape".into();
    assert!(runtime.open(forged, 1).is_err());
    let mut forged = identifier("valid");
    forged.branch = "../main".into();
    assert!(runtime.open(forged, 1).is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let first = identifier("db");
    let mut second = first.clone();
    second.namespace = Namespace::new("other", "db")?;
    let mut branch = first.clone();
    branch.branch = "fork".into();
    assert_ne!(first.cache_key()?, second.cache_key()?);
    assert_ne!(first.cache_key()?, branch.cache_key()?);
    Ok(())
}

#[test]
fn resource_accounting_is_bounded_and_absent_after_unload() -> Result<()> {
    let limits = WorkerLimits {
        segment_ram_bytes: 16 * 1024,
        segment_disk_bytes: 32 * 1024,
        ..WorkerLimits::default()
    };
    let (_, _, runtime) = fixture(limits.clone())?;
    let id = identifier("db");
    let handle = runtime.open(id.clone(), 1)?;
    handle.execute("CREATE TABLE data(value BLOB); INSERT INTO data VALUES(zeroblob(65536))")?;
    assert!(handle.resources()?.wal_bytes > 0);
    handle.checkpoint()?;
    handle.query("SELECT length(value) FROM data")?;
    let resources = handle.resources()?;
    assert!(resources.segment_ram_bytes <= limits.segment_ram_bytes);
    assert!(resources.segment_disk_bytes <= limits.segment_disk_bytes);
    let status = runtime.status()?;
    assert_eq!(
        status.databases[0]
            .resources
            .as_ref()
            .unwrap()
            .segment_loads,
        resources.segment_loads
    );
    drop(handle);
    runtime.close(&id)?;
    assert!(runtime.status()?.databases.is_empty());
    Ok(())
}

#[test]
fn admission_limits_must_be_positive() -> Result<()> {
    let (cache, provider, _) = fixture(WorkerLimits::default())?;
    for limits in [
        WorkerLimits {
            max_open_databases: 0,
            ..WorkerLimits::default()
        },
        WorkerLimits {
            max_total_sessions: 0,
            ..WorkerLimits::default()
        },
        WorkerLimits {
            max_sessions_per_database: 0,
            ..WorkerLimits::default()
        },
    ] {
        assert!(WorkerRuntime::new(provider.clone(), cache.path(), limits).is_err());
    }
    Ok(())
}

#[test]
fn concurrent_database_capacity_is_reserved_before_io() -> Result<()> {
    let (_, _, runtime) = fixture(WorkerLimits {
        max_open_databases: 3,
        ..WorkerLimits::default()
    })?;
    let start = Arc::new(Barrier::new(9));
    let finish = Arc::new(Barrier::new(9));
    let (sent, received) = mpsc::channel();
    let mut tasks = Vec::new();
    for index in 0..8 {
        let runtime = runtime.clone();
        let start = start.clone();
        let finish = finish.clone();
        let sent = sent.clone();
        tasks.push(thread::spawn(move || {
            start.wait();
            let handle = runtime.open(identifier(&format!("database-{index}")), 1);
            sent.send(handle.is_ok()).unwrap();
            finish.wait();
            drop(handle);
        }));
    }
    start.wait();
    let mut accepted = 0;
    for _ in 0..8 {
        accepted += usize::from(received.recv_timeout(Duration::from_secs(10)).unwrap());
    }
    assert_eq!(accepted, 3);
    assert_eq!(runtime.status()?.loaded_databases, 3);
    finish.wait();
    for task in tasks {
        task.join().unwrap();
    }
    Ok(())
}

#[test]
fn concurrent_session_capacity_cannot_oversubscribe() -> Result<()> {
    for (total, per_database, expected) in [(2, 8, 2), (8, 3, 3)] {
        let (_, _, runtime) = fixture(WorkerLimits {
            max_total_sessions: total,
            max_sessions_per_database: per_database,
            ..WorkerLimits::default()
        })?;
        drop(runtime.open(identifier("shared"), 1)?);
        let start = Arc::new(Barrier::new(9));
        let finish = Arc::new(Barrier::new(9));
        let (sent, received) = mpsc::channel();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let runtime = runtime.clone();
            let start = start.clone();
            let finish = finish.clone();
            let sent = sent.clone();
            tasks.push(thread::spawn(move || {
                start.wait();
                let handle = runtime.open(identifier("shared"), 1);
                sent.send(handle.is_ok()).unwrap();
                finish.wait();
                drop(handle);
            }));
        }
        start.wait();
        let mut accepted = 0;
        for _ in 0..8 {
            accepted += usize::from(received.recv_timeout(Duration::from_secs(10)).unwrap());
        }
        assert_eq!(accepted, expected);
        assert_eq!(runtime.status()?.active_sessions, expected);
        finish.wait();
        for task in tasks {
            task.join().unwrap();
        }
        assert_eq!(runtime.status()?.active_sessions, 0);
    }
    Ok(())
}

#[test]
fn failed_loading_wakes_waiters_and_leaves_no_reserved_capacity() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits::default())?;
    provider.failures.store(100, Ordering::SeqCst);
    let gate = Arc::new(Gate::default());
    *provider.slow.lock().unwrap() = Some(("fail".into(), gate.clone()));
    let (sent, received) = mpsc::channel();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let runtime = runtime.clone();
        let sent = sent.clone();
        tasks.push(thread::spawn(move || {
            sent.send(runtime.open(identifier("fail"), 1).is_err())
                .unwrap();
        }));
    }
    gate.entered();
    gate.release();
    for _ in 0..8 {
        assert!(received.recv_timeout(Duration::from_secs(10)).unwrap());
    }
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(runtime.status()?.active_sessions, 0);
    assert_eq!(runtime.status()?.loading_databases, 0);
    provider.failures.store(0, Ordering::SeqCst);
    drop(runtime.open(identifier("fail"), 1)?);
    Ok(())
}

#[test]
fn slow_native_drop_keeps_reservation_without_holding_global_mutex() -> Result<()> {
    let (_, provider, runtime) = fixture(WorkerLimits {
        max_open_databases: 2,
        ..WorkerLimits::default()
    })?;
    let gate = Arc::new(Gate::default());
    *provider.slow_drop.lock().unwrap() = Some(("slow".into(), gate.clone()));
    let id = identifier("slow");
    drop(runtime.open(id.clone(), 1)?);
    let other = runtime.clone();
    let task = thread::spawn(move || other.close(&identifier("slow")).unwrap());
    gate.entered();
    let status = runtime.status()?;
    assert_eq!(status.databases[0].lifecycle, DatabaseLifecycle::Draining);
    assert_eq!(status.databases[0].active_sessions, 0);
    assert!(runtime.open(id.clone(), 1).is_err());
    let fast = runtime.open(identifier("fast"), 1)?;
    fast.execute("CREATE TABLE data(value INTEGER)")?;
    assert!(runtime.open(identifier("overflow"), 1).is_err());
    gate.release();
    assert!(task.join().unwrap());
    assert_eq!(runtime.status()?.loaded_databases, 1);
    drop(runtime.open(id, 2)?);
    Ok(())
}
