use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use peri_loom_v1::ownership::types::{AccessRole, OwnershipCommand, OwnershipMode};

use super::remote::Fixture;

#[test]
fn heartbeat_reports_expired_database_without_admitting_new_sessions() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.acquire(AccessRole::Writer)?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    fixture.worker_clock.0.store(1000, Ordering::SeqCst);
    let response = fixture
        .runtime
        .command(OwnershipCommand::GetWorkerHeartbeat)?;
    assert_eq!(response["databases"][0]["mode"], "revoked");
    assert!(response["resources"]["databases"][0]["resources"].is_null());
    assert!(fixture.runtime.open_session(&fixture.identifier).is_err());
    drop(handle);
    Ok(())
}

#[test]
fn concurrent_watchdog_cannot_retire_a_newly_acquired_generation() -> Result<()> {
    let fixture = Fixture::new_with_grant_observer(AccessRole::Writer, 1000)
        .context("watchdog race: fixture")?;
    fixture
        .acquire(AccessRole::Writer)
        .context("watchdog race: initial acquire")?;
    let handle = fixture
        .runtime
        .open_session(&fixture.identifier)
        .context("watchdog race: setup open session")?;
    handle
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")
        .context("watchdog race: setup execute")?;
    drop(handle);
    let stopping = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| -> Result<()> {
        let runtime = fixture.runtime.clone();
        let stopped = stopping.clone();
        let watchdog = scope.spawn(move || -> Result<()> {
            while !stopped.load(Ordering::SeqCst) {
                runtime
                    .status()
                    .context("watchdog race: background status")?;
                std::thread::yield_now();
            }
            Ok(())
        });
        let result = (|| -> Result<()> {
            let mut previous_lease = fixture.lease.clone();
            for epoch in 2..=5 {
                let cleanup = fixture
                    .arm_cleanup(&previous_lease.binding)
                    .context("watchdog race: arm old cleanup")?;
                fixture
                    .runtime
                    .command(OwnershipCommand::RevokeDatabase {
                        database: fixture.identifier.clone(),
                        expected_epoch: epoch - 1,
                    })
                    .context("watchdog race: revoke")?;
                fixture
                    .await_cleanup(cleanup, &previous_lease.binding)
                    .context("watchdog race: await old cleanup")?;
                assert_eq!(
                    fixture.runtime.status()?.databases[0].mode,
                    OwnershipMode::Revoked
                );
                let lease = fixture
                    .grant_next_diagnosed(epoch)
                    .context("watchdog race: next grant")?;
                let timeout = Instant::now() + Duration::from_secs(5);
                loop {
                    let acquired = fixture.runtime.command(OwnershipCommand::AcquireDatabase {
                        lease: lease.clone(),
                        role: AccessRole::Writer,
                    });
                    match acquired {
                        Ok(_) => break,
                        Err(error) if Instant::now() >= timeout => {
                            return Err(error).context("watchdog race: acquire retry exhausted")
                        }
                        Err(_) => std::thread::yield_now(),
                    }
                }
                let handle = fixture
                    .runtime
                    .open_session(&fixture.identifier)
                    .context("watchdog race: acquired open session")?;
                assert_eq!(
                    handle
                        .query("SELECT id FROM items")
                        .context("watchdog race: acquired query")?[0][0]
                        .to_string(),
                    "1"
                );
                drop(handle);
                assert_eq!(
                    fixture
                        .runtime
                        .status()
                        .context("watchdog race: foreground status")?
                        .databases[0]
                        .mode,
                    OwnershipMode::Writer
                );
                previous_lease = lease;
            }
            Ok(())
        })();
        stopping.store(true, Ordering::SeqCst);
        watchdog.join().unwrap()?;
        result
    })
}

#[test]
fn expiry_without_controller_stops_cached_sql_and_aborts_unfinished_transaction() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.acquire(AccessRole::Writer)?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    handle.execute("BEGIN; INSERT INTO items VALUES(2)")?;
    fixture.worker_clock.0.store(1000, Ordering::SeqCst);
    assert!(handle.query("SELECT id FROM items").is_err());
    assert!(handle.execute("COMMIT").is_err());
    assert!(fixture.runtime.open_session(&fixture.identifier).is_err());
    drop(handle);
    assert_eq!(
        fixture.runtime.status()?.databases[0].mode,
        OwnershipMode::Revoked
    );
    assert_eq!(fixture.runtime.status()?.resources.loaded_databases, 0);
    let renewal = fixture.renew(1, 1000)?;
    assert!(fixture
        .runtime
        .command(OwnershipCommand::RenewLease { lease: renewal })
        .is_err());
    assert!(fixture.acquire(AccessRole::Writer).is_err());
    Ok(())
}

#[test]
fn timely_exact_renewal_extends_deadline_and_existing_native_session() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.acquire(AccessRole::Writer)?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.execute(
        "CREATE TABLE items(id INTEGER PRIMARY KEY); BEGIN; INSERT INTO items VALUES(1)",
    )?;
    fixture.worker_clock.0.store(900, Ordering::SeqCst);
    let renewal = fixture.renew(1, 1000)?;
    fixture.runtime.command(OwnershipCommand::RenewLease {
        lease: renewal.clone(),
    })?;
    assert_eq!(
        fixture.runtime.status()?.databases[0]
            .binding
            .as_ref()
            .unwrap()
            .renewal_sequence,
        1
    );
    assert!(fixture
        .runtime
        .command(OwnershipCommand::RenewLease { lease: renewal })
        .is_err());
    fixture.worker_clock.0.store(1400, Ordering::SeqCst);
    handle.execute("COMMIT")?;
    assert_eq!(handle.query("SELECT id FROM items")?[0][0].to_string(), "1");
    drop(handle);
    Ok(())
}

#[test]
fn fresh_authority_ttl_not_delayed_controller_reply_bounds_local_admission() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.authority_clock.0.store(850, Ordering::SeqCst);
    fixture.worker_clock.0.store(10, Ordering::SeqCst);
    let mut stale = fixture.lease.clone();
    stale.remaining_ttl_ms = u64::MAX;
    fixture.runtime.command(OwnershipCommand::AcquireDatabase {
        lease: stale,
        role: AccessRole::Writer,
    })?;
    assert!(fixture.runtime.status()?.databases[0].remaining_ttl_ms <= 145);
    fixture.worker_clock.0.store(156, Ordering::SeqCst);
    assert!(fixture.runtime.open_session(&fixture.identifier).is_err());
    assert_eq!(fixture.runtime.status()?.resources.loaded_databases, 1);
    assert!(fixture.runtime.status()?.failures.is_empty());
    fixture.runtime.sweep_revocations()?;
    assert_eq!(fixture.runtime.status()?.resources.loaded_databases, 0);
    Ok(())
}

#[test]
fn acquire_response_elapsed_past_ttl_cannot_publish_writer() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.gate.arm(1);
    let runtime = fixture.runtime.clone();
    let lease = fixture.lease.clone();
    let acquire = std::thread::spawn(move || {
        runtime.command(OwnershipCommand::AcquireDatabase {
            lease,
            role: AccessRole::Writer,
        })
    });
    fixture.gate.wait_entered();
    fixture.worker_clock.0.store(1000, Ordering::SeqCst);
    assert_eq!(
        fixture.runtime.status()?.databases[0].mode,
        OwnershipMode::Acquiring
    );
    assert!(fixture.runtime.open_session(&fixture.identifier).is_err());
    assert_eq!(
        fixture.runtime.status()?.databases[0].mode,
        OwnershipMode::Acquiring
    );
    fixture.gate.release();
    assert!(acquire.join().unwrap().is_err());
    assert_eq!(
        fixture.runtime.status()?.databases[0].mode,
        OwnershipMode::Revoked
    );
    assert_eq!(fixture.runtime.status()?.resources.loaded_databases, 0);
    Ok(())
}

#[test]
fn revoke_during_cold_open_invalidates_generation_without_global_io_lock() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.gate.arm(2);
    let runtime = fixture.runtime.clone();
    let lease = fixture.lease.clone();
    let acquire = std::thread::spawn(move || {
        runtime.command(OwnershipCommand::AcquireDatabase {
            lease,
            role: AccessRole::Writer,
        })
    });
    fixture.gate.wait_entered();
    assert_eq!(fixture.runtime.status()?.resources.loading_databases, 1);
    fixture.runtime.command(OwnershipCommand::RevokeDatabase {
        database: fixture.identifier.clone(),
        expected_epoch: 1,
    })?;
    assert!(fixture.acquire(AccessRole::Writer).is_err());
    fixture.gate.release();
    assert!(acquire.join().unwrap().is_err());
    let state = fixture.runtime.status()?;
    assert_eq!(state.databases[0].mode, OwnershipMode::Revoked);
    assert_eq!(state.resources.loaded_databases, 0);
    assert_eq!(state.resources.loading_databases, 0);
    Ok(())
}

#[test]
fn delayed_renewal_cannot_resurrect_local_expiration() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.acquire(AccessRole::Writer)?;
    let renewal = fixture.renew(1, 1000)?;
    fixture.worker_clock.0.store(990, Ordering::SeqCst);
    fixture.gate.arm(1);
    let runtime = fixture.runtime.clone();
    let pending = std::thread::spawn(move || {
        runtime.command(OwnershipCommand::RenewLease { lease: renewal })
    });
    fixture.gate.wait_entered();
    fixture.worker_clock.0.store(996, Ordering::SeqCst);
    fixture.gate.release();
    assert!(pending.join().unwrap().is_err());
    assert!(fixture.runtime.open_session(&fixture.identifier).is_err());
    assert_eq!(
        fixture.runtime.status()?.databases[0].mode,
        OwnershipMode::Revoked
    );
    Ok(())
}

#[test]
fn recovered_clock_and_late_renewal_cannot_resurrect_a_poisoned_lease() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.acquire(AccessRole::Writer)?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1); BEGIN; INSERT INTO items VALUES(2)")?;
    let renewal = fixture.renew(1, 1000)?;
    fixture.gate.arm(1);
    let runtime = fixture.runtime.clone();
    let pending = std::thread::spawn(move || {
        runtime.command(OwnershipCommand::RenewLease { lease: renewal })
    });
    fixture.gate.wait_entered();
    fixture.worker_clock.0.store(u64::MAX, Ordering::SeqCst);
    let failed = fixture.runtime.status()?;
    assert!(failed.databases.is_empty());
    assert_eq!(failed.failures.len(), 1);
    assert_eq!(failed.failures[0].database, fixture.identifier);
    fixture.worker_clock.0.store(1, Ordering::SeqCst);
    fixture.gate.release();
    assert!(pending.join().unwrap().is_err());
    assert!(handle.execute("COMMIT").is_err());
    drop(handle);
    let state = fixture.runtime.status()?;
    assert_eq!(state.databases[0].mode, OwnershipMode::Revoked);
    assert_eq!(state.databases[0].remaining_ttl_ms, 0);
    assert!(fixture.acquire(AccessRole::Writer).is_err());
    let lease = fixture.grant_next(2)?;
    fixture.runtime.command(OwnershipCommand::AcquireDatabase {
        lease,
        role: AccessRole::Writer,
    })?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    assert_eq!(
        handle.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    drop(handle);
    Ok(())
}

#[test]
fn drain_finishes_existing_transaction_but_not_new_statements_after_commit() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.acquire(AccessRole::Writer)?;
    let writer = fixture.runtime.open_session(&fixture.identifier)?;
    writer.execute(
        "CREATE TABLE items(id INTEGER PRIMARY KEY); BEGIN; INSERT INTO items VALUES(1)",
    )?;
    let idle = fixture.runtime.open_session(&fixture.identifier)?;
    fixture.runtime.command(OwnershipCommand::DrainDatabase {
        database: fixture.identifier.clone(),
        expected_epoch: 1,
    })?;
    writer.execute("COMMIT")?;
    assert!(!writer.in_transaction()?);
    assert!(writer
        .execute("BEGIN; INSERT INTO items VALUES(2)")
        .is_err());
    assert!(idle.query("SELECT id FROM items").is_err());
    assert!(fixture.runtime.open_session(&fixture.identifier).is_err());
    drop(writer);
    drop(idle);
    let state = fixture.runtime.status()?;
    assert_eq!(state.databases[0].mode, OwnershipMode::Detached);
    assert_eq!(state.resources.loaded_databases, 0);
    assert_eq!(state.resources.active_sessions, 0);
    Ok(())
}

#[test]
fn readonly_grant_cannot_escalate_and_native_session_is_genuinely_readonly() -> Result<()> {
    let fixture = Fixture::new(AccessRole::ReadOnly, 1000)?;
    assert!(fixture.acquire(AccessRole::Writer).is_err());
    fixture.acquire(AccessRole::ReadOnly)?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    assert!(handle.is_readonly()?);
    assert_eq!(handle.query("SELECT id FROM items")?[0][0].to_string(), "7");
    assert!(handle
        .query("INSERT INTO items VALUES(8) RETURNING id")
        .is_err());
    assert!(handle.query("PRAGMA user_version=5").is_err());
    assert!(handle.query("ATTACH ':memory:' AS denied").is_err());
    assert!(handle.compact_wal().is_err());
    drop(handle);
    Ok(())
}

#[test]
fn higher_epoch_compacts_existing_empty_baseline_without_rewriting_old_history() -> Result<()> {
    let fixture = Fixture::new(AccessRole::Writer, 1000)?;
    fixture.acquire(AccessRole::Writer)?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    handle.compact_wal()?;
    drop(handle);
    fixture.runtime.command(OwnershipCommand::RevokeDatabase {
        database: fixture.identifier.clone(),
        expected_epoch: 1,
    })?;
    let next = fixture.grant_next(2)?;
    fixture.runtime.command(OwnershipCommand::AcquireDatabase {
        lease: next,
        role: AccessRole::Writer,
    })?;
    let handle = fixture.runtime.open_session(&fixture.identifier)?;
    handle.compact_wal()?;
    assert_eq!(handle.query("SELECT id FROM items")?[0][0].to_string(), "1");
    assert_eq!(handle.resources()?.wal_bytes, 0);
    drop(handle);
    Ok(())
}
