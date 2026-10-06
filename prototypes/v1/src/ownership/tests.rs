use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::{LocalObjectStore, ObjectStore};
use crate::runtime::DatabaseId;

use super::context::{LeaseContext, LeaseState};
use super::provider::ContextStore;
use super::types::{AccessRole, LeaseBinding, LeaseClock, OwnershipMode};

#[derive(Default)]
struct FakeClock(AtomicU64);

impl LeaseClock for FakeClock {
    fn now_ms(&self) -> Result<u64> {
        let now = self.0.load(Ordering::SeqCst);
        anyhow::ensure!(now != u64::MAX, "injected clock failure");
        Ok(now)
    }
}

fn context(clock: Arc<FakeClock>) -> Result<Arc<LeaseContext>> {
    Ok(Arc::new(LeaseContext {
        clock,
        state: Mutex::new(LeaseState {
            binding: LeaseBinding {
                database: DatabaseId::new(Namespace::new("lease", "tests")?, "main")?,
                root_incarnation: "root".into(),
                authority_term: 1,
                branch_incarnation: 1,
                epoch: 1,
                worker_id: "worker".into(),
                lease_id: "lease".into(),
                grant_id: "grant".into(),
                renewal_sequence: 0,
                role: AccessRole::Writer,
            },
            mode: OwnershipMode::Writer,
            role: AccessRole::Writer,
            deadline_ms: 100,
            generation: 1,
            handles: 0,
            opening: false,
            renewing: false,
            detaching: false,
            last_now: 0,
        }),
    }))
}

#[test]
fn clock_error_permanently_revokes_check_and_description_until_fresh_acquire() -> Result<()> {
    for status in [false, true] {
        let clock = Arc::new(FakeClock::default());
        let context = context(clock.clone())?;
        context.check(None)?;
        clock.0.store(u64::MAX, Ordering::SeqCst);
        if status {
            assert!(context.description().is_err());
        } else {
            assert!(context.check(None).is_err());
        }
        clock.0.store(1, Ordering::SeqCst);
        assert_eq!(context.description()?.mode, OwnershipMode::Revoked);
        assert_eq!(context.lock()?.deadline_ms, 0);
        assert!(context.check(Some(true)).is_err());
    }
    Ok(())
}

#[test]
fn local_deadline_expires_without_controller_or_remote_io() -> Result<()> {
    let clock = Arc::new(FakeClock::default());
    let context = context(clock.clone())?;
    clock.0.store(99, Ordering::SeqCst);
    context.check(Some(false))?;
    clock.0.store(100, Ordering::SeqCst);
    assert!(context.check(Some(true)).is_err());
    assert_eq!(context.description()?.mode, OwnershipMode::Revoked);
    context.lock()?.deadline_ms = 200;
    assert!(context.check(None).is_err());
    Ok(())
}

#[test]
fn backwards_lease_clock_revokes_instead_of_extending_deadline() -> Result<()> {
    let clock = Arc::new(FakeClock::default());
    let context = context(clock.clone())?;
    clock.0.store(50, Ordering::SeqCst);
    context.check(None)?;
    clock.0.store(49, Ordering::SeqCst);
    assert!(context.check(None).is_err());
    assert_eq!(context.lock()?.mode, OwnershipMode::Revoked);
    Ok(())
}

#[test]
fn acquiring_status_does_not_expire_uninitialized_deadline() -> Result<()> {
    let clock = Arc::new(FakeClock::default());
    let context = context(clock)?;
    {
        let mut state = context.lock()?;
        state.mode = OwnershipMode::Acquiring;
        state.deadline_ms = 0;
        state.opening = true;
    }
    assert_eq!(context.description()?.mode, OwnershipMode::Acquiring);
    Ok(())
}

#[test]
fn retired_generation_cannot_reuse_a_new_live_context() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let context = context(Arc::new(FakeClock::default()))?;
    let store = ContextStore {
        inner: Arc::new(LocalObjectStore::open(objects.path())?),
        context: context.clone(),
        role: AccessRole::Writer,
        generation: 1,
    };
    store.validate_session()?;
    store.validate_local()?;
    {
        let mut state = context.lock()?;
        state.generation = 2;
        state.binding.epoch = 2;
    }
    assert!(store.validate_session().is_err());
    assert!(store.validate_local().is_err());
    let key = context.lock()?.binding.database.namespace.key("HEAD")?;
    assert!(store.get(&key).is_err());
    Ok(())
}

#[test]
fn expired_native_writer_aborts_without_flushing_and_cold_recovers_acknowledged_tail() -> Result<()>
{
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let clock = Arc::new(FakeClock::default());
    let context = context(clock.clone())?;
    let namespace = context.lock()?.binding.database.namespace.clone();
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    let store = Arc::new(ContextStore {
        inner: backing.clone(),
        context,
        role: AccessRole::Writer,
        generation: 1,
    });
    let database = Arc::new(RemoteDatabase::open(
        store,
        namespace.clone(),
        cache.path(),
        1,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let session = database.session()?;
    session.execute("BEGIN; INSERT INTO items VALUES(2)")?;
    clock.0.store(100, Ordering::SeqCst);
    assert!(session.query("SELECT id FROM items").is_err());
    assert!(session.execute("COMMIT").is_err());
    drop(session);
    drop(database);
    let cold = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(backing, namespace, cold.path(), 1)?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    restored.execute("INSERT INTO items VALUES(3)")?;
    Ok(())
}
