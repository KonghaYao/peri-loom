use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

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
        binding: context.lock()?.binding.clone(),
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
    let binding = context.lock()?.binding.clone();
    let store = Arc::new(ContextStore {
        inner: backing.clone(),
        binding,
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

/// An instant past any lease deadline this test can grant.
const EXPIRED_MS: u64 = 1_000_000;

struct ExpiringAppendBackend {
    local: LocalObjectStore,
    clock: Arc<FakeClock>,
    blob_puts: AtomicU64,
    root_puts: AtomicU64,
    root_gets: AtomicU64,
    /// Zero disables the hook. A nonzero value expires the lease on that catalog read,
    /// counted from the start of the append.
    expire_on_root_get: AtomicU64,
}

impl ObjectStore for ExpiringAppendBackend {
    fn supports_strict_fencing(&self) -> bool {
        true
    }
    fn get(&self, key: &str) -> Result<Option<crate::object_store::StoredObject>> {
        let result = self.local.get(key)?;
        if key.ends_with("/authority/root") {
            let seen = self.root_gets.fetch_add(1, Ordering::SeqCst) + 1;
            let target = self.expire_on_root_get.load(Ordering::SeqCst);
            if target != 0 && seen >= target {
                // Any deadline the lease could hold is behind this instant, so the jump
                // expires it whatever the grant clock was when the counter was armed.
                self.clock.0.store(EXPIRED_MS, Ordering::SeqCst);
            }
        }
        Ok(result)
    }
    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        condition: crate::object_store::PutCondition,
    ) -> Result<String> {
        let result = self.local.put(key, bytes, condition)?;
        if key.contains("/authority/blobs/") {
            self.blob_puts.fetch_add(1, Ordering::SeqCst);
        }
        if key.ends_with("/authority/root") {
            self.root_puts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(result)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.local.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.local.list(prefix)
    }
}

#[test]
fn append_expiry_on_final_fresh_catalog_read_publishes_zero_wal_prefix() -> Result<()> {
    use crate::authority::ownership::OwnershipAuthority;
    use crate::ownership::types::{GrantIntent, GrantRequest, LeaseIntent};
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(FakeClock::default());
    let backend = Arc::new(ExpiringAppendBackend {
        local: LocalObjectStore::open(directory.path())?,
        clock: clock.clone(),
        blob_puts: AtomicU64::new(0),
        root_puts: AtomicU64::new(0),
        root_gets: AtomicU64::new(0),
        // Armed by the test for the append alone: the hook must not fire while the
        // bootstrap grants the lease, or the lease would be granted against the expired
        // clock and stay live for its whole term.
        expire_on_root_get: AtomicU64::new(0),
    });
    let authority = OwnershipAuthority::new(backend.clone(), clock);
    let database = DatabaseId::new(Namespace::new("append", "expiry")?, "main")?;
    let intent = GrantIntent {
        database: database.clone(),
        worker_id: "worker".into(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "lease".into(),
        ttl_ms: 1000,
        role: AccessRole::Writer,
    };
    let nonce = authority
        .challenge(LeaseIntent::Grant(intent.clone()))?
        .nonce;
    let lease = authority.grant(GrantRequest {
        request_id: "grant-expiry".into(),
        nonce,
        intent,
    })?;
    let store = authority.open_session(&lease.binding, AccessRole::Writer)?;
    let before = backend.root_puts.load(Ordering::SeqCst);
    // Arm for the append alone, and count its catalog reads from zero: the append
    // resolves the WAL index on one read and decides on the head with the next, so the
    // expiry lands on the read the publication is decided on.
    backend.root_gets.store(0, Ordering::SeqCst);
    backend.expire_on_root_get.store(2, Ordering::SeqCst);
    let request = crate::wal::WalAppendRequest {
        namespace: database.namespace.clone(),
        epoch: 1,
        batch: crate::wal::WalBatch::default(),
        position: None,
        append_id: "expiry-hook".into(),
        timestamp_millis: 1,
    };
    let error = store.append_wal(&request).unwrap_err();
    assert!(format!("{error:#}").contains("LeaseExpired"));
    // One group carries the instances, the registrations and the head CAS, so a
    // publication that never reaches the store leaves nothing behind: no physical
    // instance, no head revision and no upload intent.
    assert_eq!(backend.blob_puts.load(Ordering::SeqCst), 0);
    assert_eq!(backend.root_puts.load(Ordering::SeqCst), before);
    let object = backend
        .local
        .get(&database.namespace.key("authority/root")?)?
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&object.bytes)?;
    let root: crate::authority::CatalogRoot = serde_json::from_value(envelope["root"].clone())?;
    assert!(root.branches["main"].wal.is_none());
    assert_eq!(root.uploads.len(), 0);
    assert!(root.objects.is_empty());
    eprintln!("APPEND_FINAL_EXPIRY physical_instances=0 publications=0 uploads_retained=0 wal_prefix_published=false");
    drop(store);
    Ok(())
}

fn invalidate_append(context: &LeaseContext, clock: &FakeClock, field: &str) -> Result<()> {
    let mut state = context.lock()?;
    match field {
        "generation" => state.generation += 1,
        "revoked" => state.mode = OwnershipMode::Revoked,
        "detached" => state.mode = OwnershipMode::Detached,
        "expired" => clock.0.store(100, Ordering::SeqCst),
        "readonly" => {
            state.mode = OwnershipMode::ReadOnly;
            state.role = AccessRole::ReadOnly;
            state.binding.role = AccessRole::ReadOnly;
        }
        "root_incarnation" => state.binding.root_incarnation = "different".into(),
        "authority_term" => state.binding.authority_term += 1,
        "branch_incarnation" => state.binding.branch_incarnation += 1,
        "epoch" => state.binding.epoch += 1,
        "worker_id" => state.binding.worker_id = "different".into(),
        "lease_id" => state.binding.lease_id = "different".into(),
        "grant_id" => state.binding.grant_id = "different".into(),
        "renewal_sequence" => state.binding.renewal_sequence = 0,
        "branch" => state.binding.database.branch = "different".into(),
        "namespace" => state.binding.database.namespace = Namespace::new("different", "db")?,
        _ => anyhow::bail!("unknown append invalidation"),
    }
    Ok(())
}

struct AppendCallbackHold {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

struct AppendBackend {
    local: LocalObjectStore,
    context: Arc<LeaseContext>,
    clock: Arc<FakeClock>,
    after: Option<&'static str>,
    held: Option<AppendCallbackHold>,
    calls: AtomicU64,
}

impl ObjectStore for AppendBackend {
    fn append_wal(&self, request: &crate::wal::WalAppendRequest) -> Result<Option<u64>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.local.put(
            &request.namespace.key("append-observed")?,
            request.append_id.as_bytes(),
            crate::object_store::PutCondition::Absent,
        )?;
        if let Some(held) = &self.held {
            held.entered.send(())?;
            held.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))?;
        } else if let Some(field) = self.after {
            invalidate_append(&self.context, &self.clock, field)?;
        }
        Ok(Some(1))
    }

    fn get(&self, key: &str) -> Result<Option<crate::object_store::StoredObject>> {
        self.local.get(key)
    }
    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        condition: crate::object_store::PutCondition,
    ) -> Result<String> {
        self.local.put(key, bytes, condition)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.local.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.local.list(prefix)
    }
}

#[test]
fn append_capability_preconditions_dispatch_zero_and_postconditions_never_report_success(
) -> Result<()> {
    for field in [
        "generation",
        "revoked",
        "detached",
        "expired",
        "readonly",
        "root_incarnation",
        "authority_term",
        "branch_incarnation",
        "epoch",
        "worker_id",
        "lease_id",
        "grant_id",
        "renewal_sequence",
        "branch",
        "namespace",
    ] {
        for after in [false, true] {
            let directory = tempfile::tempdir()?;
            let clock = Arc::new(FakeClock::default());
            let context = context(clock.clone())?;
            context.lock()?.binding.renewal_sequence = 1;
            let binding = context.lock()?.binding.clone();
            let interleaved = after && matches!(field, "generation" | "expired");
            let (entered, observed) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let backend = Arc::new(AppendBackend {
                local: LocalObjectStore::open(directory.path())?,
                context: context.clone(),
                clock: clock.clone(),
                after: after.then_some(field),
                held: interleaved.then_some(AppendCallbackHold {
                    entered,
                    release: Mutex::new(released),
                }),
                calls: AtomicU64::new(0),
            });
            let store = ContextStore {
                inner: backend.clone(),
                context: context.clone(),
                role: AccessRole::Writer,
                generation: 1,
                binding: binding.clone(),
            };
            let request = crate::wal::WalAppendRequest {
                namespace: binding.database.namespace.clone(),
                epoch: binding.epoch,
                batch: crate::wal::WalBatch::default(),
                position: None,
                append_id: "counterexample".into(),
                timestamp_millis: 1,
            };
            if !after {
                invalidate_append(&context, &clock, field)?;
            }
            let result = if interleaved {
                std::thread::scope(|tasks| -> Result<_> {
                    let dispatched = tasks.spawn(|| store.append_wal(&request));
                    observed.recv_timeout(Duration::from_secs(5))?;
                    assert!(backend
                        .local
                        .get(&request.namespace.key("append-observed")?)?
                        .is_some());
                    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
                    invalidate_append(&context, &clock, field)?;
                    release.send(())?;
                    Ok(dispatched.join().unwrap())
                })?
            } else {
                store.append_wal(&request)
            };
            assert!(result.is_err(), "accepted {field} after={after}");
            assert_eq!(backend.calls.load(Ordering::SeqCst), u64::from(after));
            assert_eq!(
                backend
                    .local
                    .get(&request.namespace.key("append-observed")?)?
                    .is_some(),
                after
            );
            eprintln!("APPEND_LOCAL_IDENTITY field={field} after={after} callback_hold_triggered={interleaved} physical_mutations={} reported_success=false", u64::from(after));
        }
    }
    Ok(())
}
