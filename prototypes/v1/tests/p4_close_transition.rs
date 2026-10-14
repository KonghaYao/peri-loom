#[allow(dead_code)]
#[path = "../src/authority/p4_test_support.rs"]
mod support;

use anyhow::{anyhow, ensure, Context, Result};
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, CatalogRoot},
    object_store::ObjectStore,
    ownership::types::{AccessRole, GrantRequest, GrantState, RevokeRequest},
};
use serde::Deserialize;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootEnvelope {
    root: Box<serde_json::value::RawValue>,
    checksum: String,
}

fn snapshot(store: &support::Store) -> Result<CatalogRoot> {
    let key = support::identifier("main")
        .namespace
        .key("authority/root")?;
    let object = store.memory.get(&key)?.context("raw root missing")?;
    let envelope: RootEnvelope = serde_json::from_slice(&object.bytes)?;
    ensure!(
        envelope.checksum
            == blake3::hash(envelope.root.get().as_bytes())
                .to_hex()
                .as_str(),
        "raw root checksum mismatch"
    );
    Ok(serde_json::from_str(envelope.root.get())?)
}

struct Fixture {
    store: Arc<support::Store>,
    authority: Arc<OwnershipAuthority>,
    session: Arc<dyn ObjectStore>,
    request: GrantRequest,
}

impl Fixture {
    fn new() -> Result<Self> {
        let store = Arc::new(support::Store::default());
        let clock = Arc::new(support::Clock::default());
        clock.set(0);
        let authority = OwnershipAuthority::new(store.clone(), clock);
        let binding = support::grant(&authority, 1, 0)?.binding;
        let session = authority.open_session(&binding, AccessRole::Writer)?;
        authority.revoke(RevokeRequest {
            request_id: "revoke-first".into(),
            database: support::identifier("main"),
            expected_epoch: 1,
        })?;
        let request = support::grant_request(&authority, "main", 2, 1, "grant-second", 1000)?;
        ensure!(Arc::strong_count(&session) == 1, "session is not final Arc");
        Ok(Self {
            store,
            authority,
            session,
            request,
        })
    }
}

struct CloseTask {
    release: SyncSender<()>,
    entered: Receiver<bool>,
    worker: Option<JoinHandle<Result<()>>>,
}

impl CloseTask {
    fn start(store: Arc<support::Store>, session: Arc<dyn ObjectStore>) -> Result<Self> {
        let needle = support::identifier("main")
            .namespace
            .key("authority/root")?;
        let (entered_sender, entered) = sync_channel(1);
        let (release, released) = sync_channel(1);
        let worker = thread::spawn(move || {
            let expected_thread = thread::current().id();
            let fired = Arc::new(AtomicBool::new(false));
            let failed = Arc::new(AtomicBool::new(false));
            let hook_fired = fired.clone();
            let hook_failed = failed.clone();
            *store.before_get.lock().unwrap() = Some(support::Hook {
                needle,
                skip: 0,
                action: Box::new(move || {
                    hook_fired.store(true, Ordering::SeqCst);
                    let correct_thread = thread::current().id() == expected_thread;
                    if !correct_thread
                        || entered_sender.try_send(correct_thread).is_err()
                        || released.recv_timeout(DEADLINE).is_err()
                    {
                        hook_failed.store(true, Ordering::SeqCst);
                    }
                }),
            });
            drop(session);
            ensure!(fired.load(Ordering::SeqCst), "close hook did not fire");
            ensure!(!failed.load(Ordering::SeqCst), "close barrier failed");
            ensure!(
                store.before_get.lock().unwrap().is_none(),
                "hook not consumed"
            );
            Ok(())
        });
        Ok(Self {
            release,
            entered,
            worker: Some(worker),
        })
    }

    fn entered(&self) -> Result<()> {
        ensure!(
            self.entered
                .recv_timeout(DEADLINE)
                .context("close entry timeout")?,
            "close hook thread mismatch"
        );
        Ok(())
    }

    fn join(mut self) -> Result<()> {
        let _ = self.release.try_send(());
        self.worker
            .take()
            .context("close worker missing")?
            .join()
            .map_err(|_| anyhow!("close worker panicked"))?
    }
}

impl Drop for CloseTask {
    fn drop(&mut self) {
        let _ = self.release.try_send(());
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !thread::panicking() {
                assert!(matches!(result, Ok(Ok(()))), "close cleanup failed");
            }
        }
    }
}

#[test]
fn close_mode2_rejects_grant_without_catalog_effects() -> Result<()> {
    let Fixture {
        store,
        authority,
        session,
        request,
    } = Fixture::new()?;
    let before = snapshot(&store)?;
    let catalog = before.ownership.as_ref().context("ownership missing")?;
    let owner = catalog.branches.get("main").context("owner missing")?;
    ensure!(
        owner.epoch == 1 && owner.state == GrantState::Revoked,
        "unexpected owner"
    );
    ensure!(before.pins.len() == 1, "expected one session pin");
    let mutations = store.mutations.lock().unwrap().clone();
    let close = CloseTask::start(store.clone(), session)?;
    close.entered()?;
    ensure!(!authority.quiescent(), "close transition is not held");
    let error = authority
        .grant(request.clone())
        .expect_err("overlapping grant succeeded");
    ensure!(
        error.to_string() == "Retryable admission transition busy",
        "unexpected rejection"
    );
    let held = snapshot(&store)?;
    let held_catalog = held.ownership.as_ref().context("held ownership missing")?;
    ensure!(held_catalog == catalog, "grant altered ownership catalog");
    ensure!(
        !held_catalog.commands.contains_key(&request.request_id),
        "grant receipt exists"
    );
    ensure!(
        *store.mutations.lock().unwrap() == mutations,
        "grant mutated backend"
    );
    close.join()?;
    ensure!(snapshot(&store)?.pins.is_empty(), "close retained pin");
    ensure!(authority.quiescent(), "close transition not released");
    Ok(())
}

#[test]
fn joined_close_allows_single_fresh_grant() -> Result<()> {
    let Fixture {
        store,
        authority,
        session,
        request,
    } = Fixture::new()?;
    let close = CloseTask::start(store.clone(), session)?;
    close.entered()?;
    close.join()?;
    ensure!(snapshot(&store)?.pins.is_empty(), "close retained pin");
    ensure!(authority.quiescent(), "close transition not released");
    let reply = authority.grant(request.clone())?;
    ensure!(reply.binding.epoch == 2, "fresh grant epoch mismatch");
    let after = snapshot(&store)?;
    let catalog = after.ownership.as_ref().context("ownership missing")?;
    let owner = catalog.branches.get("main").context("owner missing")?;
    ensure!(
        owner.epoch == 2 && owner.state == GrantState::Active,
        "fresh grant not active"
    );
    ensure!(
        catalog.commands.contains_key(&request.request_id),
        "fresh grant receipt missing"
    );
    Ok(())
}
