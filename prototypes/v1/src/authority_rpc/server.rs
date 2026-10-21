use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use base64::Engine;
use serde::Serialize;
use serde_json::{json, Value};

use crate::authority::ownership::{LeaseValidity, OwnershipAuthority};
use crate::object_store::{ObjectStore, PreconditionFailed};
use crate::ownership::types::{validate_label, AccessRole, LeaseBinding};
use crate::runtime::DatabaseId;

use super::auth::{AuthConfig, Principal};
use super::http;
use super::ingress::{Acceptor, Quota, Wakeup};
use super::protocol::{
    AuthorityCommand, RpcRequest, RpcResponse, SessionOperation, PROTOCOL_VERSION,
};
use super::recovery::RecoveryAdmission;
use super::startup::AuthorityStartup;

#[cfg(test)]
#[path = "batch_tests.rs"]
mod batch_tests;
#[cfg(test)]
#[path = "import_segment_tests.rs"]
mod import_segment_tests;

/// Requests the authority dispatches at once. This is the execution budget: it
/// gates work, which is why a parked connection must not hold one.
const MAX_INFLIGHT: usize = 128;

/// Connections the listener holds at once, whether they are mid-request or
/// parked. Kept well above the socket budget of a single worker
/// (`authority_rpc::client`) so reuse reduces connection churn without turning
/// an idle pool into a refusal to serve.
pub(crate) const MAX_CONNECTIONS: usize = 256;

/// Remote stores a single authority admits before refusing new opens. Orphaned scopes are
/// reaped by lease validity, so this is a live-scope bound rather than a lifetime quota.
const SESSION_CAPACITY: usize = 4096;
/// A close or forced retirement waits this long for already-dispatched work. Timing out
/// quarantines the namespace and keeps the scope in `Retiring` for a later retry.
const SESSION_RETIRE_TIMEOUT: Duration = Duration::from_secs(5);
/// The reaper retries more often than a client close, so its per-scope wait is shorter;
/// a stuck scope must not delay reaping the rest of the registry for five seconds.
const SESSION_REAP_RETIRE_TIMEOUT: Duration = Duration::from_secs(1);
const SESSION_REAP_INTERVAL: Duration = Duration::from_millis(100);
/// Closed-session proofs have to outlive a lost CloseSession response, not the process.
const SESSION_TOMBSTONE_TTL: Duration = Duration::from_secs(600);
const SESSION_TOMBSTONES: usize = 4096;
const SESSION_RETIRE_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(10);

pub struct Server {
    acceptor: Acceptor,
    state: Arc<State>,
    startup: AuthorityStartup,
    seal: super::seal::Seal,
}

struct State {
    authority: Arc<OwnershipAuthority>,
    auth: AuthConfig,
    sessions: Arc<Mutex<Sessions>>,
    authorized: Mutex<BTreeSet<(DatabaseId, String)>>,
    stopping: AtomicBool,
    connections: Arc<Quota>,
    inflight: Arc<Quota>,
    cleanup: Mutex<Vec<thread::JoinHandle<()>>>,
    wakeup: Wakeup,
    reaper_signal: Mutex<()>,
    reaper_wakeup: Condvar,
    /// One release worker serializes pin transactions. Two `FencedStore` drops that
    /// race for the namespace admission gate can lose one pin to `AdmissionBusy`.
    release_tx: Mutex<Option<mpsc::Sender<ReleaseJob>>>,
    shutdown_budget: crate::shutdown_budget::ShutdownBudget,
    recovery: RecoveryAdmission,
}

struct ReleaseJob {
    handles: Vec<Arc<Session>>,
    completed: mpsc::Sender<()>,
}

/// Opens are idempotent on this identity: a retried OPEN after a lost response must land
/// on the same scope instead of allocating a second reader pin.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SessionKey {
    worker_id: String,
    database: DatabaseId,
    authority_term: u64,
    epoch: u64,
    lease_id: String,
    grant_id: String,
    role: AccessRole,
}

impl SessionKey {
    fn new(worker_id: &str, binding: &LeaseBinding, role: AccessRole) -> Self {
        Self {
            worker_id: worker_id.to_owned(),
            database: binding.database.clone(),
            authority_term: binding.authority_term,
            epoch: binding.epoch,
            lease_id: binding.lease_id.clone(),
            grant_id: binding.grant_id.clone(),
            role,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionLifecycleState {
    Live,
    Retiring,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RetireReason {
    Closed,
    LeaseExpired,
    LeaseFenced,
    AuthorityTermChanged,
    NamespaceUnavailable,
}

impl RetireReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::LeaseExpired => "lease_expired",
            Self::LeaseFenced => "lease_fenced",
            Self::AuthorityTermChanged => "authority_term_changed",
            Self::NamespaceUnavailable => "namespace_unavailable",
        }
    }
}

impl From<LeaseValidity> for RetireReason {
    fn from(validity: LeaseValidity) -> Self {
        match validity {
            LeaseValidity::Expired => Self::LeaseExpired,
            LeaseValidity::Fenced => Self::LeaseFenced,
            LeaseValidity::AuthorityTermChanged => Self::AuthorityTermChanged,
            LeaseValidity::Live | LeaseValidity::Unavailable => Self::NamespaceUnavailable,
        }
    }
}

#[derive(Clone, Copy)]
enum SessionOutcome {
    Closed,
    Reaped,
}

struct Session {
    id: String,
    key: SessionKey,
    worker_id: String,
    binding: LeaseBinding,
    /// Holds the branch reader pin. Only the release thread drops this, after the
    /// scope has drained, so a retired session cannot keep a namespace pinned.
    store: Arc<dyn ObjectStore>,
    scope: super::scope::Scope,
    opened_at: Instant,
    lifecycle: Mutex<SessionLifecycle>,
}

struct SessionLifecycle {
    state: SessionLifecycleState,
    /// Successful OPEN responses not yet matched by a CLOSE. A lost response leaves one
    /// reference behind; the session stays observable and is reaped with its lease.
    references: usize,
    last_used: Instant,
    retire_reason: Option<RetireReason>,
    last_failure_log: Option<Instant>,
}

impl Session {
    fn new(
        id: String,
        worker_id: String,
        binding: LeaseBinding,
        role: AccessRole,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        let now = Instant::now();
        Self {
            key: SessionKey::new(&worker_id, &binding, role),
            id,
            worker_id,
            binding,
            store,
            scope: super::scope::Scope::default(),
            opened_at: now,
            lifecycle: Mutex::new(SessionLifecycle {
                state: SessionLifecycleState::Live,
                references: 1,
                last_used: now,
                retire_reason: None,
                last_failure_log: None,
            }),
        }
    }

    fn add_reference(&self) -> Result<usize> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| anyhow::anyhow!("session lifecycle poisoned"))?;
        ensure!(
            lifecycle.state == SessionLifecycleState::Live,
            "authority session retiring"
        );
        lifecycle.references = lifecycle
            .references
            .checked_add(1)
            .context("authority session reference overflow")?;
        lifecycle.last_used = Instant::now();
        Ok(lifecycle.references)
    }

    fn release_reference(&self) -> Result<usize> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| anyhow::anyhow!("session lifecycle poisoned"))?;
        lifecycle.references = lifecycle.references.saturating_sub(1);
        lifecycle.last_used = Instant::now();
        // Mark the terminal transition under the same lock as the decrement so a
        // concurrent OPEN cannot add a reference after the last CLOSE observed zero.
        if lifecycle.references == 0 && lifecycle.state == SessionLifecycleState::Live {
            lifecycle.state = SessionLifecycleState::Retiring;
            lifecycle.retire_reason = Some(RetireReason::Closed);
        }
        Ok(lifecycle.references)
    }

    /// Returns true only for the transition into retirement; later calls keep the first
    /// reason so a reaper retry cannot rewrite why the scope was isolated.
    fn mark_retiring(&self, reason: RetireReason) -> bool {
        let mut lifecycle = match self.lifecycle.lock() {
            Ok(lifecycle) => lifecycle,
            Err(poisoned) => poisoned.into_inner(),
        };
        if lifecycle.state == SessionLifecycleState::Retiring {
            if lifecycle.retire_reason.is_none() {
                lifecycle.retire_reason = Some(reason);
            }
            return false;
        }
        lifecycle.state = SessionLifecycleState::Retiring;
        lifecycle.retire_reason = Some(reason);
        true
    }

    fn state(&self) -> SessionLifecycleState {
        match self.lifecycle.lock() {
            Ok(lifecycle) => lifecycle.state,
            Err(poisoned) => poisoned.into_inner().state,
        }
    }

    fn retire_reason(&self) -> Option<RetireReason> {
        match self.lifecycle.lock() {
            Ok(lifecycle) => lifecycle.retire_reason,
            Err(poisoned) => poisoned.into_inner().retire_reason,
        }
    }

    fn should_log_failure(&self) -> bool {
        let mut lifecycle = match self.lifecycle.lock() {
            Ok(lifecycle) => lifecycle,
            Err(poisoned) => poisoned.into_inner(),
        };
        let now = Instant::now();
        if lifecycle.last_failure_log.is_some_and(|last| {
            now.saturating_duration_since(last) < SESSION_RETIRE_FAILURE_LOG_INTERVAL
        }) {
            return false;
        }
        lifecycle.last_failure_log = Some(now);
        true
    }

    fn snapshot(&self) -> (SessionLifecycleState, usize, Duration, Option<RetireReason>) {
        let lifecycle = match self.lifecycle.lock() {
            Ok(lifecycle) => lifecycle,
            Err(poisoned) => poisoned.into_inner(),
        };
        (
            lifecycle.state,
            lifecycle.references,
            lifecycle.last_used.elapsed(),
            lifecycle.retire_reason,
        )
    }
}

#[derive(Clone)]
struct ClosedSession {
    worker_id: String,
    database: DatabaseId,
    binding: LeaseBinding,
    reason: RetireReason,
    closed_at: Instant,
}

#[derive(Clone, Default)]
struct SessionCounters {
    opened: u64,
    reused: u64,
    closed: u64,
    reaped: u64,
    retire_failures: u64,
}

#[derive(Default)]
struct Sessions {
    by_id: BTreeMap<String, Arc<Session>>,
    by_key: BTreeMap<SessionKey, String>,
    closed: BTreeMap<String, ClosedSession>,
    closed_order: VecDeque<String>,
    counters: SessionCounters,
    /// Finalized sessions whose `FencedStore` has not been dropped yet. The pin is only
    /// released by that drop, so this is part of `retiring` in the metrics.
    releasing: usize,
}

impl Sessions {
    fn remember_closed(&mut self, session: &Session, reason: RetireReason) {
        let now = Instant::now();
        self.closed.insert(
            session.id.clone(),
            ClosedSession {
                worker_id: session.worker_id.clone(),
                database: session.binding.database.clone(),
                binding: session.binding.clone(),
                reason,
                closed_at: now,
            },
        );
        self.closed_order.push_back(session.id.clone());
        while self.closed_order.len() > SESSION_TOMBSTONES {
            if let Some(oldest) = self.closed_order.pop_front() {
                self.closed.remove(&oldest);
            }
        }
        while let Some(oldest) = self.closed_order.front() {
            let expired = self
                .closed
                .get(oldest)
                .is_none_or(|closed| closed.closed_at.elapsed() >= SESSION_TOMBSTONE_TTL);
            if !expired {
                break;
            }
            let oldest = self.closed_order.pop_front().expect("front checked above");
            self.closed.remove(&oldest);
        }
    }
}

fn database_label(database: &DatabaseId) -> String {
    format!(
        "{}/{}/{}",
        database.namespace.tenant, database.namespace.database, database.branch
    )
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

impl Server {
    pub fn bind(
        address: SocketAddr,
        authority: Arc<OwnershipAuthority>,
        auth: AuthConfig,
        mut startup: AuthorityStartup,
    ) -> Result<Self> {
        ensure!(
            address.ip().is_loopback(),
            "P4 authority listener must be loopback-only"
        );
        let acceptor = Acceptor::bind(address)?;
        let acceptor_wakeup = acceptor.wakeup();
        let plan = startup.plan().clone();
        let seal = super::seal::Seal::acquire_with_recovery(authority.backend(), Some(&plan))?;
        startup.record_seal_verified();
        let admission = startup.admission();
        if admission.is_restricted() {
            eprintln!(
                "level=warn event=authority_recovery_restricted pending_unknown={}",
                admission.pending_unknown().join(",")
            );
        }
        Ok(Self {
            acceptor,
            startup,
            seal,
            state: Arc::new(State {
                authority,
                auth,
                sessions: Arc::new(Mutex::new(Sessions::default())),
                authorized: Mutex::new(BTreeSet::new()),
                stopping: AtomicBool::new(false),
                connections: Quota::new(MAX_CONNECTIONS),
                inflight: Quota::new(MAX_INFLIGHT),
                cleanup: Mutex::new(Vec::new()),
                wakeup: acceptor_wakeup,
                reaper_signal: Mutex::new(()),
                reaper_wakeup: Condvar::new(),
                release_tx: Mutex::new(None),
                shutdown_budget: crate::shutdown_budget::ShutdownBudget::configured()?,
                recovery: admission,
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn recovery_admission(&self) -> &RecoveryAdmission {
        &self.state.recovery
    }

    pub(crate) fn write_recovery_plan(&self) -> Result<()> {
        self.startup.write_recovery_plan()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.acceptor
            .local_addr()
            .expect("bound authority listener")
    }

    pub fn run(mut self) -> Result<()> {
        let budget = self.state.shutdown_budget.clone();
        let _shutdown_return = budget.cancel_on_return();
        let result = self.run_inner();
        drop(self);
        budget.finish();
        result
    }

    fn run_inner(&mut self) -> Result<()> {
        let mut thread_failure = false;
        let mut workers: Vec<thread::JoinHandle<()>> = Vec::new();
        let (release_tx, release_rx) = mpsc::channel::<ReleaseJob>();
        *self
            .state
            .release_tx
            .lock()
            .map_err(|_| anyhow::anyhow!("release queue poisoned"))? = Some(release_tx);
        let releaser = {
            let state = self.state.clone();
            thread::Builder::new()
                .name("authority-session-release".into())
                .spawn(move || state.run_releases(release_rx))?
        };
        let reaper = {
            let state = self.state.clone();
            thread::Builder::new()
                .name("authority-session-reaper".into())
                .spawn(move || state.reap_loop())?
        };
        let result = (|| {
            while !self.state.stopping.load(Ordering::SeqCst) {
                let mut index = 0;
                while index < workers.len() {
                    if workers[index].is_finished() {
                        thread_failure |= workers.swap_remove(index).join().is_err();
                    } else {
                        index += 1;
                    }
                }
                self.acceptor.park()?;
                while !self.state.stopping.load(Ordering::SeqCst) {
                    let Some(mut connection) = self.acceptor.take()? else {
                        break;
                    };
                    // The connection budget is separate from `inflight`: a
                    // pooled connection is not work, so it must not spend the
                    // execution budget that gates dispatch.
                    let Some(connection_ticket) = self.state.connections.try_acquire() else {
                        let _ = http::reply(
                            &mut connection,
                            503,
                            b"{\"error\":\"authority connection limit reached\"}",
                        );
                        continue;
                    };
                    let state = self.state.clone();
                    workers.push(thread::Builder::new().spawn(move || {
                        let _ticket = connection_ticket;
                        let _ = state.serve(&mut connection);
                    })?);
                }
            }
            Ok(())
        })();
        self.state.shutdown_budget.start();
        self.state.stopping.store(true, Ordering::SeqCst);
        self.state.wakeup.wake();
        self.state.reaper_wakeup.notify_all();
        for worker in workers {
            thread_failure |= worker.join().is_err();
        }
        thread_failure |= reaper.join().is_err();
        self.state
            .release_tx
            .lock()
            .map_err(|_| anyhow::anyhow!("release queue poisoned"))?
            .take();
        thread_failure |= releaser.join().is_err();
        for cleanup in std::mem::take(
            &mut *self
                .state
                .cleanup
                .lock()
                .map_err(|_| anyhow::anyhow!("scope cleanup poisoned"))?,
        ) {
            thread_failure |= cleanup.join().is_err();
        }
        let sessions = std::mem::take(
            &mut *self
                .state
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?,
        );
        drop(sessions);
        ensure!(
            !thread_failure,
            "authority shutdown thread failed; not clean"
        );
        if result.is_ok() && self.state.authority.quiescent() {
            self.state.shutdown_budget.check()?;
            self.seal.mark_clean()?;
            // Both clean markers are backend writes, so the budget can expire
            // between the check above and the record landing. Revoke the marker
            // instead of leaving evidence a restart would trust without proof, and
            // report a failed revoke rather than hiding it behind the deadline.
            if let Err(error) = self.state.shutdown_budget.check() {
                self.seal.mark_dirty().map_err(|rollback| {
                    rollback.context(format!("seal rollback failed: {error:#}"))
                })?;
                return Err(error);
            }
            self.startup.mark_clean()?;
            if let Err(error) = self.state.shutdown_budget.check() {
                self.startup.mark_dirty().map_err(|rollback| {
                    rollback.context(format!("startup marker rollback failed: {error:#}"))
                })?;
                return Err(error);
            }
        } else {
            return Err(result.err().unwrap_or_else(|| {
                anyhow::anyhow!("authority has unsettled backend operations; startup remains dirty")
            }));
        }
        self.state.shutdown_budget.check()?;
        Ok(())
    }
}

impl State {
    /// Serve one pooled connection until the peer closes it, it goes idle, or a
    /// request must not be replayed on the same socket.
    ///
    /// Requests are strictly serialized: the next request is only read after the
    /// current response has been flushed, so a lost response can never be
    /// followed by an implicitly replayed one on the same connection.
    ///
    /// The execution budget is taken per request, not per connection: a socket
    /// parked between requests is not work and must not hold a dispatch slot.
    fn serve(&self, stream: &mut TcpStream) -> Result<()> {
        let mut first = true;
        loop {
            if self.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
            let request = match http::read_request_optional(
                stream,
                if first {
                    None
                } else {
                    Some(http::KEEP_ALIVE_IDLE)
                },
                &self.stopping,
            ) {
                Ok(Some(request)) => request,
                Ok(None) => return Ok(()),
                Err(error) => {
                    eprintln!("authority HTTP parse failure: {error}");
                    let _ = http::reply(stream, 400, b"{\"error\":\"invalid HTTP request\"}");
                    return Ok(());
                }
            };
            first = false;
            let keep_alive = request.keep_alive && !self.stopping.load(Ordering::SeqCst);
            let Some(_execution_ticket) = self.inflight.try_acquire() else {
                let _ = http::reply(stream, 503, b"{\"error\":\"authority saturated\"}");
                return Ok(());
            };
            if !self.handle(stream, request, keep_alive)? {
                return Ok(());
            }
        }
    }

    /// Returns whether the connection may serve another request.
    fn handle(
        &self,
        stream: &mut TcpStream,
        request: http::Request,
        keep_alive: bool,
    ) -> Result<bool> {
        if request.path != "/" {
            let _ = http::reply(stream, 400, b"{\"error\":\"invalid authority path\"}");
            return Ok(false);
        }
        let principal = match self.auth.authenticate(&request.authorization) {
            Ok(principal) => principal,
            Err(_) => {
                let _ = http::reply(stream, 403, b"{\"error\":\"Unauthorized\"}");
                return Ok(false);
            }
        };
        let parsed = match serde_json::from_slice::<RpcRequest>(&request.body) {
            Ok(parsed) => parsed,
            Err(error) => {
                eprintln!(
                    "authority JSON parse failure category={:?} line={} column={}",
                    error.classify(),
                    error.line(),
                    error.column()
                );
                let _ = http::reply(stream, 400, b"{\"error\":\"invalid authority request\"}");
                return Ok(false);
            }
        };
        let identifier = parsed.request_id.clone();
        let result = (|| {
            ensure!(
                parsed.version == PROTOCOL_VERSION,
                "unsupported authority protocol version"
            );
            validate_label(&parsed.request_id)?;
            ensure!(!self.stopping.load(Ordering::SeqCst), "authority stopping");
            let _reads = crate::authority::request_reads::Scope::for_rpc(&identifier);
            self.dispatch(parsed.command, &principal, &identifier)
        })();
        let response = match result {
            Ok(result) => RpcResponse {
                version: PROTOCOL_VERSION,
                request_id: identifier,
                ok: true,
                result: Some(result),
                error: None,
                precondition_failed: false,
            },
            Err(error) => RpcResponse {
                version: PROTOCOL_VERSION,
                request_id: identifier,
                ok: false,
                result: None,
                error: Some(format!("{error:#}")),
                precondition_failed: error.is::<PreconditionFailed>(),
            },
        };
        let result = http::reply_with(stream, 200, &serde_json::to_vec(&response)?, keep_alive);
        // Reuse only after a fully written response; a partial write may have
        // left the peer without the outcome, so the socket is retired.
        Ok(result.is_ok() && keep_alive)
    }

    fn control(principal: &Principal) -> Result<()> {
        ensure!(
            *principal == Principal::Control,
            "Unauthorized control operation"
        );
        Ok(())
    }

    fn worker<'principal>(
        principal: &'principal Principal,
        binding: &LeaseBinding,
    ) -> Result<&'principal str> {
        let Principal::Worker(worker) = principal else {
            anyhow::bail!("Unauthorized worker operation");
        };
        ensure!(worker == &binding.worker_id, "Unauthorized worker binding");
        Ok(worker)
    }

    fn allowed(&self, database: &DatabaseId, worker: &str) -> Result<()> {
        ensure!(
            self.authorized
                .lock()
                .map_err(|_| anyhow::anyhow!("authorization registry poisoned"))?
                .contains(&(database.clone(), worker.to_owned())),
            "Unauthorized database; no grant issued to worker in this authority term"
        );
        Ok(())
    }

    fn open_session(&self, worker: &str, binding: LeaseBinding, role: AccessRole) -> Result<Value> {
        let key = SessionKey::new(worker, &binding, role);

        // Fast path: a retried OPEN after a lost response must reuse the existing
        // scope, not allocate another reader pin.
        let existing = {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
            sessions
                .by_key
                .get(&key)
                .and_then(|identifier| sessions.by_id.get(identifier))
                .cloned()
        };
        if let Some(existing) = existing {
            let validity = self.authority.session_lease_validity(&binding);
            if validity == LeaseValidity::Unavailable {
                // An unreachable backend or blocked clock is not proof that the lease
                // became an orphan; keep the scope observable and retry next OPEN.
                anyhow::bail!(
                    "authority session {} lease validity unavailable",
                    existing.id
                );
            }
            if validity != LeaseValidity::Live {
                let reason = RetireReason::from(validity);
                existing.mark_retiring(reason);
                anyhow::bail!(
                    "authority session {} is retiring ({})",
                    existing.id,
                    reason.as_str()
                );
            }
            if let Err(error) = self.authority.validate_lease(&binding, role) {
                existing.mark_retiring(RetireReason::LeaseFenced);
                return Err(error.context("authority session lease validation failed"));
            }
            let references = existing.add_reference()?;
            self.sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?
                .counters
                .reused += 1;
            return Ok(json!({
                "session": existing.id,
                "reused": true,
                "references": references,
            }));
        }

        {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
            ensure!(
                sessions.by_id.len() < SESSION_CAPACITY,
                "authority session capacity exceeded"
            );
        }
        let store = self.authority.open_session(&binding, role)?;
        let identifier = Self::random_session_id()?;
        let session = Arc::new(Session::new(
            identifier.clone(),
            worker.to_owned(),
            binding,
            role,
            store,
        ));
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
        // Another OPEN with the same identity may have won the race while this one
        // was opening its store. Reuse that scope and let this duplicate pin drop.
        if let Some(existing_id) = sessions.by_key.get(&key).cloned() {
            let existing = sessions
                .by_id
                .get(&existing_id)
                .cloned()
                .context("authority session index mismatch")?;
            let references = existing.add_reference()?;
            sessions.counters.reused += 1;
            drop(sessions);
            drop(session);
            return Ok(json!({
                "session": existing_id,
                "reused": true,
                "references": references,
            }));
        }
        ensure!(
            sessions.by_id.len() < SESSION_CAPACITY,
            "authority session capacity exceeded"
        );
        ensure!(
            !sessions.by_id.contains_key(&identifier),
            "session identity collision"
        );
        sessions.by_key.insert(key, identifier.clone());
        sessions.by_id.insert(identifier.clone(), session);
        sessions.counters.opened += 1;
        Ok(json!({
            "session": identifier,
            "reused": false,
            "references": 1,
        }))
    }

    fn random_session_id() -> Result<String> {
        let mut random = [0u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        Ok(random.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    /// None means the session is gone or tombstoned; the caller turns that into a closed
    /// proof without revealing whether a different worker ever owned the identifier.
    fn live_session(&self, identifier: &str, worker: &str) -> Result<Option<Arc<Session>>> {
        let selected = {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
            sessions.by_id.get(identifier).cloned()
        };
        if let Some(selected) = &selected {
            ensure!(selected.worker_id == worker, "Unauthorized session");
        }
        Ok(selected)
    }

    fn closed_proof(&self, identifier: &str, worker: &str) -> Result<Value> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
        let closed = sessions
            .closed
            .get(identifier)
            .context("unknown authority session")?;
        ensure!(closed.worker_id == worker, "Unauthorized session");
        Ok(json!({
            "closed": true,
            "drained": true,
            "already_closed": true,
            "binding": closed.binding,
            "reason": closed.reason,
        }))
    }

    fn authorize_session_query(&self, principal: &Principal, worker_id: &str) -> Result<()> {
        match principal {
            Principal::Control => Ok(()),
            Principal::Worker(worker) => {
                ensure!(worker == worker_id, "Unauthorized session");
                Ok(())
            }
        }
    }

    fn session_info(&self, selected: &Session) -> Value {
        let (state, references, idle, reason) = selected.snapshot();
        json!({
            "session": selected.id,
            "state": match state {
                SessionLifecycleState::Live => "live",
                SessionLifecycleState::Retiring => "retiring",
            },
            "database": selected.binding.database,
            "worker_id": selected.worker_id,
            "binding": selected.binding,
            "opened_age_ms": millis(selected.opened_at.elapsed()),
            "last_used_age_ms": millis(idle),
            "references": references,
            "retire_reason": reason,
        })
    }

    fn retire_session(
        &self,
        selected: Arc<Session>,
        reason: RetireReason,
        outcome: SessionOutcome,
        wait: bool,
    ) -> Result<Value> {
        let identifier = selected.id.clone();
        let worker_id = selected.worker_id.clone();
        let binding = selected.binding.clone();
        let deadline = Instant::now() + SESSION_RETIRE_TIMEOUT;
        selected.mark_retiring(reason);
        if let Err(error) = selected.scope.retire(deadline) {
            self.record_retire_failure(&selected, reason, &error);
            self.authority
                .quarantine_database(&selected.binding.database)?;
            return Err(error.context("authority scope retirement deadline"));
        }
        if !self
            .authority
            .session_release_allowed(&selected.binding.database)
        {
            let error =
                anyhow::anyhow!("authority namespace unavailable; session release deferred");
            self.note_deferred_release(&selected, reason);
            return Err(error);
        }
        let completion = self.finalize_session(selected, reason, outcome)?;
        let Some(completion) = completion else {
            return self.closed_proof(&identifier, &worker_id);
        };
        if wait
            && completion
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .is_err()
        {
            self.authority.quarantine_database(&binding.database)?;
            let error = anyhow::anyhow!("scope retirement deadline; namespace quarantined");
            eprintln!(
                "level=error event=authority_session_release_timeout session={identifier} worker_id={worker_id} database={} 原因：{error:#}",
                database_label(&binding.database)
            );
            self.sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?
                .counters
                .retire_failures += 1;
            return Err(error);
        }
        Ok(json!({
            "closed": true,
            "drained": true,
            "binding": binding,
            "reason": reason,
        }))
    }

    /// Removes the scope and moves every handle this path owns into a release thread.
    /// The drain proof is `Scope::retire`; the pin is released by dropping the last
    /// `FencedStore`, so the caller waits for that drop, not a TTL.
    fn finalize_session(
        &self,
        selected: Arc<Session>,
        reason: RetireReason,
        outcome: SessionOutcome,
    ) -> Result<Option<mpsc::Receiver<()>>> {
        let removed = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
            let Some(removed) = sessions.by_id.get(&selected.id).cloned() else {
                return Ok(None);
            };
            ensure!(
                Arc::ptr_eq(&removed, &selected),
                "authority session identity mismatch"
            );
            sessions.by_id.remove(&selected.id);
            if sessions
                .by_key
                .get(&selected.key)
                .is_some_and(|identifier| identifier == &selected.id)
            {
                sessions.by_key.remove(&selected.key);
            }
            sessions.remember_closed(&selected, reason);
            match outcome {
                SessionOutcome::Closed => sessions.counters.closed += 1,
                SessionOutcome::Reaped => sessions.counters.reaped += 1,
            }
            sessions.releasing += 1;
            removed
        };
        match self.schedule_release(vec![removed, selected]) {
            Ok(completion) => Ok(Some(completion)),
            Err(error) => {
                // The failed spawn dropped the captured handles, releasing the pin
                // synchronously; the tombstone still answers a retried CLOSE.
                self.sessions
                    .lock()
                    .map_err(|_| anyhow::anyhow!("session registry poisoned"))?
                    .counters
                    .retire_failures += 1;
                Err(error)
            }
        }
    }

    fn schedule_release(&self, handles: Vec<Arc<Session>>) -> Result<mpsc::Receiver<()>> {
        let (completed, completion) = mpsc::channel();
        let job = ReleaseJob { handles, completed };
        let sender = self
            .release_tx
            .lock()
            .map_err(|_| anyhow::anyhow!("release queue poisoned"))?
            .clone();
        let Some(sender) = sender else {
            // A server that never entered `run_inner` has no worker; release
            // synchronously rather than losing the pin.
            self.release_handles(job.handles);
            let _ = job.completed.send(());
            return Ok(completion);
        };
        match sender.send(job) {
            Ok(()) => Ok(completion),
            Err(error) => {
                self.release_handles(error.0.handles);
                Err(anyhow::anyhow!("authority release worker stopped"))
            }
        }
    }

    fn run_releases(&self, jobs: mpsc::Receiver<ReleaseJob>) {
        for job in jobs {
            self.release_handles(job.handles);
            let _ = job.completed.send(());
        }
    }

    fn release_handles(&self, handles: Vec<Arc<Session>>) {
        for handle in handles {
            drop(handle);
        }
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.releasing = sessions.releasing.saturating_sub(1);
        }
    }

    fn record_retire_failure(
        &self,
        selected: &Session,
        reason: RetireReason,
        error: &anyhow::Error,
    ) {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.counters.retire_failures += 1;
        }
        if selected.should_log_failure() {
            eprintln!(
                "level=error event=authority_session_retire_failed session={} worker_id={} database={} reason={} 原因：{error:#}",
                selected.id,
                selected.worker_id,
                database_label(&selected.binding.database),
                reason.as_str(),
            );
        }
    }

    fn note_deferred_release(&self, selected: &Session, reason: RetireReason) {
        if selected.should_log_failure() {
            eprintln!(
                "level=warn event=authority_session_release_deferred session={} worker_id={} database={} reason={}",
                selected.id,
                selected.worker_id,
                database_label(&selected.binding.database),
                reason.as_str(),
            );
        }
    }

    fn session_metrics(&self) -> Value {
        let (entries, counters, releasing) = {
            let sessions = match self.sessions.lock() {
                Ok(sessions) => sessions,
                Err(poisoned) => poisoned.into_inner(),
            };
            (
                sessions.by_id.values().cloned().collect::<Vec<_>>(),
                sessions.counters.clone(),
                sessions.releasing,
            )
        };
        let now = Instant::now();
        let mut live = 0usize;
        let mut orphan = 0usize;
        let mut unavailable = 0usize;
        let mut retiring = releasing;
        let mut oldest_age_ms = 0u64;
        for session in entries {
            oldest_age_ms =
                oldest_age_ms.max(millis(now.saturating_duration_since(session.opened_at)));
            match session.state() {
                SessionLifecycleState::Retiring => retiring += 1,
                SessionLifecycleState::Live => {
                    match self.authority.session_lease_validity(&session.binding) {
                        LeaseValidity::Live => live += 1,
                        LeaseValidity::Unavailable => unavailable += 1,
                        _ => orphan += 1,
                    }
                }
            }
        }
        json!({
            "live": live,
            "orphan": orphan,
            "unavailable": unavailable,
            "retiring": retiring,
            "oldest_age_ms": oldest_age_ms,
            "retire_failures": counters.retire_failures,
            "opened_total": counters.opened,
            "closed_total": counters.closed,
            "reaped_total": counters.reaped,
            "reused_total": counters.reused,
            "capacity": SESSION_CAPACITY,
        })
    }

    fn reap_loop(self: Arc<Self>) {
        loop {
            if self.stopping.load(Ordering::SeqCst) {
                return;
            }
            self.reap_sessions();
            let signal = match self.reaper_signal.lock() {
                Ok(signal) => signal,
                Err(poisoned) => poisoned.into_inner(),
            };
            if self.stopping.load(Ordering::SeqCst) {
                return;
            }
            let _ = self
                .reaper_wakeup
                .wait_timeout(signal, SESSION_REAP_INTERVAL);
        }
    }

    fn reap_sessions(&self) {
        let candidates = {
            let sessions = match self.sessions.lock() {
                Ok(sessions) => sessions,
                Err(poisoned) => poisoned.into_inner(),
            };
            sessions.by_id.values().cloned().collect::<Vec<_>>()
        };
        for selected in candidates {
            if self.stopping.load(Ordering::SeqCst) {
                return;
            }
            self.reap_session(selected);
        }
    }

    fn reap_session(&self, selected: Arc<Session>) {
        let identifier = selected.id.clone();
        let worker_id = selected.worker_id.clone();
        let database = selected.binding.database.clone();
        let reason = if selected.state() == SessionLifecycleState::Live {
            match self.authority.session_lease_validity(&selected.binding) {
                LeaseValidity::Live => return,
                // Unavailable means the observer could not read the lease, not that
                // the lease is invalid. Retiring here would turn a transient backend
                // outage into permanent loss of every live remote scope.
                LeaseValidity::Unavailable => return,
                validity => {
                    let reason = RetireReason::from(validity);
                    if selected.mark_retiring(reason) {
                        eprintln!(
                            "level=warn event=authority_session_orphaned session={} worker_id={} database={} reason={}",
                            selected.id,
                            selected.worker_id,
                            database_label(&selected.binding.database),
                            reason.as_str(),
                        );
                    }
                    reason
                }
            }
        } else {
            selected
                .retire_reason()
                .unwrap_or(RetireReason::NamespaceUnavailable)
        };
        // Dropping the store while the namespace is isolated would lose the only
        // handle able to release the pin. Keep the scope quarantined and retry.
        if !self
            .authority
            .session_release_allowed(&selected.binding.database)
        {
            self.note_deferred_release(&selected, reason);
            return;
        }
        if let Err(error) = selected
            .scope
            .retire(Instant::now() + SESSION_REAP_RETIRE_TIMEOUT)
        {
            self.record_retire_failure(&selected, reason, &error);
            if let Err(quarantine) = self
                .authority
                .quarantine_database(&selected.binding.database)
            {
                eprintln!(
                    "level=error event=authority_session_quarantine_failed session={} 原因：{quarantine:#}",
                    selected.id
                );
            }
            return;
        }
        if let Err(error) = self.finalize_session(selected, reason, SessionOutcome::Reaped) {
            if let Ok(mut sessions) = self.sessions.lock() {
                sessions.counters.retire_failures += 1;
            }
            eprintln!(
                "level=error event=authority_session_release_failed session={identifier} worker_id={worker_id} database={} reason={} 原因：{error:#}",
                database_label(&database),
                reason.as_str(),
            );
        }
    }

    fn dispatch(
        &self,
        command: AuthorityCommand,
        principal: &Principal,
        identifier: &str,
    ) -> Result<Value> {
        let _diagnostic_request = crate::slow_diagnostics::enter(identifier);
        if !self.recovery.admits(&command) {
            eprintln!(
                "level=warn event=authority_recovery_command_refused pending_unknown={}",
                self.recovery.pending_unknown().join(",")
            );
            anyhow::bail!(
                "recovery_restricted: authority is validating a recovered state; pending_unknown={}",
                self.recovery.pending_unknown().join(",")
            );
        }
        match command {
            AuthorityCommand::GetMetrics => {
                Self::control(principal)?;
                let mut result =
                    serde_json::to_value(crate::metrics::Metrics::global().snapshot())?;
                result["capacity_trace"] = crate::capacity_trace::snapshot();
                result["authority_sessions"] = self.session_metrics();
                result["shutdown_protocol"] = json!(1);
                result["recovery"] = serde_json::to_value(&self.recovery)?;
                Ok(result)
            }
            AuthorityCommand::GetCapacityTrace {
                stream_id,
                after_sequence,
                max_events,
            } => {
                Self::control(principal)?;
                crate::capacity_trace::page(&stream_id, after_sequence, max_events)
            }
            AuthorityCommand::Challenge { intent } => {
                Self::control(principal)?;
                Ok(serde_json::to_value(self.authority.challenge(intent)?)?)
            }
            AuthorityCommand::Grant { request } => {
                Self::control(principal)?;
                ensure!(
                    request.request_id == identifier,
                    "command identity mismatch"
                );
                let grant = self.authority.grant(request)?;
                self.authorized
                    .lock()
                    .map_err(|_| anyhow::anyhow!("authorization registry poisoned"))?
                    .insert((
                        grant.binding.database.clone(),
                        grant.binding.worker_id.clone(),
                    ));
                Ok(serde_json::to_value(grant)?)
            }
            AuthorityCommand::Renew { request } => {
                Self::control(principal)?;
                ensure!(
                    request.request_id == identifier,
                    "command identity mismatch"
                );
                Ok(serde_json::to_value(self.authority.renew(request)?)?)
            }
            AuthorityCommand::Revoke { request } => {
                Self::control(principal)?;
                ensure!(
                    request.request_id == identifier,
                    "command identity mismatch"
                );
                Ok(serde_json::to_value(self.authority.revoke(request)?)?)
            }
            AuthorityCommand::Maintenance {
                request_id,
                database,
                operation,
            } => {
                Self::control(principal)?;
                ensure!(request_id == identifier, "command identity mismatch");
                self.authority
                    .maintenance(&database, &request_id, operation)
            }
            AuthorityCommand::ImportSegments {
                database,
                expected_head,
                segments,
            } => {
                Self::control(principal)?;
                ensure!(
                    !segments.is_empty()
                        && segments.len() <= super::protocol::MAX_IMPORT_SEGMENT_BATCH,
                    "invalid import segment batch"
                );
                ensure!(
                    crate::manifest::valid_hash(&expected_head),
                    "invalid expected HEAD"
                );
                let encoded_size = crate::manifest::SEGMENT_SIZE.div_ceil(3) * 4;
                let mut seen = BTreeSet::new();
                let mut decoded = Vec::with_capacity(segments.len());
                for segment in segments {
                    ensure!(
                        crate::manifest::valid_hash(&segment.hash)
                            && seen.insert(segment.hash.clone())
                            && segment.data_base64.len() == encoded_size,
                        "invalid import segment"
                    );
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(&segment.data_base64)
                        .context("invalid import segment encoding")?;
                    crate::manifest::verify_segment(&segment.hash, &bytes)?;
                    decoded.push(bytes);
                }
                Ok(serde_json::to_value(self.authority.import_segments(
                    &database,
                    &expected_head,
                    &decoded,
                )?)?)
            }
            AuthorityCommand::GetDatabaseState { database } => {
                if let Principal::Worker(worker) = principal {
                    self.allowed(&database, worker)?;
                }
                Ok(serde_json::to_value(self.authority.state(&database)?)?)
            }
            AuthorityCommand::ValidateLease { binding, role } => {
                let worker = Self::worker(principal, &binding)?;
                self.allowed(&binding.database, worker)?;
                Ok(serde_json::to_value(
                    self.authority.validate_lease(&binding, role)?,
                )?)
            }
            AuthorityCommand::OpenSession { binding, role } => {
                let worker = Self::worker(principal, &binding)?;
                self.allowed(&binding.database, worker)?;
                self.open_session(worker, binding, role)
            }
            AuthorityCommand::CloseSession { session } => {
                let Principal::Worker(worker) = principal else {
                    anyhow::bail!("Unauthorized worker operation");
                };
                let selected = match self.live_session(&session, worker)? {
                    Some(selected) => selected,
                    None => return self.closed_proof(&session, worker),
                };
                let remaining = selected.release_reference()?;
                if remaining > 0 {
                    // Another successful OPEN still owns this scope. Each CLOSE releases
                    // exactly one reference; only the last one may drain and unpin.
                    return Ok(json!({
                        "closed": true,
                        "drained": false,
                        "references": remaining,
                        "binding": selected.binding,
                    }));
                }
                self.retire_session(selected, RetireReason::Closed, SessionOutcome::Closed, true)
            }
            AuthorityCommand::RetireSession { session } => {
                let Principal::Worker(worker) = principal else {
                    anyhow::bail!("Unauthorized worker operation");
                };
                let selected = match self.live_session(&session, worker)? {
                    Some(selected) => selected,
                    None => return self.closed_proof(&session, worker),
                };
                // Worker database retirement is terminal: drain in-flight work and
                // release the pin even if lost OPEN responses left extra references.
                self.retire_session(selected, RetireReason::Closed, SessionOutcome::Closed, true)
            }
            AuthorityCommand::SessionState { session } => {
                let (selected, closed) = {
                    let sessions = self
                        .sessions
                        .lock()
                        .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
                    (
                        sessions.by_id.get(&session).cloned(),
                        sessions.closed.get(&session).cloned(),
                    )
                };
                if let Some(selected) = selected {
                    self.authorize_session_query(principal, &selected.worker_id)?;
                    return Ok(self.session_info(&selected));
                }
                let Some(closed) = closed else {
                    anyhow::bail!("unknown authority session");
                };
                self.authorize_session_query(principal, &closed.worker_id)?;
                Ok(json!({
                    "session": session,
                    "state": "closed",
                    "database": closed.database,
                    "worker_id": closed.worker_id,
                    "binding": closed.binding,
                    "retire_reason": closed.reason,
                    "closed_age_ms": millis(closed.closed_at.elapsed()),
                }))
            }
            AuthorityCommand::Session { session, operation } => {
                let Principal::Worker(worker) = principal else {
                    anyhow::bail!("Unauthorized worker operation");
                };
                let selected = self
                    .sessions
                    .lock()
                    .map_err(|_| anyhow::anyhow!("session registry poisoned"))?
                    .by_id
                    .get(&session)
                    .cloned()
                    .context("unknown authority session")?;
                ensure!(&selected.worker_id == worker, "Unauthorized session");
                {
                    let mut lifecycle = selected
                        .lifecycle
                        .lock()
                        .map_err(|_| anyhow::anyhow!("session lifecycle poisoned"))?;
                    ensure!(
                        lifecycle.state == SessionLifecycleState::Live,
                        "authority scope retired"
                    );
                    lifecycle.last_used = Instant::now();
                }
                let _dispatch = selected.scope.dispatch()?;
                let diagnostic = crate::slow_diagnostics::authority_operation(&operation);
                let mut read_bytes = 0;
                let result = (|| match operation {
                    SessionOperation::UploadSegments { segments } => {
                        super::protocol::validate_segment_batch(&segments)?;
                        ensure!(
                            selected.store.access_role() == AccessRole::Writer,
                            "ReadOnly"
                        );
                        Ok(serde_json::to_value(
                            selected.store.upload_segments(&segments)?,
                        )?)
                    }
                    SessionOperation::RegisterSegments { entries } => {
                        super::protocol::validate_registration_batch(&entries)?;
                        ensure!(
                            selected.store.access_role() == AccessRole::Writer,
                            "ReadOnly"
                        );
                        selected.store.register_segments(&entries)?;
                        Ok(Value::Null)
                    }
                    SessionOperation::AppendWal { request } => {
                        request.validate()?;
                        ensure!(
                            selected.store.access_role() == AccessRole::Writer
                                && request.namespace == selected.binding.database.namespace
                                && request.epoch == selected.binding.epoch,
                            "Fenced WAL append identity"
                        );
                        Ok(json!(selected
                            .store
                            .append_wal(&request)?
                            .context("authority WAL append capability unavailable")?))
                    }
                    SessionOperation::Validate => Ok(serde_json::to_value(
                        self.authority
                            .validate_lease(&selected.binding, selected.store.access_role())?,
                    )?),
                    SessionOperation::Get { key } => {
                        let object = selected.store.get(&key)?;
                        read_bytes = object
                            .as_ref()
                            .map_or(0, |object| object.bytes.len() as u64);
                        Ok(serde_json::to_value(object)?)
                    }
                    SessionOperation::Put {
                        key,
                        bytes,
                        condition,
                    } => Ok(json!(selected.store.put(&key, &bytes, condition)?)),
                    SessionOperation::PutGroup { writes } => {
                        super::protocol::validate_segment_group(&writes)?;
                        ensure!(
                            selected.store.access_role() == AccessRole::Writer,
                            "ReadOnly"
                        );
                        let writes = writes
                            .into_iter()
                            .map(|write| {
                                crate::object_store::GroupWrite::new(
                                    write.key,
                                    write.bytes,
                                    crate::object_store::PutCondition::Absent,
                                )
                            })
                            .collect::<Vec<_>>();
                        Ok(serde_json::to_value(selected.store.put_group(&writes)?)?)
                    }
                    SessionOperation::Delete { key, expected } => {
                        selected.store.delete(&key, expected.as_deref())?;
                        Ok(Value::Null)
                    }
                    SessionOperation::List { prefix } => Ok(json!(selected.store.list(&prefix)?)),
                })();
                if let Some(diagnostic) = diagnostic {
                    diagnostic.finish(&result, read_bytes);
                }
                result
            }
            AuthorityCommand::ShutdownWithin { remaining_ms } => {
                Self::control(principal)?;
                ensure!(
                    (1..=20_000).contains(&remaining_ms),
                    "invalid shutdown remaining budget"
                );
                self.shutdown_budget
                    .start_with(Duration::from_millis(remaining_ms));
                self.stopping.store(true, Ordering::SeqCst);
                self.wakeup.wake();
                Ok(json!({"stopping": true, "clean": false}))
            }
            AuthorityCommand::Shutdown => {
                Self::control(principal)?;
                self.shutdown_budget.start();
                self.stopping.store(true, Ordering::SeqCst);
                // The acceptor is parked on `poll`, so it has to be told; waiting
                // out the supervision period would delay every clean shutdown.
                self.wakeup.wake();
                ensure!(
                    self.authority.quiescent(),
                    "authority has unsettled operations; shutdown cannot acknowledge a clean fence"
                );
                Ok(json!({"stopping": true}))
            }
        }
    }
}
