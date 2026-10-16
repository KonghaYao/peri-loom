#[cfg(test)]
#[path = "namespace_init_tests.rs"]
mod tests;

use super::{
    admission_gate,
    backend::{Disposition, NamespaceBackend, ReadFailure},
    Memory, OwnershipAuthority, Service,
};
use crate::{
    authority::{migration::BootFailure, Authority},
    namespace::Namespace,
    object_store::PreconditionFailed,
};
use anyhow::{anyhow, Context, Result};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InitPhase {
    PreflightFencing,
    RootRead,
    RootCreateCas,
    DeepVerify,
    BootRead,
    BootPublish,
    ClockAnchor,
    ServiceBuild,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureKind {
    Retryable,
    FailedDefinite,
    BlockedUnknown,
}

struct Failure {
    phase: InitPhase,
    disposition: Disposition,
    kind: FailureKind,
    error: anyhow::Error,
    // Keep the original uncertainty evidence alive for terminal failures.
    _backend: Arc<NamespaceBackend>,
}

type Outcome = Result<Arc<Service>, Arc<Failure>>;

#[derive(Default)]
struct Attempt {
    outcome: Mutex<Option<Outcome>>,
    changed: Condvar,
}

#[derive(Default)]
enum CellState {
    #[default]
    Uninitialized,
    Initializing(Arc<Attempt>),
    Ready(Arc<Service>),
    Retryable(Arc<Failure>),
    FailedDefinite(Arc<Failure>),
    BlockedUnknown(Arc<Failure>),
}

#[derive(Default)]
pub(super) struct NamespaceCell {
    state: Mutex<CellState>,
}

fn report(outcome: Outcome) -> Result<Arc<Service>> {
    outcome.map_err(|failure| {
        anyhow!(
            "namespace initialization {:?}: {:#}",
            failure.phase,
            failure.error
        )
    })
}

impl NamespaceCell {
    pub fn acquire(
        &self,
        owner: &OwnershipAuthority,
        namespace: &Namespace,
    ) -> Result<Arc<Service>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Unavailable namespace cell"))?;
        match &*state {
            CellState::Ready(service) => return Ok(service.clone()),
            CellState::FailedDefinite(failure) | CellState::BlockedUnknown(failure) => {
                return report(Err(failure.clone()));
            }
            CellState::Initializing(attempt) => {
                // Wait on this attempt, not the latest cell state: a newer caller may
                // start a retry before a waiter has consumed the previous failure.
                let attempt = attempt.clone();
                drop(state);
                let outcome = attempt
                    .outcome
                    .lock()
                    .map_err(|_| anyhow!("Unavailable init attempt"))?;
                let outcome = attempt
                    .changed
                    .wait_while(outcome, |outcome| outcome.is_none())
                    .map_err(|_| anyhow!("Unavailable init attempt"))?;
                return report(outcome.as_ref().expect("completed attempt").clone());
            }
            CellState::Uninitialized | CellState::Retryable(_) => {}
        }
        let attempt = Arc::new(Attempt::default());
        *state = CellState::Initializing(attempt.clone());
        drop(state);
        eprintln!("level=info event=namespace_init namespace={namespace:?} phase=PreflightFencing disposition=None state=Initializing");
        let outcome = owner.initialize(namespace);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Unavailable namespace cell"))?;
        *state = match &outcome {
            Ok(service) => {
                service.backend.finish_initialization();
                eprintln!("level=info event=namespace_init namespace={namespace:?} phase=ServiceBuild disposition=Committed state=Ready");
                CellState::Ready(service.clone())
            }
            Err(failure) => {
                eprintln!("level=error event=namespace_init namespace={namespace:?} phase={:?} disposition={:?} state={:?}", failure.phase, failure.disposition, failure.kind);
                match failure.kind {
                    FailureKind::Retryable => CellState::Retryable(failure.clone()),
                    FailureKind::FailedDefinite => CellState::FailedDefinite(failure.clone()),
                    FailureKind::BlockedUnknown => CellState::BlockedUnknown(failure.clone()),
                }
            }
        };
        *attempt
            .outcome
            .lock()
            .map_err(|_| anyhow!("Unavailable init attempt"))? = Some(outcome.clone());
        attempt.changed.notify_all();
        report(outcome)
    }

    pub fn ready(&self) -> Result<Arc<Service>> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("Unavailable namespace cell"))?;
        match &*state {
            CellState::Ready(service) => Ok(service.clone()),
            CellState::Retryable(failure)
            | CellState::FailedDefinite(failure)
            | CellState::BlockedUnknown(failure) => report(Err(failure.clone())),
            _ => Err(anyhow!("Fenced namespace not admitted")),
        }
    }

    pub fn quiescent(&self) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        match &*state {
            // A cell is visible in the registry before its owner claims it: `service()`
            // publishes the entry and `acquire` flips it to `Initializing` afterwards, so
            // this state is reachable while an initialization is about to dispatch its
            // first write. Only `Retryable` and `FailedDefinite` carry evidence that the
            // last attempt dispatched nothing; answering "not quiescent" for an
            // unclaimed cell keeps the fail-closed reading of the pre-R03 empty cell.
            CellState::Uninitialized
            | CellState::Initializing(_)
            | CellState::BlockedUnknown(_) => false,
            CellState::Retryable(_) | CellState::FailedDefinite(_) => true,
            CellState::Ready(service) => service.available().is_ok() && service.gate.quiescent(),
        }
    }
}

fn classify(disposition: Disposition, error: &anyhow::Error) -> FailureKind {
    match disposition {
        Disposition::Unknown => FailureKind::BlockedUnknown,
        // Acknowledged boot CAS followed by ClockAnchor/ServiceBuild failure is
        // definite, not Unknown: no Service or grant has escaped, and there is
        // no unsettled write. Do not replay it, but allow a clean shutdown (T13).
        Disposition::Committed => FailureKind::FailedDefinite,
        Disposition::None | Disposition::Rejected => {
            if error.is::<ReadFailure>() || error.is::<PreconditionFailed>() {
                FailureKind::Retryable
            } else {
                FailureKind::FailedDefinite
            }
        }
    }
}

impl OwnershipAuthority {
    fn initialize(&self, namespace: &Namespace) -> Outcome {
        let backend = NamespaceBackend::scoped(self.store.clone(), namespace.clone());
        let mut phase = InitPhase::PreflightFencing;
        let result = (|| -> Result<Arc<Service>> {
            anyhow::ensure!(
                self.store.supports_strict_fencing(),
                "UncertainUnsupported backend may hide unsettled mutation retries"
            );
            phase = InitPhase::RootRead;
            let object = match Authority::open_root(backend.as_ref(), namespace)? {
                Some(object) => object,
                None => {
                    phase = InitPhase::RootCreateCas;
                    let object = Authority::claim_absent_root(backend.as_ref(), namespace)?;
                    backend.available()?;
                    object
                }
            };
            phase = InitPhase::DeepVerify;
            let authority = Authority::from_loaded(backend.clone(), namespace.clone(), object)?;
            phase = InitPhase::BootRead;
            let snapshot = authority
                .boot_ownership()
                .map_err(|failure| match failure {
                    BootFailure::Read(error) => error,
                    BootFailure::Unsettled(error) => {
                        phase = InitPhase::BootPublish;
                        error
                    }
                })?;
            backend.available()?;
            phase = InitPhase::ClockAnchor;
            let clock_anchor = self.clock.now_ms()?;
            phase = InitPhase::ServiceBuild;
            let catalog = snapshot
                .root
                .ownership
                .as_ref()
                .context("ownership migration missing")?;
            let gc_marks = snapshot
                .root
                .tombstones
                .entries()?
                .keys()
                .map(|blob| (blob.clone(), clock_anchor))
                .collect();
            Ok(Arc::new(Service {
                authority,
                backend: backend.clone(),
                clock: self.clock.clone(),
                gate: admission_gate::Admission::default(),
                memory: Mutex::new(Memory {
                    last_clock: Some(clock_anchor),
                    gc_marks,
                    ..Memory::default()
                }),
                authority_term: catalog.authority_term,
                incarnation: snapshot.root.incarnation,
            }))
        })();
        result.map_err(|error| {
            let disposition = backend.disposition();
            Arc::new(Failure {
                phase,
                disposition,
                kind: classify(disposition, &error),
                error,
                _backend: backend,
            })
        })
    }
}
