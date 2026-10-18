use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::work_metrics::AdmissionWait;

const MAX_WAITERS: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    Conflict,
    QueueFull,
    Deadline,
    Closing,
    Quarantine,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Conflict => "writer admission conflict",
            Self::QueueFull => "writer admission queue full",
            Self::Deadline => "writer admission deadline",
            Self::Closing => "writer admission closing",
            Self::Quarantine => "writer admission quarantine",
        })
    }
}

impl std::error::Error for AdmissionError {}

fn queued_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnsettledSqlOutcome;

impl fmt::Display for UnsettledSqlOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("current SQL operation outcome is unsettled")
    }
}

impl std::error::Error for UnsettledSqlOutcome {}

#[derive(Clone, Debug)]
pub struct AdmissionStatus {
    pub owner: Option<u64>,
    pub queued: Vec<u64>,
    pub quarantined: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SessionWriterState {
    pub owner: bool,
    pub sequence: u64,
}

pub(crate) struct SessionControl {
    pub id: u64,
    pub closing: AtomicBool,
    packed: AtomicU64,
}

impl SessionControl {
    pub fn hint(&self) -> SessionWriterState {
        let packed = self.packed.load(Ordering::Acquire);
        SessionWriterState {
            owner: packed & 1 != 0,
            sequence: packed >> 1,
        }
    }

    fn settled(&self, owner: bool) -> Result<()> {
        let next = self
            .hint()
            .sequence
            .checked_add(1)
            .filter(|sequence| *sequence <= u64::MAX >> 1)
            .ok_or(AdmissionError::Quarantine)?;
        self.packed
            .store((next << 1) | u64::from(owner), Ordering::Release);
        Ok(())
    }
}

#[derive(Default)]
struct State {
    next: u64,
    owner: Option<u64>,
    queue: VecDeque<u64>,
    maintenance: bool,
    quarantined: bool,
    controls: Vec<Weak<SessionControl>>,
}

pub(crate) struct WriterAdmission {
    state: Mutex<State>,
    changed: Condvar,
    trace: Option<Arc<crate::capacity_trace::Trace>>,
}

impl Default for WriterAdmission {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            changed: Condvar::default(),
            trace: crate::capacity_trace::Trace::new(None),
        }
    }
}

impl WriterAdmission {
    pub(crate) fn trace_identity(&self) -> Option<String> {
        self.trace.as_ref().map(|trace| trace.identity())
    }

    fn observe(&self, kind: &'static str, id: u64, holder: Option<u64>) {
        if let Some(trace) = &self.trace {
            trace.admission(kind, id, holder);
        }
    }

    pub(crate) fn try_quarantined(&self) -> Option<bool> {
        self.state.try_lock().ok().map(|state| state.quarantined)
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| AdmissionError::Quarantine.into())
    }

    pub fn register(&self) -> Result<Arc<SessionControl>> {
        let mut state = self.lock()?;
        if state.quarantined {
            return Err(AdmissionError::Quarantine.into());
        }
        state.next = state
            .next
            .checked_add(1)
            .ok_or(AdmissionError::Quarantine)?;
        let control = Arc::new(SessionControl {
            id: state.next,
            closing: AtomicBool::new(false),
            packed: AtomicU64::new(0),
        });
        state.controls.retain(|control| control.strong_count() > 0);
        state.controls.push(Arc::downgrade(&control));
        Ok(control)
    }

    pub fn status(&self) -> Result<AdmissionStatus> {
        let state = self.lock()?;
        Ok(AdmissionStatus {
            owner: state.owner,
            queued: state.queue.iter().copied().collect(),
            quarantined: state.quarantined,
        })
    }

    pub fn healthy(&self) -> Result<()> {
        if self.lock()?.quarantined {
            return Err(AdmissionError::Quarantine.into());
        }
        Ok(())
    }

    pub fn quarantine(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.quarantined = true;
        self.observe("invalidation", state.owner.unwrap_or(0), state.owner);
        for control in state.controls.iter().filter_map(Weak::upgrade) {
            control.closing.store(true, Ordering::Release);
        }
        self.changed.notify_all();
    }

    pub fn close_requested(&self, control: &SessionControl) {
        control.closing.store(true, Ordering::Release);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.queue.contains(&control.id) {
            self.observe("cancel", control.id, state.owner);
        }
        state.queue.retain(|queued| *queued != control.id);
        self.changed.notify_all();
    }

    pub fn acquire(
        self: &Arc<Self>,
        control: &Arc<SessionControl>,
        promote: bool,
        mut check: impl FnMut() -> Result<()>,
    ) -> Result<Permit> {
        check()?;
        let mut state = self.lock()?;
        if state.quarantined {
            return Err(AdmissionError::Quarantine.into());
        }
        if control.closing.load(Ordering::Acquire) {
            return Err(AdmissionError::Closing.into());
        }
        if state.owner == Some(control.id) {
            self.observe("owner_reuse", control.id, state.owner);
            return Ok(Permit {
                admission: self.clone(),
                control: control.clone(),
                armed: true,
            });
        }
        if promote && (state.owner.is_some() || !state.queue.is_empty() || state.maintenance) {
            return Err(AdmissionError::Conflict.into());
        }
        if state.owner.is_none() && state.queue.is_empty() && !state.maintenance {
            self.observe("enqueue", control.id, state.owner);
            state.owner = Some(control.id);
            self.observe("admit", control.id, None);
            return Ok(Permit {
                admission: self.clone(),
                control: control.clone(),
                armed: true,
            });
        }
        if state.queue.len() >= MAX_WAITERS {
            return Err(AdmissionError::QueueFull.into());
        }
        state.queue.push_back(control.id);
        let queued_at = crate::work_metrics::enabled().then(Instant::now);
        self.observe("enqueue", control.id, state.owner);
        drop(state);
        let mut ticket = Ticket {
            admission: self.clone(),
            id: control.id,
            queued: true,
        };
        loop {
            if let Err(error) = check() {
                let Some(started) = queued_at else {
                    self.cancel(control.id);
                    return Err(error);
                };
                let blocked = self
                    .state
                    .lock()
                    .map(|state| (state.maintenance, state.queue.len()))
                    .unwrap_or((false, 0));
                self.cancel(control.id);
                crate::work_metrics::note_admission_wait(AdmissionWait {
                    waited_ns: queued_ns(started),
                    maintenance_blocked: blocked.0,
                    queue_depth: blocked.1,
                });
                return Err(error);
            }
            let mut state = self.lock()?;
            if state.quarantined || control.closing.load(Ordering::Acquire) {
                if state.queue.contains(&control.id) {
                    self.observe("cancel", control.id, state.owner);
                }
                state.queue.retain(|queued| *queued != control.id);
                self.changed.notify_all();
                return Err(if state.quarantined {
                    AdmissionError::Quarantine
                } else {
                    AdmissionError::Closing
                }
                .into());
            }
            if state.owner.is_none()
                && !state.maintenance
                && state.queue.front() == Some(&control.id)
            {
                state.queue.pop_front();
                state.owner = Some(control.id);
                self.observe("admit", control.id, None);
                ticket.queued = false;
                return Ok(Permit {
                    admission: self.clone(),
                    control: control.clone(),
                    armed: true,
                });
            }
            let (state, _) = self
                .changed
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| AdmissionError::Quarantine)?;
            drop(state);
        }
    }

    fn cancel(&self, id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.queue.contains(&id) {
            self.observe("cancel", id, state.owner);
        }
        state.queue.retain(|queued| *queued != id);
        self.changed.notify_all();
    }

    pub fn settle(&self, control: &SessionControl, writer: bool) -> Result<()> {
        let mut state = self.lock()?;
        if writer && state.owner != Some(control.id) {
            state.quarantined = true;
            self.observe("invalidation", control.id, state.owner);
            self.changed.notify_all();
            return Err(AdmissionError::Quarantine.into());
        }
        control.settled(writer)?;
        if !writer && state.owner == Some(control.id) {
            state.owner = None;
            self.observe("release", control.id, None);
        }
        self.changed.notify_all();
        Ok(())
    }

    pub fn maintenance(self: &Arc<Self>) -> Result<Maintenance> {
        let mut state = self.lock()?;
        if state.quarantined {
            return Err(AdmissionError::Quarantine.into());
        }
        if state.owner.is_some() || !state.queue.is_empty() || state.maintenance {
            return Err(AdmissionError::Conflict.into());
        }
        state.maintenance = true;
        Ok(Maintenance {
            admission: self.clone(),
            settled: false,
            dispatched: false,
        })
    }
}

struct Ticket {
    admission: Arc<WriterAdmission>,
    id: u64,
    queued: bool,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if self.queued {
            self.admission.cancel(self.id);
        }
    }
}

#[cfg(test)]
mod tests;

pub(crate) struct Permit {
    admission: Arc<WriterAdmission>,
    control: Arc<SessionControl>,
    armed: bool,
}

impl Permit {
    pub fn settle(mut self, writer: bool) -> Result<()> {
        self.admission.settle(&self.control, writer)?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if self.armed {
            self.admission.quarantine();
        }
    }
}

pub(crate) struct Maintenance {
    admission: Arc<WriterAdmission>,
    settled: bool,
    dispatched: bool,
}

impl Maintenance {
    pub fn dispatch(&mut self) {
        self.dispatched = true;
    }

    pub fn complete(mut self) -> Result<()> {
        self.admission.lock()?.maintenance = false;
        self.admission.changed.notify_all();
        self.settled = true;
        Ok(())
    }
}

impl Drop for Maintenance {
    fn drop(&mut self) {
        if !self.settled {
            if self.dispatched {
                self.admission.quarantine();
            } else {
                let mut state = self
                    .admission
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.maintenance = false;
                self.admission.changed.notify_all();
            }
        }
    }
}
