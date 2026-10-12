use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use turso_core::{LimboError, IO};

use super::RemoteIO;
use crate::session::AdmissionError;

#[derive(Default)]
pub(super) struct NativeIoGate {
    disabled: AtomicBool,
    pub(super) calls: RwLock<()>,
    active: AtomicUsize,
}

pub(super) struct NativeCall<'gate> {
    gate: &'gate NativeIoGate,
    _ticket: RwLockReadGuard<'gate, ()>,
}

impl Drop for NativeCall<'_> {
    fn drop(&mut self) {
        self.gate.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct NativeIoToken<'owner> {
    pub io: Arc<dyn IO>,
    _calls: RwLockWriteGuard<'owner, ()>,
}

impl NativeIoGate {
    pub fn health(&self) -> serde_json::Value {
        serde_json::json!({"pending_native_operations": self.active.load(Ordering::Acquire), "native_dispatch_disabled": self.disabled.load(Ordering::Acquire), "callback_tracking": if cfg!(unix) { "synchronous_callbacks_included_in_operations" } else { "unavailable_async_callbacks" }})
    }

    pub fn enter(&self) -> turso_core::Result<NativeCall<'_>> {
        self.enter_checked(|| {})
    }

    fn enter_checked(&self, mut checked: impl FnMut()) -> turso_core::Result<NativeCall<'_>> {
        loop {
            self.check_enabled()?;
            checked();
            match self.calls.try_read() {
                Ok(ticket) => {
                    self.check_enabled()?;
                    self.active.fetch_add(1, Ordering::AcqRel);
                    return Ok(NativeCall {
                        gate: self,
                        _ticket: ticket,
                    });
                }
                Err(TryLockError::Poisoned(_)) => {
                    return Err(LimboError::InternalError(
                        "native instance IO gate poisoned".into(),
                    ));
                }
                Err(TryLockError::WouldBlock) => std::thread::yield_now(),
            }
        }
    }

    fn check_enabled(&self) -> turso_core::Result<()> {
        if self.disabled.load(Ordering::Acquire) {
            Err(LimboError::InternalError(
                "native instance IO retired".into(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn disable(&self) {
        self.disabled.store(true, Ordering::Release);
    }

    pub fn retirement_token(
        &self,
        io: Arc<RemoteIO>,
        deadline: Instant,
    ) -> Result<NativeIoToken<'_>> {
        self.disable();
        ensure!(
            cfg!(unix),
            "native local retirement requires synchronous UnixIO"
        );
        loop {
            match self.calls.try_write() {
                Ok(calls) => return Ok(NativeIoToken { io, _calls: calls }),
                Err(TryLockError::Poisoned(_)) => return Err(AdmissionError::Quarantine.into()),
                Err(TryLockError::WouldBlock) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(AdmissionError::Deadline.into());
                    }
                    std::thread::sleep(remaining.min(Duration::from_millis(10)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
