use std::sync::RwLockWriteGuard;

use anyhow::Result;
use turso_core::{NativeIoRetirementGuard, IO};

use super::RemoteDatabase;
use crate::io::NativeIoToken;
use crate::session::{AdmissionError, RequestDeadline};

pub(crate) struct DatabaseRetirementGuard<'instance> {
    native: NativeIoToken<'instance>,
    _operations: RwLockWriteGuard<'instance, ()>,
}

impl NativeIoRetirementGuard for DatabaseRetirementGuard<'_> {
    fn io(&self) -> &std::sync::Arc<dyn IO> {
        &self.native.io
    }
}

impl RemoteDatabase {
    pub(crate) fn retirement_health(&self) -> serde_json::Value {
        let sample = (|| -> Option<serde_json::Value> {
            let quarantined = self.admission.try_quarantined()?;
            let pending = self.pending.try_lock().ok()?;
            let engine = self.engine.try_lock().ok()?;
            let lanes = self.lanes.try_lock().ok()?;
            let mut connections = 0;
            for lane in lanes.iter().filter_map(std::sync::Weak::upgrade) {
                connections += usize::from(lane.connection.try_lock().ok()?.is_some());
            }
            Some(serde_json::json!({
                "available": true,
                "quarantined": quarantined,
                "cleanup_active": self.cleanup.load(std::sync::atomic::Ordering::Acquire),
                "pending_retained_statements": pending.len(),
                "live_engines": usize::from(engine.is_some()),
                "live_connections": connections,
                "retained_engines": if quarantined { usize::from(engine.is_some()) } else { 0 },
                "retained_connections": if quarantined { connections } else { 0 },
                "io": self.io.native_health(),
            }))
        })();
        sample.unwrap_or_else(
            || serde_json::json!({"available": false, "reason": "native_lock_unavailable"}),
        )
    }

    pub(crate) fn retirement_guard(
        &self,
        deadline: &RequestDeadline,
    ) -> Result<DatabaseRetirementGuard<'_>> {
        self.admission.quarantine();
        self.io.disable_native_dispatch();
        let operations = loop {
            match self.operations.try_write() {
                Ok(operations) => break operations,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(AdmissionError::Quarantine.into());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    deadline.check()?;
                    let remaining = deadline
                        .end()
                        .saturating_duration_since(std::time::Instant::now());
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(10)));
                }
            }
        };
        let native = self.io.retirement_token(deadline.end())?;
        self.storage().store().retire_scope(deadline.end())?;
        Ok(DatabaseRetirementGuard {
            native,
            _operations: operations,
        })
    }
}

#[cfg(all(test, unix))]
#[path = "reclamation/native_io_tests.rs"]
mod native_io_tests;

#[cfg(all(test, unix))]
#[path = "reclamation/health_tests.rs"]
mod health_tests;
