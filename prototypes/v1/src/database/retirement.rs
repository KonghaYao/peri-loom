use std::time::Instant;

use anyhow::Result;
use turso_core::{NativeIoRetirementGuard, IO};

use super::{OperationWriteGuard, RemoteDatabase};
use crate::io::NativeIoToken;
use crate::session::RequestDeadline;

pub(crate) struct DatabaseRetirementGuard<'instance> {
    native: NativeIoToken<'instance>,
    _operations: OperationWriteGuard<'instance>,
}

impl NativeIoRetirementGuard for DatabaseRetirementGuard<'_> {
    fn io(&self) -> &std::sync::Arc<dyn IO> {
        &self.native.io
    }
}

impl RemoteDatabase {
    pub(crate) fn retirement_health(&self) -> serde_json::Value {
        let started = Instant::now();
        // Every field below is a non-blocking probe. The user needs the *name* of the
        // lock and the database it belongs to, because shutdown cannot distinguish a
        // transient sampler race from real outstanding work from a bare boolean.
        let fail = |lock: &'static str, state: &'static str| {
            serde_json::json!({
                "available": false,
                "reason": "native_lock_unavailable",
                "lock": lock,
                "lock_held": true,
                "state": state,
                "database": self.storage().namespace().prefix(),
                "elapsed_us": started
                    .elapsed()
                    .as_micros()
                    .min(u64::MAX as u128) as u64,
                "quarantined": null,
                "cleanup_active": self.cleanup.load(std::sync::atomic::Ordering::Acquire),
                "pending_retained_statements": null,
                "live_engines": null,
                "live_connections": null,
                "retained_engines": null,
                "retained_connections": null,
                "io": self.io.native_health(),
            })
        };
        let Some(quarantined) = self.admission.try_quarantined() else {
            return fail("admission", "state_locked");
        };
        let Ok(pending) = self.pending.try_lock() else {
            return fail("pending", "state_locked");
        };
        let Ok(engine) = self.engine.try_lock() else {
            return fail("engine", "state_locked");
        };
        let Ok(lanes) = self.lanes.try_lock() else {
            return fail("lanes", "state_locked");
        };
        let mut connections = 0;
        for lane in lanes.iter().filter_map(std::sync::Weak::upgrade) {
            let Ok(connection) = lane.connection.try_lock() else {
                return fail("lane.connection", "state_locked");
            };
            connections += usize::from(connection.is_some());
        }
        serde_json::json!({
            "available": true,
            "quarantined": quarantined,
            "cleanup_active": self.cleanup.load(std::sync::atomic::Ordering::Acquire),
            "pending_retained_statements": pending.len(),
            "live_engines": usize::from(engine.is_some()),
            "live_connections": connections,
            "retained_engines": if quarantined { usize::from(engine.is_some()) } else { 0 },
            "retained_connections": if quarantined { connections } else { 0 },
            "io": self.io.native_health(),
            "sampled_after_us": started
                .elapsed()
                .as_micros()
                .min(u64::MAX as u128) as u64,
        })
    }

    pub(crate) fn retirement_guard(
        &self,
        deadline: &RequestDeadline,
    ) -> Result<DatabaseRetirementGuard<'_>> {
        self.admission.quarantine();
        self.io.disable_native_dispatch();
        let operations = self.operations.write_until(deadline, || {
            deadline.check()?;
            Ok(())
        })?;
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
