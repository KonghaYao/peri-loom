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
        Ok(DatabaseRetirementGuard {
            native,
            _operations: operations,
        })
    }
}
