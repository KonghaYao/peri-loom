use std::sync::Arc;

use turso_core::io::{
    Buffer, Completion, File, FileSyncType, SharedWalLockKind, SharedWalMappedRegion,
};
use turso_core::{CompletionError, LimboError};

use super::native::NativeIoGate;

pub(super) struct NativeFile {
    inner: Arc<dyn File>,
    gate: Arc<NativeIoGate>,
    synchronous: bool,
}

impl NativeFile {
    pub fn wrap_synchronous(inner: Arc<dyn File>, gate: Arc<NativeIoGate>) -> Arc<dyn File> {
        Arc::new(Self {
            inner,
            gate,
            synchronous: true,
        })
    }

    pub fn wrap_platform(inner: Arc<dyn File>, gate: Arc<NativeIoGate>) -> Arc<dyn File> {
        Arc::new(Self {
            inner,
            gate,
            synchronous: cfg!(unix),
        })
    }

    fn submit(
        &self,
        completion: Completion,
        dispatch: impl FnOnce(Completion) -> turso_core::Result<Completion>,
    ) -> turso_core::Result<Completion> {
        let _ticket = match self.gate.enter() {
            Ok(ticket) => ticket,
            Err(error) => {
                terminal_failure(&completion, &error);
                return Err(error);
            }
        };
        let result = dispatch(completion.clone());
        if self.synchronous {
            if let Err(error) = &result {
                terminal_failure(&completion, error);
            }
        }
        result
    }
}

fn terminal_failure(completion: &Completion, error: &LimboError) {
    if !completion.finished() {
        let error = match error {
            LimboError::CompletionError(error) => *error,
            _ => {
                CompletionError::IOError(std::io::ErrorKind::Other, "native file operation failed")
            }
        };
        completion.error(error);
    }
}

impl File for NativeFile {
    fn checkpoint_wal_position(&self, salts: [u32; 2], frame_count: u64) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.checkpoint_wal_position(salts, frame_count)
    }

    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, completion: Completion) -> turso_core::Result<Completion> {
        self.submit(completion, |completion| self.inner.pread(pos, completion))
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        self.submit(completion, |completion| {
            self.inner.pwrite(pos, buffer, completion)
        })
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        self.submit(completion, |completion| {
            self.inner.pwritev(pos, buffers, completion)
        })
    }

    fn sync(
        &self,
        completion: Completion,
        sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        self.submit(completion, |completion| {
            self.inner.sync(completion, sync_type)
        })
    }

    fn size(&self) -> turso_core::Result<u64> {
        let _ticket = self.gate.enter()?;
        self.inner.size()
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        self.submit(completion, |completion| {
            self.inner.truncate(len, completion)
        })
    }

    fn has_hole(&self, pos: usize, len: usize) -> turso_core::Result<bool> {
        let _ticket = self.gate.enter()?;
        self.inner.has_hole(pos, len)
    }

    fn punch_hole(&self, pos: usize, len: usize) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.punch_hole(pos, len)
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> turso_core::Result<bool> {
        let _ticket = self.gate.enter()?;
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_probe_exclusive_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> turso_core::Result<bool> {
        let _ticket = self.gate.enter()?;
        self.inner.shared_wal_probe_exclusive_byte(offset, kind)
    }

    fn shared_wal_probe_exclusive_while_shared_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> turso_core::Result<bool> {
        let _ticket = self.gate.enter()?;
        self.inner
            .shared_wal_probe_exclusive_while_shared_byte(offset, kind)
    }

    fn shared_wal_unlock_byte(
        &self,
        offset: u64,
        kind: SharedWalLockKind,
    ) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.shared_wal_unlock_byte(offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> turso_core::Result<()> {
        let _ticket = self.gate.enter()?;
        self.inner.shared_wal_set_len(len)
    }

    fn shared_wal_map(
        &self,
        offset: u64,
        len: usize,
    ) -> turso_core::Result<Box<dyn SharedWalMappedRegion>> {
        let _ticket = self.gate.enter()?;
        self.inner.shared_wal_map(offset, len)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod native_tests;
