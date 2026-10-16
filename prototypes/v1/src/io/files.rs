use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use turso_core::io::{Buffer, Completion, File, FileSyncType};

use super::{finish, Shared};
use crate::manifest::WalPosition;
use crate::recovery::committed_position;
use crate::wal::{WalBatch, WalOperation};

pub(super) struct RemoteFile {
    pub(super) shared: Arc<Shared>,
    pub(super) wal: bool,
    pub(super) readonly: bool,
}

impl RemoteFile {
    fn write(&self, pos: u64, buffers: Vec<Arc<Buffer>>) -> Result<usize> {
        self.shared.check_local_writer()?;
        ensure!(!self.readonly, "file opened read-only");
        let total = buffers
            .iter()
            .try_fold(0usize, |total, buffer| total.checked_add(buffer.len()))
            .context("write size overflow")?;
        ensure!(
            total <= i32::MAX as usize,
            "write exceeds completion byte limit"
        );
        pos.checked_add(total as u64)
            .context("write offset overflow")?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(total)?;
        for buffer in buffers {
            bytes.extend_from_slice(buffer.as_slice());
        }
        let mut state = self.shared.lock()?;
        if self.wal {
            ensure!(
                !state.wal_failed,
                "WAL durability failure requires cold recovery"
            );
            if pos < 32 {
                self.shared.allow_reset(&state)?;
            }
            let resetting =
                pos == 0 && bytes.len() >= 32 && committed_position(&bytes[..32]).is_ok();
            let mut operations = Vec::new();
            if resetting {
                operations.push(WalOperation::Truncate { size: 0 });
            }
            operations.push(WalOperation::Write { offset: pos, bytes });
            let batch = WalBatch { operations };
            state.image.apply(&batch)?;
            if resetting {
                state.last_commit = None;
            }
            state.batch.operations.extend(batch.operations);
        } else {
            state.dirty = true;
            let written = self.shared.storage.write_at(pos, &bytes)?;
            ensure!(written == total, "short remote database write");
        }
        Ok(total)
    }
}

impl File for RemoteFile {
    fn checkpoint_wal_position(&self, salts: [u32; 2], frame_count: u64) -> turso_core::Result<()> {
        self.shared
            .check_local_writer()
            .map_err(super::adapter_error)?;
        self.shared.lock()?.position = Some(WalPosition { salts, frame_count });
        Ok(())
    }

    fn lock_file(&self, _exclusive: bool) -> turso_core::Result<()> {
        Ok(())
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        Ok(())
    }

    fn pread(&self, pos: u64, completion: Completion) -> turso_core::Result<Completion> {
        let result = (|| {
            self.shared.storage.store().validate_local()?;
            let state = self.shared.lock()?;
            let buffer = completion.as_read().buf();
            buffer.as_mut_slice().fill(0);
            if self.wal {
                let pos = usize::try_from(pos)?;
                let image = state.image.bytes();
                let count = buffer.len().min(image.len().saturating_sub(pos));
                buffer.as_mut_slice().fill(0);
                if count > 0 {
                    buffer.as_mut_slice()[..count].copy_from_slice(&image[pos..pos + count]);
                }
                Ok(count)
            } else {
                self.shared.storage.read_at(pos, buffer.as_mut_slice())
            }
        })();
        finish(completion, result)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        finish(completion, self.write(pos, vec![buffer]))
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        completion: Completion,
    ) -> turso_core::Result<Completion> {
        finish(completion, self.write(pos, buffers))
    }

    fn sync(
        &self,
        completion: Completion,
        _sync_type: FileSyncType,
    ) -> turso_core::Result<Completion> {
        let result = (|| {
            self.shared.check_writer()?;
            let mut state = self.shared.lock()?;
            if self.wal {
                self.shared.sync_wal(&mut state)?;
            } else {
                self.shared.publish(&mut state)?;
            }
            Ok(0)
        })();
        finish(completion, result)
    }

    fn size(&self) -> turso_core::Result<u64> {
        self.shared
            .storage
            .store()
            .validate_local()
            .map_err(super::adapter_error)?;
        let state = self.shared.lock()?;
        Ok(if self.wal {
            state.image.len() as u64
        } else {
            self.shared.storage.size()
        })
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        let result = (|| {
            self.shared.check_local_writer()?;
            ensure!(!self.readonly, "file opened read-only");
            let mut state = self.shared.lock()?;
            if self.wal {
                self.shared.allow_reset(&state)?;
                let size = usize::try_from(len)?;
                state.image.resize(size)?;
                state
                    .batch
                    .operations
                    .push(WalOperation::Truncate { size: len });
                self.shared.sync_wal(&mut state)?;
            } else {
                state.dirty = true;
                self.shared.storage.truncate(len)?;
            }
            Ok(0)
        })();
        finish(completion, result)
    }
}
