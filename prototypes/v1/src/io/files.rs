use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use turso_core::io::{Buffer, Completion, File, FileSyncType};

use super::{finish, Shared};
use crate::manifest::WalPosition;
use crate::recovery::committed_position;
use crate::wal::{apply_batch, WalBatch, WalOperation};

pub(super) struct RemoteFile {
    pub(super) shared: Arc<Shared>,
    pub(super) wal: bool,
    pub(super) readonly: bool,
}

impl RemoteFile {
    fn write(&self, pos: u64, buffers: Vec<Arc<Buffer>>) -> Result<usize> {
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
            apply_batch(&mut state.bytes, &batch)?;
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
            let state = self.shared.lock()?;
            let buffer = completion.as_read().buf();
            buffer.as_mut_slice().fill(0);
            if self.wal {
                let pos = usize::try_from(pos)?;
                let count = buffer.len().min(state.bytes.len().saturating_sub(pos));
                buffer.as_mut_slice().fill(0);
                if count > 0 {
                    buffer.as_mut_slice()[..count].copy_from_slice(&state.bytes[pos..pos + count]);
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
        let state = self.shared.lock()?;
        Ok(if self.wal {
            state.bytes.len() as u64
        } else {
            self.shared.storage.size()
        })
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        let result = (|| {
            ensure!(!self.readonly, "file opened read-only");
            let mut state = self.shared.lock()?;
            if self.wal {
                self.shared.allow_reset(&state)?;
                let size = usize::try_from(len)?;
                if size > state.bytes.len() {
                    let additional = size - state.bytes.len();
                    state.bytes.try_reserve(additional)?;
                }
                state.bytes.resize(size, 0);
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
