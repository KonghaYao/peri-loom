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
        let _append = self.shared.append_guard().map_err(super::adapter_error)?;
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
            // Capacity is checked before the write joins the image or the pending batch.
            // Header/restart writes and non-WAL files are control paths: they either shrink
            // the log or do not touch it, and refusing them would block cleanup.
            self.shared
                .check_wal_budget(&state, &batch, resetting || pos < 32)?;
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
        let _append = self.shared.append_guard().map_err(super::adapter_error)?;
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
            let buffer = completion.as_read().buf();
            buffer.as_mut_slice().fill(0);
            if self.wal {
                let state = self.shared.lock()?;
                let pos = usize::try_from(pos)?;
                let image = state.image.bytes();
                let count = buffer.len().min(image.len().saturating_sub(pos));
                if count > 0 {
                    buffer.as_mut_slice()[..count].copy_from_slice(&image[pos..pos + count]);
                }
                Ok(count)
            } else {
                // The main database is served by storage, which pins its revision around the
                // read and retries if a mutation lands; this file's state carries only WAL
                // bookkeeping, so holding it here would spread a slow segment fetch across
                // every reader of the same database.
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
            if self.wal {
                let append = self.shared.append_guard()?;
                self.shared.sync_wal(&append)?;
            } else {
                // Freeze under the append gate so no append is in flight and no batch is
                // pending. The uploads then run without it: a WAL commit that lands while
                // the database image is uploading is newer than the published checkpoint
                // position, so it stays in the WAL for recovery to replay instead of
                // making publication fail or making every writer wait for the upload.
                let publication = {
                    let _append = self.shared.append_guard()?;
                    let mut state = self.shared.lock()?;
                    self.shared.freeze_publication(&mut state)?
                };
                let published = self.shared.publish_frozen(&publication);
                let _append = self.shared.append_guard()?;
                let mut state = self.shared.lock()?;
                self.shared
                    .finish_publication(&mut state, &publication, published)?;
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
        // The main database size lives in storage. Read it without the WAL state lock so a
        // parked publication, which holds the storage lock across its remote writes, cannot
        // block WAL readers through this file handle.
        if self.wal {
            let state = self.shared.lock()?;
            Ok(state.image.len() as u64)
        } else {
            Ok(self.shared.storage.size())
        }
    }

    fn truncate(&self, len: u64, completion: Completion) -> turso_core::Result<Completion> {
        let result = (|| {
            self.shared.check_local_writer()?;
            ensure!(!self.readonly, "file opened read-only");
            let _append = self.shared.append_guard().map_err(super::adapter_error)?;
            let mut state = self.shared.lock()?;
            if self.wal {
                self.shared.allow_reset(&state)?;
                let size = usize::try_from(len)?;
                state.image.resize(size)?;
                state
                    .batch
                    .operations
                    .push(WalOperation::Truncate { size: len });
                self.shared.sync_wal_locked(&mut state)?;
            } else {
                state.dirty = true;
                self.shared.storage.truncate(len)?;
            }
            Ok(0)
        })();
        finish(completion, result)
    }
}
