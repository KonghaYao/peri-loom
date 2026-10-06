use std::fs::{File, OpenOptions};
use std::path::Path;

use anyhow::{Context, Result};

pub(super) struct DirectoryLock {
    file: File,
}

impl DirectoryLock {
    pub(super) fn acquire(root: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".peri-loom.lock"))?;
        file.try_lock()
            .context("remote cache directory already locked")?;
        Ok(Self { file })
    }
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_only_retains_lock_while_a_duplicate_descriptor_survives() -> Result<()> {
        let root = tempfile::tempdir()?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.path().join(".peri-loom.lock"))?;
        owner.try_lock()?;
        let inherited = owner.try_clone()?;
        drop(owner);
        assert!(DirectoryLock::acquire(root.path()).is_err());
        drop(inherited);
        let _reopened = DirectoryLock::acquire(root.path())?;
        Ok(())
    }

    #[test]
    fn final_owner_explicitly_unlocks_even_with_a_surviving_duplicate() -> Result<()> {
        let root = tempfile::tempdir()?;
        let owner = DirectoryLock::acquire(root.path())?;
        let inherited = owner.file.try_clone()?;
        assert!(DirectoryLock::acquire(root.path()).is_err());
        drop(owner);
        let replacement = DirectoryLock::acquire(root.path())?;
        drop(inherited);
        assert!(DirectoryLock::acquire(root.path()).is_err());
        drop(replacement);
        let _reopened = DirectoryLock::acquire(root.path())?;
        Ok(())
    }
}
