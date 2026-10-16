use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{ensure, Context, Result};

pub struct AuthorityStartup {
    file: File,
}

impl AuthorityStartup {
    pub fn acquire(directory: impl AsRef<Path>, confirmed_quiescence: bool) -> Result<Self> {
        let directory = directory.as_ref();
        let created = !directory.exists();
        std::fs::create_dir_all(directory)?;
        let metadata = std::fs::symlink_metadata(directory)?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "authority state directory must be a real directory"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if created {
                std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
            }
            ensure!(
                std::fs::metadata(directory)?.permissions().mode() & 0o077 == 0,
                "authority state directory must be private (0700)"
            );
        }
        let path = directory.join("authority.lock");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(path)?;
        file.try_lock()
            .context("authority state directory already exclusively locked")?;
        ensure!(
            file.metadata()?.len() <= 16,
            "invalid authority startup marker"
        );
        let mut marker = String::new();
        file.read_to_string(&mut marker)?;
        ensure!(
            matches!(marker.as_str(), "" | "clean\n" | "dirty\n"),
            "invalid authority startup marker; refusing reset"
        );
        ensure!(marker != "dirty\n" || confirmed_quiescence, "unclean authority shutdown: confirm old backend operations quiescent before --reconciled restart");
        let mut startup = Self { file };
        startup.write_marker(b"dirty\n")?;
        File::open(directory)?.sync_all()?;
        Ok(startup)
    }

    pub(crate) fn mark_clean(&mut self) -> Result<()> {
        self.write_marker(b"clean\n")
    }

    /// Revokes a clean marker that landed after the shutdown deadline. The marker
    /// is what authorizes reuse of the state directory, so an over-budget stop
    /// must not leave it behind.
    pub(crate) fn mark_dirty(&mut self) -> Result<()> {
        self.write_marker(b"dirty\n")
    }

    fn write_marker(&mut self, marker: &[u8]) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(marker)?;
        self.file.set_len(marker.len() as u64)?;
        self.file.sync_all()?;
        Ok(())
    }
}

impl Drop for AuthorityStartup {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}
