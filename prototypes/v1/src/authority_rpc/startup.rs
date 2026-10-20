use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};

use super::recovery::{
    self, BackendKind, MarkerState, RecoveryAdmission, RecoveryEvidence, RecoveryPlan,
};

pub struct RecoveryRequest {
    pub backend: BackendKind,
    pub evidence: Option<RecoveryEvidence>,
    pub confirmed_quiescence: bool,
    /// Optional machine-checkable plan artifact. The pre-seal plan is written
    /// before the authority decides, and the final plan is rewritten after the
    /// seal takeover has been verified.
    pub plan_out: Option<PathBuf>,
}

pub struct AuthorityStartup {
    file: File,
    plan: RecoveryPlan,
    plan_out: Option<PathBuf>,
}

impl AuthorityStartup {
    pub fn acquire(directory: impl AsRef<Path>, confirmed_quiescence: bool) -> Result<Self> {
        Self::acquire_with_recovery(
            directory,
            RecoveryRequest {
                backend: BackendKind::Local,
                evidence: None,
                confirmed_quiescence,
                plan_out: None,
            },
        )
    }

    pub fn acquire_with_recovery(
        directory: impl AsRef<Path>,
        request: RecoveryRequest,
    ) -> Result<Self> {
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
        let marker = match marker.as_str() {
            "" | "clean\n" => MarkerState::Clean,
            "dirty\n" => MarkerState::Dirty,
            _ => anyhow::bail!("invalid authority startup marker; refusing reset"),
        };
        let plan = recovery::evaluate(
            marker,
            request.backend,
            request.evidence.as_ref(),
            request.confirmed_quiescence,
            true,
        );
        if let Some(path) = &request.plan_out {
            write_plan(path, &plan)?;
        }
        ensure!(plan.may_start(), "{}", plan.refusal_message());
        let mut startup = Self {
            file,
            plan,
            plan_out: request.plan_out,
        };
        startup.write_marker(b"dirty\n")?;
        File::open(directory)?.sync_all()?;
        Ok(startup)
    }

    pub(crate) fn plan(&self) -> &RecoveryPlan {
        &self.plan
    }

    pub(crate) fn admission(&self) -> RecoveryAdmission {
        self.plan.admission()
    }

    pub(crate) fn record_seal_verified(&mut self) {
        self.plan.record_seal_verified();
    }

    pub(crate) fn write_recovery_plan(&self) -> Result<()> {
        if let Some(path) = &self.plan_out {
            write_plan(path, &self.plan)?;
        }
        Ok(())
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

fn write_plan(path: &Path, plan: &RecoveryPlan) -> Result<()> {
    let mut json = plan.to_json_pretty()?;
    json.push('\n');
    std::fs::write(path, json)
        .with_context(|| format!("write authority recovery plan {}", path.display()))
}

impl Drop for AuthorityStartup {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}
