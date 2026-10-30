//! Test-only coordination for the segmented directory lock.
//!
//! `SegmentedIO::open` takes `.peri-loom.lock` through `flock`. A child forked
//! by any sibling test inherits that lock's open file description until `exec`
//! closes the CLOEXEC descriptor. Tests in one binary run in parallel, so a
//! lock attempt that overlaps a sibling's fork window sees a lock that no live
//! database owns. Every `Command` spawn holds `FORK_GATE` across the fork/exec
//! window and every in-process lock attempt holds it too: a lock attempt then
//! can never run while an inherited descriptor is still open, and a spawn can
//! never fork during another test's lock attempt.

#![allow(dead_code)]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use anyhow::Result;
use peri_loom_p0::database::SegmentedDatabase;
use peri_loom_p0::io::SegmentedIO;

static FORK_GATE: OnceLock<Mutex<()>> = OnceLock::new();

fn fork_gate() -> MutexGuard<'static, ()> {
    FORK_GATE
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn spawn(configure: impl FnOnce(&mut Command)) -> std::io::Result<Child> {
    let _gate = fork_gate();
    let mut command = Command::new(env!("CARGO_BIN_EXE_peri-loom-p0"));
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    configure(&mut command);
    command.spawn()
}

pub fn open_database(root: &Path) -> Result<SegmentedDatabase> {
    let _gate = fork_gate();
    SegmentedDatabase::open(root)
}

pub fn open_io(root: &Path) -> Result<Arc<SegmentedIO>> {
    let _gate = fork_gate();
    SegmentedIO::open(root)
}
