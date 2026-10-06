use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use peri_loom_p0::io::SegmentedIO;
use peri_loom_p0::manifest::{WalPosition, SEGMENT_SIZE};
use peri_loom_p0::storage::SegmentedStorage;
use serde_json::Value;
use sha2::{Digest, Sha256};

fn cli(root: &Path, arguments: &[&str]) -> Result<Value> {
    let output = Command::new(env!("CARGO_BIN_EXE_peri-loom-p0"))
        .arg(root)
        .args(arguments)
        .output()?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

struct KillChild(Child);

impl Drop for KillChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ready(root: &Path, arguments: &[&str]) -> Result<KillChild> {
    let mut child = KillChild(
        Command::new(env!("CARGO_BIN_EXE_peri-loom-p0"))
            .arg(root)
            .args(arguments)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let stdout = child.0.stdout.take().context("missing child stdout")?;
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line);
        let _ = sender.send((result, line));
    });
    let (result, line) = receiver.recv_timeout(Duration::from_secs(30))?;
    ensure!(result? > 0, "child exited before readiness");
    let message: Value = serde_json::from_str(&line)?;
    ensure!(message["ready"] == true, "child not ready: {message}");
    Ok(child)
}

#[cfg(unix)]
fn kill_and_prove(child: &mut KillChild) -> Result<()> {
    use std::os::unix::process::ExitStatusExt;
    child.0.kill()?;
    let status = child.0.wait()?;
    ensure!(status.signal() == Some(9), "expected SIGKILL: {status}");
    println!("independent kill evidence: pid={} signal=9", child.0.id());
    Ok(())
}

fn wal_last_commit(root: &Path) -> Result<WalPosition> {
    let bytes = fs::read(root.join("database-wal"))?;
    ensure!(bytes.len() >= 32, "missing WAL header");
    let word = |offset: usize| -> u32 {
        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap())
    };
    let page_size = word(8) as usize;
    ensure!((512..=65536).contains(&page_size), "invalid WAL page size");
    let frame_size = page_size + 24;
    let mut last_commit = 0;
    for (index, frame) in bytes[32..].chunks_exact(frame_size).enumerate() {
        ensure!(frame[8..16] == bytes[16..24], "unexpected WAL epoch");
        if u32::from_be_bytes(frame[4..8].try_into().unwrap()) > 0 {
            last_commit = index as u64 + 1;
        }
    }
    ensure!(last_commit > 0, "no committed WAL frame");
    Ok(WalPosition {
        salts: [word(16), word(20)],
        frame_count: last_commit,
    })
}

#[test]
fn full_checkpoint_records_native_wal_commit_and_new_epoch() -> Result<()> {
    let root = tempfile::tempdir()?;
    cli(root.path(), &["demo", "120"])?;
    let first = SegmentedStorage::open(root.path())?.manifest();
    assert_eq!(first.applied_wal_position, wal_last_commit(root.path())?);
    cli(root.path(), &["exec", "PRAGMA wal_checkpoint(TRUNCATE)"])?;
    assert_eq!(fs::metadata(root.path().join("database-wal"))?.len(), 0);
    cli(
        root.path(),
        &["exec", "INSERT INTO p0_records VALUES(121, 'new epoch')"],
    )?;
    cli(root.path(), &["checkpoint"])?;
    let second = SegmentedStorage::open(root.path())?.manifest();
    assert_eq!(second.applied_wal_position, wal_last_commit(root.path())?);
    assert_ne!(
        first.applied_wal_position.salts,
        second.applied_wal_position.salts
    );
    assert!(second.generation > first.generation);
    assert_eq!(
        cli(root.path(), &["query", "PRAGMA integrity_check"])?[0][0],
        "ok"
    );
    println!(
        "native watermark evidence: first={:?} second={:?}",
        first.applied_wal_position, second.applied_wal_position
    );
    Ok(())
}

#[test]
#[cfg(unix)]
fn sigkill_lock_release_committed_tail_and_torn_frame() -> Result<()> {
    let root = tempfile::tempdir()?;
    cli(root.path(), &["demo", "120"])?;
    let mut child = ready(root.path(), &["checkpoint-wait"])?;
    let denied = Command::new(env!("CARGO_BIN_EXE_peri-loom-p0"))
        .arg(root.path())
        .args(["query", "SELECT count(*) FROM p0_records"])
        .output()?;
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stderr).contains("already locked"));
    kill_and_prove(&mut child)?;
    let storage = SegmentedStorage::open(root.path())?;
    assert_eq!(storage.stats().segment_loads, 0);
    assert!(storage.manifest().segments.len() > 1);
    assert!(!root.path().join("database").exists());
    assert!(!root.path().join("database.db").exists());
    let head = fs::read(root.path().join("HEAD"))?;
    let mut child = ready(root.path(), &["crash-write", "7"])?;
    kill_and_prove(&mut child)?;
    assert_eq!(fs::read(root.path().join("HEAD"))?, head);
    let mut wal = fs::OpenOptions::new()
        .append(true)
        .open(root.path().join("database-wal"))?;
    wal.write_all(&[0x55; 19])?;
    wal.sync_all()?;
    drop(wal);
    let rows = cli(
        root.path(),
        &[
            "query",
            "SELECT count(*), sum(length(payload)) FROM p0_records",
        ],
    )?;
    assert_eq!(rows, serde_json::json!([[127, 127 * 2048]]));
    cli(root.path(), &["checkpoint"])?;
    assert_eq!(
        cli(root.path(), &["query", "PRAGMA integrity_check"])?[0][0],
        "ok"
    );
    Ok(())
}

#[test]
fn corrupt_unrequested_segment_is_lazy_and_fails_closed() -> Result<()> {
    let root = tempfile::tempdir()?;
    let storage = SegmentedStorage::open(root.path())?;
    for index in 0..3 {
        storage.write_at(index * u64::from(SEGMENT_SIZE), &[index as u8 + 1; 64])?;
    }
    storage.publish(WalPosition::default())?;
    let corrupt = root
        .path()
        .join("segments")
        .join(&storage.manifest().segments[&2]);
    fs::write(corrupt, vec![0xaa; SEGMENT_SIZE as usize])?;
    let recovered = SegmentedStorage::open(root.path())?;
    assert_eq!(recovered.stats().segment_loads, 0);
    let mut bytes = [0; 64];
    assert_eq!(recovered.read_at(0, &mut bytes)?, 64);
    assert_eq!(bytes, [1; 64]);
    assert_eq!(recovered.stats().segment_loads, 1);
    assert!(recovered
        .read_at(2 * u64::from(SEGMENT_SIZE), &mut bytes)
        .is_err());
    Ok(())
}

#[test]
fn concurrent_head_readers_never_see_partial_or_missing_objects() -> Result<()> {
    let root = tempfile::tempdir()?;
    let storage = SegmentedStorage::open(root.path())?;
    let stopped = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| -> Result<()> {
        let reader_root = root.path();
        let reader_stop = stopped.clone();
        let reader_count = observed.clone();
        let reader = scope.spawn(move || -> Result<()> {
            while !reader_stop.load(Ordering::Acquire) {
                let head: Value = serde_json::from_slice(&fs::read(reader_root.join("HEAD"))?)?;
                let manifest = fs::read(
                    reader_root
                        .join("manifests")
                        .join(head["manifest"].as_str().context("manifest name")?),
                )?;
                assert_eq!(
                    format!("{:x}", Sha256::digest(&manifest)),
                    head["sha256"].as_str().unwrap()
                );
                let manifest: Value = serde_json::from_slice(&manifest)?;
                assert_eq!(manifest["generation"], head["generation"]);
                for object in manifest["segments"]
                    .as_object()
                    .context("segment map")?
                    .values()
                {
                    let name = object.as_str().context("segment name")?;
                    let bytes = fs::read(reader_root.join("segments").join(name))?;
                    assert_eq!(bytes.len(), SEGMENT_SIZE as usize);
                    assert_eq!(format!("{:x}.seg", Sha256::digest(bytes)), name);
                }
                reader_count.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        });
        let result = (|| -> Result<()> {
            for value in 1..=20 {
                storage.write_at(0, &[value; 64])?;
                storage.publish(WalPosition::default())?;
            }
            Ok(())
        })();
        stopped.store(true, Ordering::Release);
        reader.join().expect("HEAD reader panicked")?;
        result
    })?;
    assert!(observed.load(Ordering::Relaxed) > 0);
    println!(
        "atomic HEAD evidence: {} complete snapshots",
        observed.load(Ordering::Relaxed)
    );
    Ok(())
}

#[test]
#[cfg(unix)]
fn directory_symlink_alias_cannot_bypass_wal_publication_guard() -> Result<()> {
    use std::os::unix::fs::symlink;
    use turso_core::io::{Buffer, Completion, FileSyncType, OpenFlags, IO};

    let parent = tempfile::tempdir()?;
    let real = parent.path().join("real");
    fs::create_dir(&real)?;
    let alias = parent.path().join("alias");
    symlink(&real, &alias)?;
    let io = SegmentedIO::open(&real)?;
    let wal_path = alias.join("database-wal");
    let wal = io.open_file(
        wal_path.to_str().context("WAL path")?,
        OpenFlags::Create,
        false,
    )?;
    let buffer = Arc::new(Buffer::new_temporary(64));
    buffer.as_mut_slice().fill(0);
    let completion = wal.pwrite(
        0,
        buffer,
        Completion::new_write(|result| assert_eq!(result.unwrap(), 64)),
    )?;
    assert!(completion.finished());
    let main_path = alias.join("database");
    let main = io.open_file(
        main_path.to_str().context("main path")?,
        OpenFlags::Create,
        false,
    )?;
    let buffer = Arc::new(Buffer::new_temporary(4));
    buffer.as_mut_slice().copy_from_slice(b"data");
    let completion = main.pwrite(
        0,
        buffer,
        Completion::new_write(|result| assert_eq!(result.unwrap(), 4)),
    )?;
    assert!(completion.finished());
    let completion = Completion::new_sync(|result| assert!(result.is_err()));
    assert!(main.sync(completion.clone(), FileSyncType::Fsync).is_err());
    assert!(completion.finished());
    let completion = Completion::new_trunc(|result| assert!(result.is_err()));
    assert!(wal.truncate(0, completion.clone()).is_err());
    assert!(completion.finished());
    assert_eq!(fs::metadata(real.join("database-wal"))?.len(), 64);
    assert!(!real.join("database").exists());
    Ok(())
}
