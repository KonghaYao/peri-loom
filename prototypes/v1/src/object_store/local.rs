use super::{
    check_condition, fresh_etag, validate_key, validate_prefix, ObjectStore, PreconditionFailed,
    PutCondition, StoredObject,
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
    time::Instant,
};

mod pack;
mod physical_diagnostics;
#[cfg(test)]
mod read_tests;

pub struct LocalObjectStore {
    root: PathBuf,
    packed: bool,
    physical: pack::Counters,
    physical_diagnostics: Option<physical_diagnostics::CountersDiagnostic>,
}

/// One body staged for a group publication. `file` is `None` when staging found an
/// existing object with identical bytes: nothing to flush, and the stored etag is
/// kept. The final state is rechecked under the group lock before publication.
struct StagedBody {
    temporary: Option<PathBuf>,
    target: PathBuf,
    file: Option<File>,
    etag: String,
    timing: StageTiming,
}

/// What publishing a staged body must do once every condition has been rechecked.
enum StagedPlan {
    Rename,
    Replace,
    Adopt(String),
}

fn group_phase_enabled() -> bool {
    crate::storage_diagnostics::enabled()
}

/// Bounded staging width. Local A/B measured the 540-body roll at 4 workers
/// faster than at 2 or 8; create/fsync contend on the shared directory, so the
/// upper bound stays at 4. The diagnostic override exists only to re-run that
/// attribution; production never reads the variable.
fn stage_worker_count(writes: usize) -> usize {
    let default = if writes < 8 {
        1
    } else {
        std::thread::available_parallelism()
            .map(|parallel| parallel.get())
            .unwrap_or(4)
            .clamp(2, 4)
    };
    if !group_phase_enabled() {
        return default;
    }
    static OVERRIDE: OnceLock<Option<usize>> = OnceLock::new();
    OVERRIDE
        .get_or_init(|| {
            std::env::var("PERI_DIAG_STAGE_WORKERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .map(|workers: usize| workers.clamp(1, 32))
        })
        .unwrap_or(default)
}

/// Only the first relative component is inspected; the key itself is never emitted.
fn group_class(writes: &[super::GroupWrite]) -> crate::work_metrics::GroupClass {
    use crate::work_metrics::GroupClass;
    let key = writes.first().map_or("", |write| write.key.as_str());
    let relative = key.splitn(3, '/').nth(2).unwrap_or(key);
    if relative.starts_with("wal/receipt") || relative.starts_with("receipt") {
        GroupClass::Receipt
    } else if relative == "wal" || relative.starts_with("wal/") {
        GroupClass::Wal
    } else if relative == "authority" || relative.starts_with("authority/") {
        GroupClass::Authority
    } else {
        GroupClass::Other
    }
}

fn elapsed_ns(started: Option<Instant>) -> u64 {
    started.map_or(0, |started| started.elapsed().as_nanos() as u64)
}

#[derive(Default)]
struct GroupPhase {
    /// Timing is armed by either switch: the work counters need the same stage clocks the
    /// stderr diagnostic already takes.
    timed: bool,
    /// The stderr line stays owned by `PERI_DIAG_GROUP_PHASE=1`; work metrics aggregate
    /// silently into the exit snapshot.
    stderr: bool,
    writes: usize,
    bytes: usize,
    class: crate::work_metrics::GroupClass,
    unique: u64,
    reused: u64,
    stage_ns: u64,
    precheck_ns: u64,
    worker_select_ns: u64,
    worker_start_wait_max_ns: u64,
    worker_wall_max_ns: u64,
    join_tail_ns: u64,
    collect_ns: u64,
    stage_max_ns: u64,
    prep_ns: u64,
    hash_ns: u64,
    encode_ns: u64,
    open_ns: u64,
    spawn_ns: u64,
    workers: usize,
    body_total_max_ns: u64,
    body_write_ns: u64,
    body_fsync_ns: u64,
    body_fsync_max_ns: u64,
    lock_wait_ns: u64,
    recheck_ns: u64,
    rename_ns: u64,
    dir_flush_ns: u64,
    barrier_ns: u64,
    total_started: Option<Instant>,
    stamp: Option<crate::storage_diagnostics::Stamp>,
    group_id: u64,
    shape: Option<crate::storage_diagnostics::GroupShape>,
}

impl GroupPhase {
    fn start(writes: &[super::GroupWrite]) -> Self {
        let stderr = group_phase_enabled();
        let timed = stderr || crate::work_metrics::enabled();
        static NEXT_GROUP: AtomicU64 = AtomicU64::new(1);
        let mut phase = Self {
            timed,
            stderr,
            total_started: timed.then(Instant::now),
            stamp: stderr.then(crate::storage_diagnostics::Stamp::new),
            group_id: if stderr {
                NEXT_GROUP.fetch_add(1, Ordering::Relaxed)
            } else {
                0
            },
            shape: stderr.then(|| crate::storage_diagnostics::GroupShape::from_writes(writes)),
            ..Self::default()
        };
        if timed {
            phase.writes = writes.len();
            phase.bytes = writes.iter().map(|write| write.bytes.len()).sum();
            phase.class = group_class(writes);
        }
        phase
    }

    fn record_staged_body(&mut self, timing: &StageTiming) {
        self.prep_ns += timing.prep_ns;
        self.hash_ns += timing.hash_ns;
        self.encode_ns += timing.encode_ns;
        self.open_ns += timing.open_ns;
        self.body_write_ns += timing.write_ns;
        self.body_fsync_ns += timing.fsync_ns;
        self.body_fsync_max_ns = self.body_fsync_max_ns.max(timing.fsync_ns);
        self.stage_max_ns = self.stage_max_ns.max(timing.write_ns + timing.fsync_ns);
        self.body_total_max_ns = self.body_total_max_ns.max(timing.total_ns());
    }

    fn emit(&self) {
        let total_ns = elapsed_ns(self.total_started);
        if self.timed {
            crate::work_metrics::observe_group(
                self.writes,
                self.class,
                crate::work_metrics::GroupTimings {
                    barrier_ns: self.barrier_ns,
                    body_fsync_ns: self.body_fsync_ns,
                    dir_flush_ns: self.dir_flush_ns,
                    total_ns,
                },
            );
        }
        if !self.stderr {
            return;
        }
        let stamp = self.stamp.as_ref().expect("diagnostic stamp missing");
        if let Some(shape) = &self.shape {
            shape.emit(stamp, self.group_id);
        }
        let _ = writeln!(
            std::io::stderr().lock(),
            concat!(
                "GROUP_PHASE pid={} writes={} bytes={} unique={} reused={} class={} ",
                "stage_ns={} stage_max_ns={} body_write_ns={} body_fsync_ns={} ",
                "body_fsync_max_ns={} lock_wait_ns={} recheck_ns={} rename_ns={} ",
                "dir_flush_ns={} barrier_ns={} total_ns={} prep_ns={} hash_ns={} ",
                "encode_ns={} open_ns={} spawn_ns={} workers={} body_total_max_ns={} ",
                "precheck_ns={} worker_select_ns={} worker_start_wait_max_ns={} ",
                "worker_wall_max_ns={} join_tail_ns={} collect_ns={} ",
                "start_unix_ns={} publication_id={} request_id={} group_id={}"
            ),
            std::process::id(),
            self.writes,
            self.bytes,
            self.unique,
            self.reused,
            self.class.name(),
            self.stage_ns,
            self.stage_max_ns,
            self.body_write_ns,
            self.body_fsync_ns,
            self.body_fsync_max_ns,
            self.lock_wait_ns,
            self.recheck_ns,
            self.rename_ns,
            self.dir_flush_ns,
            self.barrier_ns,
            total_ns,
            self.prep_ns,
            self.hash_ns,
            self.encode_ns,
            self.open_ns,
            self.spawn_ns,
            self.workers,
            self.body_total_max_ns,
            self.precheck_ns,
            self.worker_select_ns,
            self.worker_start_wait_max_ns,
            self.worker_wall_max_ns,
            self.join_tail_ns,
            self.collect_ns,
            stamp.start_unix_ns,
            stamp.publication_id,
            stamp.request_id.as_deref().unwrap_or("none"),
            self.group_id,
        );
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    key: String,
    #[serde(with = "super::payload")]
    bytes: Vec<u8>,
    etag: String,
    checksum: String,
}

/// Borrowing form of [`Envelope`] used on the staging path. The payload clone is
/// the only field that scales with the body, and the encoded bytes are consumed
/// immediately, so staging can serialize the caller's body in place. Field order
/// and encoding stay identical to `Envelope`; the stored format is unchanged.
#[derive(Serialize)]
struct EnvelopeRef<'a> {
    key: &'a str,
    #[serde(with = "super::payload")]
    bytes: &'a [u8],
    etag: &'a str,
    checksum: &'a str,
}

#[derive(Default)]
struct StageTiming {
    prep_ns: u64,
    hash_ns: u64,
    encode_ns: u64,
    open_ns: u64,
    write_ns: u64,
    fsync_ns: u64,
}

impl StageTiming {
    fn total_ns(&self) -> u64 {
        self.prep_ns + self.hash_ns + self.encode_ns + self.open_ns + self.write_ns + self.fsync_ns
    }
}

/// `serde_json::to_vec` starts from a small buffer and grows; this upper-bound
/// estimate of the base64 payload and the fixed fields removes the common
/// reallocation chain for ordinary bodies without changing the encoded bytes.
/// It is an estimate for the common path, not a strict capacity guarantee.
fn encode_envelope(envelope: &EnvelopeRef<'_>, payload_len: usize) -> Result<Vec<u8>> {
    let overhead = envelope.key.len() + envelope.etag.len() + envelope.checksum.len() + 64;
    let mut encoded = Vec::with_capacity(payload_len.saturating_mul(4).div_ceil(3) + overhead);
    serde_json::to_writer(&mut encoded, envelope)?;
    Ok(encoded)
}

impl LocalObjectStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let requested = match std::env::var("PERI_LOCAL_OBJECT_PACK") {
            Ok(value) if value == "1" => true,
            Ok(value) if value == "0" || value.is_empty() => false,
            Ok(_) => anyhow::bail!("PERI_LOCAL_OBJECT_PACK must be 0 or 1"),
            Err(std::env::VarError::NotPresent) => false,
            Err(error) => return Err(error.into()),
        };
        Self::open_layout(root.as_ref(), requested)
    }

    /// Opt in on a new, empty volume. Existing packed volumes retain their format
    /// even when the environment opt-in is no longer present.
    pub fn open_packed(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_layout(root.as_ref(), true)
    }

    fn open_layout(root: &Path, requested: bool) -> Result<Self> {
        fs::create_dir_all(root)?;
        ensure!(
            !fs::symlink_metadata(root)?.file_type().is_symlink(),
            "symlink store root rejected"
        );
        let mut store = Self {
            root: root.canonicalize()?,
            packed: false,
            physical: pack::Counters::default(),
            physical_diagnostics: None,
        };
        let _guard = store.lock()?;
        store.packed = pack::open(&store, requested)?;
        flush_directory(&store.root)?;
        if let Some(parent) = store.root.parent() {
            flush_directory(parent)?;
        }
        publish_directory_barrier(&store.root)?;
        // Only successfully opened handles receive a diagnostic identity. Failed
        // format admission must not be reported as an opened legacy/packed store.
        store.physical_diagnostics =
            physical_diagnostics::CountersDiagnostic::new(group_phase_enabled());
        Ok(store)
    }

    /// Physical calls, including maintenance, rather than logical group counts.
    pub fn local_storage_metrics(&self) -> serde_json::Value {
        self.physical.snapshot(self.packed)
    }

    fn emit_physical_counters(
        &self,
        reason: physical_diagnostics::SnapshotReason,
        group_id: Option<u64>,
    ) {
        physical_diagnostics::emit_if_enabled(
            self.physical_diagnostics.as_ref(),
            reason,
            group_id,
            || self.local_storage_metrics(),
        );
    }

    fn flush_body(&self, file: &File) -> std::io::Result<()> {
        self.physical.body_syncs.fetch_add(1, Ordering::Relaxed);
        flush_file(file)
    }

    fn flush_objects_directory(&self) -> Result<()> {
        self.physical
            .directory_syncs
            .fetch_add(1, Ordering::Relaxed);
        flush_directory(&self.root)
    }

    /// A per-attempt staging name. `etag` alone is not enough now that staging runs
    /// outside the lock: two groups can stage identical bytes at the same time, and a
    /// crashed attempt can leave the same name behind. The process id and the counter
    /// keep every live attempt distinct; the final key and envelope never carry this
    /// name.
    fn temporary_path(&self, etag: &str) -> PathBuf {
        static ATTEMPT: AtomicU64 = AtomicU64::new(0);
        let attempt = ATTEMPT.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(format!(".{etag}.{}.{attempt}.tmp", std::process::id()))
    }

    /// Encode and flush one group body to a temporary file. Runs outside the group
    /// lock; the final key is only touched by `publish_staged` after every condition
    /// has been rechecked under that lock.
    fn stage_body_timed(
        &self,
        write: &super::GroupWrite,
        etag_override: Option<String>,
        diagnostics: bool,
    ) -> Result<StagedBody> {
        let prep_started = diagnostics.then(Instant::now);
        let target = self.path(&write.key);
        if matches!(write.condition, PutCondition::Any) {
            match self.read(&write.key) {
                Ok(Some(existing)) if existing.bytes == write.bytes => {
                    // An earlier attempt already landed identical bytes. The etag is
                    // provisional until the lock recheck; staging nothing keeps the
                    // per-body flush count unchanged for retries.
                    return Ok(StagedBody {
                        temporary: None,
                        target,
                        file: None,
                        etag: existing.etag,
                        timing: StageTiming {
                            prep_ns: elapsed_ns(prep_started),
                            ..StageTiming::default()
                        },
                    });
                }
                Ok(Some(_)) => {
                    anyhow::bail!("immutable object conflict: {}", write.key)
                }
                Ok(None) => {}
                // A partial body from a crashed attempt fails its envelope. It is
                // replaced under the lock; here only the replacement is staged.
                Err(_) => {}
            }
        }
        let prep_ns = elapsed_ns(prep_started);
        let hash_started = diagnostics.then(Instant::now);
        let checksum = blake3::hash(&write.bytes).to_hex();
        // The etag is derived from the body and process-local ordering; computing it
        // here rather than in a serial pre-pass keeps the hash work inside the
        // per-body workers, where large receipt rolls already run in parallel.
        let etag = etag_override.unwrap_or_else(|| fresh_etag(&write.bytes));
        let hash_ns = elapsed_ns(hash_started);
        let encode_started = diagnostics.then(Instant::now);
        let envelope = EnvelopeRef {
            key: &write.key,
            bytes: &write.bytes,
            etag: &etag,
            checksum: checksum.as_str(),
        };
        let bytes = encode_envelope(&envelope, write.bytes.len())?;
        let encode_ns = elapsed_ns(encode_started);
        let open_started = diagnostics.then(Instant::now);
        let temporary = self.temporary_path(&etag);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let open_ns = elapsed_ns(open_started);
        let write_started = diagnostics.then(Instant::now);
        let write_result = file.write_all(&bytes);
        let write_ns = elapsed_ns(write_started);
        if let Err(error) = write_result {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
        let fsync_started = diagnostics.then(Instant::now);
        let fsync_result = self.flush_body(&file);
        let fsync_ns = elapsed_ns(fsync_started);
        if let Err(error) = fsync_result {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
        Ok(StagedBody {
            temporary: Some(temporary),
            target,
            file: Some(file),
            etag,
            timing: StageTiming {
                prep_ns,
                hash_ns,
                encode_ns,
                open_ns,
                write_ns,
                fsync_ns,
            },
        })
    }

    #[cfg(test)]
    fn stage_body(&self, write: &super::GroupWrite, etag: String) -> Result<StagedBody> {
        self.stage_body_timed(write, Some(etag), false)
    }

    /// Stage every body outside the group lock. Non-`Any` conditions are checked once
    /// here so an already-stale group is rejected before the work; the authoritative
    /// check runs again in `publish_staged_with_phase` under the lock.
    fn stage_group_with_phase(
        &self,
        writes: &[super::GroupWrite],
        phase: &mut GroupPhase,
    ) -> Result<Vec<StagedBody>> {
        let stage_started = phase.timed.then(Instant::now);
        for write in writes {
            if !matches!(write.condition, PutCondition::Any) {
                check_condition(self.read(&write.key)?.as_ref(), &write.condition)?;
            }
        }
        phase.precheck_ns = elapsed_ns(stage_started);
        let select_started = phase.timed.then(Instant::now);
        // A publication is 25-30 bodies on the steady path and several hundred when a
        // receipt roll lands. Attribution on 2 KiB bodies shows create/fsync dominating
        // the per-body work while encoding stays in the low single-digit percent, so
        // the bodies are staged on a bounded set of threads to overlap that I/O; the
        // renames, the one directory flush and the one media barrier still happen in
        // publication order on this thread, after every body is durable.
        let workers = if self.packed {
            // Duplicate-key packed groups retain the legacy staging semantics,
            // including under diagnostics, but not an override above four workers.
            stage_worker_count(writes.len()).min(4)
        } else {
            stage_worker_count(writes.len())
        };
        let diagnostics = phase.timed;
        phase.worker_select_ns = elapsed_ns(select_started);
        let mut spawn_ns = 0;
        let mut worker_start_wait_max_ns = 0;
        let mut worker_wall_max_ns = 0;
        let mut last_worker_finished = None;
        let staged: Vec<Result<StagedBody>> = if workers == 1 {
            let worker_started = diagnostics.then(Instant::now);
            let staged = writes
                .iter()
                .map(|write| self.stage_body_timed(write, None, diagnostics))
                .collect();
            worker_wall_max_ns = elapsed_ns(worker_started);
            last_worker_finished = diagnostics.then(Instant::now);
            staged
        } else {
            std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(workers);
                for offset in 0..workers {
                    let spawn_started = diagnostics.then(Instant::now);
                    handles.push(scope.spawn(move || {
                        let worker_started = diagnostics.then(Instant::now);
                        let wait_ns = elapsed_ns(spawn_started);
                        let staged = (offset..writes.len())
                            .step_by(workers)
                            .map(|index| {
                                (
                                    index,
                                    self.stage_body_timed(&writes[index], None, diagnostics),
                                )
                            })
                            .collect::<Vec<_>>();
                        let wall_ns = elapsed_ns(worker_started);
                        (staged, wait_ns, wall_ns, diagnostics.then(Instant::now))
                    }));
                    spawn_ns += elapsed_ns(spawn_started);
                }
                let mut staged = Vec::with_capacity(writes.len());
                for handle in handles {
                    let (bodies, wait_ns, wall_ns, finished) = handle.join().unwrap();
                    worker_start_wait_max_ns = worker_start_wait_max_ns.max(wait_ns);
                    worker_wall_max_ns = worker_wall_max_ns.max(wall_ns);
                    last_worker_finished = last_worker_finished.max(finished);
                    staged.extend(bodies);
                }
                staged.sort_by_key(|(index, _)| *index);
                staged.into_iter().map(|(_, result)| result).collect()
            })
        };
        if phase.timed {
            phase.workers = workers;
            phase.spawn_ns = spawn_ns;
            phase.worker_start_wait_max_ns = worker_start_wait_max_ns;
            phase.worker_wall_max_ns = worker_wall_max_ns;
            // Time after the final worker's completion, not summed join waits (which
            // overlap the actual I/O). This closes the former scheduling blind spot.
            phase.join_tail_ns = elapsed_ns(last_worker_finished);
        }
        let collect_started = phase.timed.then(Instant::now);
        let mut bodies = Vec::with_capacity(staged.len());
        let mut failure = None;
        for result in staged {
            match result {
                Ok(body) => {
                    if phase.timed {
                        phase.record_staged_body(&body.timing);
                    }
                    bodies.push(body);
                }
                Err(error) => failure = failure.or(Some(error)),
            }
        }
        if let Some(error) = failure {
            remove_temporaries(&bodies);
            return Err(error);
        }
        if phase.timed {
            phase.collect_ns = elapsed_ns(collect_started);
            phase.stage_ns = elapsed_ns(stage_started);
        }
        Ok(bodies)
    }

    #[cfg(test)]
    fn stage_group(&self, writes: &[super::GroupWrite]) -> Result<Vec<StagedBody>> {
        let mut phase = GroupPhase::default();
        self.stage_group_with_phase(writes, &mut phase)
    }

    /// Publish bodies staged outside the lock. Every condition is re-evaluated while
    /// holding the lock before the first rename, so a rejection has no final object
    /// to undo. Any other error may leave already-renamed content-addressed bodies;
    /// the group contract allows that for non-`PreconditionFailed` failures.
    fn publish_staged_with_phase(
        &self,
        writes: &[super::GroupWrite],
        bodies: Vec<StagedBody>,
        phase: &mut GroupPhase,
    ) -> Result<Vec<String>> {
        let result = self.publish_staged_locked(writes, &bodies, phase);
        if result.is_err() {
            remove_temporaries(&bodies);
        }
        result
    }

    #[cfg(test)]
    fn publish_staged(
        &self,
        writes: &[super::GroupWrite],
        bodies: Vec<StagedBody>,
    ) -> Result<Vec<String>> {
        let mut phase = GroupPhase::default();
        self.publish_staged_with_phase(writes, bodies, &mut phase)
    }

    fn publish_staged_locked(
        &self,
        writes: &[super::GroupWrite],
        bodies: &[StagedBody],
        phase: &mut GroupPhase,
    ) -> Result<Vec<String>> {
        let lock_started = phase.timed.then(Instant::now);
        let _guard = self.lock()?;
        if phase.timed {
            phase.lock_wait_ns = elapsed_ns(lock_started);
        }
        pack::validate_layout(self)?;
        ensure!(writes.len() == bodies.len(), "staged group length mismatch");
        let recheck_started = phase.timed.then(Instant::now);
        let mut plans = Vec::with_capacity(bodies.len());
        let mut resolved_any: HashMap<&str, (usize, String)> = HashMap::new();
        for (index, (write, body)) in writes.iter().zip(bodies).enumerate() {
            if matches!(write.condition, PutCondition::Any) {
                if let Some((prior, etag)) = resolved_any.get(write.key.as_str()) {
                    ensure!(
                        writes[*prior].bytes == write.bytes,
                        "immutable object conflict: {}",
                        write.key
                    );
                    plans.push(StagedPlan::Adopt(etag.clone()));
                    continue;
                }
                let (plan, etag) = self.plan_any(write, body)?;
                resolved_any.insert(write.key.as_str(), (index, etag));
                plans.push(plan);
            } else {
                check_condition(self.read(&write.key)?.as_ref(), &write.condition)?;
                ensure!(
                    body.temporary.is_some(),
                    "staged body missing: {}",
                    write.key
                );
                plans.push(StagedPlan::Rename);
            }
        }
        if phase.timed {
            phase.recheck_ns = elapsed_ns(recheck_started);
        }
        let rename_started = phase.timed.then(Instant::now);
        let mut etags = Vec::with_capacity(bodies.len());
        let mut renamed = false;
        for (body, plan) in bodies.iter().zip(plans) {
            match plan {
                StagedPlan::Adopt(etag) => {
                    if let Some(temporary) = &body.temporary {
                        let _ = fs::remove_file(temporary);
                    }
                    if phase.timed {
                        phase.reused += 1;
                    }
                    etags.push(etag);
                }
                StagedPlan::Rename => {
                    let temporary = body.temporary.as_ref().context("staged body missing")?;
                    if self.packed {
                        pack::prepare_replacement(self, &body.target)?;
                    }
                    if self.packed && etags.len() + 1 == bodies.len() && bodies.len() > 1 {
                        // An adopted alias may have been renamed by an earlier
                        // attempt whose directory sync failed. Its presence is
                        // not proof of durability: fence every dependency before
                        // publishing the last alias, including an all-adopt retry.
                        pack::fault("before_dependencies_sync")?;
                        self.flush_objects_directory()?;
                        publish_directory_barrier(&self.root)?;
                        pack::fault("after_dependencies_sync")?;
                    }
                    fs::rename(temporary, &body.target)?;
                    renamed = true;
                    if phase.timed {
                        phase.unique += 1;
                    }
                    etags.push(body.etag.clone());
                }
                StagedPlan::Replace => {
                    let temporary = body.temporary.as_ref().context("staged body missing")?;
                    if self.packed && etags.len() + 1 == bodies.len() && bodies.len() > 1 {
                        pack::fault("before_dependencies_sync")?;
                        self.flush_objects_directory()?;
                        publish_directory_barrier(&self.root)?;
                        pack::fault("after_dependencies_sync")?;
                    }
                    fs::rename(temporary, &body.target)?;
                    renamed = true;
                    if phase.timed {
                        phase.unique += 1;
                    }
                    etags.push(body.etag.clone());
                }
            }
        }
        if phase.timed {
            phase.rename_ns = elapsed_ns(rename_started);
        }
        if renamed {
            let file = bodies
                .iter()
                .find_map(|body| body.file.as_ref())
                .context("staged publication file missing")?;
            let dir_started = phase.timed.then(Instant::now);
            if self.packed {
                pack::fault("after_root_rename")?;
            }
            self.flush_objects_directory()?;
            if phase.timed {
                phase.dir_flush_ns = elapsed_ns(dir_started);
            }
            let barrier_started = phase.timed.then(Instant::now);
            publish_barrier(file)?;
            if phase.timed {
                phase.barrier_ns = elapsed_ns(barrier_started);
            }
        } else if self.packed && !bodies.is_empty() {
            // A whole immutable group can be an all-adopt retry after its last
            // rename succeeded but the directory sync failed. Preserve the
            // stored etags and avoid body writes, while completing durability
            // before reporting success for that group.
            let dir_started = phase.timed.then(Instant::now);
            self.flush_objects_directory()?;
            if phase.timed {
                phase.dir_flush_ns = elapsed_ns(dir_started);
            }
            let barrier_started = phase.timed.then(Instant::now);
            publish_directory_barrier(&self.root)?;
            if phase.timed {
                phase.barrier_ns = elapsed_ns(barrier_started);
            }
        }
        Ok(etags)
    }

    /// Decide what to do with one `Any` body whose final key may already exist. Called
    /// with the group lock held; the returned etag is the one the store holds after
    /// publication.
    fn plan_any(
        &self,
        write: &super::GroupWrite,
        body: &StagedBody,
    ) -> Result<(StagedPlan, String)> {
        let exists = match fs::symlink_metadata(&body.target) {
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        if !exists {
            ensure!(
                body.temporary.is_some(),
                "staged body missing: {}",
                write.key
            );
            return Ok((StagedPlan::Rename, body.etag.clone()));
        }
        match self.read(&write.key) {
            Ok(Some(existing)) if existing.bytes == write.bytes => {
                Ok((StagedPlan::Adopt(existing.etag.clone()), existing.etag))
            }
            Ok(Some(_)) => anyhow::bail!("immutable object conflict: {}", write.key),
            Ok(None) => {
                ensure!(
                    body.temporary.is_some(),
                    "staged body missing: {}",
                    write.key
                );
                Ok((StagedPlan::Rename, body.etag.clone()))
            }
            // A partial body from a crashed attempt fails its envelope, so no durable
            // head can reference it; replace it under the lock.
            Err(_) => {
                ensure!(
                    body.temporary.is_some(),
                    "staged body missing: {}",
                    write.key
                );
                Ok((StagedPlan::Replace, body.etag.clone()))
            }
        }
    }

    fn lock(&self) -> Result<File> {
        let path = self.root.join(".lock");
        reject_symlink(&path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        lock.lock()?;
        Ok(lock)
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root
            .join(format!("{}.obj", blake3::hash(key.as_bytes()).to_hex()))
    }

    fn read(&self, key: &str) -> Result<Option<StoredObject>> {
        let started = std::time::Instant::now();
        let path = self.path(key);
        if self.packed {
            return pack::read(self, &path, key);
        }
        match fs::symlink_metadata(&path) {
            Ok(metadata) => ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "nonregular store file rejected"
            ),
            // A missing observation is complete at this lookup. Staging still
            // rechecks the final object under STORE before publishing, so a
            // concurrent creator does not require a second failed open here.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let encoded = match fs::read(path) {
            Ok(encoded) => encoded,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let envelope: Envelope =
            serde_json::from_slice(&encoded).context("invalid local object envelope")?;
        ensure!(envelope.key == key, "local object key mismatch");
        ensure!(
            envelope.checksum == blake3::hash(&envelope.bytes).to_hex().as_str(),
            "local object checksum mismatch"
        );
        ensure!(!envelope.etag.is_empty(), "missing local object etag");
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "NATIVE_LOCAL_READ pid={} key={key} bytes={} encoded_bytes={} elapsed_us={}",
                std::process::id(),
                envelope.bytes.len(),
                encoded.len(),
                started.elapsed().as_micros()
            );
        }
        Ok(Some(StoredObject {
            bytes: envelope.bytes,
            etag: envelope.etag,
        }))
    }
}

impl Drop for LocalObjectStore {
    fn drop(&mut self) {
        // Diagnostic failure (including panic) must never turn teardown into a
        // business failure. A missing Drop record is not evidence of zero work.
        // Do not enter formatting, cache locking or stderr during another unwind.
        if self.physical_diagnostics.is_none() || std::thread::panicking() {
            return;
        }
        self.emit_physical_counters(physical_diagnostics::SnapshotReason::StoreDrop, None);
    }
}

fn remove_temporaries(bodies: &[StagedBody]) {
    for body in bodies {
        if let Some(temporary) = &body.temporary {
            let _ = fs::remove_file(temporary);
        }
    }
}

thread_local! {
    /// Hardware barriers issued by the current thread. `publish_barrier` is the only
    /// path that asks the platform to push the device cache to media, so this counter
    /// is the barrier count a publication pays.
    static BARRIERS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Barriers issued by the current thread; thread-local so a concurrent test
/// cannot shift the count a measurement is reading. Test-only: production code
/// never reads the counter.
#[cfg(test)]
pub(crate) fn barrier_count() -> u64 {
    BARRIERS.with(std::cell::Cell::get)
}

fn note_barrier() {
    BARRIERS.with(|count| count.set(count.get().saturating_add(1)));
}

/// Bytes reached the device. Cheap, and never a media flush: the publication still
/// needs its `publish_barrier`.
fn flush_file(file: &File) -> std::io::Result<()> {
    barrier::flush(file)
}

fn flush_directory(path: &Path) -> Result<()> {
    barrier::flush(&File::open(path)?)?;
    Ok(())
}

/// A hardware barrier after the phase's writes and renames have been flushed.
/// `F_FULLFSYNC` is device-wide on macOS, so one call covers all preceding body
/// flushes. Legacy groups use the final barrier; packed groups also order durable
/// dependencies before publishing their last alias.
fn publish_barrier(file: &File) -> std::io::Result<()> {
    note_barrier();
    barrier::durable(file)
}

fn publish_directory_barrier(path: &Path) -> Result<()> {
    let directory = File::open(path)?;
    publish_barrier(&directory)?;
    Ok(())
}

#[cfg(target_os = "macos")]
mod barrier {
    use std::fs::File;
    use std::io;
    use std::os::fd::AsRawFd;

    /// `File::sync_all` and `File::sync_data` both reach `F_FULLFSYNC` on macOS, so a
    /// group of writes pays one media flush per object. A publication wants the opposite
    /// order instead: cheap `fsync(2)` per object to order bytes into the device cache,
    /// then one device-wide flush at the end.
    pub(super) fn flush(file: &File) -> io::Result<()> {
        if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn durable(file: &File) -> io::Result<()> {
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == 0 {
            Ok(())
        } else {
            // A volume that cannot flush its own cache still needs the write ordered.
            flush(file)
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod barrier {
    use std::fs::File;
    use std::io;

    /// `fsync` is already the media barrier on these platforms.
    pub(super) fn flush(file: &File) -> io::Result<()> {
        file.sync_all()
    }

    pub(super) fn durable(_file: &File) -> io::Result<()> {
        Ok(())
    }
}

fn reject_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "nonregular store file rejected"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

impl ObjectStore for LocalObjectStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn group_rejection_publishes_nothing(&self) -> bool {
        // `put_group` rechecks every condition under the lock before the first rename
        // and stages bodies only in temporary files, so a rejected group leaves no
        // final object behind on this backend.
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let started = std::time::Instant::now();
        validate_key(key)?;
        let result = self.read(key);
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "NATIVE_LOCAL_GET pid={} key={key} elapsed_us={} ok={}",
                std::process::id(),
                started.elapsed().as_micros(),
                result.is_ok()
            );
        }
        result
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if self.packed {
            return pack::put(self, key, bytes, condition);
        }
        let started = std::time::Instant::now();
        validate_key(key)?;
        let _guard = self.lock()?;
        pack::validate_layout(self)?;
        // `Any` imposes no condition, so the existing object cannot change the outcome and
        // reading it would only decode and checksum bytes the publication overwrites.
        if !matches!(condition, PutCondition::Any) {
            check_condition(self.read(key)?.as_ref(), &condition)?;
        }
        let etag = fresh_etag(bytes);
        let checksum = blake3::hash(bytes).to_hex();
        let envelope = EnvelopeRef {
            key,
            bytes,
            etag: &etag,
            checksum: checksum.as_str(),
        };
        let temporary = self.temporary_path(&etag);
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            let started = std::time::Instant::now();
            let encoded = serde_json::to_vec(&envelope)?;
            if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
                eprintln!(
                    "NATIVE_LOCAL_ENCODE pid={} key={key} bytes={} encoded_bytes={} elapsed_us={}",
                    std::process::id(),
                    bytes.len(),
                    encoded.len(),
                    started.elapsed().as_micros()
                );
            }
            file.write_all(&encoded)?;
            self.flush_body(&file)?;
            fs::rename(&temporary, self.path(key))?;
            self.flush_objects_directory()?;
            publish_barrier(&file)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            eprintln!(
                "NATIVE_LOCAL_PUT pid={} key={key} bytes={} elapsed_us={}",
                std::process::id(),
                bytes.len(),
                started.elapsed().as_micros()
            );
        }
        Ok(etag)
    }

    /// Group publication with the final alias ordered after its dependencies.
    ///
    /// Bodies and the root are encoded and flushed to temporary files outside the
    /// group lock. Under the lock every condition is rechecked, the temporary files
    /// are renamed in publication order. Legacy groups use one final directory
    /// flush and media barrier. Packed groups also make the dependencies durable
    /// before their last alias is published; even an all-adopt packed retry must
    /// complete directory durability before returning success.
    fn put_group(&self, writes: &[super::GroupWrite]) -> Result<Vec<String>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let mut phase = GroupPhase::start(writes);
        let result = (|| -> Result<Vec<String>> {
            for write in writes {
                validate_key(&write.key)?;
            }
            let staged = if self.packed {
                pack::stage_group(self, writes, &mut phase)?
            } else {
                self.stage_group_with_phase(writes, &mut phase)?
            };
            self.publish_staged_with_phase(writes, staged, &mut phase)
        })();
        if result.is_ok() {
            phase.emit();
            // These are handle-lifetime counters, not costs of this group. Other
            // callers may have advanced them concurrently. Emission is outside
            // the old group-total clock and still costs the enclosing request.
            self.emit_physical_counters(
                physical_diagnostics::SnapshotReason::SuccessfulGroupBoundary,
                phase.stderr.then_some(phase.group_id),
            );
        }
        result
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        validate_key(key)?;
        let _guard = self.lock()?;
        pack::validate_layout(self)?;
        let Some(current) = self.read(key)? else {
            if self.packed {
                // The missing alias may be from an earlier unlink whose
                // directory sync failed. Complete that idempotent deletion's
                // durability before returning success on the same handle.
                self.flush_objects_directory()?;
                publish_directory_barrier(&self.root)?;
            }
            return Ok(());
        };
        if let Some(expected) = expected {
            ensure!(current.etag == expected, PreconditionFailed);
        }
        if self.packed {
            pack::prepare_replacement(self, &self.path(key))?;
        }
        fs::remove_file(self.path(key))?;
        if self.packed {
            pack::fault("after_alias_delete")?;
        }
        self.flush_objects_directory()?;
        publish_directory_barrier(&self.root)?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        let _guard = self.lock()?;
        let mut keys = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "obj")
            {
                reject_symlink(&entry.path())?;
                if self.packed {
                    let key = pack::key_for_alias(self, &entry.path())?;
                    if key.starts_with(prefix) {
                        keys.push(key);
                    }
                    continue;
                }
                let envelope: Envelope = serde_json::from_slice(&fs::read(entry.path())?)?;
                validate_key(&envelope.key)?;
                ensure!(
                    self.path(&envelope.key) == entry.path(),
                    "local object filename mismatch"
                );
                self.read(&envelope.key)?;
                if envelope.key.starts_with(prefix) {
                    keys.push(envelope.key);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}

#[cfg(test)]
mod group_tests {
    use super::*;
    use crate::object_store::GroupWrite;
    use std::sync::{Arc, Barrier};

    fn temporary_files(store: &LocalObjectStore) -> Vec<std::path::PathBuf> {
        fs::read_dir(&store.root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "tmp"))
            .collect()
    }

    fn final_objects(store: &LocalObjectStore) -> Vec<std::path::PathBuf> {
        fs::read_dir(&store.root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "obj"))
            .collect()
    }

    #[test]
    fn group_phase_aggregates_into_work_metrics() {
        crate::work_metrics::set_enabled(true);
        let before = crate::work_metrics::snapshot().expect("enabled snapshot");
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();

        let single = [GroupWrite::new(
            "tenant/db/authority/root",
            b"root".to_vec(),
            PutCondition::Any,
        )];
        store.put_group(&single).unwrap();
        let batch: Vec<GroupWrite> = (0..5)
            .map(|index| {
                GroupWrite::new(
                    format!("tenant/db/wal/chunks/{index}"),
                    vec![index as u8; 32],
                    PutCondition::Any,
                )
            })
            .collect();
        store.put_group(&batch).unwrap();

        let after = crate::work_metrics::snapshot().expect("enabled snapshot");
        assert!(after.group.calls - before.group.calls >= 2);
        assert!(after.group.writes.one - before.group.writes.one >= 1);
        assert!(
            after.group.writes.five_to_sixteen - before.group.writes.five_to_sixteen >= 1,
            "the five-write group must land in the 5_16 bucket"
        );
        assert!(
            after.group.class["authority"] - before.group.class["authority"] >= 1,
            "the head publication must charge the authority class"
        );
        assert!(
            after.group.class["wal"] - before.group.class["wal"] >= 1,
            "the chunk group must charge the wal class"
        );
    }

    #[test]
    fn group_phase_helpers_aggregate_without_environment() {
        let mut phase = GroupPhase::default();
        phase.record_staged_body(&StageTiming {
            prep_ns: 1,
            hash_ns: 2,
            encode_ns: 3,
            open_ns: 4,
            write_ns: 10,
            fsync_ns: 20,
        });
        phase.record_staged_body(&StageTiming {
            prep_ns: 5,
            hash_ns: 6,
            encode_ns: 7,
            open_ns: 8,
            write_ns: 5,
            fsync_ns: 50,
        });
        assert_eq!(phase.prep_ns, 6);
        assert_eq!(phase.hash_ns, 8);
        assert_eq!(phase.encode_ns, 10);
        assert_eq!(phase.open_ns, 12);
        assert_eq!(phase.body_write_ns, 15);
        assert_eq!(phase.body_fsync_ns, 70);
        assert_eq!(phase.body_fsync_max_ns, 50);
        assert_eq!(phase.stage_max_ns, 55);
        assert_eq!(phase.body_total_max_ns, 81);

        let class = |key: &str| group_class(&[GroupWrite::absent(key, Vec::new())]).name();
        assert_eq!(class("tenant/db/wal/index"), "wal");
        assert_eq!(class("tenant/db/wal/receipt-pages/x"), "receipt");
        assert_eq!(class("tenant/db/authority/root"), "authority");
        assert_eq!(class("tenant/db/receipt/pages/x"), "receipt");
        assert_eq!(class("tenant/db/segments/x"), "other");
    }

    #[test]
    fn borrowed_envelope_encodes_exactly_like_the_stored_form() {
        let key = "tenant/db/pages/a";
        let bytes = vec![0u8, 1, 2, 255];
        let etag = "opaque-etag";
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let owned = Envelope {
            key: key.to_owned(),
            bytes: bytes.clone(),
            etag: etag.to_owned(),
            checksum: checksum.clone(),
        };
        let borrowed = EnvelopeRef {
            key,
            bytes: &bytes,
            etag,
            checksum: &checksum,
        };
        let owned = serde_json::to_vec(&owned).unwrap();
        let borrowed = serde_json::to_vec(&borrowed).unwrap();
        assert_eq!(
            owned, borrowed,
            "the borrowed form must not change the format"
        );

        let decoded: Envelope = serde_json::from_slice(&borrowed).unwrap();
        assert_eq!(decoded.key, key);
        assert_eq!(decoded.bytes, bytes);
        assert_eq!(decoded.etag, etag);
        assert_eq!(decoded.checksum, checksum);
    }

    #[test]
    fn group_publish_pays_one_media_barrier_for_the_whole_group() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes: Vec<GroupWrite> = (0..6)
            .map(|index| GroupWrite::absent(format!("pages/{index:02}"), vec![index as u8; 4096]))
            .collect();

        let before = barrier_count();
        let etags = store.put_group(&writes).unwrap();
        let grouped = barrier_count() - before;

        let before = barrier_count();
        for (index, write) in writes.iter().enumerate() {
            store
                .put(
                    &format!("sequential/{index:02}"),
                    &write.bytes,
                    PutCondition::Absent,
                )
                .unwrap();
        }
        let sequential = barrier_count() - before;

        assert_eq!(etags.len(), 6);
        assert_eq!(grouped, 1, "the whole group reaches media with one barrier");
        assert_eq!(sequential, 6, "one barrier per single write");
        for (write, etag) in writes.iter().zip(etags) {
            let stored = store.get(&write.key).unwrap().unwrap();
            assert_eq!(stored.bytes, write.bytes);
            assert_eq!(stored.etag, etag);
        }
    }

    #[test]
    fn rejected_group_leaves_no_write_behind() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        store.put("root", b"current", PutCondition::Any).unwrap();
        let writes = vec![
            GroupWrite::absent("pages/00", vec![7; 64]),
            GroupWrite::new(
                "root",
                b"next".to_vec(),
                PutCondition::Match("stale".into()),
            ),
        ];
        assert!(store.put_group(&writes).is_err());
        assert!(store.get("pages/00").unwrap().is_none());
        assert_eq!(store.get("root").unwrap().unwrap().bytes, b"current");
    }

    #[test]
    fn repeated_content_addressed_body_keeps_its_stored_etag() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let stored = store.put("pages/a", b"body", PutCondition::Any).unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"body".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/b", b"other".to_vec(), PutCondition::Any),
        ];
        let etags = store.put_group(&writes).unwrap();
        assert_eq!(etags[0], stored, "a skip reports the etag the store holds");
        assert_eq!(store.get("pages/a").unwrap().unwrap().etag, stored);
        assert_eq!(store.get("pages/b").unwrap().unwrap().bytes, b"other");
    }

    #[test]
    fn partial_body_from_a_crashed_attempt_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let target = store.path("pages/a");
        fs::write(&target, b"partial envelope").unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"complete body".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/b", b"another body".to_vec(), PutCondition::Any),
        ];
        store.put_group(&writes).unwrap();
        assert_eq!(
            store.get("pages/a").unwrap().unwrap().bytes,
            b"complete body"
        );
        assert_eq!(
            store.get("pages/b").unwrap().unwrap().bytes,
            b"another body"
        );
    }

    #[test]
    fn condition_recheck_after_staging_publishes_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        store.put("root", b"current", PutCondition::Any).unwrap();
        let current = store.get("root").unwrap().unwrap().etag;
        let writes = vec![
            GroupWrite::absent("pages/00", vec![7; 64]),
            GroupWrite::new("root", b"next".to_vec(), PutCondition::Match(current)),
        ];
        let staged = store.stage_group(&writes).unwrap();
        assert_eq!(temporary_files(&store).len(), 2);

        // Another writer wins the root CAS after staging but before the lock.
        store.put("root", b"newer", PutCondition::Any).unwrap();
        let error = store.publish_staged(&writes, staged).unwrap_err();
        assert!(error.downcast_ref::<PreconditionFailed>().is_some());
        assert!(store.get("pages/00").unwrap().is_none());
        assert_eq!(store.get("root").unwrap().unwrap().bytes, b"newer");
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn object_created_after_staging_is_reused_when_bytes_match() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"body".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/b", b"other".to_vec(), PutCondition::Any),
        ];
        let staged = store.stage_group(&writes).unwrap();
        assert_eq!(temporary_files(&store).len(), 2);

        let stored = store.put("pages/a", b"body", PutCondition::Any).unwrap();
        let etags = store.publish_staged(&writes, staged).unwrap();
        assert_eq!(etags[0], stored);
        assert_eq!(store.get("pages/a").unwrap().unwrap().etag, stored);
        assert_eq!(store.get("pages/b").unwrap().unwrap().bytes, b"other");
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn object_created_after_staging_with_different_bytes_is_not_overwritten() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"new".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/b", b"other".to_vec(), PutCondition::Any),
        ];
        let staged = store.stage_group(&writes).unwrap();
        store.put("pages/a", b"old", PutCondition::Any).unwrap();

        let error = store.publish_staged(&writes, staged).unwrap_err();
        assert!(error.to_string().contains("immutable object conflict"));
        assert_eq!(store.get("pages/a").unwrap().unwrap().bytes, b"old");
        assert!(store.get("pages/b").unwrap().is_none());
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn concurrent_same_key_groups_publish_once() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::open(directory.path().join("objects")).unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let writes = vec![
                    GroupWrite::absent("pages/race", vec![9; 64]),
                    GroupWrite::absent("pages/filler", vec![3; 64]),
                ];
                let staged = store.stage_group(&writes).unwrap();
                barrier.wait();
                store.publish_staged(&writes, staged)
            }));
        }
        let results: Vec<Result<Vec<String>>> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "one group wins the Absent condition"
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.downcast_ref::<PreconditionFailed>().is_some())
                })
                .count(),
            1,
            "the loser is a precondition failure, not partial publication"
        );
        assert_eq!(store.get("pages/race").unwrap().unwrap().bytes, vec![9; 64]);
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn concurrent_identical_any_groups_share_the_stored_etag() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::open(directory.path().join("objects")).unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let writes = vec![
                    GroupWrite::new("pages/a", b"same".to_vec(), PutCondition::Any),
                    GroupWrite::new("pages/b", b"filler".to_vec(), PutCondition::Any),
                ];
                let staged = store.stage_group(&writes).unwrap();
                barrier.wait();
                store.publish_staged(&writes, staged)
            }));
        }
        let results: Vec<Vec<String>> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        assert_eq!(results[0][0], results[1][0]);
        assert_eq!(store.get("pages/a").unwrap().unwrap().etag, results[0][0]);
        assert_eq!(store.get("pages/a").unwrap().unwrap().bytes, b"same");
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn concurrent_identical_any_put_groups_share_one_object() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::open(directory.path().join("objects")).unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let writes = vec![GroupWrite::new(
                    "pages/a",
                    b"same".to_vec(),
                    PutCondition::Any,
                )];
                barrier.wait();
                store.put_group(&writes)
            }));
        }
        let results: Vec<Vec<String>> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        assert_eq!(results[0], results[1]);
        assert_eq!(results[0].len(), 1);
        assert_eq!(store.get("pages/a").unwrap().unwrap().etag, results[0][0]);
        assert_eq!(store.get("pages/a").unwrap().unwrap().bytes, b"same");
        assert_eq!(final_objects(&store).len(), 1);
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn concurrent_staging_with_the_same_etag_uses_distinct_temporaries() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::open(directory.path().join("objects")).unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                // A fixed etag reproduces the collision the old `.{etag}.tmp` name
                // would have when two attempts stage the same bytes.
                let write = GroupWrite::new("pages/a", b"same".to_vec(), PutCondition::Any);
                barrier.wait();
                store
                    .stage_body(&write, "same-etag".to_owned())
                    .unwrap()
                    .temporary
                    .expect("staged temporary")
            }));
        }
        let paths: Vec<std::path::PathBuf> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_ne!(paths[0], paths[1]);
        for path in &paths {
            fs::remove_file(path).unwrap();
        }
        assert!(temporary_files(&store).is_empty());
        assert!(store.get("pages/a").unwrap().is_none());
    }

    #[test]
    fn legacy_crash_leftover_temporary_does_not_block_staging() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let legacy = store.root.join(".legacy-etag.tmp");
        fs::write(&legacy, b"crashed attempt").unwrap();

        // `put_group` generates a fresh etag; pin it here to exercise the worst-case
        // retry where a crashed attempt left the legacy name.
        let writes = vec![GroupWrite::new(
            "pages/a",
            b"body".to_vec(),
            PutCondition::Any,
        )];
        let staged = store
            .stage_body(&writes[0], "legacy-etag".to_owned())
            .unwrap();
        let temporary = staged.temporary.clone().expect("staged temporary");
        assert_ne!(temporary, legacy);
        assert_eq!(fs::read(&legacy).unwrap(), b"crashed attempt");

        store.publish_staged(&writes, vec![staged]).unwrap();
        assert_eq!(store.get("pages/a").unwrap().unwrap().bytes, b"body");
        assert!(!temporary.exists());

        fs::remove_file(&legacy).unwrap();
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn duplicate_any_writes_with_same_bytes_share_the_etag() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"same".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/a", b"same".to_vec(), PutCondition::Any),
        ];
        let etags = store.put_group(&writes).unwrap();
        assert_eq!(etags[0], etags[1]);
        assert_eq!(store.get("pages/a").unwrap().unwrap().bytes, b"same");
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn duplicate_any_writes_with_different_bytes_fail() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes = vec![
            GroupWrite::new("pages/a", b"first".to_vec(), PutCondition::Any),
            GroupWrite::new("pages/a", b"second".to_vec(), PutCondition::Any),
        ];
        let error = store.put_group(&writes).unwrap_err();
        assert!(error.to_string().contains("immutable object conflict"));
        assert!(store.get("pages/a").unwrap().is_none());
        assert!(temporary_files(&store).is_empty());
    }

    #[test]
    fn single_any_group_does_not_overwrite_different_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        store.put("pages/a", b"old", PutCondition::Any).unwrap();
        let writes = vec![GroupWrite::new(
            "pages/a",
            b"new".to_vec(),
            PutCondition::Any,
        )];
        let error = store.put_group(&writes).unwrap_err();
        assert!(error.to_string().contains("immutable object conflict"));
        assert_eq!(store.get("pages/a").unwrap().unwrap().bytes, b"old");
        assert!(temporary_files(&store).is_empty());
    }
}

#[cfg(test)]
mod group_latency_tests {
    use super::*;
    use crate::object_store::GroupWrite;
    use std::time::Instant;

    /// Prints the wall cost of one directory barrier on this host, so the
    /// reduction can be turned into milliseconds without re-running the fixture.
    #[test]
    fn group_publish_latency_delta() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let writes: Vec<GroupWrite> = (0..6)
            .map(|index| GroupWrite::absent(format!("pages/{index:02}"), vec![index as u8; 4096]))
            .collect();
        let mut grouped = Vec::new();
        let mut sequential = Vec::new();
        for round in 0..12 {
            let prefix = format!("g{round}");
            let batch: Vec<GroupWrite> = writes
                .iter()
                .enumerate()
                .map(|(index, write)| {
                    GroupWrite::absent(format!("{prefix}/{index:02}"), write.bytes.clone())
                })
                .collect();
            let started = Instant::now();
            store.put_group(&batch).unwrap();
            grouped.push(started.elapsed().as_micros());

            let started = Instant::now();
            for (index, write) in batch.iter().enumerate() {
                store
                    .put(
                        &format!("{prefix}/s{index:02}"),
                        &write.bytes,
                        PutCondition::Absent,
                    )
                    .unwrap();
            }
            sequential.push(started.elapsed().as_micros());
        }
        grouped.sort_unstable();
        sequential.sort_unstable();
        let median = |values: &[u128]| values[values.len() / 2];
        println!(
            "GROUP_LATENCY writes=6 grouped_us_median={} grouped_us_min={} sequential_us_median={} sequential_us_min={} saved_us_median={} per_barrier_us={}",
            median(&grouped),
            grouped[0],
            median(&sequential),
            sequential[0],
            median(&sequential) as i128 - median(&grouped) as i128,
            (median(&sequential) as i128 - median(&grouped) as i128) / 5,
        );
    }

    /// Samples the shape a publication actually writes: 25 immutable bodies per group.
    /// Both `Any` and `Absent` now stage through temp+rename; the two samples differ
    /// only in the condition each write carries.
    #[test]
    fn content_addressed_group_latency_sample() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        let mut direct = Vec::new();
        let mut renamed = Vec::new();
        for round in 0..20 {
            let batch: Vec<GroupWrite> = (0..25)
                .map(|index| {
                    GroupWrite::new(
                        format!("d{round}/{index:02}"),
                        vec![(round * 25 + index) as u8; 4096],
                        PutCondition::Any,
                    )
                })
                .collect();
            let started = Instant::now();
            store.put_group(&batch).unwrap();
            direct.push(started.elapsed().as_micros());

            let batch: Vec<GroupWrite> = (0..25)
                .map(|index| {
                    GroupWrite::absent(
                        format!("a{round}/{index:02}"),
                        vec![(round * 25 + index) as u8; 4096],
                    )
                })
                .collect();
            let started = Instant::now();
            store.put_group(&batch).unwrap();
            renamed.push(started.elapsed().as_micros());
        }
        direct.sort_unstable();
        renamed.sort_unstable();
        let median = |values: &[u128]| values[values.len() / 2];
        println!(
            "GROUP_ANY_LATENCY writes=25 direct_us_median={} direct_us_min={} renamed_us_median={} renamed_us_min={} saved_us_median={}",
            median(&direct),
            direct[0],
            median(&renamed),
            renamed[0],
            median(&renamed) as i128 - median(&direct) as i128,
        );
    }

    /// T04 attribution sample: the steady 21-write group and a receipt-roll-sized
    /// 540-write group, with 2 KiB bodies like the diagnostic campaign. Run
    /// explicitly with `PERI_DIAG_GROUP_PHASE=1` to read the stage split.
    #[test]
    #[ignore = "manual T04 staging attribution sample"]
    fn stage_breakdown_sample() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(directory.path().join("objects")).unwrap();
        for (writes_per_group, rounds) in [(21usize, 20usize), (540, 3)] {
            let mut totals = Vec::new();
            for round in 0..rounds {
                let batch: Vec<GroupWrite> = (0..writes_per_group)
                    .map(|index| {
                        GroupWrite::new(
                            format!("s{writes_per_group}/{round}/{index:04}"),
                            vec![(round + index) as u8; 2048],
                            PutCondition::Any,
                        )
                    })
                    .collect();
                let started = Instant::now();
                store.put_group(&batch).unwrap();
                totals.push(started.elapsed().as_micros());
            }
            totals.sort_unstable();
            println!(
                "STAGE_BREAKDOWN writes={writes_per_group} rounds={rounds} total_us_median={} total_us_min={}",
                totals[totals.len() / 2],
                totals[0],
            );
        }
    }
}
