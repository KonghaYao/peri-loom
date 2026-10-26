use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize};

enum StageJob {
    Pack(Vec<usize>),
    Legacy(usize),
}

/// Successful jobs remain owned until every worker has joined. Dropping a
/// partial worker result (including during unwind) removes its temporary names.
#[derive(Default)]
pub(super) struct StagedBatch {
    pub(super) bodies: Vec<(usize, StagedBody)>,
}

impl Drop for StagedBatch {
    fn drop(&mut self) {
        for (_, body) in &self.bodies {
            remove_temporaries(std::slice::from_ref(body));
        }
    }
}

#[derive(Default)]
struct TemporaryNames(Vec<PathBuf>);

impl Drop for TemporaryNames {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = fs::remove_file(path);
        }
    }
}

pub(super) fn stage_group(
    store: &LocalObjectStore,
    writes: &[super::super::super::GroupWrite],
    phase: &mut GroupPhase,
) -> Result<Vec<StagedBody>> {
    maintenance::cleanup_staging(store)?;
    // The general interface permits repeated conditional keys. Preserve its
    // ordered legacy handling rather than putting ambiguous duplicate entries
    // into a pack directory.
    let mut keys = BTreeSet::new();
    if writes.iter().any(|write| !keys.insert(&write.key)) {
        return store.stage_group_with_phase(writes, phase);
    }
    let started = phase.timed.then(Instant::now);
    let mut staged: Vec<Option<StagedBody>> = (0..writes.len()).map(|_| None).collect();
    let mut batches: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut jobs = Vec::new();
    for (index, write) in writes.iter().enumerate() {
        if matches!(write.condition, PutCondition::Any) {
            match store.read(&write.key) {
                Ok(Some(existing)) if existing.bytes == write.bytes => {
                    staged[index] = Some(StagedBody {
                        temporary: None,
                        target: store.path(&write.key),
                        file: None,
                        etag: existing.etag,
                        timing: StageTiming::default(),
                    });
                    continue;
                }
                Ok(Some(_)) => anyhow::bail!("immutable object conflict: {}", write.key),
                Ok(None) | Err(_) => {}
            }
        } else {
            check_condition(store.read(&write.key)?.as_ref(), &write.condition)?;
        }
        if write.bytes.len() > MAX_SMALL_OBJECT {
            jobs.push(StageJob::Legacy(index));
            continue;
        }
        batches
            .entry(lifetime_group(index, write))
            .or_default()
            .push(index);
    }
    for indices in batches.values() {
        let mut begin = 0;
        while begin < indices.len() {
            let mut end = begin;
            let mut bytes = 0;
            while end < indices.len() && end - begin < MAX_ENTRIES {
                let size = writes[indices[end]].bytes.len();
                if end > begin && bytes + size > MAX_PAYLOAD {
                    break;
                }
                bytes += size;
                end += 1;
            }
            jobs.push(StageJob::Pack(indices[begin..end].to_vec()));
            begin = end;
        }
    }
    // A real publication often has only 4-5 physical jobs after packing. Unlike
    // the legacy logical-body cutoff, parallelize any independent pair; retain
    // its bounded width and diagnostic override, capped at four in this layout.
    let workers = if jobs.len() < 2 {
        jobs.len()
    } else {
        jobs.len().min(stage_worker_count(8).min(4))
    };
    let timed = phase.timed;
    let (mut completed, spawn_ns) = run_jobs(jobs.len(), workers, phase.timed, |index| {
        stage_job(store, writes, &jobs[index], timed)
    })?;
    for batch in &mut completed {
        for (index, body) in batch.bodies.drain(..) {
            phase.record_staged_body(&body.timing);
            staged[index] = Some(body);
        }
    }
    if phase.timed {
        phase.stage_ns = elapsed_ns(started);
        phase.workers = workers;
        phase.spawn_ns = spawn_ns;
    }
    if staged.iter().any(Option::is_none) {
        let bodies: Vec<_> = staged.into_iter().flatten().collect();
        remove_temporaries(&bodies);
        anyhow::bail!("packed staging omitted an object");
    }
    Ok(staged.into_iter().flatten().collect())
}

fn stage_job(
    store: &LocalObjectStore,
    writes: &[super::super::super::GroupWrite],
    job: &StageJob,
    timed: bool,
) -> Result<StagedBatch> {
    match job {
        StageJob::Legacy(index) => Ok(StagedBatch {
            bodies: vec![(
                *index,
                store.stage_body_timed(&writes[*index], None, timed)?,
            )],
        }),
        StageJob::Pack(indices) => {
            let etags: Vec<_> = indices
                .iter()
                .map(|index| fresh_etag(&writes[*index].bytes))
                .collect();
            let items: Vec<_> = indices
                .iter()
                .zip(&etags)
                .map(|(index, etag)| {
                    let write = &writes[*index];
                    (write.key.as_str(), write.bytes.as_slice(), etag.as_str())
                })
                .collect();
            let mut phase = GroupPhase {
                timed,
                ..GroupPhase::default()
            };
            let bodies = stage_pack(store, &items, &mut phase, false)?;
            Ok(StagedBatch {
                bodies: indices.iter().copied().zip(bodies).collect(),
            })
        }
    }
}

/// Workers own all their staged files until joined; no worker publishes aliases
/// or runs orphan cleanup, and the caller never holds STORE while joining.
pub(super) fn run_jobs(
    jobs: usize,
    workers: usize,
    timed: bool,
    action: impl Fn(usize) -> Result<StagedBatch> + Sync,
) -> Result<(Vec<StagedBatch>, u64)> {
    ensure!(
        workers <= 4 && workers <= jobs,
        "invalid packed staging width"
    );
    if jobs == 0 {
        return Ok((Vec::new(), 0));
    }
    ensure!(workers > 0, "packed staging has no worker");
    let cancelled = AtomicBool::new(false);
    let next = AtomicUsize::new(0);
    #[cfg(test)]
    let inherited_fault = FAULT.with(|fault| fault.get());
    let worker = || -> Result<StagedBatch> {
        #[cfg(test)]
        FAULT.with(|fault| fault.set(inherited_fault));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut completed = StagedBatch::default();
            loop {
                if cancelled.load(Ordering::Acquire) {
                    break;
                }
                // A slow large legacy body must not own the next pack as well
                // while another worker has already finished its smaller job.
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= jobs {
                    break;
                }
                let mut batch = action(index)?;
                completed.bodies.append(&mut batch.bodies);
            }
            Ok(completed)
        }))
        .unwrap_or_else(|_| Err(anyhow::anyhow!("packed staging worker panicked")));
        if result.is_err() {
            cancelled.store(true, Ordering::Release);
        }
        result
    };
    if workers == 1 {
        return worker().map(|batch| (vec![batch], 0));
    }
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        let mut failure = None;
        let mut spawn_ns = 0;
        let worker = &worker;
        for _ in 0..workers {
            let spawn_started = timed.then(Instant::now);
            match std::thread::Builder::new().spawn_scoped(scope, worker) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    cancelled.store(true, Ordering::Release);
                    failure = Some(anyhow::Error::new(error));
                    break;
                }
            }
            spawn_ns += elapsed_ns(spawn_started);
        }
        let mut completed = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.join() {
                Ok(Ok(batch)) => completed.push(batch),
                Ok(Err(error)) => {
                    failure = failure.or(Some(error));
                }
                Err(_) => {
                    cancelled.store(true, Ordering::Release);
                    failure =
                        failure.or_else(|| Some(anyhow::anyhow!("packed staging worker panicked")));
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok((completed, spawn_ns))
    })
}

/// The raw store cannot infer the lifetime of an opaque Authority blob. Keep
/// those blobs and mutable heads separate; batch only clearly named page families
/// from the same namespace. Receipt archives never share a pack with short heads.
fn lifetime_group(index: usize, write: &super::super::super::GroupWrite) -> String {
    if !matches!(write.condition, PutCondition::Any) {
        return format!("single:{index}");
    }
    for family in [
        "/authority/index-pages/",
        "/authority/recovery-index/",
        "/wal/receipt-pages/",
    ] {
        if let Some((namespace, _)) = write.key.split_once(family) {
            return format!("{namespace}{family}");
        }
    }
    format!("single:{index}")
}

pub(super) fn stage_single(
    store: &LocalObjectStore,
    write: &super::super::super::GroupWrite,
    etag: Option<String>,
    phase: &mut GroupPhase,
) -> Result<StagedBody> {
    if write.bytes.len() > MAX_SMALL_OBJECT {
        // Staging must not apply group-Any immutable adoption to mutable put(Any).
        let mut staged = write.clone();
        staged.condition = PutCondition::Absent;
        return store.stage_body_timed(&staged, etag, phase.timed);
    }
    let etag = etag.unwrap_or_else(|| fresh_etag(&write.bytes));
    stage_pack(
        store,
        &[(write.key.as_str(), &write.bytes, &etag)],
        phase,
        false,
    )?
    .pop()
    .context("single pack staging missing")
}

/// Each returned temporary name is a hard link to the same durable inode. Its
/// open descriptor retains the advisory lock until publication ends, so bounded
/// orphan cleanup cannot unlink an active attempt's temporary names.
pub(super) fn stage_pack(
    store: &LocalObjectStore,
    items: &[(&str, &[u8], &str)],
    phase: &mut GroupPhase,
    store_lock_held: bool,
) -> Result<Vec<StagedBody>> {
    let anchor = store.root.join(STAGING).join(
        store
            .temporary_path("pack")
            .file_name()
            .context("pack temporary filename")?,
    );
    let mut temporaries = TemporaryNames(Vec::with_capacity(items.len() + 1));
    let mut file = {
        // Orphan cleanup takes this same lock. Protect create+lock from the small
        // interval where another process could mistake a new file for an orphan.
        let _guard = if store_lock_held {
            None
        } else {
            Some(store.lock()?)
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&anchor)?;
        temporaries.0.push(anchor.clone());
        file.lock()?;
        file
    };
    let mut bodies = Vec::with_capacity(items.len());
    let result = (|| -> Result<()> {
        let started = phase.timed.then(Instant::now);
        codec::write(&mut file, items)?;
        let write_ns = elapsed_ns(started);
        fault("before_body_sync")?;
        let started = phase.timed.then(Instant::now);
        store.flush_body(&file)?;
        let fsync_ns = elapsed_ns(started);
        fault("after_body_sync")?;
        store.physical.pack_files.fetch_add(1, Ordering::Relaxed);
        store
            .physical
            .pack_entries
            .fetch_add(items.len() as u64, Ordering::Relaxed);
        for (index, (key, _, etag)) in items.iter().enumerate() {
            let temporary = store.root.join(STAGING).join(
                store
                    .temporary_path(etag)
                    .file_name()
                    .context("pack alias temporary filename")?,
            );
            fs::hard_link(&anchor, &temporary)?;
            temporaries.0.push(temporary.clone());
            fault("after_alias_link")?;
            let timing = if index == 0 {
                StageTiming {
                    write_ns,
                    fsync_ns,
                    ..StageTiming::default()
                }
            } else {
                StageTiming::default()
            };
            phase.record_staged_body(&timing);
            bodies.push(StagedBody {
                temporary: Some(temporary),
                target: store.path(key),
                file: Some(file.try_clone()?),
                etag: (*etag).to_owned(),
                timing,
            });
        }
        Ok(())
    })();
    result?;
    fs::remove_file(&anchor)?;
    temporaries.0.clear();
    Ok(bodies)
}
