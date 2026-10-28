//! Ignored mechanism experiment: real authority publication and legacy local storage.
//! This is neither an HTTP/SDK latency benchmark nor a power-loss recovery test.

use super::*;
use crate::manifest::WalPosition;
use crate::object_store::{GroupWrite, LocalObjectStore, StoredObject};
use crate::recovery::fixture::{write_at, WalBuilder, FRAME_SIZE, PAGE_SIZE};
use crate::recovery::WAL_HEADER;
use crate::wal::{RemoteWal, WalAppendRequest};
use serde_json::{json, Value};
use std::io::Write;
use std::sync::Mutex;

#[derive(Clone, Default, Serialize)]
struct Calls {
    reads: u64,
    read_bytes: u64,
    puts: u64,
    put_bytes: u64,
    groups: u64,
    group_submitted: u64,
    group_submitted_bytes: u64,
    group_unique: u64,
    group_unique_bytes: u64,
    new_keys: u64,
    new_key_bytes: u64,
    deletes: u64,
    lists: u64,
    listed_keys: u64,
    registry_pages: u64,
    physical_blobs: u64,
    authority_roots: u64,
    other_writes: u64,
}

#[derive(Default)]
struct CountState {
    calls: Calls,
    live_keys: BTreeSet<String>,
}

struct CountedLocal {
    inner: LocalObjectStore,
    state: Mutex<CountState>,
}

impl CountedLocal {
    fn snapshot(&self) -> Result<Value> {
        Ok(serde_json::to_value(&self.state.lock().unwrap().calls)?)
    }

    fn note_write(state: &mut CountState, write: &GroupWrite) {
        if state.live_keys.insert(write.key.clone()) {
            state.calls.new_keys += 1;
            state.calls.new_key_bytes += write.bytes.len() as u64;
        }
        if write.key.contains("/authority/index-pages/") {
            state.calls.registry_pages += 1;
        } else if write.key.contains("/authority/blobs/") {
            state.calls.physical_blobs += 1;
        } else if write.key.ends_with("/authority/root") {
            state.calls.authority_roots += 1;
        } else {
            state.calls.other_writes += 1;
        }
    }
}

impl ObjectStore for CountedLocal {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn group_rejection_publishes_nothing(&self) -> bool {
        self.inner.group_rejection_publishes_nothing()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let result = self.inner.get(key)?;
        let mut state = self.state.lock().unwrap();
        state.calls.reads += 1;
        state.calls.read_bytes += result.as_ref().map_or(0, |item| item.bytes.len() as u64);
        Ok(result)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let result = self.inner.put(key, bytes, condition.clone())?;
        let mut state = self.state.lock().unwrap();
        state.calls.puts += 1;
        state.calls.put_bytes += bytes.len() as u64;
        Self::note_write(&mut state, &GroupWrite::new(key, bytes.to_vec(), condition));
        Ok(result)
    }

    fn put_group(&self, writes: &[GroupWrite]) -> Result<Vec<String>> {
        let result = self.inner.put_group(writes)?;
        let mut state = self.state.lock().unwrap();
        state.calls.groups += 1;
        state.calls.group_submitted += writes.len() as u64;
        state.calls.group_submitted_bytes +=
            writes.iter().map(|w| w.bytes.len() as u64).sum::<u64>();
        let unique = writes
            .iter()
            .map(|w| (w.key.as_str(), w))
            .collect::<BTreeMap<_, _>>();
        state.calls.group_unique += unique.len() as u64;
        state.calls.group_unique_bytes +=
            unique.values().map(|w| w.bytes.len() as u64).sum::<u64>();
        for write in unique.values() {
            Self::note_write(&mut state, write);
        }
        Ok(result)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)?;
        let mut state = self.state.lock().unwrap();
        state.calls.deletes += 1;
        state.live_keys.remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let result = self.inner.list(prefix)?;
        let mut state = self.state.lock().unwrap();
        state.calls.lists += 1;
        state.calls.listed_keys += result.len() as u64;
        Ok(result)
    }
}

fn delta(after: &Value, before: &Value) -> Value {
    Value::Object(
        after
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| {
                let number = value.as_u64().unwrap();
                let old = before[key].as_u64().unwrap();
                (
                    key.clone(),
                    json!(number.checked_sub(old).expect("counter decreased")),
                )
            })
            .collect(),
    )
}

fn physical(store: &CountedLocal) -> Value {
    let metrics = store.inner.local_storage_metrics();
    assert_eq!(metrics["format"], "legacy-json");
    json!({
        "body_sync_calls": metrics["body_sync_calls"],
        "directory_sync_calls": metrics["directory_sync_calls"],
    })
}

fn process_usage() -> Result<Value> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // getrusage fills the complete struct on success. RUSAGE_SELF includes this
        // process's worker threads, but excludes concurrent build/test processes.
        let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        ensure!(
            status == 0,
            "getrusage failed: {}",
            std::io::Error::last_os_error()
        );
        let usage = unsafe { usage.assume_init() };
        let micros = |time: libc::timeval| -> Result<u64> {
            u64::try_from(time.tv_sec)?
                .checked_mul(1_000_000)
                .and_then(|v| v.checked_add(u64::try_from(time.tv_usec).ok()?))
                .context("process CPU counter overflow")
        };
        Ok(json!({"available":true,"user_us":micros(usage.ru_utime)?,
            "system_us":micros(usage.ru_stime)?,"maxrss_os_units":usage.ru_maxrss,
            "maxrss_unit":if cfg!(target_os="macos") {"bytes"} else {"KiB"}}))
    }
    #[cfg(not(unix))]
    Ok(json!({"available":false}))
}

/// Freeze one complete 64×65 cadence interval after the initial 256 receipts. Seeds
/// alter logical append identities and authority identities; every append gets its own
/// deterministic identity domain so a roll cannot shift all later object identifiers.
#[test]
#[ignore = "manual 4160-append full-publication work experiment; not an SDK latency benchmark"]
fn receipt_roll_full_publication_work() -> Result<()> {
    let collapse = match std::env::var("PERI_RECEIPT_WORK_PIN_COLLAPSE").as_deref() {
        Ok("0") => false,
        Ok("1") => true,
        _ => anyhow::bail!("PERI_RECEIPT_WORK_PIN_COLLAPSE must be 0 or 1"),
    };
    let _policy = super::paged_index::CollapsePolicyGuard::new(collapse);
    let output = std::env::var("PERI_RECEIPT_WORK_OUTPUT")
        .context("PERI_RECEIPT_WORK_OUTPUT must name a new JSONL file")?;
    let label = std::env::var("PERI_RECEIPT_WORK_LABEL").unwrap_or_else(|_| "unlabelled".into());
    let seeds = std::env::var("PERI_RECEIPT_WORK_SEEDS")
        .unwrap_or_else(|_| "1".into())
        .split(',')
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(!seeds.is_empty(), "at least one seed is required");
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    writeln!(
        output,
        "{}",
        json!({"schema":"peri-receipt-full-publication-work-v1",
        "label":label,"prelude":256,"measured":4160,"seeds":seeds,"pin_collapse":collapse,
        "backend":"legacy LocalObjectStore","scope":"synthetic committed WAL through local-v6 BranchStore",
        "limits":["not HTTP or SDK","no crash/power-loss proof","not a latency acceptance test",
            "group writes are logical submissions, physical Sync attempts are separate",
            "registry_pages combines all registries; it is not pin-only work",
            "new-key counts use a shadow key set, with no measurement-only store reads"]})
    )?;
    for seed in seeds {
        let collapse_start = super::paged_index::collapse_test_stats().calls;
        // Keep failed runs available for investigation. Remove only after cold validation
        // succeeds and the complete measurement summary has been written and flushed.
        let directory = tempfile::tempdir()?.keep();
        writeln!(
            output,
            "{}",
            json!({"kind":"volume","seed":seed,"path":directory,
            "cleanup":"remove only on complete success"})
        )?;
        let store = Arc::new(CountedLocal {
            inner: LocalObjectStore::open(&directory)?,
            state: Mutex::new(CountState::default()),
        });
        let namespace = Namespace::new("receipt-work", "main")?;
        let (authority, branch) = {
            let _ids = deterministic_test_ids::Scope::new(seed, 0);
            let authority = Authority::open_local(store.clone(), namespace.clone())?;
            let branch = authority.branch_store("main")?;
            (authority, branch)
        };
        let wal = RemoteWal::new(branch.clone());
        let salts = [41, 42];
        let mut builder = WalBuilder::new(salts);
        let mut previous_archive = None;
        let mut previous_inline = 0;
        let mut total_before = Value::Null;
        let mut sync_before = Value::Null;
        let mut usage_before = Value::Null;
        let mut roll_count = 0;
        let mut moved_count = 0;
        for ordinal in 1..=4416u64 {
            let _ids = deterministic_test_ids::Scope::new(seed, ordinal);
            let measured = ordinal > 256;
            if ordinal == 257 {
                // Observer reads are outside the interval: report the actual pin tree.
                let root = authority.snapshot()?.root;
                let (entries, pages) = root.pin_entries.entries_counted()?;
                writeln!(
                    output,
                    "{}",
                    json!({"kind":"initial_pin_observation","seed":seed,
                    "after_lsn":256,"entries":entries.len(),"pages_read":pages,
                    "paged":root.pin_entries.pointer().is_some(),"excluded_from_measurement":true})
                )?;
                total_before = store.snapshot()?;
                sync_before = physical(&store);
                usage_before = process_usage()?;
            }
            let before = store.snapshot()?;
            let before_sync = physical(&store);
            let before_collapse = serde_json::to_value(super::paged_index::collapse_test_stats())?;
            let batch = if ordinal == 1 {
                write_at(0, &builder.image(1))
            } else {
                let frame = builder.frame(1, PAGE_SIZE as u32);
                write_at(WAL_HEADER + (ordinal as usize - 1) * FRAME_SIZE, &frame)
            };
            let request = WalAppendRequest {
                namespace: namespace.clone(),
                epoch: 1,
                batch,
                position: Some(WalPosition {
                    salts,
                    frame_count: ordinal,
                }),
                append_id: blake3::hash(format!("receipt-cycle-{seed}-{ordinal}").as_bytes())
                    .to_hex()
                    .to_string(),
                timestamp_millis: 1_700_000_000_000 + ordinal,
            };
            let mut publication_calls = 0;
            let mut rolled = false;
            let mut archived = 0;
            let mut receipt_pages = 0;
            let mut segment_pages = 0;
            let lsn = wal.append_using(&request, |encoded, metadata, expected, pages| {
                publication_calls += 1;
                rolled = metadata.receipt_index != previous_archive;
                archived = previous_inline + 1 - metadata.receipts.len();
                receipt_pages = pages
                    .iter()
                    .filter(|w| w.key.contains("/wal/receipt-pages/"))
                    .count();
                segment_pages = pages
                    .iter()
                    .filter(|w| w.key.contains("/wal/index-segments/"))
                    .count();
                branch.publish_wal_append(encoded, metadata, expected, pages)?;
                previous_archive = metadata.receipt_index.clone();
                previous_inline = metadata.receipts.len();
                Ok(())
            })?;
            assert_eq!(lsn, ordinal);
            assert_eq!(
                publication_calls, 1,
                "mechanism run must not contain an append retry"
            );
            let collapse_after = serde_json::to_value(super::paged_index::collapse_test_stats())?;
            let mut collapse_delta = delta(&collapse_after, &before_collapse);
            collapse_delta
                .as_object_mut()
                .unwrap()
                .remove("max_final_draft_charge");
            if !collapse {
                assert_eq!(collapse_delta["calls"], 0);
            }
            if measured {
                roll_count += usize::from(rolled);
                moved_count += archived;
            }
            writeln!(
                output,
                "{}",
                json!({"kind":"append","seed":seed,"ordinal":ordinal,
                "measured":measured,"rolled":rolled,"archived_receipts":archived,
                "receipt_staged_pages":receipt_pages,"segment_staged_pages":segment_pages,
                "inline_receipts":previous_inline,
                "store_calls":delta(&store.snapshot()?,&before),
                "physical_sync_attempts":delta(&physical(&store),&before_sync),
                "collapse_work":collapse_delta,
                "collapse_final_draft_charge_high_water":collapse_after["max_final_draft_charge"]})
            )?;
        }
        let observed_calls = super::paged_index::collapse_test_stats().calls - collapse_start;
        if !collapse {
            assert_eq!(
                observed_calls, 0,
                "baseline must not execute the new pin policy"
            );
        }
        let total_after = store.snapshot()?;
        let sync_after = physical(&store);
        let usage_after = process_usage()?;
        let usage = if usage_after["available"] == true {
            json!({"available":true,
                "user_us":usage_after["user_us"].as_u64().unwrap()-usage_before["user_us"].as_u64().unwrap(),
                "system_us":usage_after["system_us"].as_u64().unwrap()-usage_before["system_us"].as_u64().unwrap(),
                "process_lifetime_maxrss":usage_after["maxrss_os_units"],"maxrss_unit":usage_after["maxrss_unit"],
                "scope":"whole fixture interval, including counters and JSONL writes; not SDK latency"})
        } else {
            usage_after
        };
        let root = authority.snapshot()?.root;
        let final_pin_entries = root.pin_entries.entries()?.len();
        // Full cold validation runs after counters are frozen. Resolve the immutable
        // snapshot directly; opening a fresh branch would add unrelated durable pins.
        let index_entry = root
            .branches
            .get("main")
            .context("main branch missing")?
            .wal
            .as_ref()
            .context("final WAL missing")?;
        let index_bytes = authority.read_blob(index_entry)?;
        let logical = |relative: &str| -> Result<Vec<u8>> {
            let entry = root
                .objects
                .get(relative)?
                .context("cold-validation object missing")?;
            authority.read_blob(&entry)
        };
        let validated = crate::recovery::validate_index_state(
            &index_bytes,
            &namespace,
            |hash| logical(&format!("wal/index-segments/{hash}")),
            |key| {
                logical(
                    key.strip_prefix(&namespace.prefix())
                        .context("chunk outside namespace")?,
                )
            },
            |hash| logical(&format!("wal/receipt-pages/{hash}")),
        )?;
        assert_eq!(
            validated.commit(),
            Some(crate::wal::WalCommit {
                lsn: 4416,
                position: WalPosition {
                    salts,
                    frame_count: 4416
                }
            })
        );
        let reachable = authority.reachable(&root)?.len();
        writeln!(
            output,
            "{}",
            json!({"kind":"summary","seed":seed,"start_lsn":257,"end_lsn":4416,
            "measured":4160,"rolls":roll_count,"archived_receipts":moved_count,
            "final_inline_receipts":previous_inline,"final_pin_entries":final_pin_entries,
            "pin_policy_exercised":observed_calls>0,
            "pin_applicability":if observed_calls>0 {"paged_path_executed"} else {"not_exercised"},
            "process_usage":usage,
            "post_measurement_cold_validation":"pass","post_measurement_reachable_blobs":reachable,
            "store_calls":delta(&total_after,&total_before),
            "physical_sync_attempts":delta(&sync_after,&sync_before)})
        )?;
        output.flush()?;
        // Normal drops and temporary-volume deletion follow the recorded interval.
        drop(wal);
        drop(branch);
        drop(authority);
        drop(store);
        std::fs::remove_dir_all(directory)?;
    }
    Ok(())
}
