use super::*;
use crate::authority::ownership::OwnershipAuthority;
use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::{LocalObjectStore, StoredObject};
use crate::ownership::types::*;
use crate::runtime::DatabaseId;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Default, Debug)]
struct Counters {
    gets: usize,
    page_gets: usize,
    root_bytes: usize,
    puts: usize,
    page_puts: usize,
    groups: usize,
    group_bodies: usize,
    group_us: u128,
    root_writes: usize,
    lists: usize,
    get_us: u128,
    put_us: u128,
    list_us: u128,
    sites: BTreeMap<String, (usize, u128)>,
}

struct Probe {
    local: LocalObjectStore,
    counts: Mutex<Counters>,
    fail_root_puts: AtomicBool,
}

impl Probe {
    fn open(root: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            local: LocalObjectStore::open(root)?,
            counts: Mutex::default(),
            fail_root_puts: AtomicBool::new(false),
        })
    }

    fn reset(&self) {
        *self.counts.lock().unwrap() = Counters::default();
    }

    fn snapshot(&self) -> Counters {
        std::mem::take(&mut *self.counts.lock().unwrap())
    }
}

fn is_page(key: &str) -> bool {
    key.contains("/authority/index-pages/")
}

impl ObjectStore for Probe {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn group_rejection_publishes_nothing(&self) -> bool {
        self.local.group_rejection_publishes_nothing()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let start = Instant::now();
        let result = self.local.get(key);
        let mut counts = self.counts.lock().unwrap();
        counts.gets += 1;
        counts.page_gets += usize::from(is_page(key));
        counts.get_us += start.elapsed().as_micros();
        if key.ends_with("/authority/root") {
            counts.root_bytes += result
                .as_ref()
                .ok()
                .and_then(|value| value.as_ref())
                .map_or(0, |value| value.bytes.len());
        }
        let kind = if key.ends_with("/authority/root") {
            "root"
        } else if is_page(key) {
            "page"
        } else {
            "object"
        };
        let site = counts
            .sites
            .entry(format!("{}/get/{kind}", scaling_probe::site()))
            .or_default();
        site.0 += 1;
        site.1 += start.elapsed().as_micros();
        result
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if self.fail_root_puts.load(Ordering::SeqCst) && key.ends_with("/authority/root") {
            anyhow::bail!("injected head publication failure");
        }
        let start = Instant::now();
        let result = self.local.put(key, bytes, condition);
        let mut counts = self.counts.lock().unwrap();
        counts.puts += 1;
        counts.page_puts += usize::from(is_page(key));
        counts.root_writes += usize::from(key.ends_with("/authority/root"));
        counts.put_us += start.elapsed().as_micros();
        result
    }

    fn put_group(&self, writes: &[crate::object_store::GroupWrite]) -> Result<Vec<String>> {
        if self.fail_root_puts.load(Ordering::SeqCst)
            && writes
                .iter()
                .any(|write| write.key.ends_with("/authority/root"))
        {
            anyhow::bail!("injected head publication failure");
        }
        let start = Instant::now();
        let result = self.local.put_group(writes);
        let mut counts = self.counts.lock().unwrap();
        counts.groups += 1;
        counts.group_bodies += writes.len();
        counts.group_us += start.elapsed().as_micros();
        counts.root_writes += writes
            .iter()
            .filter(|write| write.key.ends_with("/authority/root"))
            .count();
        result
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.local.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let start = Instant::now();
        let result = self.local.list(prefix);
        let mut counts = self.counts.lock().unwrap();
        counts.lists += 1;
        counts.list_us += start.elapsed().as_micros();
        result
    }
}

struct Clock;

impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1000)
    }
}

fn authority(store: Arc<Probe>) -> Result<(Arc<OwnershipAuthority>, Arc<Authority>)> {
    let namespace = Namespace::new("scaling", "sql")?;
    let control = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
    let root = Authority::open(store, namespace)?;
    Ok((control, root))
}

fn database(store: Arc<Probe>, cache: &std::path::Path) -> Result<RemoteDatabase> {
    let (control, _) = authority(store)?;
    let namespace = Namespace::new("scaling", "sql")?;
    let intent = GrantIntent {
        database: DatabaseId::new(namespace.clone(), "main")?,
        worker_id: "worker".into(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "lease".into(),
        ttl_ms: 10000,
        role: AccessRole::Writer,
    };
    let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
    let lease = control.grant(GrantRequest {
        intent,
        nonce,
        request_id: "grant".into(),
    })?;
    let scoped = control.open_session(&lease.binding, AccessRole::Writer)?;
    RemoteDatabase::open(scoped, namespace, cache, 1)
}

/// Realistic catalog growth: every publication registers physical segments (`blobs`) and
/// logical objects (`objects`) through the production upload path, so the store scale is
/// driven by the same registries a real workload grows.
fn grow_catalog(root: &Arc<Authority>, objects: usize) -> Result<()> {
    for batch in 0..objects.div_ceil(128) {
        let size = (objects - batch * 128).min(128);
        let segments = (0..size)
            .map(|index| {
                let mut bytes = vec![0u8; crate::manifest::SEGMENT_SIZE];
                bytes[..8].copy_from_slice(&((batch * 128 + index) as u64).to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        let entries = root.upload_segments(&segments)?;
        root.register_segments(&entries)?;
    }
    Ok(())
}

fn head_bytes(root: &Arc<Authority>) -> Result<usize> {
    Ok(codec::encode(&root.snapshot()?.root)?.len())
}

fn measure(
    store: &Probe,
    label: &str,
    mut operation: impl FnMut() -> Result<()>,
) -> Result<BTreeMap<String, scaling_probe::Sample>> {
    let mut elapsed = Vec::new();
    let mut stages = BTreeMap::new();
    for _ in 0..7 {
        store.reset();
        scaling_probe::start();
        let start = Instant::now();
        operation()?;
        elapsed.push(start.elapsed().as_micros());
        stages = scaling_probe::finish();
        assert_eq!(store.snapshot().lists, 0, "SQL hot path listed the store");
    }
    elapsed.sort();
    let counts = store.snapshot();
    eprintln!(
        "SCALING {label} p50_us={} gets={} page_gets={} puts={} page_puts={} root_bytes={} get_us={} put_us={}",
        elapsed[3], counts.gets, counts.page_gets, counts.puts, counts.page_puts, counts.root_bytes, counts.get_us, counts.put_us
    );
    for (stage, sample) in &stages {
        if sample.calls > 0 {
            eprintln!(
                "SCALING {label} stage={stage} calls={} micros={}",
                sample.calls, sample.micros
            );
        }
    }
    Ok(stages)
}

/// Statements measured against a catalog of the given scale, with the deterministic
/// counters a gate can assert on: root decodes/validates/encodes and page reads.
struct Sample {
    stages: BTreeMap<String, scaling_probe::Sample>,
    head_bytes: usize,
    page_gets: usize,
    gets: usize,
}

impl Sample {
    /// Stages nest (a snapshot decode happens inside `transact`), so gate assertions
    /// aggregate by leaf name instead of by the full call path.
    fn calls(&self, stage: &str) -> usize {
        self.stages
            .iter()
            .filter(|(path, _)| path.rsplit('/').next() == Some(stage))
            .map(|(_, sample)| sample.calls)
            .sum()
    }
}

fn statement_sample(objects: usize) -> Result<Sample> {
    let directory = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let db = database(store.clone(), cache.path())?;
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1, 'fixed')")?;
    let (_, root) = authority(store.clone())?;
    grow_catalog(&root, objects)?;
    store.reset();
    scaling_probe::start();
    db.query("SELECT value FROM items WHERE id=1")?;
    db.execute("INSERT INTO items(value) VALUES('fixed')")?;
    let stages = scaling_probe::finish();
    let counts = store.snapshot();
    Ok(Sample {
        stages,
        head_bytes: head_bytes(&root)?,
        page_gets: counts.page_gets,
        gets: counts.gets,
    })
}

/// Runs one gate again in a process that has no other test's statements in it, and
/// carries the child's own output into the failure message: the GATE lines and the
/// assertion that tripped are what a reader needs, and they are printed only on failure.
fn run_gate_isolated(test: &str) -> Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", test, "--nocapture"])
        .env("PERI_SCALING_GATE_CHILD", "1")
        .output()?;
    assert!(
        output.status.success(),
        "the isolated gate {test} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Regression gate for the reason this module exists: a statement must not walk the
/// store, and the encoded head must not grow with the catalog. Deterministic counters
/// only, so the gate cannot flake with host load.
///
/// The stage probe counts process-wide (statement work runs on session lanes), so a test
/// running statements alongside this one would land in these windows and read as catalog
/// growth. The gate therefore measures in a child process of its own, the way the crash
/// injection does, and only the parent reports the result.
#[test]
fn statement_cost_does_not_scale_with_catalog_size() -> Result<()> {
    if std::env::var_os("PERI_SCALING_GATE_CHILD").is_none() {
        return run_gate_isolated(
            "authority::store_scaling_tests::statement_cost_does_not_scale_with_catalog_size",
        );
    }
    let small = statement_sample(100)?;
    let large = statement_sample(1_200)?;
    for (label, sample) in [("small", &small), ("large", &large)] {
        eprintln!(
            "GATE {label} gets={} page_gets={} head_bytes={} stages={:?}",
            sample.gets,
            sample.page_gets,
            sample.head_bytes,
            sample
                .stages
                .iter()
                .map(|(path, sample)| (path.as_str(), sample.calls))
                .collect::<BTreeMap<_, _>>()
        );
    }
    for stage in ["decode", "encode"] {
        assert!(
            small.calls(stage) > 0,
            "{stage} was never exercised; the gate would be vacuous"
        );
        assert!(
            large.calls(stage) <= small.calls(stage) + 2,
            "root {stage} count grew with catalog size: {} -> {}",
            small.calls(stage),
            large.calls(stage)
        );
        assert!(
            large.calls(stage) <= 32,
            "root {stage} count is unbounded: {}",
            large.calls(stage)
        );
    }
    assert!(
        small.calls("verify_deep") == 0 && large.calls("verify_deep") == 0,
        "statement walked the whole store"
    );
    // Reads are constant-bounded, not equal: a paged registry adds page hops instead of
    // carrying the whole registry. Walking the store (the regression this gate exists
    // for) would need at least one read per catalog entry.
    assert!(
        large.gets <= 96,
        "statement store reads are unbounded: {}",
        large.gets
    );
    assert!(
        large.gets <= small.gets * 3,
        "statement store reads grew with the catalog: {} -> {}",
        small.gets,
        large.gets
    );
    assert!(
        large.page_gets <= 64,
        "statement page reads are unbounded: {}",
        large.page_gets
    );
    assert!(
        large.head_bytes <= small.head_bytes + 4096,
        "authority head grew with catalog size: {} -> {}",
        small.head_bytes,
        large.head_bytes
    );
    assert!(large.head_bytes <= 32 * 1024, "head exceeded its budget");
    Ok(())
}

/// The same gate for the other axis a session grows along. `grow_catalog` adds registry
/// entries without committing, so it never grows the recovery history or the WAL index;
/// a real session does both. This fixture commits real rows through the production
/// statement path and asserts that a late commit costs what an early one cost, which is
/// the property the catalog-size gate cannot see.
#[test]
fn commit_cost_does_not_grow_with_retained_operations() -> Result<()> {
    const COMMITS: usize = 120;
    const WARM: usize = 60;
    let directory = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let db = database(store.clone(), cache.path())?;
    db.execute(
        "CREATE TABLE growth(id INTEGER PRIMARY KEY, label TEXT NOT NULL, payload TEXT NOT NULL)",
    )?;
    let mut warm = None;
    let mut cold = None;
    for index in 1..=COMMITS {
        store.reset();
        scaling_probe::start();
        db.execute(&format!(
            "INSERT INTO growth(id,label,payload) VALUES ({index},'g-{index:04}','{}')",
            "x".repeat(64)
        ))?;
        scaling_probe::finish();
        let counters = store.snapshot();
        assert_eq!(counters.lists, 0, "SQL hot path listed the store");
        eprintln!(
            "GATE retained-ops commit={index} gets={} page_gets={} page_puts={} root_bytes={}",
            counters.gets, counters.page_gets, counters.page_puts, counters.root_bytes
        );
        if index == WARM {
            warm = Some((counters.gets, counters.page_gets));
        }
        if index == COMMITS {
            cold = Some((counters.gets, counters.page_gets));
        }
    }
    let (warm_gets, warm_pages) = warm.context("warm sample missing")?;
    let (cold_gets, cold_pages) = cold.context("cold sample missing")?;
    eprintln!(
        "GATE retained-ops warm={warm_gets} gets/{warm_pages} pages cold={cold_gets} gets/{cold_pages} pages"
    );
    assert!(
        cold_pages <= 64,
        "a commit's page reads are unbounded: {cold_pages}"
    );
    assert!(
        cold_gets <= 96,
        "a commit's store reads are unbounded: {cold_gets}"
    );
    assert!(
        cold_pages <= warm_pages + 8,
        "a commit paid for the operations it retained: page reads {warm_pages} at commit {WARM} -> {cold_pages} at commit {COMMITS}"
    );
    assert!(
        cold_gets <= warm_gets + 16,
        "a commit reads grew with retained operations: {warm_gets} at commit {WARM} -> {cold_gets} at commit {COMMITS}"
    );
    Ok(())
}

/// One measurement point: same fixture, same statements, only the catalog layout differs.
#[derive(Debug, Clone)]
struct Curve {
    select_p50_us: u128,
    insert_p50_us: u128,
    head_bytes: usize,
    root_bytes_per_insert: usize,
    gets_per_insert: usize,
    page_gets_per_insert: usize,
    page_puts_per_insert: usize,
    puts_per_insert: usize,
    put_us_per_insert: u128,
    groups_per_insert: usize,
    group_bodies_per_insert: usize,
    group_us_per_insert: u128,
    /// The authority head is the single commit point of a publication: exactly one head
    /// write per commit, so a second ~15 KB root write cannot come from this path.
    root_writes_per_insert: usize,
    get_us_per_insert: u128,
    decode_calls_per_insert: usize,
}

/// What the tier grows. `Catalog` registers entries in this database's catalog through
/// the real upload path, `Unrelated` adds physical objects that no catalog entry points
/// at: the control axis that showed store size alone was never the statement cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Growth {
    Catalog,
    Unrelated,
}

impl Growth {
    fn label(self) -> &'static str {
        match self {
            Growth::Catalog => "catalog-items",
            Growth::Unrelated => "unrelated-objects",
        }
    }
}

fn scratch(case: &str) -> Result<std::path::PathBuf> {
    let root = std::path::Path::new("/private/tmp/peri-perf/store").join(case);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

fn host_load() -> String {
    std::process::Command::new("uptime")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .unwrap_or_else(|| "uptime unavailable".into())
        .trim()
        .to_owned()
}

fn grow(store: &Probe, root: &Arc<Authority>, growth: Growth, objects: usize) -> Result<()> {
    match growth {
        Growth::Catalog => grow_catalog(root, objects),
        Growth::Unrelated => {
            for index in 0..objects {
                store.local.put(
                    &format!("unrelated/blob-{index:05}"),
                    &vec![42u8; 32 * 1024],
                    PutCondition::Absent,
                )?;
            }
            Ok(())
        }
    }
}

fn curve_sample(objects: usize, growth: Growth, budget: usize) -> Result<Curve> {
    registry::INLINE_BUDGET.store(budget, Ordering::Relaxed);
    let result = (|| -> Result<Curve> {
        let directory = scratch(&format!("{}-{objects}", growth.label()))?;
        let cache = directory.join("cache");
        let store = Arc::new(Probe::open(directory.join("objects"))?);
        let db = database(store.clone(), &cache)?;
        db.execute(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1, 'fixed')",
        )?;
        let (_, root) = authority(store.clone())?;
        grow(&store, &root, growth, objects)?;
        let (select_p50_us, _, _) = p50(&store, || {
            db.query("SELECT value FROM items WHERE id=1")?;
            Ok(())
        })?;
        let (insert_p50_us, counters, stages) = p50(&store, || {
            db.execute("INSERT INTO items(value) VALUES('fixed')")
        })?;
        Ok(Curve {
            select_p50_us,
            insert_p50_us,
            head_bytes: head_bytes(&root)?,
            root_bytes_per_insert: counters.root_bytes,
            gets_per_insert: counters.gets,
            page_gets_per_insert: counters.page_gets,
            page_puts_per_insert: counters.page_puts,
            puts_per_insert: counters.puts,
            put_us_per_insert: counters.put_us,
            groups_per_insert: counters.groups,
            group_bodies_per_insert: counters.group_bodies,
            group_us_per_insert: counters.group_us,
            root_writes_per_insert: counters.root_writes,
            get_us_per_insert: counters.get_us,
            decode_calls_per_insert: stages
                .iter()
                .filter(|(path, _)| path.rsplit('/').next() == Some("decode"))
                .map(|(_, sample)| sample.calls)
                .sum(),
        })
    })();
    registry::INLINE_BUDGET.store(registry::MAX_INLINE_BYTES, Ordering::Relaxed);
    result
}

/// Seven runs, counter reset per run: the median is reported with the counters of the
/// final run, so latency and call counts describe the same execution.
fn p50(
    store: &Probe,
    mut operation: impl FnMut() -> Result<()>,
) -> Result<(u128, Counters, BTreeMap<String, scaling_probe::Sample>)> {
    let mut elapsed = Vec::new();
    let mut counters = Counters::default();
    let mut stages = BTreeMap::new();
    for _ in 0..7 {
        store.reset();
        scaling_probe::start();
        let start = Instant::now();
        operation()?;
        elapsed.push(start.elapsed().as_micros());
        counters = store.snapshot();
        stages = scaling_probe::finish();
        assert_eq!(counters.lists, 0, "SQL hot path listed the store");
    }
    elapsed.sort();
    Ok((elapsed[3], counters, stages))
}

/// Before/after under one process and one fixture: raising the inline budget keeps every
/// registry inside the head (`monolithic`, the layout this change removed), while the
/// shipped budget pages them (`paged`). Nothing else differs between the two columns.
///
/// Both groups run in the same tier, paired and adjacent in time, and every cell records
/// the host load, so absolute numbers are comparable within a run rather than across runs.
#[test]
#[ignore = "四档受控变量基准：显式运行以避免和其它 owner 的测量重叠"]
fn store_scaling_curve() -> Result<()> {
    const TIERS: [usize; 4] = [100, 300, 600, 1_200];
    let mut points: BTreeMap<(&'static str, &'static str, usize), (u128, u128)> = BTreeMap::new();
    for objects in TIERS {
        for growth in [Growth::Catalog, Growth::Unrelated] {
            for (mode, budget) in [
                ("monolithic", usize::MAX),
                ("paged", registry::MAX_INLINE_BYTES),
            ] {
                let curve = curve_sample(objects, growth, budget)?;
                points.insert(
                    (growth.label(), mode, objects),
                    (curve.select_p50_us, curve.insert_p50_us),
                );
                eprintln!(
                    "SCALING tier={objects} group={} mode={mode} pk_p50_us={} insert_p50_us={} head_bytes={} root_bytes_per_insert={} decode_calls={} gets={} page_gets={} page_puts={} load={}",
                    growth.label(),
                    curve.select_p50_us,
                    curve.insert_p50_us,
                    curve.head_bytes,
                    curve.root_bytes_per_insert,
                    curve.decode_calls_per_insert,
                    curve.gets_per_insert,
                    curve.page_gets_per_insert,
                    curve.page_puts_per_insert,
                    host_load()
                );
            }
        }
    }
    for group in ["catalog-items", "unrelated-objects"] {
        for mode in ["monolithic", "paged"] {
            let first = points[&(group, mode, TIERS[0])];
            let last = points[&(group, mode, TIERS[3])];
            let span = (TIERS[3] - TIERS[0]) as f64;
            eprintln!(
                "SLOPE group={group} mode={mode} pk_us_per_entry={:.3} insert_us_per_entry={:.3} pk_ratio={:.2} insert_ratio={:.2}",
                (last.0 as f64 - first.0 as f64) / span,
                (last.1 as f64 - first.1 as f64) / span,
                last.0 as f64 / first.0 as f64,
                last.1 as f64 / first.1 as f64,
            );
        }
    }
    Ok(())
}

/// Catalog share of one commit, release build, tiers that bracket the live stack.
///
/// Both arms run in the same process, and the order is reversed on the second pass, so a
/// measurement cannot be explained by load drifting between the two arms.
#[test]
#[ignore = "目录占比基准：显式运行以避免和其它 owner 的测量重叠"]
fn catalog_share_curve() -> Result<()> {
    for objects in [100, 600, 1_200] {
        for (pass, arm_order) in [
            ("A", [("group-off", false), ("group-on", true)]),
            ("B", [("group-on", true), ("group-off", false)]),
        ] {
            for (arm, grouped) in arm_order {
                super::GROUP_COMMIT.store(grouped, Ordering::Relaxed);
                let curve = curve_sample(objects, Growth::Catalog, registry::MAX_INLINE_BYTES)?;
                eprintln!(
                    "CATALOGSHARE tier={objects} pass={pass} arm={arm} insert_p50_us={} pk_p50_us={} head_bytes={} root_bytes_per_insert={} decode_calls={} gets={} page_gets={} puts={} page_puts={} put_us={} groups={} group_bodies={} group_us={} root_writes={} get_us={} load={}",
                    curve.insert_p50_us,
                    curve.select_p50_us,
                    curve.head_bytes,
                    curve.root_bytes_per_insert,
                    curve.decode_calls_per_insert,
                    curve.gets_per_insert,
                    curve.page_gets_per_insert,
                    curve.puts_per_insert,
                    curve.page_puts_per_insert,
                    curve.put_us_per_insert,
                    curve.groups_per_insert,
                    curve.group_bodies_per_insert,
                    curve.group_us_per_insert,
                    curve.root_writes_per_insert,
                    curve.get_us_per_insert,
                    host_load()
                );
            }
        }
    }
    super::GROUP_COMMIT.store(true, Ordering::Relaxed);
    Ok(())
}

/// A publication that loses the head write must leave the previous head fully readable:
/// registry pages are immutable and only the head CAS commits.
#[test]
fn interrupted_publication_keeps_previous_head_readable() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let (_, root) = authority(store.clone())?;
    grow_catalog(&root, 400)?;
    let before = root.snapshot()?;
    let entries = before.root.objects.entries()?;
    assert!(!entries.is_empty());
    store.fail_root_puts.store(true, Ordering::SeqCst);
    let segments = vec![vec![7u8; crate::manifest::SEGMENT_SIZE]];
    assert!(root.upload_segments(&segments).is_err());
    store.fail_root_puts.store(false, Ordering::SeqCst);
    let after = root.snapshot()?;
    assert_eq!(after.root.revision, before.root.revision);
    assert_eq!(after.root.objects.entries()?, entries);
    assert_eq!(after.root.blobs.entries()?, before.root.blobs.entries()?);
    // The next publication still succeeds and remains complete.
    let published = root.upload_segments(&segments)?;
    root.register_segments(&published)?;
    assert_eq!(
        root.snapshot()?.root.objects.entries()?.len(),
        entries.len() + 1
    );
    Ok(())
}

#[test]
fn corrupt_registry_page_fails_closed_and_recovers_only_with_intact_pages() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let (_, root) = authority(store.clone())?;
    grow_catalog(&root, 400)?;
    let snapshot = root.snapshot()?;
    let pointer = snapshot
        .root
        .objects
        .pointer()
        .context("catalog did not spill")?
        .to_owned();
    let page = snapshot
        .root
        .namespace
        .key(&format!("authority/index-pages/{pointer}"))?;
    let object = store.local.get(&page)?.context("index page missing")?;
    let mut corrupt = object.bytes.clone();
    corrupt[0] ^= 1;
    store
        .local
        .put(&page, &corrupt, PutCondition::Match(object.etag))?;
    assert!(
        root.snapshot()?.root.objects.entries().is_err(),
        "corrupt page resolved a lookup"
    );
    assert!(
        Authority::open(store.clone(), snapshot.root.namespace.clone()).is_err(),
        "corrupt page survived boot verification"
    );
    store.local.put(&page, &object.bytes, PutCondition::Any)?;
    assert_eq!(root.snapshot()?.root.objects.entries()?.len(), 400);
    Ok(())
}

#[test]
fn reopened_store_rebuilds_registry_from_pages() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let namespace = Namespace::new("scaling", "sql")?;
    let (entries, blobs) = {
        let store = Arc::new(Probe::open(directory.path())?);
        let (_, root) = authority(store)?;
        grow_catalog(&root, 400)?;
        let snapshot = root.snapshot()?;
        assert!(
            snapshot.root.objects.pointer().is_some(),
            "catalog stayed inline; the reopen check would be vacuous"
        );
        (
            snapshot.root.objects.entries()?,
            snapshot.root.blobs.entries()?,
        )
    };
    let reopened = Arc::new(Probe::open(directory.path())?);
    let root = Authority::open(reopened, namespace)?;
    let snapshot = root.snapshot()?;
    assert_eq!(snapshot.root.objects.entries()?, entries);
    assert_eq!(snapshot.root.blobs.entries()?, blobs);
    let (segment, entry) = entries
        .iter()
        .find(|(key, _)| key.starts_with("segments/"))
        .context("no segment registered")?;
    assert_eq!(entry.hash, segment.trim_start_matches("segments/"));
    Ok(())
}

/// Where a killed child stops inside a publication group. The points are chosen so each
/// one lands on a different side of the only visibility boundary (the head CAS).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashPoint {
    /// Some immutable page bodies are durable, the head CAS is not.
    PartwayBodies,
    /// The same, but only the first body of the group is durable.
    BetweenBodies,
    /// The whole group is durable, including the head CAS: the caller never learns it.
    AckedNotReturned,
}

impl CrashPoint {
    fn label(self) -> &'static str {
        match self {
            CrashPoint::PartwayBodies => "partway-bodies",
            CrashPoint::BetweenBodies => "between-bodies",
            CrashPoint::AckedNotReturned => "acked-not-returned",
        }
    }
}

/// Evidence for the injection report: what the head references versus what is on disk.
fn crash_evidence(label: &str, directory: &std::path::Path, namespace: &Namespace) -> Result<()> {
    let probe = LocalObjectStore::open(directory.join("objects"))?;
    let head = match probe.get(&namespace.key("authority/root")?)? {
        Some(object) => object,
        None => {
            eprintln!("CRASH_EVIDENCE {label} head=absent");
            return Ok(());
        }
    };
    let head: serde_json::Value = serde_json::from_slice(&head.bytes)?;
    let root = &head["root"];
    eprintln!(
        "CRASH_EVIDENCE {label} revision={} blob_index={:?} object_index={:?} upload_index={:?} receipt_index={:?}",
        root["revision"],
        root.get("blob_index"),
        root.get("object_index"),
        root.get("upload_index"),
        root.get("receipt_index")
    );
    let pages = probe.list(&namespace.key("authority/index-pages")?)?;
    eprintln!("CRASH_EVIDENCE {label} page_objects={}", pages.len());
    for key in &pages {
        let bytes = probe.get(key)?.unwrap().bytes;
        let page: serde_json::Value = serde_json::from_slice(&bytes)?;
        eprintln!(
            "CRASH_EVIDENCE {label} page={} bytes={} kind={}",
            key.rsplit('/').next().unwrap(),
            bytes.len(),
            page["kind"]
        );
    }
    Ok(())
}

/// A store that cuts a publication group at a deterministic point, then waits to be
/// killed. Everything below it is the real local store, so the durable side effects are
/// exactly the ones a crash would leave behind.
struct CuttingStore {
    inner: Arc<LocalObjectStore>,
    point: CrashPoint,
    reached: std::path::PathBuf,
}

impl ObjectStore for CuttingStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }

    fn put_group(&self, writes: &[crate::object_store::GroupWrite]) -> Result<Vec<String>> {
        let bodies = writes.len().saturating_sub(1);
        let cut = match self.point {
            CrashPoint::BetweenBodies => 1,
            CrashPoint::PartwayBodies => bodies / 2,
            // Fully durable, then withhold the answer: the caller dies without a reply.
            CrashPoint::AckedNotReturned => {
                let etags = self.inner.put_group(writes)?;
                std::fs::write(&self.reached, "group-durable")?;
                std::thread::sleep(std::time::Duration::from_secs(30));
                return Ok(etags);
            }
        };
        for write in &writes[..cut.min(bodies)] {
            self.inner
                .put(&write.key, &write.bytes, PutCondition::Any)?;
        }
        std::fs::write(&self.reached, "mid-group")?;
        std::thread::sleep(std::time::Duration::from_secs(30));
        Ok(writes.iter().map(|write| write.key.clone()).collect())
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

/// Child half of the crash injection: runs one publication, is killed inside it. Only the
/// parent test spawns this with the environment set, otherwise it returns immediately.
#[test]
fn crash_injection_child() -> Result<()> {
    let Ok(point) = std::env::var("PERI_CRASH_POINT") else {
        return Ok(());
    };
    let point = match point.as_str() {
        "partway-bodies" => CrashPoint::PartwayBodies,
        "between-bodies" => CrashPoint::BetweenBodies,
        _ => CrashPoint::AckedNotReturned,
    };
    let directory = std::path::PathBuf::from(std::env::var("PERI_CRASH_DIR")?);
    let store = Arc::new(CuttingStore {
        inner: Arc::new(LocalObjectStore::open(directory.join("objects"))?),
        point,
        reached: directory.join("reached"),
    });
    let root = Authority::open(store, Namespace::new("crash", "injected")?)?;
    // One transaction, many registry additions: the publication group has several
    // immutable page bodies before the head CAS.
    let segments = (0..16)
        .map(|index| {
            let mut bytes = vec![0u8; crate::manifest::SEGMENT_SIZE];
            bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
            bytes
        })
        .collect::<Vec<_>>();
    let entries = root.upload_segments(&segments)?;
    root.register_segments(&entries)?;
    Ok(())
}

fn crash_injection_point(point: CrashPoint) -> Result<()> {
    let directory = scratch(&format!("crash-{}", point.label()))?;
    let cache = directory.join("cache");
    let namespace = Namespace::new("crash", "injected")?;
    // Setup in this process: a database, an acknowledged commit, and physical blobs whose
    // registration the child will try to publish.
    let acked_revision;
    let uploads_before;
    let entries;
    {
        let store = Arc::new(Probe::open(directory.join("objects"))?);
        eprintln!("CRASH_STEP opened-probe");
        let control = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
        let root = Authority::open(store.clone(), namespace.clone())?;
        eprintln!("CRASH_STEP opened-raw-authority");
        let segments = (0..16)
            .map(|index| {
                let mut bytes = vec![0u8; crate::manifest::SEGMENT_SIZE];
                bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        entries = root.upload_segments(&segments)?;
        eprintln!("CRASH_STEP uploaded-intents");
        crash_evidence("after-upload", &directory, &namespace)?;
        let intent = GrantIntent {
            database: DatabaseId::new(namespace.clone(), "main")?,
            worker_id: "worker".into(),
            epoch: 1,
            expected_epoch: 0,
            lease_id: "lease".into(),
            ttl_ms: 10_000,
            role: AccessRole::Writer,
        };
        store.reset();
        let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
        let counts = store.snapshot();
        eprintln!(
            "CRASH_STEP challenged groups={} bodies={} puts={} page_objects={} root_writes={}",
            counts.groups,
            counts.group_bodies,
            counts.puts,
            directory.join("objects").read_dir()?.count(),
            counts.root_writes
        );
        let lease = control.grant(GrantRequest {
            intent,
            nonce,
            request_id: "grant".into(),
        })?;
        let scoped = control.open_session(&lease.binding, AccessRole::Writer)?;
        let db = RemoteDatabase::open(scoped, namespace.clone(), cache.clone(), 1)?;
        eprintln!("CRASH_STEP opened-database");
        db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
        db.execute("INSERT INTO items(value) VALUES('acked')")?;
        // The engine registers its own `segments/*` while materialising the database, so
        // the injected batch is tracked by hash instead of by a bare count.
        let entries_before = root.snapshot()?.root.objects.entries()?;
        assert!(
            entries
                .iter()
                .all(|entry| !entries_before.contains_key(&format!("segments/{}", entry.hash))),
            "the injected segments were registered before the crash"
        );
        drop(db);
        // Read the acknowledged revision after the engine closed: its shutdown can
        // commit, and the child's group must land on top of that exact revision.
        let acknowledged = root.snapshot()?;
        acked_revision = acknowledged.root.revision;
        uploads_before = acknowledged.root.uploads.entries()?.len();
    }

    crash_evidence("after-setup", &directory, &namespace)?;
    // Child process, killed inside the publication.
    let reached = directory.join("reached");
    let child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "authority::store_scaling_tests::crash_injection_child",
            "--nocapture",
        ])
        .env("PERI_CRASH_POINT", point.label())
        .env("PERI_CRASH_DIR", &directory)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let pid = child.id();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !reached.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "child never reached the injection point ({point:?})"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    // SIGKILL, not a graceful stop: the process dies inside the store call.
    assert_eq!(
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) },
        0,
        "kill failed"
    );
    let mut child = child;
    let status = child.wait()?;
    assert!(!status.success(), "child was expected to die by signal");

    // Reopen and read: no partially visible group, and the acknowledged commit is intact.
    let store = Arc::new(Probe::open(directory.join("objects"))?);
    crash_evidence("after-crash", &directory, &namespace)?;
    let control = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
    let root = Authority::open(store.clone(), namespace.clone())?;
    let snapshot = root.snapshot()?;
    let entries_after = snapshot.root.objects.entries()?;
    // Visibility is all-or-nothing at the head CAS: either none of the group's
    // registrations are visible, or all sixteen of them are.
    let registered = entries
        .iter()
        .filter(|entry| entries_after.contains_key(&format!("segments/{}", entry.hash)))
        .count();
    let uploads_after = snapshot.root.uploads.entries()?.len();
    match point {
        CrashPoint::AckedNotReturned => {
            // The child's first publication is the intent reservation for its sixteen
            // uploads; the group that became durable is exactly that one, whole.
            assert_eq!(
                snapshot.root.revision,
                acked_revision + 1,
                "the durable group was not visible after the crash"
            );
            assert_eq!(
                uploads_after - uploads_before,
                16,
                "a durable group was only partly visible"
            );
            assert_eq!(registered, 0, "an attempt that never ran became visible");
        }
        CrashPoint::PartwayBodies | CrashPoint::BetweenBodies => {
            assert_eq!(
                snapshot.root.revision, acked_revision,
                "the cut group became visible"
            );
            assert_eq!(
                uploads_after, uploads_before,
                "the cut group leaked intents"
            );
            assert_eq!(registered, 0, "the group became partially visible");
        }
    }
    eprintln!(
        "CRASH_RESULT point={} revision_before={} revision_after={} injected_visible={}/16 groups={}",
        point.label(),
        acked_revision,
        snapshot.root.revision,
        registered,
        point.label()
    );
    // Whatever the child left behind must be unreferenced immutable bodies: every page
    // object still hashes to its own key, and the reachable tree resolves completely.
    for key in store.local.list(&namespace.key("authority/index-pages")?)? {
        let bytes = store.local.get(&key)?.unwrap().bytes;
        let hash = key.rsplit('/').next().unwrap().to_owned();
        assert_eq!(
            blake3::hash(&bytes).to_hex().as_str(),
            hash,
            "a page body was left non-content-addressed"
        );
    }
    // The acknowledged commit is readable through the engine, and the store accepts
    // further commits without any manual cleanup.
    // The post-crash grant continues from the epoch the setup left behind: a fence that
    // reused an old epoch would be refused, which is itself the fencing property.
    let intent = GrantIntent {
        database: DatabaseId::new(namespace.clone(), "main")?,
        worker_id: "worker-2".into(),
        epoch: 2,
        expected_epoch: 1,
        lease_id: "lease-2".into(),
        ttl_ms: 10_000,
        role: AccessRole::Writer,
    };
    let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
    let lease = control.grant(GrantRequest {
        intent,
        nonce,
        request_id: "grant-2".into(),
    })?;
    let scoped = control.open_session(&lease.binding, AccessRole::Writer)?;
    let db = RemoteDatabase::open(scoped, namespace.clone(), cache.clone(), 2)?;
    let rows = db.query("SELECT value FROM items")?;
    assert_eq!(rows.len(), 1, "acknowledged row disappeared");
    assert_eq!(rows[0][0].to_string(), "acked");
    db.execute("INSERT INTO items(value) VALUES('after-crash')")?;
    assert_eq!(
        db.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "2"
    );
    drop(db);
    // Registration can also be retried without cleanup: the child's blobs are still
    // upload intents, and the retry publishes them as a fresh group.
    if registered == 0 {
        root.register_segments(&entries)?;
        let retried = root.snapshot()?.root.objects.entries()?;
        assert!(
            entries
                .iter()
                .all(|entry| retried.contains_key(&format!("segments/{}", entry.hash))),
            "retry after the crash did not register the injected batch"
        );
    }
    Ok(())
}

#[test]
fn crash_between_group_bodies_leaves_no_partial_publication() -> Result<()> {
    crash_injection_point(CrashPoint::BetweenBodies)
}

#[test]
fn crash_partway_through_group_bodies_leaves_no_partial_publication() -> Result<()> {
    crash_injection_point(CrashPoint::PartwayBodies)
}

#[test]
fn crash_after_head_cas_publishes_the_whole_group_or_nothing() -> Result<()> {
    crash_injection_point(CrashPoint::AckedNotReturned)
}

/// The real stack publishes through `NamespaceBackend`, so the group must survive that
/// wrapper: an engine-driven commit has to show group calls, not per-write fallbacks.
#[test]
fn engine_commits_publish_through_the_backend_group() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let db = database(store.clone(), cache.path())?;
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    store.reset();
    db.execute("INSERT INTO items(value) VALUES('grouped')")?;
    let counts = store.snapshot();
    assert!(
        counts.groups >= 1,
        "the publication never reached the store as a group: {counts:?}"
    );
    assert_eq!(
        counts.page_puts, 0,
        "page bodies fell back to per-write publication"
    );
    Ok(())
}

/// R2 evidence: one commit publishes the authority head exactly once. The WAL pair's
/// bodies ride in that same group, so the instances, the registrations that make them
/// reachable and the head CAS become one durable publication instead of an intent
/// revision followed by a registration revision. Nothing is referenced before the group
/// lands, and the write order inside the group keeps the head CAS last, so a reader can
/// never see a registration whose bytes are missing.
#[test]
fn a_commit_publishes_the_head_once() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let db = database(store.clone(), cache.path())?;
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    for _ in 0..3 {
        store.reset();
        db.execute("INSERT INTO items(value) VALUES('fixed')")?;
        let counts = store.snapshot();
        assert_eq!(
            counts.root_writes, 1,
            "commit head writes changed; re-derive the publication split"
        );
        assert_eq!(
            counts.puts + counts.groups,
            1,
            "a commit published more than its own group"
        );
    }
    Ok(())
}

/// R5 evidence: a lease validation must not cost a durable write. Every `session/validate`
/// RPC the worker issues is a read of the head plus an in-memory lease check; the durable
/// writes of a request come from the publication group only.
#[test]
fn lease_validation_costs_no_durable_write() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let (control, _) = authority(store.clone())?;
    let namespace = Namespace::new("scaling", "sql")?;
    let intent = GrantIntent {
        database: DatabaseId::new(namespace.clone(), "main")?,
        worker_id: "worker".into(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "lease".into(),
        ttl_ms: 10_000,
        role: AccessRole::Writer,
    };
    let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
    let lease = control.grant(GrantRequest {
        intent,
        nonce,
        request_id: "grant".into(),
    })?;
    let scoped = control.open_session(&lease.binding, AccessRole::Writer)?;
    store.reset();
    for _ in 0..5 {
        scoped.validate_session()?;
    }
    let counts = store.snapshot();
    assert_eq!(
        counts.puts + counts.groups,
        0,
        "lease validation performed a durable write"
    );
    assert!(counts.root_writes == 0, "lease validation wrote the head");
    assert!(counts.gets > 0, "lease validation never read the head");
    Ok(())
}

fn percentiles(mut samples: Vec<u128>) -> (u128, u128) {
    samples.sort_unstable();
    (samples[samples.len() / 2], samples[samples.len() * 9 / 10])
}

/// E1: what one lease validation pays, split between the head read, the head decode and
/// the rest of the check, in the two head shapes. `legacy-inline` is the pre-v5 shape
/// where the idempotency ledger lived entirely in the head; `paged` is what the writer
/// now produces: a bounded inline window over an immutable page tree. Every validation
/// re-reads and re-decodes the whole head, so only the first shape follows the number of
/// commands ever issued. The decode is reused across validations of one unchanged head
/// (`decode_cache`), which is why the curve reports a cold decode — a document this
/// process has not parsed — next to the warm one. Prints one `LEASE_COST` line per shape
/// and size; asserts the structural facts the split rests on, never a wall-clock threshold.
#[test]
fn lease_validation_cost_curve() -> Result<()> {
    const ITERATIONS: usize = 201;
    const COLD_ITERATIONS: usize = 21;
    for entries in [1usize, 200, 2000] {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(Probe::open(directory.path())?);
        let namespace = Namespace::new("scaling", "leasecost")?;
        let control = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
        let root = Authority::open(store.clone(), namespace.clone())?;
        let intent = GrantIntent {
            database: DatabaseId::new(namespace.clone(), "main")?,
            worker_id: "worker".into(),
            epoch: 1,
            expected_epoch: 0,
            lease_id: "lease".into(),
            ttl_ms: 10_000,
            role: AccessRole::Writer,
        };
        let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
        let lease = control.grant(GrantRequest {
            intent,
            nonce,
            request_id: "grant".into(),
        })?;
        let scoped = control.open_session(&lease.binding, AccessRole::Writer)?;

        let base = root.snapshot()?.root;
        let receipt = serde_json::to_value(&lease)?;
        let key = namespace.key("authority/root")?;
        for shape in ["legacy-inline", "paged"] {
            let mut document = base.clone();
            let mut archived = 0usize;
            {
                let catalog = document.ownership.as_mut().context("ownership missing")?;
                // The grant that produced the binding already left its own receipt behind,
                // so the ledger is grown to the target size rather than by a fixed number.
                let additions = entries.saturating_sub(catalog.commands.len());
                for index in 0..additions {
                    catalog.commands.insert(
                        format!("command-{index:016}"),
                        CommandReceipt {
                            digest: blake3::hash(format!("payload-{index}").as_bytes())
                                .to_hex()
                                .to_string(),
                            result: receipt.clone(),
                        },
                    );
                }
            }
            match shape {
                "legacy-inline" => {
                    document.version = 4;
                    let catalog = document.ownership.as_mut().context("ownership missing")?;
                    ensure!(
                        catalog.command_index.is_none() && catalog.commands.len() == entries,
                        "the legacy ledger under measurement was not grown: {} vs {entries}",
                        catalog.commands.len()
                    );
                }
                "paged" => {
                    let catalog = document.ownership.as_mut().context("ownership missing")?;
                    let split = catalog.commands.len().saturating_sub(INLINE_COMMANDS_MAX);
                    let archived_entries: BTreeMap<String, serde_json::Value> = catalog
                        .commands
                        .iter()
                        .take(split)
                        .map(|(identifier, receipt)| {
                            Ok((identifier.clone(), serde_json::to_value(receipt)?))
                        })
                        .collect::<Result<_>>()?;
                    let pointer = paged_index::PagedIndex::new(store.as_ref(), &namespace)
                        .build(archived_entries)?;
                    let window: BTreeMap<_, _> = catalog
                        .commands
                        .iter()
                        .skip(split)
                        .map(|(identifier, receipt)| (identifier.clone(), receipt.clone()))
                        .collect();
                    catalog.commands = window;
                    catalog.command_index = pointer;
                    document.version = 5;
                    archived = split;
                    ensure!(
                        document.ownership.as_ref().unwrap().commands.len() <= INLINE_COMMANDS_MAX,
                        "the paged window was not bounded"
                    );
                }
                _ => unreachable!(),
            }
            let encoded = codec::encode(&document)?;
            store.put(&key, &encoded, PutCondition::Any)?;
            let bytes = store.get(&key)?.context("authority root missing")?.bytes;
            assert_eq!(bytes.len(), encoded.len());

            let mut read = Vec::new();
            let mut warm = Vec::new();
            let mut validate = Vec::new();
            let mut session = Vec::new();
            for _ in 0..ITERATIONS {
                let started = Instant::now();
                store.get(&key)?;
                read.push(started.elapsed().as_micros());
                let started = Instant::now();
                codec::decode(&bytes)?;
                warm.push(started.elapsed().as_micros());
                // The production shape: `SessionOperation::Validate` reaches the authority as
                // this call, which reads the head once.
                let started = Instant::now();
                control.validate_lease(&lease.binding, AccessRole::Writer)?;
                validate.push(started.elapsed().as_micros());
                // The worker-store shape: a fenced session store validates the binding and
                // then asks its own branch store, which reads the head again.
                let started = Instant::now();
                scoped.validate_session()?;
                session.push(started.elapsed().as_micros());
            }
            // A document this process has not parsed, one revision apart from the last so the
            // bytes differ: the only way to see what the decode costs without the reuse.
            let mut cold = Vec::new();
            let mut cold_bytes = Vec::new();
            for index in 0..COLD_ITERATIONS {
                document.revision = 1_000_000 + index as u64;
                cold_bytes.push(codec::encode(&document)?);
            }
            for encoded in &cold_bytes {
                let started = Instant::now();
                codec::decode(encoded)?;
                cold.push(started.elapsed().as_micros());
            }

            let (read_p50, read_p90) = percentiles(read);
            let (warm_p50, warm_p90) = percentiles(warm);
            let (cold_p50, cold_p90) = percentiles(cold);
            let (validate_p50, validate_p90) = percentiles(validate);
            let (session_p50, session_p90) = percentiles(session);
            eprintln!(
            "LEASE_COST shape={shape} entries={entries} archived={archived} head_bytes={} raw_bytes={} receipt_bytes={} read_p50_us={read_p50} read_p90_us={read_p90} decode_cold_p50_us={cold_p50} decode_cold_p90_us={cold_p90} decode_warm_p50_us={warm_p50} decode_warm_p90_us={warm_p90} validate_p50_us={validate_p50} validate_p90_us={validate_p90} session_p50_us={session_p50} session_p90_us={session_p90}",
            encoded.len(),
            serde_json::to_string(&document)?.len(),
            serde_json::to_vec(&receipt)?.len()
        );
            ensure!(
                validate_p50 > 0 && cold_p50 > 0,
                "the curve was measured below the clock resolution"
            );
        }
    }
    Ok(())
}

fn decode_calls(stages: BTreeMap<String, scaling_probe::Sample>) -> usize {
    stages
        .iter()
        .filter(|(path, _)| path.rsplit('/').next() == Some("decode"))
        .map(|(_, sample)| sample.calls)
        .sum()
}

/// Every lease validation re-reads the head, and the worker asks for one per request, so
/// the decode of an unchanged document is paid once and then reused (`decode_cache`).
/// Deterministic counters only; the stage probe counts process-wide, so this measures in a
/// child process of its own like the catalog gate does.
#[test]
fn a_head_is_decoded_once_per_document() -> Result<()> {
    if std::env::var_os("PERI_SCALING_GATE_CHILD").is_none() {
        return run_gate_isolated(
            "authority::store_scaling_tests::a_head_is_decoded_once_per_document",
        );
    }
    const VALIDATIONS: usize = 64;
    let directory = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let namespace = Namespace::new("scaling", "decoded")?;
    let control = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
    let root = Authority::open(store.clone(), namespace.clone())?;
    let intent = GrantIntent {
        database: DatabaseId::new(namespace.clone(), "main")?,
        worker_id: "worker".into(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "lease".into(),
        ttl_ms: 10_000,
        role: AccessRole::Writer,
    };
    let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
    let lease = control.grant(GrantRequest {
        intent,
        nonce,
        request_id: "grant".into(),
    })?;
    let key = namespace.key("authority/root")?;
    let mut document = root.snapshot()?.root;

    // Warm: the head this process has already decoded. Every later validation of the same
    // document must serve that decode rather than parse it again.
    control.validate_lease(&lease.binding, AccessRole::Writer)?;
    scaling_probe::start();
    for _ in 0..VALIDATIONS {
        control.validate_lease(&lease.binding, AccessRole::Writer)?;
    }
    let unchanged = decode_calls(scaling_probe::finish());
    assert_eq!(
        unchanged, 0,
        "an unchanged head was decoded {unchanged} times across {VALIDATIONS} validations"
    );

    // A head that moved is a document this process has not decoded, and the memo must not
    // answer with the one it replaced: exactly one decode, for the document now in the
    // store.
    document.revision += 1;
    store.put(&key, &codec::encode(&document)?, PutCondition::Any)?;
    scaling_probe::start();
    for _ in 0..VALIDATIONS {
        control.validate_lease(&lease.binding, AccessRole::Writer)?;
    }
    let changed = decode_calls(scaling_probe::finish());
    assert_eq!(
        changed, 1,
        "a moved head was decoded {changed} times across {VALIDATIONS} validations"
    );

    // A document that does not decode is reported as one: the memo holds decodes, never a
    // substitute for reading the store.
    store.put(&key, b"{ not an envelope", PutCondition::Any)?;
    assert!(control
        .validate_lease(&lease.binding, AccessRole::Writer)
        .is_err());
    Ok(())
}

#[test]
fn sql_reads_and_commits_do_not_list_physical_store() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(Probe::open(directory.path())?);
    let db = database(store.clone(), cache.path())?;
    db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    measure(&store, "gate insert", || {
        db.execute("INSERT INTO items(value) VALUES('fixed')")
    })?;
    measure(&store, "gate select", || {
        db.query("SELECT value FROM items WHERE id=1")?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn paged_receipt_lookup_and_update_do_not_enumerate_store() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Probe::open(directory.path())?;
    let namespace = Namespace::new("scaling", "receipts")?;
    let index = paged_index::PagedIndex::new(&store, &namespace);
    let entries = (0..600)
        .map(|number| (format!("receipt-{number}"), serde_json::json!(number)))
        .collect();
    let root = index.build(entries)?.unwrap();
    store.reset();
    assert_eq!(
        index.get(Some(&root), "receipt-42")?,
        Some(serde_json::json!(42))
    );
    let expected_gets = store.snapshot().gets;
    for number in 0..32 {
        store.local.put(
            &format!("unrelated/key-{number}"),
            b"payload",
            PutCondition::Absent,
        )?;
    }
    store.reset();
    assert_eq!(
        index.get(Some(&root), "receipt-42")?,
        Some(serde_json::json!(42))
    );
    assert_eq!(store.snapshot().gets, expected_gets);
    let next = index.insert_many(
        Some(&root),
        BTreeMap::from([("new-receipt".into(), serde_json::json!(601))]),
    )?;
    assert_eq!(store.snapshot().lists, 0);
    drop(store);
    let reopened = LocalObjectStore::open(directory.path())?;
    let index = paged_index::PagedIndex::new(&reopened, &namespace);
    assert_eq!(index.get(Some(&root), "new-receipt")?, None);
    assert_eq!(
        index.get(Some(&next), "new-receipt")?,
        Some(serde_json::json!(601))
    );
    Ok(())
}

/// Cumulative committed rows to measure, and the fixed statement a measured commit runs.
const COMMIT_BENCH_ROWS_ENV: &str = "PERI_COMMIT_BENCH_ROWS";
const COMMIT_BENCH_ROUNDS_ENV: &str = "PERI_COMMIT_BENCH_ROUNDS";
const COMMIT_BENCH_COMMITS_ENV: &str = "PERI_COMMIT_BENCH_COMMITS";
const COMMIT_BENCH_BATCH_ENV: &str = "PERI_COMMIT_BENCH_BATCH";
const COMMIT_BENCH_DEFAULT_ROWS: &str = "1000,4000,12000";
const COMMIT_BENCH_DEFAULT_ROUNDS: usize = 3;
const COMMIT_BENCH_DEFAULT_COMMITS: usize = 16;
const COMMIT_BENCH_DEFAULT_BATCH: usize = 200;

fn commit_bench_tiers() -> Vec<usize> {
    std::env::var(COMMIT_BENCH_ROWS_ENV)
        .unwrap_or_else(|_| COMMIT_BENCH_DEFAULT_ROWS.into())
        .split(',')
        .filter_map(|value| value.trim().parse().ok())
        .collect()
}

fn commit_bench_setting(name: &str, default: usize, floor: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(floor)
}

fn commit_bench_median(values: &mut [u128]) -> u128 {
    values.sort_unstable();
    values[values.len() / 2]
}

/// One arm of the paired commit benchmark: its own store, session, database and row
/// counter, so a measurement never depends on the other arm's state.
struct CommitArm {
    _objects: tempfile::TempDir,
    _cache: tempfile::TempDir,
    store: Arc<Probe>,
    /// The fenced session the engine writes through: the logical `wal/index` key resolves
    /// through it, so verification reads exactly what the database published.
    session: Arc<dyn ObjectStore>,
    database: RemoteDatabase,
    next_id: u64,
}

impl CommitArm {
    fn open() -> Result<Self> {
        let objects = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let store = Arc::new(Probe::open(objects.path())?);
        let namespace = Namespace::new("scaling", "sql")?;
        let (control, _root) = authority(store.clone())?;
        let intent = GrantIntent {
            database: DatabaseId::new(namespace.clone(), "main")?,
            worker_id: "worker".into(),
            epoch: 1,
            expected_epoch: 0,
            lease_id: "lease".into(),
            ttl_ms: 10000,
            role: AccessRole::Writer,
        };
        let nonce = control.challenge(LeaseIntent::Grant(intent.clone()))?.nonce;
        let lease = control.grant(GrantRequest {
            intent,
            nonce,
            request_id: "grant".into(),
        })?;
        let session = control.open_session(&lease.binding, AccessRole::Writer)?;
        let database = RemoteDatabase::open(session.clone(), namespace, cache.path(), 1)?;
        database.execute("CREATE TABLE bench(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")?;
        Ok(Self {
            _objects: objects,
            _cache: cache,
            store,
            session,
            database,
            next_id: 1,
        })
    }

    /// Grows the table to `rows` with batched statements: the tier is fixture, not the
    /// measured transaction, and every measured commit below keeps the same statement size.
    fn grow_to(&mut self, rows: usize, batch: usize) -> Result<()> {
        while (self.next_id as usize) <= rows {
            let size = batch.min(rows - self.next_id as usize + 1);
            let values = (0..size)
                .map(|offset| format!("({},'{}')", self.next_id + offset as u64, "g".repeat(64)))
                .collect::<Vec<_>>()
                .join(",");
            self.database
                .execute(&format!("INSERT INTO bench(id,payload) VALUES {values}"))?;
            self.next_id += size as u64;
        }
        Ok(())
    }

    /// The fixed transaction every measured commit runs: one row, so each commit appends
    /// the same frames whatever the tier.
    fn commit_one(&mut self) -> Result<()> {
        self.database.execute(&format!(
            "INSERT INTO bench(id,payload) VALUES ({},'{}')",
            self.next_id,
            "x".repeat(64)
        ))?;
        self.next_id += 1;
        Ok(())
    }

    /// The accumulated WAL must still end in a commit marker, and a cold replay of the same
    /// index must reach the same marker: the two arms must not disagree on durability.
    fn verify(&self) -> Result<crate::wal::WalCommit> {
        let namespace = Namespace::new("scaling", "sql")?;
        let index = self
            .session
            .get(&namespace.key("wal/index")?)?
            .context("missing WAL index")?
            .bytes;
        let load = |key: &str| -> Result<Vec<u8>> {
            Ok(self
                .session
                .get(key)?
                .context("missing committed chunk")?
                .bytes)
        };
        let commit = crate::recovery::validate_index_commit(&index, &namespace, load)?
            .context("index lost its commit marker")?;
        assert!(
            commit.position.frame_count > 0,
            "commit marker without frames"
        );
        let cold = crate::recovery::validate_index_state(
            &index,
            &namespace,
            |hash| load(&namespace.key(&format!("wal/index-segments/{hash}"))?),
            load,
        )?;
        assert_eq!(
            cold.commit(),
            Some(commit),
            "cold replay disagrees with the validated index"
        );
        Ok(commit)
    }
}

struct CommitSample {
    wall_us: u128,
    scan_us: u128,
    scanned_bytes: u64,
    copied_bytes: u64,
    scan_calls: u64,
    /// T0 group A: the same commits read through the opt-in workload counters, which is the
    /// only view that also carries the WAL bytes and the commit-position resolution.
    work: crate::work_metrics::WalWorkload,
    gets: usize,
    puts: usize,
    groups: usize,
    page_gets: usize,
}

/// Workload-counter deltas for one sample. Both sides are `None` while the switch is off, so
/// an unmeasured run prints zeros instead of a wrong number.
fn work_delta(
    before: Option<crate::work_metrics::WorkMetricsSnapshot>,
    after: Option<crate::work_metrics::WorkMetricsSnapshot>,
) -> crate::work_metrics::WalWorkload {
    let (before, after) = (before.unwrap_or_default(), after.unwrap_or_default());
    let (before, after) = (before.wal_workload, after.wal_workload);
    let counters = |before: &std::collections::BTreeMap<&'static str, u64>,
                    after: &std::collections::BTreeMap<&'static str, u64>| {
        after
            .iter()
            .map(|(name, value)| (*name, value - before.get(name).copied().unwrap_or(0)))
            .collect()
    };
    crate::work_metrics::WalWorkload {
        append_calls: after.append_calls - before.append_calls,
        native_bytes: after.native_bytes - before.native_bytes,
        delta_bytes: after.delta_bytes - before.delta_bytes,
        position_frames: after.position_frames - before.position_frames,
        scan_calls: after.scan_calls - before.scan_calls,
        scanned_bytes: after.scanned_bytes - before.scanned_bytes,
        scanned_frames: after.scanned_frames - before.scanned_frames,
        copied_bytes: after.copied_bytes - before.copied_bytes,
        scan_ns: after.scan_ns - before.scan_ns,
        scan_resets: counters(&before.scan_resets, &after.scan_resets),
        scan_stops: counters(&before.scan_stops, &after.scan_stops),
        append_position: counters(&before.append_position, &after.append_position),
    }
}

fn commit_sample(arm: &mut CommitArm, commits: usize, incremental: bool) -> Result<CommitSample> {
    // The control arm lives on the arm's own WAL image: two arms — or two tests running in
    // parallel — hold separate images, so neither can switch the other's scan. It covers the
    // Worker `sync_wal` site; the Authority-side scan of a validated append runs on the
    // Authority's own image, which this hook does not reach.
    if !incremental {
        arm.database.io().force_whole_image_scan();
    }
    arm.store.reset();
    crate::recovery::reset_recovery_observations();
    let before = crate::recovery::recovery_observations();
    let work_before = crate::work_metrics::snapshot();
    let started = Instant::now();
    for _ in 0..commits {
        arm.commit_one()?;
    }
    let wall_us = started.elapsed().as_micros();
    let counters = arm.store.snapshot();
    let observed = crate::recovery::recovery_observations();
    let work = work_delta(work_before, crate::work_metrics::snapshot());
    Ok(CommitSample {
        wall_us,
        scan_us: u128::from((observed.scan_nanos - before.scan_nanos) / 1_000),
        scanned_bytes: observed.scanned_bytes - before.scanned_bytes,
        copied_bytes: observed.copied_bytes - before.copied_bytes,
        scan_calls: observed.scan_calls - before.scan_calls,
        work,
        gets: counters.gets,
        puts: counters.puts,
        groups: counters.groups,
        page_gets: counters.page_gets,
    })
}

/// Paired commit benchmark. Both arms run the same statements against their own database and
/// differ only in the scan arm of their own WAL image, so the reported difference is the scan
/// state — at the Worker `sync_wal` site, which is the only site a per-image switch can reach.
/// The wall time is a single-node commit through `RemoteIO` and the Authority — it is not a
/// statement latency, a platform end-to-end figure or a capacity result.
#[test]
#[ignore = "commit 增量扫描对照基准：显式运行时才计时，避免与其它 owner 的测量重叠"]
fn commit_incremental_scan_paired_curve() -> Result<()> {
    let rounds = commit_bench_setting(COMMIT_BENCH_ROUNDS_ENV, COMMIT_BENCH_DEFAULT_ROUNDS, 3);
    let commits = commit_bench_setting(COMMIT_BENCH_COMMITS_ENV, COMMIT_BENCH_DEFAULT_COMMITS, 1);
    let batch = commit_bench_setting(COMMIT_BENCH_BATCH_ENV, COMMIT_BENCH_DEFAULT_BATCH, 1);
    for tier in commit_bench_tiers() {
        let mut arms = [CommitArm::open()?, CommitArm::open()?];
        for arm in &mut arms {
            arm.grow_to(tier, batch)?;
        }
        let mut samples: [Vec<u128>; 2] = [Vec::new(), Vec::new()];
        for round in 1..=rounds {
            // Alternating arm order, so a trend in the machine cannot favour one arm.
            let order = if round % 2 == 1 { [0, 1] } else { [1, 0] };
            for index in order {
                let incremental = index == 0;
                let sample = commit_sample(&mut arms[index], commits, incremental)?;
                samples[index].push(sample.wall_us);
                eprintln!(
                    "COMMIT_BENCH tier_rows={tier} arm={} round={round} commits={commits} \
                     wall_us={} per_commit_us={} scan_us={} scanned_bytes={} copied_bytes={} \
                     scan_calls={} gets={} puts={} groups={} page_gets={} \
                     work_appends={} work_native_bytes={} work_delta_bytes={} \
                     work_scanned_frames={} work_position_reused={} work_position_fallback={} \
                     work_reset_write={} work_reset_header={} work_reset_truncate={} \
                     work_reset_forced={}",
                    if incremental {
                        "incremental"
                    } else {
                        "whole-image"
                    },
                    sample.wall_us,
                    sample.wall_us / commits as u128,
                    sample.scan_us,
                    sample.scanned_bytes,
                    sample.copied_bytes,
                    sample.scan_calls,
                    sample.gets,
                    sample.puts,
                    sample.groups,
                    sample.page_gets,
                    sample.work.append_calls,
                    sample.work.native_bytes,
                    sample.work.delta_bytes,
                    sample.work.scanned_frames,
                    sample.work.append_position["reused"],
                    sample.work.append_position["unscanned"]
                        + sample.work.append_position["unchanged"]
                        + sample.work.append_position["not_covered"],
                    sample.work.scan_resets["write_in_prefix"],
                    sample.work.scan_resets["header_changed"],
                    sample.work.scan_resets["truncate_in_prefix"],
                    sample.work.scan_resets["forced_whole_image"],
                );
            }
        }
        for (index, values) in samples.iter_mut().enumerate() {
            eprintln!(
                "COMMIT_BENCH_SUMMARY tier_rows={tier} arm={} rounds={rounds} median_wall_us={} \
                 median_per_commit_us={}",
                if index == 0 {
                    "incremental"
                } else {
                    "whole-image"
                },
                commit_bench_median(values),
                commit_bench_median(values) / commits as u128,
            );
        }
        for (index, arm) in arms.iter().enumerate() {
            let commit = arm.verify()?;
            eprintln!(
                "COMMIT_BENCH_VERIFY tier_rows={tier} arm={} lsn={} frames={}",
                if index == 0 {
                    "incremental"
                } else {
                    "whole-image"
                },
                commit.lsn,
                commit.position.frame_count,
            );
        }
    }
    Ok(())
}
