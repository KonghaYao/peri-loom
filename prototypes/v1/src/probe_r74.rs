// R74 measurement probe: worker open_session cost split and a direct connect-pool
// ceiling arm. Probe branch only; never merged into main.
//
// Two switches, both read once from the environment:
//   PERI_R74_SEGMENTS=1      collect per-open segment samples and print a summary
//                            every 500 completed opens. Off means no Instant::now
//                            calls on the open path (only one relaxed flag read).
//   PERI_R74_CONNECT_POOL=1  hand Database::session() a pre-connected and
//                            pre-configured native connection from a pool of 1-2
//                            instead of connecting inline. Each connection serves
//                            exactly one session and is then dropped, never reset.
//
// The pool arm is an upper bound for "move connect off the request path", not a
// production design: the refiller holds a strong reference to the database for
// the life of the process, which is acceptable for a measurement container.

use std::cell::Cell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use turso_core::Connection;

use crate::database::RemoteDatabase;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Seg {
    PreOpen,
    SessionEntry,
    OperationGate,
    ValidateAuthority,
    EngineLock,
    Connect,
    ValidateLocal,
    Configure,
    LaneNew,
    Register,
    Attach,
    SessionOpenTotal,
    IsReadonly,
    Urandom,
    EntryNew,
    RegistryInsert,
}

const SEGMENT_NAMES: [&str; 16] = [
    "pre_open",
    "session_entry",
    "operation_gate",
    "validate_authority",
    "engine_lock",
    "connect",
    "validate_local",
    "configure",
    "lane_new",
    "register",
    "attach",
    "session_open_total",
    "is_readonly",
    "urandom",
    "entry_new",
    "registry_insert",
];

const SEGMENT_COUNT: usize = SEGMENT_NAMES.len();
const MILESTONE: u64 = 500;
const MAX_SAMPLES: usize = 20_000;

thread_local! {
    static OPEN_START: Cell<Option<Instant>> = const { Cell::new(None) };
    static LAST: Cell<Option<Instant>> = const { Cell::new(None) };
}

static SEGMENTS: OnceLock<bool> = OnceLock::new();
static POOL: OnceLock<bool> = OnceLock::new();

fn flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn segments_enabled() -> bool {
    *SEGMENTS.get_or_init(|| flag("PERI_R74_SEGMENTS"))
}

fn pool_enabled() -> bool {
    *POOL.get_or_init(|| flag("PERI_R74_CONNECT_POOL"))
}

struct Summary {
    started: u64,
    completed: u64,
    counts: [u64; SEGMENT_COUNT],
    sums_ns: [u64; SEGMENT_COUNT],
    samples: [Vec<u64>; SEGMENT_COUNT],
    open_total: Vec<u64>,
}

impl Summary {
    fn new() -> Self {
        Self {
            started: 0,
            completed: 0,
            counts: [0; SEGMENT_COUNT],
            sums_ns: [0; SEGMENT_COUNT],
            samples: std::array::from_fn(|_| Vec::new()),
            open_total: Vec::new(),
        }
    }
}

static SUMMARY: Mutex<Option<Summary>> = Mutex::new(None);

fn with_summary<R>(body: impl FnOnce(&mut Summary) -> R) -> R {
    let mut guard = SUMMARY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let summary = guard.get_or_insert_with(Summary::new);
    body(summary)
}

fn percentile(sorted: &[u64], numerator: usize, denominator: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * numerator / denominator;
    sorted[index]
}

fn print_summary(summary: &Summary) {
    eprintln!(
        "R74WORKER milestone={} started={} completed={}",
        summary.completed, summary.started, summary.completed
    );
    for (index, name) in SEGMENT_NAMES.iter().enumerate() {
        let mut sorted = summary.samples[index].clone();
        sorted.sort_unstable();
        let mean = if summary.counts[index] > 0 {
            summary.sums_ns[index] as f64 / summary.counts[index] as f64 / 1000.0
        } else {
            0.0
        };
        eprintln!(
            "R74WORKER seg={} n={} mean_us={:.3} p50_us={:.3} p95_us={:.3}",
            name,
            summary.counts[index],
            mean,
            percentile(&sorted, 50, 100) as f64 / 1000.0,
            percentile(&sorted, 95, 100) as f64 / 1000.0,
        );
    }
    let mut sorted = summary.open_total.clone();
    sorted.sort_unstable();
    let mean = if sorted.is_empty() {
        0.0
    } else {
        sorted.iter().sum::<u64>() as f64 / sorted.len() as f64 / 1000.0
    };
    eprintln!(
        "R74WORKER seg=open_total n={} mean_us={:.3} p50_us={:.3} p95_us={:.3}",
        sorted.len(),
        mean,
        percentile(&sorted, 50, 100) as f64 / 1000.0,
        percentile(&sorted, 95, 100) as f64 / 1000.0,
    );
    let (takes, failures) = (
        POOL_TAKES.load(Ordering::Relaxed),
        POOL_FAILURES.load(Ordering::Relaxed),
    );
    eprintln!("R74WORKER pool takes={} failures={}", takes, failures);
}

/// First statement of Sessions::open. A no-op with segments disabled.
pub(crate) fn begin_open() {
    if !segments_enabled() {
        return;
    }
    let now = Instant::now();
    OPEN_START.with(|cell| cell.set(Some(now)));
    LAST.with(|cell| cell.set(Some(now)));
    with_summary(|summary| summary.started += 1);
}

/// Records the interval that just ended. A no-op outside an open.
pub(crate) fn mark(segment: Seg) {
    let Some(previous) = LAST.with(|cell| cell.get()) else {
        return;
    };
    let now = Instant::now();
    LAST.with(|cell| cell.set(Some(now)));
    let elapsed = now
        .checked_duration_since(previous)
        .unwrap_or_default()
        .as_nanos() as u64;
    let index = segment as usize;
    with_summary(|summary| {
        summary.counts[index] += 1;
        summary.sums_ns[index] += elapsed;
        if summary.samples[index].len() < MAX_SAMPLES {
            summary.samples[index].push(elapsed);
        }
    });
}

/// Last statement of a successful Sessions::open.
pub(crate) fn end_open() {
    let Some(started) = OPEN_START.with(|cell| cell.take()) else {
        return;
    };
    LAST.with(|cell| cell.set(None));
    let elapsed = Instant::now()
        .checked_duration_since(started)
        .unwrap_or_default()
        .as_nanos() as u64;
    with_summary(|summary| {
        summary.completed += 1;
        if summary.open_total.len() < MAX_SAMPLES {
            summary.open_total.push(elapsed);
        }
        if summary.completed % MILESTONE == 0 {
            print_summary(summary);
        }
    });
}

// ---------------------------------------------------------------------------
// Connect-pool ceiling arm
// ---------------------------------------------------------------------------

struct ConnectPool {
    connections: Mutex<VecDeque<Arc<Connection>>>,
    room: Condvar,
}

const POOL_SIZE: usize = 2;

impl ConnectPool {
    fn start(database: Arc<RemoteDatabase>, engine: Arc<turso_core::Database>) -> Arc<Self> {
        let pool = Arc::new(Self {
            connections: Mutex::new(VecDeque::new()),
            room: Condvar::new(),
        });
        let worker = Arc::clone(&pool);
        let _ = std::thread::Builder::new()
            .name("r74-connect-pool".to_owned())
            .spawn(move || {
                loop {
                    {
                        let mut queue = worker
                            .connections
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        while queue.len() >= POOL_SIZE {
                            queue = worker
                                .room
                                .wait_timeout(queue, Duration::from_millis(100))
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .0;
                        }
                    }
                    match database.probe_connect_configured(&engine) {
                        Ok(connection) => {
                            let mut queue = worker
                                .connections
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            if queue.len() < POOL_SIZE {
                                queue.push_back(connection);
                                POOL_CREATED.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(1)),
                    }
                }
            });
        pool
    }

    fn take(&self) -> Option<Arc<Connection>> {
        let mut queue = self
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.pop_front()
    }
}

static POOLS: Mutex<Option<HashMap<usize, Arc<ConnectPool>>>> = Mutex::new(None);
static POOL_FAILURES: AtomicU64 = AtomicU64::new(0);
static POOL_TAKES: AtomicU64 = AtomicU64::new(0);
static POOL_MISSES: AtomicU64 = AtomicU64::new(0);
static POOL_CREATED: AtomicU64 = AtomicU64::new(0);

fn note_pool_progress() {
    let takes = POOL_TAKES.load(Ordering::Relaxed);
    let misses = POOL_MISSES.load(Ordering::Relaxed);
    if (takes + misses) % 500 != 0 {
        return;
    }
    eprintln!(
        "R74POOL takes={} misses={} created={} failures={}",
        takes,
        misses,
        POOL_CREATED.load(Ordering::Relaxed),
        POOL_FAILURES.load(Ordering::Relaxed),
    );
}

/// Returns a pre-connected connection when the pool arm is enabled and warm.
/// None means the caller must connect inline; the request never fails or blocks
/// on an empty pool.
pub(crate) fn take_connection(database: &Arc<RemoteDatabase>) -> Option<Arc<Connection>> {
    if !pool_enabled() {
        return None;
    }
    let Some(engine) = database.probe_engine() else {
        POOL_FAILURES.fetch_add(1, Ordering::Relaxed);
        return None;
    };
    let identity = Arc::as_ptr(database) as usize;
    let pool = {
        let mut pools = POOLS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pools
            .get_or_insert_with(HashMap::new)
            .entry(identity)
            .or_insert_with(|| ConnectPool::start(Arc::clone(database), engine))
            .clone()
    };
    match pool.take() {
        Some(connection) => {
            POOL_TAKES.fetch_add(1, Ordering::Relaxed);
            note_pool_progress();
            Some(connection)
        }
        None => {
            POOL_MISSES.fetch_add(1, Ordering::Relaxed);
            note_pool_progress();
            None
        }
    }
}
