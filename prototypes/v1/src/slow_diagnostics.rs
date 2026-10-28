use std::cell::RefCell;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

const THRESHOLD: Duration = Duration::from_millis(50);
const QUEUE_CAPACITY: usize = 1024;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Default)]
struct Counts {
    dropped: AtomicU64,
    write_errors: AtomicU64,
}

struct Runtime {
    origin: Instant,
    sender: SyncSender<Event>,
    counts: Arc<Counts>,
}

fn runtime() -> Option<&'static Runtime> {
    static RUNTIME: OnceLock<Option<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            if std::env::var("PERI_DIAG_SLOW").as_deref() != Ok("1") {
                return None;
            }
            let directory = PathBuf::from(std::env::var_os("PERI_DIAG_DIR")?);
            let (sender, receiver) = sync_channel(QUEUE_CAPACITY);
            let counts = Arc::new(Counts::default());
            let writer_counts = counts.clone();
            let origin = Instant::now();
            std::thread::Builder::new()
                .name("peri-slow-diagnostics".into())
                .spawn(move || sink::consume(directory, receiver, writer_counts))
                .ok()?;
            Some(Runtime {
                origin,
                sender,
                counts,
            })
        })
        .as_ref()
}

#[derive(Clone, Default)]
struct Context {
    request_id: Option<String>,
    native: Option<NativeCounts>,
}

thread_local! {
    static RETRIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(crate) fn enabled() -> bool {
    runtime().is_some()
}

/// Storage diagnostics share the already-sanitized RPC identity, never object keys.
pub(crate) fn request_id() -> Option<String> {
    CONTEXT.with(|slot| slot.borrow().request_id.clone())
}

pub(crate) fn retry() {
    if runtime().is_some() {
        RETRIES.with(|count| count.set(count.get().saturating_add(1)));
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct NativeCounts {
    pub rows: u64,
    pub result_bytes: u64,
    pub vm_steps: u64,
}

thread_local! {
    static CONTEXT: RefCell<Context> = RefCell::new(Context::default());
}

pub(crate) struct RequestGuard(Option<Context>, PhantomData<Rc<()>>);

pub(crate) fn enter(request_id: &str) -> RequestGuard {
    let previous = runtime().map(|_| {
        CONTEXT.with(|slot| {
            slot.replace(Context {
                request_id: safe_id(request_id),
                native: None,
            })
        })
    });
    RequestGuard(previous, PhantomData)
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.0.take() {
            CONTEXT.with(|slot| {
                slot.replace(previous);
            });
        }
    }
}

fn safe_id(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    .then(|| value.to_owned())
}

pub(crate) fn native_counts(rows: usize, bytes: usize) {
    if runtime().is_some() {
        CONTEXT.with(|slot| {
            let mut context = slot.borrow_mut();
            let counts = context.native.get_or_insert_with(NativeCounts::default);
            counts.rows = rows as u64;
            counts.result_bytes = bytes as u64;
        });
    }
}

pub(crate) fn native_steps(steps: u64) {
    if runtime().is_some() {
        CONTEXT.with(|slot| {
            slot.borrow_mut()
                .native
                .get_or_insert_with(NativeCounts::default)
                .vm_steps = steps;
        });
    }
}

#[derive(Serialize)]
pub(crate) struct Event {
    schema: &'static str,
    evidence_class: &'static str,
    pid: u32,
    request_id: Option<String>,
    parent_request_id: Option<String>,
    op_id: Option<String>,
    start_mono_ns: u64,
    end_mono_ns: u64,
    ts_unix_ms: Option<u64>,
    duration_ns: u64,
    result: &'static str,
    retry_count: Option<u64>,
    retry_scope: &'static str,
    #[serde(flatten)]
    detail: Detail,
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum Detail {
    AuthorityRpcLink {
        scope: &'static str,
        command: &'static str,
        phase: &'static str,
        parent_id_status: &'static str,
        child_id_status: &'static str,
        link_available: bool,
        send_invoked: bool,
        response_headers_received: bool,
        capture: &'static str,
        complete: bool,
    },
    AuthorityObjectIo {
        scope: &'static str,
        object_class: &'static str,
        method: &'static str,
        bytes: u64,
        bytes_scope: &'static str,
        writes: usize,
        backend: &'static str,
        cache: &'static str,
        append_id: Option<String>,
    },
    NativeExecute {
        command: &'static str,
        scope: &'static str,
        rows: Option<u64>,
        result_bytes: Option<u64>,
        result_bytes_scope: &'static str,
        vm_steps: Option<u64>,
    },
    WalDurability {
        append_id: Option<String>,
        bytes: u64,
        bytes_scope: &'static str,
        record_index: usize,
        record_count: usize,
        append_duration_ns: u64,
        commit_duration_ns: Option<u64>,
        commit_position_present: bool,
        attempts: Option<u64>,
        unknown: Option<bool>,
    },
}

pub(crate) struct Timer {
    runtime: &'static Runtime,
    started: Instant,
    retries_at_start: u64,
}

impl Timer {
    pub(crate) fn begin() -> Option<Self> {
        Self::start(runtime(), Instant::now)
    }

    fn start(runtime: Option<&'static Runtime>, now: impl FnOnce() -> Instant) -> Option<Self> {
        runtime.map(|runtime| Self {
            runtime,
            started: now(),
            retries_at_start: RETRIES.with(|count| count.get()),
        })
    }

    pub(crate) fn finish(
        self,
        result: &'static str,
        retries: Option<u64>,
        detail: impl FnOnce(u64) -> Detail,
    ) {
        self.finish_at(Instant::now(), result, retries, detail);
    }

    fn finish_at(
        self,
        ended: Instant,
        result: &'static str,
        retries: Option<u64>,
        detail: impl FnOnce(u64) -> Detail,
    ) {
        let elapsed = ended.duration_since(self.started);
        let observed = RETRIES.with(|count| count.get().saturating_sub(self.retries_at_start));
        let retries = if observed > 0 {
            Some(observed)
        } else {
            retries
        };
        if !selected(elapsed, THRESHOLD, result, retries) {
            return;
        }
        let duration_ns = nanos(elapsed);
        let event = Event {
            schema: "peri-slow-v1",
            evidence_class: "non_harness",
            pid: std::process::id(),
            request_id: CONTEXT.with(|slot| slot.borrow().request_id.clone()),
            parent_request_id: None,
            op_id: None,
            start_mono_ns: nanos(self.started.duration_since(self.runtime.origin)),
            end_mono_ns: nanos(ended.duration_since(self.runtime.origin)),
            ts_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|time| time.as_millis().min(u64::MAX as u128) as u64),
            duration_ns,
            result,
            retry_count: retries,
            retry_scope: "observed_same_thread_s3_and_wal_cas",
            detail: detail(duration_ns),
        };
        enqueue(&self.runtime.sender, &self.runtime.counts, event);
    }
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn selected(duration: Duration, threshold: Duration, result: &str, retries: Option<u64>) -> bool {
    duration >= threshold || result != "ok" || retries.is_some_and(|count| count > 0)
}

fn enqueue(sender: &SyncSender<Event>, counts: &Counts, event: Event) {
    if sender.try_send(event).is_err() {
        counts.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn outcome<T>(result: &anyhow::Result<T>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(error) if error.is::<crate::session::UnsettledSqlOutcome>() => "unknown",
        Err(_) => "error",
    }
}

pub(crate) fn native<T>(
    command: &'static str,
    call: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let timer = Timer::begin();
    if timer.is_some() {
        CONTEXT.with(|slot| {
            slot.borrow_mut().native = None;
        });
    }
    let result = call();
    if let Some(timer) = timer {
        timer.finish(outcome(&result), Some(0), |_| {
            let counts = CONTEXT.with(|slot| slot.borrow().native);
            Detail::NativeExecute {
                command,
                scope: "native_call_including_admission_and_settlement",
                rows: counts.map(|value| value.rows),
                result_bytes: counts.map(|value| value.result_bytes),
                result_bytes_scope: "execution_budget_memory_accounting",
                vm_steps: counts.map(|value| value.vm_steps),
            }
        });
    }
    result
}

pub(crate) fn wal_bytes(batch: &crate::wal::WalBatch) -> u64 {
    batch
        .operations
        .iter()
        .map(|operation| match operation {
            crate::wal::WalOperation::Write { bytes, .. } => bytes.len() as u64,
            crate::wal::WalOperation::Truncate { .. } => 0,
        })
        .sum()
}

pub(crate) fn append_id(value: &str) -> Option<String> {
    safe_id(value)
}

pub(crate) fn object_class(key: &str) -> &'static str {
    if key == "HEAD"
        || key.ends_with("/HEAD")
        || key == "authority/root"
        || key.ends_with("/authority/root")
    {
        return "root";
    }
    if key == "wal/index" || key.ends_with("/wal/index") {
        return "wal_index";
    }
    for (prefix, class) in [
        ("wal/chunks/", "wal_chunk"),
        ("wal/index-segments/", "wal_index_segment"),
        ("wal/receipt-pages/", "wal_receipt_page"),
        ("segments/", "segment"),
        ("manifests/", "manifest"),
    ] {
        if key.starts_with(prefix)
            || key
                .split_once(prefix)
                .is_some_and(|(before, _)| before.ends_with('/'))
        {
            return class;
        }
    }
    "other"
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn object_io(
    scope: &'static str,
    method: &'static str,
    class: &'static str,
    backend: &'static str,
    bytes: u64,
    writes: usize,
    append_id: Option<String>,
) -> Detail {
    Detail::AuthorityObjectIo {
        scope,
        object_class: class,
        method,
        bytes,
        bytes_scope: if method == "APPEND_WAL" {
            "wal_write_payload"
        } else {
            "object_payload"
        },
        writes,
        backend,
        cache: "unknown",
        append_id,
    }
}

pub(crate) struct AuthorityOperation {
    timer: Timer,
    method: &'static str,
    class: &'static str,
    bytes: u64,
    writes: usize,
    append_id: Option<String>,
}

pub(crate) fn authority_operation(
    operation: &crate::authority_rpc::protocol::SessionOperation,
) -> Option<AuthorityOperation> {
    use crate::authority_rpc::protocol::SessionOperation;
    let timer = Timer::begin()?;
    let (method, class, bytes, writes, append_id) = match operation {
        SessionOperation::Get { key } => ("GET", object_class(key), 0, 0, None),
        SessionOperation::Put { key, bytes, .. } => {
            ("PUT", object_class(key), bytes.len() as u64, 1, None)
        }
        SessionOperation::PutGroup { writes } => (
            "PUT_GROUP",
            "segment",
            writes.iter().map(|write| write.bytes.len() as u64).sum(),
            writes.len(),
            None,
        ),
        SessionOperation::AppendWal { request } => (
            "APPEND_WAL",
            "wal_chunk",
            wal_bytes(&request.batch),
            1,
            append_id(&request.append_id),
        ),
        SessionOperation::UploadSegments { segments } => (
            "UPLOAD_SEGMENTS",
            "segment",
            segments.iter().map(|segment| segment.len() as u64).sum(),
            segments.len(),
            None,
        ),
        _ => return None,
    };
    Some(AuthorityOperation {
        timer,
        method,
        class,
        bytes,
        writes,
        append_id,
    })
}

impl AuthorityOperation {
    pub(crate) fn finish<T>(self, result: &anyhow::Result<T>, read_bytes: u64) {
        self.timer.finish(outcome(result), None, |_| {
            object_io(
                "logical",
                self.method,
                self.class,
                "wrapped",
                if self.method == "GET" {
                    read_bytes
                } else {
                    self.bytes
                },
                self.writes,
                self.append_id,
            )
        });
    }
}

mod rpc_link;
mod sink;

pub(crate) use rpc_link::{AuthorityRpcLink, RpcPhase};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_timer_never_reads_clock() {
        assert!(Timer::start(None, || panic!("关闭诊断不得读取时钟")).is_none());
    }

    #[test]
    fn injected_clock_preserves_intervals_and_records_fast_errors_and_retries() {
        let (sender, receiver) = sync_channel(4);
        let runtime = Box::leak(Box::new(Runtime {
            origin: Instant::now(),
            sender,
            counts: Arc::new(Counts::default()),
        }));
        let previous = CONTEXT.with(|slot| {
            slot.replace(Context {
                request_id: Some("sp01-measure-r1-1".into()),
                native: None,
            })
        });
        let _guard = RequestGuard(Some(previous), PhantomData);
        let start = runtime.origin + Duration::from_millis(10);
        let timer = Timer::start(Some(runtime), || start).unwrap();
        timer.finish_at(start + THRESHOLD, "ok", Some(0), |_| event().detail);
        let observed = receiver.try_recv().unwrap();
        assert_eq!(observed.start_mono_ns, 10_000_000);
        assert_eq!(observed.end_mono_ns, 60_000_000);
        assert_eq!(observed.duration_ns, 50_000_000);
        assert_eq!(observed.request_id.as_deref(), Some("sp01-measure-r1-1"));
        for result in ["error", "unknown"] {
            Timer::start(Some(runtime), || start)
                .unwrap()
                .finish_at(start, result, None, |_| event().detail);
            assert_eq!(receiver.try_recv().unwrap().result, result);
        }
        let timer = Timer::start(Some(runtime), || start).unwrap();
        RETRIES.with(|count| count.set(count.get() + 1));
        timer.finish_at(start, "ok", Some(0), |_| event().detail);
        assert_eq!(receiver.try_recv().unwrap().retry_count, Some(1));
        Timer::start(Some(runtime), || start)
            .unwrap()
            .finish_at(start, "ok", Some(0), |_| panic!("快成功路径不得构造事件"));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn diagnostic_outcome_does_not_rewrite_unknown_or_returned_values() {
        let error = anyhow::Error::new(crate::session::UnsettledSqlOutcome);
        let result: anyhow::Result<()> = Err(error);
        assert_eq!(outcome(&result), "unknown");
        let returned = native("execute", || result);
        assert!(returned
            .unwrap_err()
            .is::<crate::session::UnsettledSqlOutcome>());
        assert_eq!(native("query", || Ok(42)).unwrap(), 42);
    }

    #[test]
    fn selection_uses_injected_duration_and_threshold() {
        assert!(!selected(
            Duration::from_millis(49),
            THRESHOLD,
            "ok",
            Some(0)
        ));
        assert!(selected(THRESHOLD, THRESHOLD, "ok", None));
        for result in ["error", "unknown"] {
            assert!(selected(Duration::ZERO, THRESHOLD, result, Some(0)));
        }
        assert!(selected(Duration::ZERO, THRESHOLD, "ok", Some(1)));
    }

    #[test]
    fn classification_and_labels_never_return_input() {
        for (key, expected) in [
            ("ns/authority/root", "root"),
            ("HEAD", "root"),
            ("ns/wal/index", "wal_index"),
            ("ns/wal/chunks/hash", "wal_chunk"),
            ("ns/wal/index-segments/hash", "wal_index_segment"),
            ("ns/wal/receipt-pages/hash", "wal_receipt_page"),
            ("ns/segments/hash", "segment"),
            ("ns/manifests/hash", "manifest"),
            ("private-data", "other"),
        ] {
            assert_eq!(object_class(key), expected);
        }
        assert!(safe_id("line\nbreak").is_none());
        assert!(safe_id(&"x".repeat(129)).is_none());
        assert_eq!(
            safe_id("sp01-measure-r1-1").as_deref(),
            Some("sp01-measure-r1-1")
        );
    }

    pub(super) fn event() -> Event {
        Event {
            schema: "peri-slow-v1",
            evidence_class: "non_harness",
            pid: 1,
            request_id: None,
            parent_request_id: None,
            op_id: None,
            start_mono_ns: 1,
            end_mono_ns: 2,
            ts_unix_ms: Some(3),
            duration_ns: 1,
            result: "ok",
            retry_count: None,
            retry_scope: "observed_same_thread_s3_and_wal_cas",
            detail: Detail::NativeExecute {
                command: "query",
                scope: "native_call_including_admission_and_settlement",
                rows: Some(1),
                result_bytes: Some(8),
                result_bytes_scope: "execution_budget_memory_accounting",
                vm_steps: Some(3),
            },
        }
    }

    #[test]
    fn full_and_disconnected_queue_count_drops_without_waiting() {
        let (sender, receiver) = sync_channel(1);
        let counts = Counts::default();
        enqueue(&sender, &counts, event());
        enqueue(&sender, &counts, event());
        assert_eq!(counts.dropped.load(Ordering::Relaxed), 1);
        let value = serde_json::to_value(receiver.try_recv().unwrap()).unwrap();
        assert_eq!(value["event"], "native_execute");
        assert!(value.get("sql").is_none());
        assert!(value.get("payload").is_none());
        drop(receiver);
        enqueue(&sender, &counts, event());
        assert_eq!(counts.dropped.load(Ordering::Relaxed), 2);
    }
}
