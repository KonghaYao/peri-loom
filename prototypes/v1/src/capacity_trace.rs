use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};

use crate::namespace::Namespace;
use crate::runtime::DatabaseId;

pub const CAPACITY: usize = 1024;
const HANDOFF_CAPACITY: usize = 1024;
const MAX_HANDOFF_BYTES: usize = 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 4096;
const MAX_PROCESS_HANDOFF_BYTES: usize = 16 * 1024 * 1024;
const MAX_SOURCES: usize = 256;
const MAX_EXPORT_BYTES: usize = 256 * 1024;
const MAX_PAGE_BYTES: usize = 64 * 1024;
static EVICTED_SOURCES: AtomicU64 = AtomicU64::new(0);
static REGISTRATION_LOST: AtomicU64 = AtomicU64::new(0);
static HANDOFF_BYTES: AtomicUsize = AtomicUsize::new(0);

pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PERI_CAPACITY_TRACE").as_deref() == Ok("1"))
}

struct Source {
    start: Instant,
    wall: u64,
}

fn source() -> &'static Source {
    static SOURCE: OnceLock<Source> = OnceLock::new();
    SOURCE.get_or_init(|| Source {
        start: Instant::now(),
        wall: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u64::MAX as u128) as u64,
    })
}

pub fn micros() -> u64 {
    source().start.elapsed().as_micros().min(u64::MAX as u128) as u64
}

#[derive(Clone)]
pub(crate) struct Context {
    pub database: DatabaseId,
    pub epoch: u64,
    pub request_id: String,
    pub ring: Option<Arc<Trace>>,
    pub control_id: Option<String>,
    pub started: u64,
    pub cleanup: Option<&'static str>,
    pub reserved_lane: Option<&'static str>,
}

thread_local! {
    static CONTEXT: RefCell<Option<Context>> = const { RefCell::new(None) };
}

pub(crate) struct RequestGuard(Option<Context>);

pub(crate) fn enter(
    database: DatabaseId,
    epoch: u64,
    request_id: String,
    cleanup: Option<&'static str>,
    reserved_lane: Option<&'static str>,
) -> RequestGuard {
    RequestGuard(CONTEXT.with(|context| {
        context.replace(Some(Context {
            database,
            epoch,
            request_id,
            ring: None,
            control_id: None,
            started: micros(),
            cleanup,
            reserved_lane,
        }))
    }))
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        CONTEXT.with(|context| {
            context.replace(self.0.take());
        });
    }
}

pub(crate) fn context() -> Option<Context> {
    CONTEXT.with(|context| context.borrow().clone())
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Event {
    pub sequence: u64,
    pub monotonic_micros: u64,
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder_control_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reserved_lane: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_before: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub io_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_sequence: Option<u64>,
}

struct State {
    database: Option<(DatabaseId, u64)>,
    events: VecDeque<Event>,
    backend: crate::metrics::PhysicalBackendMetrics,
    receiver: Receiver<Message>,
    rejected_events: u64,
}

enum Pending {
    Record(Box<(Event, Option<(DatabaseId, u64)>)>),
    Get(bool, Option<u64>),
}

struct Message {
    pending: Pending,
    bytes: usize,
    reservation: Option<Reservation>,
}

struct Reservation {
    counter: &'static AtomicUsize,
    bytes: usize,
}

impl Reservation {
    fn acquire(counter: &'static AtomicUsize, bytes: usize, maximum: usize) -> Option<Self> {
        let previous = counter.fetch_add(bytes, Ordering::SeqCst);
        if bytes > maximum || previous > maximum - bytes {
            counter.fetch_sub(bytes, Ordering::SeqCst);
            None
        } else {
            Some(Self { counter, bytes })
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

fn event_payload_bytes(event: &Event) -> usize {
    [
        &event.control_id,
        &event.holder_control_id,
        &event.request_id,
    ]
    .into_iter()
    .flatten()
    .map(String::capacity)
    .sum()
}

impl Message {
    fn new(pending: Pending) -> Self {
        let mut bytes = std::mem::size_of::<Self>();
        if let Pending::Record(record) = &pending {
            let (event, database) = record.as_ref();
            bytes += std::mem::size_of_val(record.as_ref()) + event_payload_bytes(event);
            if let Some((database, _)) = database {
                bytes += database.namespace.tenant.capacity()
                    + database.namespace.database.capacity()
                    + database.branch.capacity();
            }
        }
        Self {
            pending,
            bytes,
            reservation: None,
        }
    }
}

pub(crate) struct Trace {
    identity: String,
    namespace: Option<Namespace>,
    sequence: AtomicU64,
    lost: AtomicU64,
    evicted: AtomicU64,
    rejected_events: AtomicU64,
    rejected_gets: AtomicU64,
    pending_bytes: AtomicUsize,
    pending_messages: AtomicUsize,
    sender: SyncSender<Message>,
    state: Mutex<State>,
}

fn registry() -> &'static Mutex<VecDeque<Arc<Trace>>> {
    static REGISTRY: OnceLock<Mutex<VecDeque<Arc<Trace>>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

impl Trace {
    #[cfg(test)]
    pub(crate) fn test_events(&self) -> Value {
        self.read(Some((0, CAPACITY)))
    }

    pub(crate) fn identity(&self) -> String {
        self.identity.clone()
    }

    pub(crate) fn new(namespace: Option<Namespace>) -> Option<Arc<Self>> {
        if !enabled() {
            return None;
        }
        Some(Self::active(namespace))
    }

    fn active(namespace: Option<Namespace>) -> Arc<Self> {
        let trace = Self::create(namespace);
        match registry().try_lock() {
            Ok(mut registry) => {
                if registry.len() == MAX_SOURCES {
                    registry.pop_front();
                    EVICTED_SOURCES.fetch_add(1, Ordering::Relaxed);
                }
                registry.push_back(trace.clone());
            }
            Err(_) => {
                trace.lost.fetch_add(1, Ordering::Relaxed);
                REGISTRATION_LOST.fetch_add(1, Ordering::Relaxed);
            }
        }
        trace
    }

    fn create(namespace: Option<Namespace>) -> Arc<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let (sender, receiver) = sync_channel(HANDOFF_CAPACITY);
        Arc::new(Self {
            identity: format!(
                "{}-{}-{}",
                std::process::id(),
                source().wall,
                NEXT.fetch_add(1, Ordering::Relaxed)
            ),
            namespace,
            sequence: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
            rejected_events: AtomicU64::new(0),
            rejected_gets: AtomicU64::new(0),
            pending_bytes: AtomicUsize::new(0),
            pending_messages: AtomicUsize::new(0),
            sender,
            state: Mutex::new(State {
                database: None,
                events: VecDeque::new(),
                backend: crate::metrics::PhysicalBackendMetrics::default(),
                receiver,
                rejected_events: 0,
            }),
        })
    }

    fn control(&self, id: u64) -> String {
        format!("{}:{id}", self.identity)
    }

    pub(crate) fn admission(self: &Arc<Self>, kind: &'static str, id: u64, holder: Option<u64>) {
        let control_id = self.control(id);
        let mut cleanup_start = None;
        let request_id = CONTEXT.with(|context| {
            let mut context = context.borrow_mut();
            context.as_mut().map(|context| {
                if context.ring.is_none() {
                    if let Some(operation) = context.cleanup {
                        cleanup_start = Some(Event {
                            kind: "cleanup_start",
                            operation: Some(operation),
                            monotonic_micros: context.started,
                            control_id: Some(control_id.clone()),
                            request_id: Some(context.request_id.clone()),
                            reserved_lane: context.reserved_lane,
                            ..Event::default()
                        });
                    }
                }
                context.ring = Some(self.clone());
                context.control_id = Some(control_id.clone());
                context.request_id.clone()
            })
        });
        if let Some(event) = cleanup_start {
            self.record(event);
        }
        self.record(Event {
            kind,
            control_id: Some(control_id),
            holder_control_id: holder.map(|id| self.control(id)),
            request_id,
            ..Event::default()
        });
    }

    pub(crate) fn record(&self, mut event: Event) {
        let database = context().map(|context| {
            if event.request_id.is_none() {
                event.request_id = Some(context.request_id);
            }
            (context.database, context.epoch)
        });
        if event.monotonic_micros == 0 {
            event.monotonic_micros = micros();
        }
        self.publish(Message::new(Pending::Record(Box::new((event, database)))));
    }

    fn publish(&self, message: Message) {
        self.publish_before_send(message, || {});
    }

    fn publish_before_send(&self, mut message: Message, before_send: impl FnOnce()) {
        let is_event = matches!(&message.pending, Pending::Record(..));
        let bytes = message.bytes;
        self.pending_messages.fetch_add(1, Ordering::SeqCst);
        let previous = self.pending_bytes.fetch_add(bytes, Ordering::SeqCst);
        if bytes <= MAX_MESSAGE_BYTES && previous <= MAX_HANDOFF_BYTES - bytes {
            message.reservation =
                Reservation::acquire(&HANDOFF_BYTES, bytes, MAX_PROCESS_HANDOFF_BYTES);
        }
        before_send();
        if bytes > MAX_MESSAGE_BYTES
            || previous > MAX_HANDOFF_BYTES - bytes
            || message.reservation.is_none()
            || self.sender.try_send(message).is_err()
        {
            self.pending_bytes.fetch_sub(bytes, Ordering::SeqCst);
            if is_event {
                self.rejected_events.fetch_add(1, Ordering::SeqCst);
            } else {
                self.rejected_gets.fetch_add(1, Ordering::SeqCst);
            }
            self.pending_messages.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn consume(&self, state: &mut State, pending: Pending) {
        match pending {
            Pending::Get(success, bytes) => {
                update_backend(&mut state.backend, Some("get"), success, bytes);
            }
            Pending::Record(record) => {
                let (mut event, database) = *record;
                if self.namespace.is_none() && state.database.is_none() {
                    state.database = database;
                }
                event.sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
                if event.kind == "backend_start" && event.operation == Some("put") {
                    event.bytes_before = Some(state.backend.put_payload_bytes);
                }
                if event.kind == "backend_finish" {
                    update_backend(
                        &mut state.backend,
                        event.operation,
                        event.success == Some(true),
                        event.bytes,
                    );
                    if event.operation == Some("put") && event.success == Some(true) {
                        event.bytes_after = Some(state.backend.put_payload_bytes);
                    }
                }
                if state.events.len() == CAPACITY {
                    state.events.pop_front();
                    self.lost.fetch_add(1, Ordering::Relaxed);
                    self.evicted.fetch_add(1, Ordering::Relaxed);
                }
                state.events.push_back(event);
            }
        }
    }

    fn loss(&self, rejected_events: u64) -> u64 {
        self.lost.load(Ordering::Relaxed)
            + rejected_events
            + self.rejected_gets.load(Ordering::SeqCst)
    }

    pub(crate) fn snapshot(&self) -> Value {
        self.read(None)
    }

    fn read(&self, cursor: Option<(u64, usize)>) -> Value {
        self.read_before_lock(cursor, || {})
    }

    fn read_before_lock(&self, cursor: Option<(u64, usize)>, before_lock: impl FnOnce()) -> Value {
        before_lock();
        match self.state.try_lock() {
            Ok(mut state) => {
                for _ in 0..HANDOFF_CAPACITY {
                    let Ok(message) = state.receiver.try_recv() else {
                        break;
                    };
                    self.pending_bytes
                        .fetch_sub(message.bytes, Ordering::SeqCst);
                    self.consume(&mut state, message.pending);
                    self.pending_messages.fetch_sub(1, Ordering::SeqCst);
                }
                let pending_messages = self.pending_messages.load(Ordering::SeqCst);
                let rejected_events = self.rejected_events.load(Ordering::SeqCst);
                self.sequence
                    .fetch_add(rejected_events - state.rejected_events, Ordering::Relaxed);
                state.rejected_events = rejected_events;
                let sequence = self.sequence.load(Ordering::Relaxed);
                let lost = self.loss(rejected_events);
                let evicted = self.evicted.load(Ordering::Relaxed);
                let oldest = state.events.front().map(|event| event.sequence);
                let tail_gap = sequence > state.events.back().map_or(0, |event| event.sequence);
                let events = cursor.map(|(after, limit)| {
                    state
                        .events
                        .iter()
                        .filter(|event| event.sequence > after)
                        .take(limit)
                        .cloned()
                        .collect::<Vec<_>>()
                });
                let prefix_gap = cursor.is_some_and(|(after, _)| {
                    oldest.is_some_and(|oldest| after.saturating_add(1) < oldest)
                        || after > sequence
                });
                let database = state.database.clone();
                let backend = state.backend.clone();
                drop(state);
                json!({
                    "streamId": self.identity,
                    "queueInstance": self.identity, "namespace": self.namespace,
                "databaseId": database.as_ref().map(|(database, _)| database),
                "epoch": database.as_ref().map(|(_, epoch)| epoch),
                    "scope": if self.namespace.is_some() { "physical_inner" } else { "actualWriterAdmission" },
                    "lost": lost, "evicted": evicted, "dropped": lost.saturating_sub(evicted),
                    "gap": lost != 0 || prefix_gap || tail_gap, "prefixGap": prefix_gap,
                    "sequence": sequence, "oldestSequence": oldest,
                    "pendingMessages": pending_messages,
                "status": if prefix_gap || tail_gap { "sequence_gap" } else if lost != 0 { "gap" } else if self.namespace.is_none() && database.is_none() { "unmapped" } else { "available" },
                    "events": events,
                "backendMetrics": if self.namespace.is_some() { Some(&backend) } else { None },
                })
            }
            Err(_) => {
                let lost = self.loss(self.rejected_events.load(Ordering::SeqCst));
                json!({"streamId": self.identity, "queueInstance": self.identity, "status": "lock_unavailable", "lost": lost, "gap": true, "events": null})
            }
        }
    }

    pub(crate) fn backend_get(&self, success: bool, bytes: Option<u64>) {
        self.publish(Message::new(Pending::Get(success, bytes)));
    }
}

fn update_backend(
    metrics: &mut crate::metrics::PhysicalBackendMetrics,
    operation: Option<&str>,
    success: bool,
    bytes: Option<u64>,
) {
    match (operation, success) {
        (Some("get"), true) => {
            metrics.get_successes = metrics.get_successes.saturating_add(1);
            metrics.get_payload_bytes =
                metrics.get_payload_bytes.saturating_add(bytes.unwrap_or(0));
        }
        (Some("put"), true) => {
            metrics.put_successes = metrics.put_successes.saturating_add(1);
            metrics.put_payload_bytes =
                metrics.put_payload_bytes.saturating_add(bytes.unwrap_or(0));
        }
        (Some("get"), false) => metrics.get_errors = metrics.get_errors.saturating_add(1),
        (Some("put"), false) => metrics.put_errors = metrics.put_errors.saturating_add(1),
        _ => {}
    }
}

#[cfg(test)]
pub(crate) fn test_trace() -> Arc<Trace> {
    Trace::create(None)
}

#[cfg(test)]
pub(crate) fn test_namespace_trace(namespace: Namespace) -> Arc<Trace> {
    Trace::create(Some(namespace))
}

pub fn snapshot() -> Value {
    if !enabled() {
        return json!({"schemaVersion": 1, "enabled": false, "status": "disabled"});
    }
    let traces = match registry().try_lock() {
        Ok(registry) => registry.iter().cloned().collect::<Vec<_>>(),
        Err(_) => {
            return json!({"schemaVersion": 1, "enabled": true, "status": "lock_unavailable", "gap": true})
        }
    };
    let mut databases = Vec::new();
    let mut namespaces = Vec::new();
    for trace in traces {
        let snapshot = trace.snapshot();
        if trace.namespace.is_some() {
            namespaces.push(snapshot);
        } else {
            databases.push(snapshot);
        }
    }
    let lost: u64 = databases
        .iter()
        .chain(&namespaces)
        .filter_map(|trace| trace["lost"].as_u64())
        .sum();
    let evicted_sources = EVICTED_SOURCES.load(Ordering::Relaxed);
    let registration_lost = REGISTRATION_LOST.load(Ordering::Relaxed);
    let gap = registration_lost != 0
        || evicted_sources != 0
        || databases
            .iter()
            .chain(&namespaces)
            .any(|trace| trace["status"] != "available");
    let snapshot = json!({"schemaVersion": 1, "enabled": true, "capacity": CAPACITY,
        "source": source_snapshot(),
        "status": if gap { "incomplete" } else if databases.is_empty() && namespaces.is_empty() { "missing" } else { "available" },
        "gap": gap, "lost": lost, "evictedSources": evicted_sources,
        "registrationLost": registration_lost, "maxSources": MAX_SOURCES,
        "databases": databases, "namespaces": namespaces});
    bound_export(snapshot)
}

fn source_snapshot() -> Value {
    let now = micros();
    json!({"pid": std::process::id(), "startUnixMicros": source().wall,
        "generation": format!("{}-{}", std::process::id(), source().wall),
        "wallUnixMicros": source().wall.saturating_add(now), "monotonicMicros": now})
}

pub fn page(stream_id: &str, after_sequence: u64, max_events: usize) -> anyhow::Result<Value> {
    anyhow::ensure!(
        (1..=128).contains(&max_events),
        "invalid capacity trace page size"
    );
    anyhow::ensure!(
        !stream_id.is_empty()
            && stream_id.len() <= 128
            && stream_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid capacity trace stream"
    );
    if !enabled() {
        return Ok(json!({"schemaVersion": 1, "enabled": false, "status": "disabled"}));
    }
    let trace = match registry().try_lock() {
        Ok(registry) => registry
            .iter()
            .find(|trace| trace.identity == stream_id)
            .cloned(),
        Err(_) => {
            return Ok(
                json!({"schemaVersion": 1, "enabled": true, "status": "lock_unavailable", "gap": true, "source": source_snapshot()}),
            )
        }
    };
    let Some(trace) = trace else {
        return Ok(
            json!({"schemaVersion": 1, "enabled": true, "status": "stream_unavailable", "gap": true, "source": source_snapshot()}),
        );
    };
    let stream = trace.read(Some((after_sequence, max_events)));
    let next_sequence = stream["events"]
        .as_array()
        .and_then(|events| events.last())
        .and_then(|event| event["sequence"].as_u64())
        .unwrap_or(after_sequence);
    let has_more = stream["sequence"]
        .as_u64()
        .is_some_and(|sequence| sequence > next_sequence);
    let page = json!({"schemaVersion": 1, "enabled": true, "source": source_snapshot(),
        "status": stream["status"], "stream": stream, "afterSequence": after_sequence,
        "nextSequence": next_sequence, "hasMore": has_more});
    Ok(bound_page(page))
}

fn bound_page(mut page: Value) -> Value {
    while !serde_json::to_vec(&page).is_ok_and(|bytes| bytes.len() <= MAX_PAGE_BYTES) {
        let popped = page["stream"]["events"]
            .as_array_mut()
            .and_then(Vec::pop)
            .is_some();
        if !popped {
            return json!({"schemaVersion": 1, "enabled": true, "status": "export_truncated", "exportTruncated": true, "gap": true, "source": page["source"]});
        }
        let next = page["stream"]["events"]
            .as_array()
            .and_then(|events| events.last())
            .map(|event| event["sequence"].clone())
            .unwrap_or_else(|| page["afterSequence"].clone());
        page["nextSequence"] = next;
        page["hasMore"] = json!(true);
    }
    page
}

pub(crate) fn bound_export(mut snapshot: Value) -> Value {
    if serde_json::to_vec(&snapshot).is_ok_and(|bytes| bytes.len() <= MAX_EXPORT_BYTES) {
        return snapshot;
    }
    snapshot["exportTruncated"] = json!(true);
    snapshot["status"] = json!("export_truncated");
    snapshot["gap"] = json!(true);
    for field in ["databases", "namespaces"] {
        if let Some(rings) = snapshot[field].as_array_mut() {
            for ring in rings {
                ring["events"] = Value::Null;
                ring["exportTruncated"] = json!(true);
            }
        }
    }
    if serde_json::to_vec(&snapshot).is_ok_and(|bytes| bytes.len() <= MAX_EXPORT_BYTES) {
        snapshot
    } else {
        json!({"schemaVersion": 1, "enabled": true, "status": "export_truncated", "exportTruncated": true, "gap": true, "source": snapshot["source"]})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eviction_between_reader_start_and_lock_cannot_report_available() -> anyhow::Result<()> {
        let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
        for _ in 0..CAPACITY {
            trace.record(Event {
                kind: "test",
                ..Event::default()
            });
        }
        assert_eq!(trace.snapshot()["status"], "available");
        let snapshot = std::thread::scope(|scope| {
            let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
            let (resume_sender, resume_receiver) = std::sync::mpsc::channel();
            let trace_reader = trace.clone();
            let reader = scope.spawn(move || {
                trace_reader.read_before_lock(None, || {
                    ready_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                })
            });
            ready_receiver.recv().unwrap();
            trace.record(Event {
                kind: "test",
                ..Event::default()
            });
            resume_sender.send(()).unwrap();
            reader.join().unwrap()
        });
        assert_eq!(snapshot["sequence"], CAPACITY as u64 + 1);
        assert_eq!(snapshot["oldestSequence"], 2);
        assert_eq!(snapshot["lost"], 1);
        assert_eq!(snapshot["evicted"], 1);
        assert_eq!(snapshot["dropped"], 0);
        assert_eq!(snapshot["gap"], true);
        assert_eq!(snapshot["status"], "gap");
        Ok(())
    }

    #[test]
    fn invalid_pages_are_rejected_before_trace_registry_access() {
        assert!(page("stream", 0, 0).is_err());
        assert!(page("stream", 0, 129).is_err());
        assert!(page("session:bearer", 0, 1).is_err());
        assert!(page(&"x".repeat(129), 0, 1).is_err());
    }

    #[test]
    fn dropped_sequence_before_loss_counter_publication_is_explicit_gap() -> anyhow::Result<()> {
        let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
        trace.sequence.fetch_add(1, Ordering::Relaxed);
        let snapshot = trace.snapshot();
        assert_eq!(snapshot["lost"], 0);
        assert_eq!(snapshot["gap"], true);
        assert_eq!(snapshot["status"], "sequence_gap");
        Ok(())
    }

    #[test]
    fn directory_does_not_export_events_and_pages_are_bounded() {
        let trace = test_trace();
        for _ in 0..200 {
            trace.record(Event {
                kind: "test",
                request_id: Some("x".repeat(1000)),
                ..Event::default()
            });
        }
        assert!(trace.snapshot()["events"].is_null());
        let stream = trace.read(Some((0, 128)));
        assert_eq!(stream["events"].as_array().unwrap().len(), 128);
        let page = bound_page(
            json!({"schemaVersion": 1, "stream": stream, "afterSequence": 0,
            "nextSequence": 128, "hasMore": true, "source": source_snapshot()}),
        );
        assert!(serde_json::to_vec(&page).unwrap().len() <= MAX_PAGE_BYTES);
        assert!(page["nextSequence"].as_u64().unwrap() < 128);
        assert_eq!(page["hasMore"], true);
    }

    #[test]
    fn page_cursor_gap_is_distinct_from_safe_eviction() {
        let trace = test_trace();
        for _ in 0..CAPACITY + 3 {
            trace.record(Event {
                kind: "test",
                ..Event::default()
            });
            if trace.pending_messages.load(Ordering::SeqCst) == HANDOFF_CAPACITY {
                trace.snapshot();
            }
        }
        assert_eq!(trace.read(Some((0, 128)))["prefixGap"], true);
        let resumed = trace.read(Some((3, 128)));
        assert_eq!(resumed["prefixGap"], false);
        assert_eq!(resumed["evicted"], 3);
        assert_eq!(resumed["dropped"], 0);
        assert_eq!(resumed["events"][0]["sequence"], 4);
    }

    #[test]
    fn safe_eviction_page_status_is_relative_to_captured_cursor() -> anyhow::Result<()> {
        let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
        for _ in 0..CAPACITY {
            trace.record(Event {
                kind: "test",
                ..Event::default()
            });
        }
        assert_eq!(trace.snapshot()["sequence"], CAPACITY);
        for _ in 0..3 {
            trace.record(Event {
                kind: "test",
                ..Event::default()
            });
        }
        for (after, expected_status, expected_prefix_gap) in [
            (2, "sequence_gap", true),
            (3, "gap", false),
            (1000, "gap", false),
            (CAPACITY as u64 + 3, "gap", false),
            (CAPACITY as u64 + 4, "sequence_gap", true),
        ] {
            let page = trace.read(Some((after, 128)));
            assert_eq!(page["status"], expected_status);
            assert_eq!(page["prefixGap"], expected_prefix_gap);
            assert_eq!(page["oldestSequence"], 4);
            assert_eq!(page["sequence"], CAPACITY as u64 + 3);
            assert_eq!(page["lost"], 3);
            assert_eq!(page["evicted"], 3);
            assert_eq!(page["dropped"], 0);
            assert_eq!(page["gap"], true);
            assert_eq!(page["pendingMessages"], 0);
            let events = page["events"].as_array().unwrap();
            assert!(events.len() <= 128);
            if !expected_prefix_gap {
                let expected =
                    ((after + 1)..=(CAPACITY as u64 + 3).min(after + 128)).collect::<Vec<_>>();
                assert_eq!(
                    events
                        .iter()
                        .map(|event| event["sequence"].as_u64().unwrap())
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            assert!(serde_json::to_vec(&page)?.len() <= MAX_PAGE_BYTES);
        }
        Ok(())
    }

    #[test]
    fn page_eviction_between_reader_start_and_lock_uses_one_cursor_cut() -> anyhow::Result<()> {
        for (after, expected_status, expected_prefix_gap) in [
            (0, "sequence_gap", true),
            (1, "gap", false),
            (CAPACITY as u64, "gap", false),
        ] {
            let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
            for _ in 0..CAPACITY {
                trace.record(Event {
                    kind: "test",
                    ..Event::default()
                });
            }
            assert_eq!(trace.snapshot()["status"], "available");
            let page = std::thread::scope(|scope| {
                let reader_trace = trace.clone();
                let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
                let (resume_sender, resume_receiver) = std::sync::mpsc::channel();
                let reader = scope.spawn(move || {
                    reader_trace.read_before_lock(Some((after, 128)), || {
                        ready_sender.send(()).unwrap();
                        resume_receiver.recv().unwrap();
                    })
                });
                ready_receiver.recv().unwrap();
                trace.record(Event {
                    kind: "test",
                    ..Event::default()
                });
                resume_sender.send(()).unwrap();
                reader.join().unwrap()
            });
            assert_eq!(page["status"], expected_status);
            assert_eq!(page["prefixGap"], expected_prefix_gap);
            assert_eq!(page["oldestSequence"], 2);
            assert_eq!(page["sequence"], CAPACITY as u64 + 1);
            assert_eq!(page["lost"], 1);
            assert_eq!(page["evicted"], 1);
            assert_eq!(page["dropped"], 0);
            assert_eq!(page["pendingMessages"], 0);
            let events = page["events"].as_array().unwrap();
            assert_eq!(events[0]["sequence"], (after + 1).max(2));
            if !expected_prefix_gap {
                assert_eq!(
                    events.last().unwrap()["sequence"],
                    (CAPACITY as u64 + 1).min(after + 128)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn directory_export_never_exceeds_byte_budget() {
        let snapshot = json!({"source": source_snapshot(), "databases": [{"events": ["x".repeat(MAX_EXPORT_BYTES * 2)]}], "namespaces": []});
        let snapshot = bound_export(snapshot);
        assert!(serde_json::to_vec(&snapshot).unwrap().len() <= MAX_EXPORT_BYTES);
        assert_eq!(snapshot["exportTruncated"], true);
        assert!(snapshot["databases"][0]["events"].is_null());
    }

    #[test]
    fn bounded_ring_reports_overflow_and_sequence_gap() {
        let trace = test_trace();
        for _ in 0..CAPACITY + 3 {
            trace.record(Event {
                kind: "test",
                ..Event::default()
            });
            if trace.pending_messages.load(Ordering::SeqCst) == HANDOFF_CAPACITY {
                trace.snapshot();
            }
        }
        let snapshot = trace.read(Some((0, CAPACITY)));
        assert_eq!(snapshot["events"].as_array().unwrap().len(), CAPACITY);
        assert_eq!(snapshot["lost"], 3);
        assert_eq!(snapshot["events"][0]["sequence"], 4);
        assert_eq!(snapshot["status"], "sequence_gap");
    }

    #[test]
    fn contended_trace_never_blocks_or_reports_zero_loss() {
        let trace = test_trace();
        let guard = trace.state.lock().unwrap();
        for _ in 0..HANDOFF_CAPACITY {
            trace.record(Event {
                kind: "test",
                ..Event::default()
            });
        }
        trace.record(Event {
            kind: "test",
            ..Event::default()
        });
        assert_eq!(trace.snapshot()["status"], "lock_unavailable");
        drop(guard);
        assert_eq!(trace.snapshot()["lost"], 1);
        let snapshot = trace.test_events();
        assert_eq!(snapshot["dropped"], 1);
        assert_eq!(snapshot["evicted"], 0);
        assert_eq!(snapshot["sequence"], HANDOFF_CAPACITY as u64 + 1);
        assert_eq!(snapshot["status"], "sequence_gap");
        assert_eq!(snapshot["pendingMessages"], 0);
    }

    #[test]
    fn reader_state_lock_does_not_drop_or_block_producers() {
        let trace = test_trace();
        let guard = trace.state.lock().unwrap();
        std::thread::scope(|scope| {
            let producer = trace.clone();
            let (done_sender, done_receiver) = std::sync::mpsc::channel();
            scope.spawn(move || {
                producer.record(Event {
                    kind: "test",
                    ..Event::default()
                });
                producer.backend_get(true, Some(3));
                done_sender.send(()).unwrap();
            });
            let completed = done_receiver.recv_timeout(std::time::Duration::from_secs(1));
            assert_eq!(trace.snapshot()["status"], "lock_unavailable");
            drop(guard);
            completed.unwrap();
        });
        let snapshot = trace.test_events();
        assert_eq!(snapshot["lost"], 0);
        assert_eq!(snapshot["dropped"], 0);
        assert_eq!(snapshot["sequence"], 1);
        assert_eq!(snapshot["events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn pending_get_and_put_metrics_have_complete_payload_windows() -> anyhow::Result<()> {
        let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
        let guard = trace.state.lock().unwrap();
        trace.backend_get(true, Some(3));
        trace.backend_get(false, Some(99));
        trace.backend_get(true, Some(5));
        trace.record(Event {
            kind: "backend_start",
            operation: Some("put"),
            ..Event::default()
        });
        trace.record(Event {
            kind: "backend_finish",
            operation: Some("put"),
            success: Some(true),
            bytes: Some(7),
            ..Event::default()
        });
        trace.record(Event {
            kind: "backend_finish",
            operation: Some("put"),
            success: Some(false),
            bytes: Some(99),
            ..Event::default()
        });
        drop(guard);
        let snapshot = trace.test_events();
        assert_eq!(snapshot["lost"], 0);
        assert_eq!(snapshot["sequence"], 3);
        assert_eq!(snapshot["backendMetrics"]["get_successes"], 2);
        assert_eq!(snapshot["backendMetrics"]["get_errors"], 1);
        assert_eq!(snapshot["backendMetrics"]["get_payload_bytes"], 8);
        assert_eq!(snapshot["backendMetrics"]["put_payload_bytes"], 7);
        assert_eq!(snapshot["events"][0]["bytesBefore"], 0);
        assert_eq!(snapshot["events"][1]["bytesAfter"], 7);
        assert!(snapshot["events"][2]["bytesAfter"].is_null());
        Ok(())
    }

    #[test]
    fn unpublished_pending_has_no_sequence_hole_or_false_caught_up_prefix() -> anyhow::Result<()> {
        let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
        std::thread::scope(|scope| {
            let producer = trace.clone();
            let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
            let (resume_sender, resume_receiver) = std::sync::mpsc::channel();
            scope.spawn(move || {
                producer.publish_before_send(
                    Message::new(Pending::Record(Box::new((
                        Event {
                            kind: "test",
                            ..Event::default()
                        },
                        None,
                    )))),
                    || {
                        ready_sender.send(()).unwrap();
                        resume_receiver.recv().unwrap();
                    },
                );
            });
            ready_receiver.recv().unwrap();
            let pending = trace.test_events();
            resume_sender.send(()).unwrap();
            assert_eq!(pending["sequence"], 0);
            assert_eq!(pending["lost"], 0);
            assert_eq!(pending["gap"], false);
            assert_eq!(pending["pendingMessages"], 1);
        });
        let applied = trace.test_events();
        assert_eq!(applied["sequence"], 1);
        assert_eq!(applied["pendingMessages"], 0);
        assert_eq!(applied["events"][0]["sequence"], 1);
        assert_eq!(trace.pending_bytes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn full_get_and_disconnected_event_handoff_keep_real_loss_sticky() -> anyhow::Result<()> {
        let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
        for _ in 0..HANDOFF_CAPACITY {
            trace.backend_get(true, Some(3));
        }
        trace.backend_get(true, Some(99));
        let dropped = trace.test_events();
        assert_eq!(dropped["sequence"], 0);
        assert_eq!(dropped["backendMetrics"]["get_successes"], HANDOFF_CAPACITY);
        assert_eq!(
            dropped["backendMetrics"]["get_payload_bytes"],
            HANDOFF_CAPACITY * 3
        );
        assert_eq!(dropped["lost"], 1);
        assert_eq!(dropped["dropped"], 1);
        assert_eq!(dropped["evicted"], 0);
        assert_eq!(dropped["status"], "gap");
        trace.backend_get(false, None);
        assert_eq!(trace.snapshot()["dropped"], 1);
        let mut disconnected = test_trace();
        let (sender, receiver) = sync_channel(HANDOFF_CAPACITY);
        drop(receiver);
        Arc::get_mut(&mut disconnected).unwrap().sender = sender;
        disconnected.record(Event {
            kind: "test",
            ..Event::default()
        });
        let fault = disconnected.test_events();
        assert_eq!(fault["lost"], 1);
        assert_eq!(fault["dropped"], 1);
        assert_eq!(fault["sequence"], 1);
        assert_eq!(fault["status"], "sequence_gap");
        assert_eq!(fault["pendingMessages"], 0);
        assert_eq!(disconnected.pending_bytes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn unpublished_get_never_exports_half_count_payload_or_false_final_prefix() -> anyhow::Result<()>
    {
        let trace = test_namespace_trace(Namespace::new("tenant", "db")?);
        std::thread::scope(|scope| {
            let producer = trace.clone();
            let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
            let (resume_sender, resume_receiver) = std::sync::mpsc::channel();
            scope.spawn(move || {
                producer.publish_before_send(Message::new(Pending::Get(true, Some(7))), || {
                    ready_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                });
            });
            ready_receiver.recv().unwrap();
            for _ in 0..HANDOFF_CAPACITY {
                trace.record(Event {
                    kind: "test",
                    ..Event::default()
                });
            }
            let pending = trace.test_events();
            resume_sender.send(()).unwrap();
            assert_eq!(pending["sequence"], HANDOFF_CAPACITY);
            assert_eq!(pending["lost"], 0);
            assert_eq!(pending["pendingMessages"], 1);
            assert_eq!(pending["backendMetrics"]["get_successes"], 0);
            assert_eq!(pending["backendMetrics"]["get_payload_bytes"], 0);
        });
        let applied = trace.test_events();
        assert_eq!(applied["pendingMessages"], 0);
        assert_eq!(applied["sequence"], HANDOFF_CAPACITY);
        assert_eq!(applied["backendMetrics"]["get_successes"], 1);
        assert_eq!(applied["backendMetrics"]["get_payload_bytes"], 7);
        Ok(())
    }

    #[test]
    fn handoff_byte_and_payload_limits_fail_closed_without_safe_eviction() {
        let trace = test_trace();
        let event = Event {
            kind: "test",
            request_id: Some("x".repeat(MAX_MESSAGE_BYTES)),
            ..Event::default()
        };
        trace.record(event);
        let oversized = trace.test_events();
        assert_eq!(oversized["dropped"], 1);
        assert_eq!(oversized["evicted"], 0);
        assert_eq!(oversized["events"].as_array().unwrap().len(), 0);
        assert_eq!(trace.pending_bytes.load(Ordering::SeqCst), 0);
        let trace = test_trace();
        let event = Event {
            kind: "test",
            request_id: Some("x".repeat(3000)),
            ..Event::default()
        };
        let bytes = Message::new(Pending::Record(Box::new((event.clone(), None)))).bytes;
        let accepted = MAX_HANDOFF_BYTES / bytes;
        assert!(accepted < HANDOFF_CAPACITY);
        for _ in 0..accepted + 1 {
            trace.record(event.clone());
        }
        assert!(trace.pending_bytes.load(Ordering::SeqCst) <= MAX_HANDOFF_BYTES);
        let limited = trace.test_events();
        assert_eq!(limited["events"].as_array().unwrap().len(), accepted);
        assert_eq!(limited["dropped"], 1);
        assert_eq!(limited["evicted"], 0);
        assert_eq!(trace.pending_messages.load(Ordering::SeqCst), 0);
        assert_eq!(trace.pending_bytes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn global_reservations_release_on_consume_full_disconnect_and_channel_drop() {
        static BUDGET: AtomicUsize = AtomicUsize::new(0);
        let make_message = || {
            let mut message = Message::new(Pending::Get(true, Some(3)));
            message.reservation =
                Reservation::acquire(&BUDGET, message.bytes, MAX_PROCESS_HANDOFF_BYTES);
            assert!(message.reservation.is_some());
            message
        };
        assert!(Reservation::acquire(
            &BUDGET,
            MAX_PROCESS_HANDOFF_BYTES + 1,
            MAX_PROCESS_HANDOFF_BYTES
        )
        .is_none());
        assert_eq!(BUDGET.load(Ordering::SeqCst), 0);
        let limit = Reservation::acquire(
            &BUDGET,
            MAX_PROCESS_HANDOFF_BYTES,
            MAX_PROCESS_HANDOFF_BYTES,
        )
        .unwrap();
        assert!(Reservation::acquire(&BUDGET, 1, MAX_PROCESS_HANDOFF_BYTES).is_none());
        drop(limit);
        assert_eq!(BUDGET.load(Ordering::SeqCst), 0);
        let (sender, receiver) = sync_channel(1);
        sender.try_send(make_message()).ok().unwrap();
        let rejected = sender.try_send(make_message()).err().unwrap();
        drop(rejected);
        assert_eq!(
            BUDGET.load(Ordering::SeqCst),
            std::mem::size_of::<Message>()
        );
        drop(receiver.try_recv().unwrap());
        assert_eq!(BUDGET.load(Ordering::SeqCst), 0);
        sender.try_send(make_message()).ok().unwrap();
        drop(receiver);
        drop(sender.try_send(make_message()).err().unwrap());
        drop(sender);
        assert_eq!(BUDGET.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn resident_budget_sizes_include_event_context_and_string_capacity() {
        let event = Event {
            kind: "test",
            request_id: Some(String::with_capacity(2000)),
            ..Event::default()
        };
        let database = DatabaseId::new(Namespace::new("tenant", "db").unwrap(), "main").unwrap();
        let context_bytes = database.namespace.tenant.capacity()
            + database.namespace.database.capacity()
            + database.branch.capacity();
        let message = Message::new(Pending::Record(Box::new((event, Some((database, 7))))));
        let record_bytes = std::mem::size_of::<(Event, Option<(DatabaseId, u64)>)>();
        assert_eq!(
            message.bytes,
            std::mem::size_of::<Message>() + record_bytes + 2000 + context_bytes
        );
        println!("budget_bytes Event={} Pending={} Message={} boxed_record={} State={} Trace={} slot_payload_256_sources={} history_slots_256_sources={} global_queued={} max_message={} max_event_string_capacity={}",
            std::mem::size_of::<Event>(), std::mem::size_of::<Pending>(), std::mem::size_of::<Message>(), record_bytes,
            std::mem::size_of::<State>(), std::mem::size_of::<Trace>(), MAX_SOURCES * HANDOFF_CAPACITY * std::mem::size_of::<Message>(),
            MAX_SOURCES * CAPACITY * std::mem::size_of::<Event>(), MAX_PROCESS_HANDOFF_BYTES, MAX_MESSAGE_BYTES,
            MAX_MESSAGE_BYTES - std::mem::size_of::<Message>() - record_bytes);
    }

    #[test]
    fn concurrent_handoff_preserves_producer_fifo_context_cleanup_and_backend_windows(
    ) -> anyhow::Result<()> {
        let trace = test_trace();
        let database = DatabaseId::new(Namespace::new("tenant", "db")?, "main")?;
        let barrier = Arc::new(std::sync::Barrier::new(4));
        std::thread::scope(|scope| {
            for producer_id in 0..4 {
                let producer = trace.clone();
                let database = database.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    for request in 0..16 {
                        let guard = enter(
                            database.clone(),
                            7,
                            format!("producer-{producer_id}-{request}"),
                            Some("close"),
                            Some("cleanup"),
                        );
                        producer.admission("enqueue", producer_id * 16 + request + 1, None);
                        producer.admission("admit", producer_id * 16 + request + 1, None);
                        producer.record(Event {
                            kind: "backend_start",
                            operation: Some("put"),
                            ..Event::default()
                        });
                        producer.backend_get(true, Some(3));
                        producer.record(Event {
                            kind: "backend_finish",
                            operation: Some("put"),
                            success: Some(true),
                            bytes: Some(2),
                            ..Event::default()
                        });
                        producer.record(Event {
                            kind: "cleanup_finish",
                            operation: Some("close"),
                            success: Some(true),
                            ..Event::default()
                        });
                        drop(guard);
                        assert!(context().is_none());
                    }
                });
            }
        });
        let snapshot = trace.test_events();
        assert_eq!(snapshot["lost"], 0);
        assert_eq!(snapshot["pendingMessages"], 0);
        assert_eq!(snapshot["databaseId"], json!(database));
        assert_eq!(snapshot["epoch"], 7);
        let events = snapshot["events"].as_array().unwrap();
        assert_eq!(events.len(), 4 * 16 * 6);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event["sequence"], index as u64 + 1);
            assert!(event["monotonicMicros"].as_u64().unwrap() > 0);
        }
        for producer_id in 0..4 {
            let mut previous = 0;
            for request in 0..16 {
                let request_id = format!("producer-{producer_id}-{request}");
                let selected = events
                    .iter()
                    .filter(|event| event["requestId"] == request_id)
                    .collect::<Vec<_>>();
                assert_eq!(
                    selected
                        .iter()
                        .map(|event| event["kind"].as_str().unwrap())
                        .collect::<Vec<_>>(),
                    [
                        "cleanup_start",
                        "enqueue",
                        "admit",
                        "backend_start",
                        "backend_finish",
                        "cleanup_finish"
                    ]
                );
                assert!(selected[0]["sequence"].as_u64().unwrap() > previous);
                previous = selected[5]["sequence"].as_u64().unwrap();
                assert_eq!(selected[0]["reservedLane"], "cleanup");
                assert_eq!(selected[0]["controlId"], selected[1]["controlId"]);
            }
        }
        let finished = events
            .iter()
            .filter(|event| event["kind"] == "backend_finish")
            .collect::<Vec<_>>();
        for (index, event) in finished.iter().enumerate() {
            assert_eq!(event["bytesAfter"], (index as u64 + 1) * 2);
        }
        assert_eq!(snapshot["backendMetrics"], Value::Null);
        let state = trace.state.lock().unwrap();
        assert_eq!(state.backend.get_successes, 64);
        assert_eq!(state.backend.get_payload_bytes, 192);
        assert_eq!(state.backend.put_successes, 64);
        assert_eq!(state.backend.put_payload_bytes, 128);
        Ok(())
    }

    #[test]
    fn queue_identity_is_distinct_and_request_context_restores() -> anyhow::Result<()> {
        let first = test_trace();
        let second = test_trace();
        assert_ne!(first.identity, second.identity);
        let database = DatabaseId::new(Namespace::new("tenant", "db")?, "main")?;
        let guard = enter(database.clone(), 7, "request".into(), None, None);
        first.admission("enqueue", 1, None);
        let snapshot = first.read(Some((0, CAPACITY)));
        assert_eq!(snapshot["epoch"], 7);
        assert_eq!(snapshot["databaseId"], json!(database));
        assert_eq!(snapshot["events"][0]["requestId"], "request");
        assert_ne!(snapshot["events"][0]["controlId"], "1");
        drop(guard);
        assert!(context().is_none());
        Ok(())
    }
}
