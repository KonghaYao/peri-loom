//! Opt-in storage attribution. Durability and scheduling never depend on these records.
use std::cell::Cell;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::work_metrics::{PublishOrigin, PublishStage};

mod group_shape;
pub(crate) use group_shape::GroupShape;

pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("PERI_DIAG_GROUP_PHASE").as_deref() == Ok("1"))
}

thread_local! {
    static PUBLICATION: Cell<u64> = const { Cell::new(0) };
}

#[derive(Default)]
pub(crate) struct Stamp {
    pub request_id: Option<String>,
    pub start_unix_ns: u64,
    pub publication_id: u64,
}

impl Stamp {
    pub(crate) fn new() -> Self {
        Self {
            request_id: crate::slow_diagnostics::request_id(),
            start_unix_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |time| time.as_nanos().min(u64::MAX as u128) as u64),
            publication_id: PUBLICATION.with(Cell::get),
        }
    }
}

pub(crate) struct Publication {
    pub stamp: Stamp,
    previous: u64,
    origin: &'static str,
    attempt: usize,
    result: &'static str,
    stages: [u64; 11],
}

impl Publication {
    pub(crate) fn begin() -> Option<Self> {
        if !enabled() {
            return None;
        }
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let identity = NEXT.fetch_add(1, Ordering::Relaxed);
        let previous = PUBLICATION.with(|current| current.replace(identity));
        Some(Self {
            stamp: Stamp::new(),
            previous,
            origin: "unspecified",
            attempt: 0,
            result: "error_or_early_return",
            stages: [0; 11],
        })
    }

    pub(crate) fn identity(&mut self, origin: PublishOrigin, attempt: usize) {
        self.origin = origin.name();
        self.attempt = attempt;
    }

    pub(crate) fn stage(&mut self, stage: PublishStage, elapsed: u64) {
        self.stages[stage.index()] += elapsed;
    }

    pub(crate) fn result(&mut self, result: &'static str) {
        self.result = result;
    }

    pub(crate) fn emit(&self, total_ns: u64) {
        let stages: std::collections::BTreeMap<_, _> = PublishStage::ALL
            .into_iter()
            .map(|stage| (stage.name(), self.stages[stage.index()]))
            .collect();
        let record = serde_json::json!({
            "event": "root_publication", "evidence_class": "diagnostic",
            "pid": std::process::id(), "request_id": self.stamp.request_id,
            "start_unix_ns": self.stamp.start_unix_ns,
            "publication_id": self.stamp.publication_id,
            "origin": self.origin, "attempt": self.attempt,
            "result": self.result, "total_ns": total_ns, "stages_ns": stages,
        });
        let _ = writeln!(std::io::stderr().lock(), "STORAGE_PHASE {record}");
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        PUBLICATION.with(|current| current.set(self.previous));
    }
}

pub(crate) fn receipt_roll(
    stamp: &Stamp,
    receipts: usize,
    pages: usize,
    bytes: usize,
    duration_ns: u64,
) {
    let record = serde_json::json!({
        "event": "wal_receipt_roll", "evidence_class": "diagnostic",
        "pid": std::process::id(), "request_id": stamp.request_id,
        "start_unix_ns": stamp.start_unix_ns, "duration_ns": duration_ns,
        "receipts": receipts, "pages": pages, "bytes": bytes,
    });
    let _ = writeln!(std::io::stderr().lock(), "STORAGE_PHASE {record}");
}
