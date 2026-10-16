use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Debug, Default)]
pub(super) struct Sample {
    pub calls: usize,
    pub units: usize,
    pub micros: u128,
}

/// Statement work runs on session lanes, so stage accounting must be process-wide: a
/// thread-local trace would miss every counter the gates depend on.
static ACTIVE: AtomicBool = AtomicBool::new(false);
static STAGES: Mutex<Option<BTreeMap<String, Sample>>> = Mutex::new(None);

thread_local! {
    static STACK: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn start() {
    *STAGES.lock().unwrap() = Some(BTreeMap::new());
    ACTIVE.store(true, Ordering::SeqCst);
}

pub(super) fn finish() -> BTreeMap<String, Sample> {
    ACTIVE.store(false, Ordering::SeqCst);
    STAGES.lock().unwrap().take().unwrap_or_default()
}

pub(super) fn site() -> String {
    STACK.with(|stack| stack.borrow().join("/"))
}

pub(super) struct Stage {
    started: Instant,
    units: usize,
}

pub(super) fn stage(name: &'static str, units: usize) -> Option<Stage> {
    if !ACTIVE.load(Ordering::SeqCst) {
        return None;
    }
    STACK.with(|stack| stack.borrow_mut().push(name));
    Some(Stage {
        started: Instant::now(),
        units,
    })
}

impl Drop for Stage {
    fn drop(&mut self) {
        let path = STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            let path = stack.join("/");
            stack.pop();
            path
        });
        if let Some(stages) = STAGES.lock().unwrap().as_mut() {
            let sample = stages.entry(path).or_default();
            sample.calls += 1;
            sample.units += self.units;
            sample.micros += self.started.elapsed().as_micros();
        }
    }
}

pub(super) fn entries(root: &super::CatalogRoot) -> usize {
    root.blobs.window_len()
        + root.objects.window_len()
        + root.uploads.window_len()
        + root.tombstones.window_len()
}
