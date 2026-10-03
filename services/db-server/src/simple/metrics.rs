//! Opt-in timing for the Simple request path. Labels are fixed strings only.
use std::sync::OnceLock;
use std::time::Instant;

static ENABLED: OnceLock<bool> = OnceLock::new();

pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var_os("PERI_LOOM_SIMPLE_STAGE_METRICS").as_deref()
            == Some(std::ffi::OsStr::new("1"))
    })
}

pub(crate) struct StageTimer {
    stage: &'static str,
    started: Option<Instant>,
}

impl StageTimer {
    pub(crate) fn start(stage: &'static str) -> Self {
        Self {
            stage,
            started: enabled().then(Instant::now),
        }
    }
}

impl Drop for StageTimer {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            metrics::histogram!("simple_stage_micros", "stage" => self.stage)
                .record(started.elapsed().as_secs_f64() * 1_000_000.0);
        }
    }
}
