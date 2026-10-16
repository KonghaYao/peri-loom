use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{ensure, Result};

#[derive(Default)]
struct ClockState {
    elapsed: Duration,
    active: Option<Duration>,
}

#[derive(Default)]
pub(super) struct ExecutionClock(Mutex<ClockState>);

fn thread_cpu() -> Result<Duration> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // 线程 CPU 时钟排除阻塞 I/O 和宿主调度等待，不使用进程 CPU（其他请求）。
    ensure!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } == 0,
        "thread CPU clock unavailable"
    );
    Ok(Duration::new(value.tv_sec as u64, value.tv_nsec as u32))
}

impl ExecutionClock {
    pub fn elapsed(&self) -> Result<Duration> {
        let state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("execution clock poisoned"))?;
        Ok(state.elapsed.saturating_add(match state.active {
            Some(started) => thread_cpu()?.saturating_sub(started),
            None => Duration::ZERO,
        }))
    }

    pub fn enter(self: &Arc<Self>) -> Result<StepClock> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("execution clock poisoned"))?;
        ensure!(state.active.is_none(), "nested execution clock");
        state.active = Some(thread_cpu()?);
        Ok(StepClock(self.clone()))
    }
}

pub(super) struct StepClock(Arc<ExecutionClock>);

impl StepClock {
    pub fn finish(self) -> Result<()> {
        let elapsed = self.0.elapsed();
        let mut state = self
            .0
             .0
            .lock()
            .map_err(|_| anyhow::anyhow!("execution clock poisoned"))?;
        state.active = None;
        state.elapsed = elapsed?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_sleep_and_setup_do_not_consume_execution_cpu() -> Result<()> {
        let clock = Arc::new(ExecutionClock::default());
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(clock.elapsed()?, Duration::ZERO);
        let step = clock.enter()?;
        std::thread::sleep(Duration::from_millis(80));
        step.finish()?;
        assert!(clock.elapsed()? < Duration::from_millis(40));
        let elapsed = clock.elapsed()?;
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(clock.elapsed()?, elapsed);
        Ok(())
    }
}
