use anyhow::{anyhow, ensure, Result};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError,
};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(super) struct Admission {
    transitioning: AtomicU8,
    tickets: RwLock<()>,
    completion: Mutex<()>,
    changed: Condvar,
}

pub(super) struct Transition<'admission> {
    admission: &'admission Admission,
    barrier: Option<RwLockWriteGuard<'admission, ()>>,
}

#[derive(Debug)]
pub(super) struct AdmissionBusy;

impl std::fmt::Display for AdmissionBusy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Retryable admission transition busy")
    }
}

impl std::error::Error for AdmissionBusy {}

impl Admission {
    pub fn read(&self) -> Result<RwLockReadGuard<'_, ()>> {
        loop {
            self.wait_renewal()?;
            match self.tickets.try_read() {
                Ok(ticket) => {
                    if self.transitioning.load(Ordering::SeqCst) == 0 {
                        return Ok(ticket);
                    }
                    drop(ticket);
                }
                Err(TryLockError::Poisoned(_)) => {
                    return Err(anyhow!("Unavailable admission gate"))
                }
                Err(TryLockError::WouldBlock) => std::thread::yield_now(),
            }
        }
    }

    fn wait_renewal(&self) -> Result<()> {
        let mut completion = self
            .completion
            .lock()
            .map_err(|_| anyhow!("Unavailable admission gate"))?;
        while self.transitioning.load(Ordering::SeqCst) == 2 {
            completion = self
                .changed
                .wait(completion)
                .map_err(|_| anyhow!("Unavailable admission gate"))?;
        }
        ensure!(
            self.transitioning.load(Ordering::SeqCst) == 0,
            "Retryable admission closed"
        );
        Ok(())
    }

    pub fn write(&self) -> Result<Transition<'_>> {
        self.transition(1)
    }

    pub fn write_wait(&self) -> Result<Transition<'_>> {
        self.transition(2)
    }

    pub fn write_for_import(&self) -> Result<Transition<'_>> {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            match self.transition(2) {
                Ok(ticket) => return Ok(ticket),
                Err(error) if error.is::<AdmissionBusy>() && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn transition(&self, mode: u8) -> Result<Transition<'_>> {
        self.transitioning
            .compare_exchange(0, mode, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| AdmissionBusy)?;
        let mut transition = Transition {
            admission: self,
            barrier: None,
        };
        transition.barrier = Some(
            self.tickets
                .write()
                .map_err(|_| anyhow!("Unavailable admission gate"))?,
        );
        Ok(transition)
    }

    pub fn quiescent(&self) -> bool {
        if self.transitioning.load(Ordering::SeqCst) != 0 {
            return false;
        }
        let Ok(_barrier) = self.tickets.try_write() else {
            return false;
        };
        self.transitioning.load(Ordering::SeqCst) == 0
    }
}

impl Drop for Transition<'_> {
    fn drop(&mut self) {
        drop(self.barrier.take());
        let _completion = self
            .admission
            .completion
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.admission.transitioning.store(0, Ordering::SeqCst);
        self.admission.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn import_waits_for_existing_transition_before_mutating() -> Result<()> {
        let gate = Arc::new(Admission::default());
        let held = gate.write()?;
        let other = gate.clone();
        let waiting = std::thread::spawn(move || -> Result<()> {
            let _transition = other.write_for_import()?;
            Ok(())
        });
        std::thread::sleep(Duration::from_millis(20));
        ensure!(!waiting.is_finished(), "import did not wait for transition");
        drop(held);
        waiting
            .join()
            .map_err(|_| anyhow!("import test panicked"))??;
        Ok(())
    }
}
