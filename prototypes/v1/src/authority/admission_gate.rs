use anyhow::{anyhow, ensure, Result};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError,
};

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

    fn transition(&self, mode: u8) -> Result<Transition<'_>> {
        self.transitioning
            .compare_exchange(0, mode, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| anyhow!("Retryable admission transition busy"))?;
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
