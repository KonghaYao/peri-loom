use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Instant;

use anyhow::Result;

use super::budget::RequestDeadline;
use super::AdmissionError;

/// Broadcast used by the in-process gates: one session lane and the database operations gate.
///
/// A `Mutex`/`RwLock` alone cannot bound a wait by an absolute deadline, and polling it
/// quantised every contended acquisition to the poll interval. The generation counter plus
/// this condvar lets a waiter sleep until a release, a cancellation or its own deadline.
///
/// The notifier does not take the wait mutex on the common path: it increments the
/// generation only, and takes the mutex just before waking when a waiter is registered. The
/// waiter registers before it rechecks the generation under that mutex, which closes the race
/// between "read the generation" and "sleep": a notification that raced the read is seen by
/// the recheck, and one that arrives after registration cannot slip in between the recheck
/// and `wait_timeout` because the notifier then waits for the mutex the waiter releases only
/// inside that call.
pub(crate) struct GateNotify {
    generation: AtomicU64,
    waiters: AtomicUsize,
    state: Mutex<()>,
    changed: Condvar,
}

impl Default for GateNotify {
    fn default() -> Self {
        Self::new()
    }
}

impl GateNotify {
    pub(crate) const fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            waiters: AtomicUsize::new(0),
            state: Mutex::new(()),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Threads currently inside [`Self::wait`]. Gates use this to keep a newcomer from
    /// consuming the release a registered waiter is about to wake for.
    pub(crate) fn waiters(&self) -> usize {
        self.waiters.load(Ordering::Acquire)
    }

    /// Tells every waiter that the guarded state may be available. Callers must call this
    /// after releasing the guarded lock, never while still holding it: a waiter woken early
    /// would fail to acquire and sleep again, and the real release would have no second wake.
    pub(crate) fn notify_all(&self) {
        // The generation and the waiter count form a store-load handshake with `wait`
        // (the notifier increments then reads the count; the waiter increments the count
        // then reads the generation). Both sides use `SeqCst` so at least one of the two
        // reads observes the other side even when they race; otherwise a notification could
        // be skipped while the waiter still sleeps on the old generation.
        self.generation.fetch_add(1, Ordering::SeqCst);
        if self.waiters.load(Ordering::SeqCst) == 0 {
            return;
        }
        // The mutex only orders the notification handshake; a panic cannot make the
        // generation unusable, so recovering the poison keeps the gate live.
        let _state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.changed.notify_all();
    }

    /// Sleeps until the generation moves past `seen` or the absolute deadline expires.
    ///
    /// The caller owns no queue entry or permit: a timeout just drops this registration, so a
    /// cancelled waiter cannot leak a wait count or block the next release. Spurious wakeups
    /// only recheck the generation, and a stale `seen` returns immediately instead of
    /// sleeping past a notification.
    pub(crate) fn wait(&self, seen: u64, deadline: &RequestDeadline) -> Result<()> {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        // The drop guard also covers a panic inside the wait; the counter must not keep a
        // thread that is no longer waiting.
        let registration = WaiterRegistration { gate: self };
        let result = self.wait_registered(seen, deadline);
        let failed = result.is_err();
        drop(registration);
        if failed {
            // A gate that keeps newcomers from barging may have parked them behind this
            // waiter. If it times out or is cancelled without taking the gate, the resource
            // may already be free while they still sleep, and no release is coming to wake
            // them.
            self.notify_all();
        }
        result
    }

    fn wait_registered(&self, seen: u64, deadline: &RequestDeadline) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while self.generation.load(Ordering::SeqCst) == seen {
            let remaining = deadline.end().saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(AdmissionError::Deadline.into());
            }
            let (next, _) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
        }
        Ok(())
    }
}

struct WaiterRegistration<'gate> {
    gate: &'gate GateNotify,
}

impl Drop for WaiterRegistration<'_> {
    fn drop(&mut self) {
        self.gate.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}
