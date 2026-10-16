//! Shared ingress path for the two loopback RPC listeners.
//!
//! It lives beside `http` because the authority listener owns the framing both
//! listeners speak, and the worker already reaches into this module for it.
//!
//! Both listeners used to spin on a non-blocking `accept()` with a fixed sleep.
//! That sleep bounded accept latency for every new connection and kept a core
//! waking up hundreds of times a second while nothing was happening. Parking on
//! `poll()` removes both: a queued connection is accepted as soon as the kernel
//! has it, and the same call still returns on a bounded period so a listener can
//! run its supervision (reaping finished connection threads, watching its own
//! failure flag) without a second thread.
//!
//! A self-pipe carries explicit wakeups, so stop latency does not depend on the
//! supervision period.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};

/// Longest a listener parks before it runs supervision again. Bounds how long a
/// finished connection thread can stay unreaped while the listener is idle.
#[cfg(test)]
pub(crate) const SUPERVISION_PERIOD: std::time::Duration =
    std::time::Duration::from_millis(SUPERVISION_PERIOD_MS as u64);

const SUPERVISION_PERIOD_MS: libc::c_int = 250;

struct Pipe {
    read: RawFd,
    write: RawFd,
}

impl Drop for Pipe {
    fn drop(&mut self) {
        // SAFETY: both descriptors come from the `pipe` call in `Acceptor::bind`
        // and are closed exactly once, when the last clone is dropped.
        unsafe {
            libc::close(self.read);
            libc::close(self.write);
        }
    }
}

/// Explicit wakeup for a parked `Acceptor`.
///
/// The handle owns its pipe through an `Arc`, so a listener can never close the
/// descriptors out from under a thread that is still holding a handle.
#[derive(Clone)]
pub(crate) struct Wakeup {
    pipe: Arc<Pipe>,
}

impl Wakeup {
    /// Interrupt a parked `Acceptor::park`. Best effort: a failure only means the
    /// listener waits out its supervision period instead.
    pub(crate) fn wake(&self) {
        let byte = 0u8;
        // SAFETY: the descriptor is live for as long as this handle exists and
        // was set non-blocking, so a full pipe reports `EAGAIN` and never blocks.
        unsafe {
            libc::write(self.pipe.write, std::ptr::addr_of!(byte).cast(), 1);
        }
    }
}

pub(crate) struct Acceptor {
    socket: TcpListener,
    pipe: Arc<Pipe>,
}

impl Acceptor {
    pub(crate) fn bind(address: SocketAddr) -> Result<Self> {
        let socket = TcpListener::bind(address)?;
        socket.set_nonblocking(true)?;
        let mut descriptors = [0 as RawFd; 2];
        // SAFETY: `descriptors` has room for the two descriptors `pipe` writes.
        if unsafe { libc::pipe(descriptors.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error()).context("listener wake pipe");
        }
        let pipe = Pipe {
            read: descriptors[0],
            write: descriptors[1],
        };
        for descriptor in descriptors {
            // SAFETY: both descriptors are live until `pipe` is dropped.
            let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
            let set = flags >= 0
                && unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0;
            if !set {
                return Err(io::Error::last_os_error()).context("listener wake pipe flags");
            }
        }
        Ok(Self {
            socket,
            pipe: Arc::new(pipe),
        })
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub(crate) fn wakeup(&self) -> Wakeup {
        Wakeup {
            pipe: self.pipe.clone(),
        }
    }

    /// Park until a connection is queued, the wake pipe fires, or the
    /// supervision period elapses.
    ///
    /// Every outcome returns `Ok`, so a caller runs one loop body and then drains
    /// `take` rather than switching on the reason it woke up.
    pub(crate) fn park(&self) -> Result<()> {
        let mut descriptors = [
            libc::pollfd {
                fd: self.socket.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.pipe.read,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            // SAFETY: `descriptors` is a live array of two `pollfd` values.
            let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, SUPERVISION_PERIOD_MS) };
            if ready >= 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error).context("listener poll");
            }
        }
        // Drain the wake pipe, otherwise a later `park` returns immediately on a
        // byte nobody is waiting for. The listener socket is drained by `take`.
        let mut drained = [0u8; 64];
        // SAFETY: the read end is non-blocking and `drained` is a live buffer.
        while unsafe { libc::read(self.pipe.read, drained.as_mut_ptr().cast(), drained.len()) } > 0
        {
        }
        Ok(())
    }

    /// Non-blocking accept; `Ok(None)` means nothing is queued right now.
    pub(crate) fn take(&self) -> Result<Option<TcpStream>> {
        match self.socket.accept() {
            Ok((stream, _)) => Ok(Some(stream)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// Bounded counter that hands out lease-style tickets.
///
/// A load-then-increment admits one holder per racing thread, which is what let
/// the old accept path run past its stated bound under a burst. `try_acquire`
/// uses a compare-exchange loop so the limit is a real bound, and the ticket
/// releases on drop even when a connection thread panics.
pub(crate) struct Quota {
    limit: usize,
    used: AtomicUsize,
}

impl Quota {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    pub(crate) fn try_acquire(self: &Arc<Self>) -> Option<Ticket> {
        let mut used = self.used.load(Ordering::SeqCst);
        loop {
            if used >= self.limit {
                return None;
            }
            match self.used.compare_exchange_weak(
                used,
                used + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(Ticket(self.clone())),
                Err(current) => used = current,
            }
        }
    }

    pub(crate) fn in_use(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }
}

pub(crate) struct Ticket(Arc<Quota>);

impl Drop for Ticket {
    fn drop(&mut self) {
        self.0.used.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use anyhow::Result;

    use super::{Acceptor, Quota, SUPERVISION_PERIOD};

    fn loopback() -> std::net::SocketAddr {
        "127.0.0.1:0".parse().expect("loopback literal")
    }

    #[test]
    fn parking_acceptor_returns_as_soon_as_a_connection_arrives() -> Result<()> {
        let acceptor = Acceptor::bind(loopback())?;
        let address = acceptor.local_addr()?;
        let connected = Arc::new(AtomicUsize::new(0));
        let observer = connected.clone();
        let connector = thread::spawn(move || {
            let stream = TcpStream::connect(address).expect("connect to fixture listener");
            observer.fetch_add(1, Ordering::SeqCst);
            stream
        });
        let started = Instant::now();
        acceptor.park()?;
        let parked = started.elapsed();
        // The kernel queues the connection and reports the listener readable before
        // the connector thread runs the increment after `connect`, so the parked
        // path may only assert on what is settled by then. Take and join first; the
        // assertions below then read state no concurrent thread can still change.
        let stream = acceptor.take()?;
        drop(connector.join().expect("connector thread"));
        assert!(
            parked < SUPERVISION_PERIOD,
            "accept did not happen on readiness: {parked:?}"
        );
        assert!(stream.is_some(), "queued connection was not accepted");
        assert_eq!(connected.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn wakeup_interrupts_parking_without_a_connection() -> Result<()> {
        let acceptor = Acceptor::bind(loopback())?;
        let wakeup = acceptor.wakeup();
        let waker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            wakeup.wake();
        });
        let started = Instant::now();
        acceptor.park()?;
        assert!(
            started.elapsed() < SUPERVISION_PERIOD,
            "wakeup did not interrupt park"
        );
        assert!(acceptor.take()?.is_none());
        waker.join().expect("waker thread");
        Ok(())
    }

    #[test]
    fn park_without_traffic_returns_on_the_supervision_period() -> Result<()> {
        let acceptor = Acceptor::bind(loopback())?;
        let started = Instant::now();
        acceptor.park()?;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= SUPERVISION_PERIOD,
            "park returned early with nothing to do: {elapsed:?}"
        );
        Ok(())
    }

    #[test]
    fn quota_never_admits_more_than_its_limit() {
        let quota = Quota::new(4);
        let admitted = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..16 {
            let quota = quota.clone();
            let admitted = admitted.clone();
            let peak = peak.clone();
            threads.push(thread::spawn(move || {
                if let Some(ticket) = quota.try_acquire() {
                    let live = admitted.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(live, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(5));
                    admitted.fetch_sub(1, Ordering::SeqCst);
                    drop(ticket);
                }
            }));
        }
        for thread in threads {
            thread.join().expect("quota contender");
        }
        assert!(peak.load(Ordering::SeqCst) <= 4);
        assert_eq!(quota.in_use(), 0);
    }

    #[test]
    fn dropped_ticket_returns_its_slot_even_while_unwinding() {
        let quota = Quota::new(1);
        let held = Arc::new(quota.clone());
        let panicked = thread::spawn(move || {
            let _ticket = held.try_acquire().expect("first slot");
            panic!("synthetic connection thread panic");
        });
        assert!(panicked.join().is_err());
        assert_eq!(quota.in_use(), 0);
        assert!(quota.try_acquire().is_some());
    }
}
