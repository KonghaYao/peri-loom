//! Single-flight table for content-addressed object fetches.
//!
//! Concurrent misses of the same immutable object must produce one in-flight request, not one
//! per caller. The first caller to join a key owns the fetch and performs the I/O; every later
//! caller waits for that owner's outcome. Ownership is what keeps the waiters safe: a waiter
//! holds a shared slot and no part of the request, so a waiter that goes away can neither cancel
//! the fetch nor strand another waiter, and the owner never waits for a waiter to finish.
//!
//! A key must be the complete identity of the object, so two revisions of the same page are two
//! entries. An entry exists from the join that found the key absent until the owner's guard
//! retires it, which happens on success, on failure and while unwinding, so the table only ever
//! holds requests that are still in flight.
//!
//! Errors are shared like results: a failing fetch wakes every waiter with the owner's message
//! instead of letting each waiter issue its own request.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

/// One immutable body, shared by the owner and every waiter.
pub(crate) type Segment = Arc<Vec<u8>>;

/// What a waiter reads. It is `Clone` because every waiter takes the same copy, and the error is
/// the owner's formatted chain so that a waiter does not re-issue the request to learn it.
type Outcome = Result<Segment, String>;

#[derive(Default)]
pub(crate) struct Fetches {
    entries: Mutex<HashMap<String, Arc<Slot>>>,
}

struct Slot {
    outcome: Mutex<Option<Outcome>>,
    ready: Condvar,
}

/// What one join found: ownership of the fetch, or a fetch to wait for.
pub(crate) enum Joined<'a> {
    Leader(Leader<'a>),
    Waiter(Waiter),
}

impl Fetches {
    /// Joins the fetch for one complete object identity, starting one when none is in flight.
    pub(crate) fn join(&self, key: &str) -> Joined<'_> {
        let mut entries = self.entries.lock().expect("in-flight table poisoned");
        if let Some(slot) = entries.get(key) {
            return Joined::Waiter(Waiter(slot.clone()));
        }
        let slot = Arc::new(Slot {
            outcome: Mutex::new(None),
            ready: Condvar::new(),
        });
        entries.insert(key.to_owned(), slot.clone());
        Joined::Leader(Leader {
            fetches: self,
            key: key.to_owned(),
            slot,
            retired: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().expect("in-flight table poisoned").len()
    }
}

/// Ownership of one in-flight fetch. The owner publishes exactly once; a drop without a publish
/// means the owner unwound, and it releases the waiters with an error rather than leaving them
/// waiting for a result that will never be published.
pub(crate) struct Leader<'a> {
    fetches: &'a Fetches,
    key: String,
    slot: Arc<Slot>,
    retired: bool,
}

impl Leader<'_> {
    pub(crate) fn publish(mut self, outcome: Outcome) {
        self.retire(outcome);
    }

    fn retire(&mut self, outcome: Outcome) {
        self.retired = true;
        // Poison is recovered here because this also runs while unwinding: a panicking owner
        // must wake its waiters instead of making every later fetch of this key fail.
        *self
            .slot
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(outcome);
        self.slot.ready.notify_all();
        self.fetches
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.key);
    }
}

impl Drop for Leader<'_> {
    fn drop(&mut self) {
        if !self.retired {
            self.retire(Err("object fetch abandoned by its owner".to_owned()));
        }
    }
}

/// A shared fetch another caller owns. Dropping one drops only this waiter's interest in it.
pub(crate) struct Waiter(Arc<Slot>);

impl Waiter {
    pub(crate) fn wait(&self) -> Outcome {
        let mut outcome = self
            .0
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        loop {
            if let Some(result) = outcome.clone() {
                return result;
            }
            outcome = self
                .0
                .ready
                .wait(outcome)
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{channel, Receiver};
    use std::time::Duration;

    /// Every wait in these tests is bounded: a waiter that is never woken fails its test with a
    /// message instead of hanging the run.
    const DEADLINE: Duration = Duration::from_secs(10);

    /// A caller waiting on a shared fetch: whether it reached the fetch, and what it observed.
    struct Caller {
        reached: Receiver<()>,
        outcome: Receiver<Outcome>,
    }

    /// Spawns a caller that must find the fetch already in flight and wait for it. The first
    /// channel reports that it reached the shared fetch, the second its outcome.
    fn join_as_waiter(fetches: Arc<Fetches>, key: &str) -> Caller {
        let (joined, reached) = channel();
        let (sender, outcome) = channel();
        let key = key.to_owned();
        std::thread::spawn(move || match fetches.join(&key) {
            Joined::Waiter(waiter) => {
                let _ = joined.send(());
                let _ = sender.send(waiter.wait());
            }
            Joined::Leader(leader) => {
                let error = "expected a shared fetch to wait for".to_owned();
                leader.publish(Err(error.clone()));
                let _ = joined.send(());
                let _ = sender.send(Err(error));
            }
        });
        Caller { reached, outcome }
    }

    fn reached(callers: &[Caller]) {
        for caller in callers {
            caller
                .reached
                .recv_timeout(DEADLINE)
                .expect("a caller never reached the shared fetch");
        }
    }

    fn outcome(caller: &Caller) -> Outcome {
        caller
            .outcome
            .recv_timeout(DEADLINE)
            .expect("a caller never observed the outcome")
    }

    #[test]
    fn concurrent_joins_share_one_fetch_and_its_result() {
        let fetches = Arc::new(Fetches::default());
        let Joined::Leader(leader) = fetches.join("hash-a") else {
            panic!("the first join must own the fetch");
        };
        let callers = [
            join_as_waiter(fetches.clone(), "hash-a"),
            join_as_waiter(fetches.clone(), "hash-a"),
        ];
        reached(&callers);
        assert_eq!(fetches.len(), 1);
        leader.publish(Ok(Arc::new(vec![7; 8])));
        for caller in &callers {
            assert_eq!(outcome(caller).unwrap().as_slice(), &[7; 8]);
        }
        assert_eq!(fetches.len(), 0);
    }

    #[test]
    fn one_failure_reaches_every_waiter() {
        let fetches = Arc::new(Fetches::default());
        let Joined::Leader(leader) = fetches.join("hash-b") else {
            panic!("the first join must own the fetch");
        };
        let callers: Vec<Caller> = (0..4)
            .map(|_| join_as_waiter(fetches.clone(), "hash-b"))
            .collect();
        reached(&callers);
        leader.publish(Err("injected fetch failure".to_owned()));
        for caller in &callers {
            let error = outcome(caller).expect_err("every waiter must observe the failure");
            assert_eq!(error, "injected fetch failure");
        }
        assert_eq!(fetches.len(), 0);
    }

    /// The analogue of cancelling the caller that started the request: the owner disappears
    /// without publishing, and every waiter must still be released.
    #[test]
    fn an_abandoned_owner_releases_every_waiter() {
        let fetches = Arc::new(Fetches::default());
        let Joined::Leader(leader) = fetches.join("hash-c") else {
            panic!("the first join must own the fetch");
        };
        let callers: Vec<Caller> = (0..3)
            .map(|_| join_as_waiter(fetches.clone(), "hash-c"))
            .collect();
        reached(&callers);
        assert_eq!(fetches.len(), 1);
        drop(leader);
        for caller in &callers {
            let error = outcome(caller).expect_err("a vanished owner must not strand a waiter");
            assert!(error.contains("abandoned"), "unexpected error: {error}");
        }
        assert_eq!(fetches.len(), 0);
    }

    /// A waiter owns nothing but its interest in the outcome: one that goes away without waiting
    /// must not cancel the fetch or disturb the waiters that stayed.
    #[test]
    fn dropping_a_waiter_leaves_the_fetch_and_the_other_waiters_alone() {
        let fetches = Arc::new(Fetches::default());
        let Joined::Leader(leader) = fetches.join("hash-d") else {
            panic!("the first join must own the fetch");
        };
        let Joined::Waiter(abandoned) = fetches.join("hash-d") else {
            panic!("a second join must wait for the fetch");
        };
        drop(abandoned);
        let kept = join_as_waiter(fetches.clone(), "hash-d");
        reached(std::slice::from_ref(&kept));
        assert_eq!(fetches.len(), 1);
        leader.publish(Ok(Arc::new(vec![9; 4])));
        assert_eq!(outcome(&kept).unwrap().as_slice(), &[9; 4]);
        assert_eq!(fetches.len(), 0);
    }

    #[test]
    fn different_identities_never_share_a_fetch() {
        let fetches = Fetches::default();
        let Joined::Leader(first) = fetches.join("hash-e") else {
            panic!("first identity must start its own fetch");
        };
        let Joined::Leader(second) = fetches.join("hash-f") else {
            panic!("another identity must start its own fetch");
        };
        assert_eq!(fetches.len(), 2);
        first.publish(Ok(Arc::new(vec![1; 4])));
        second.publish(Ok(Arc::new(vec![2; 4])));
        assert_eq!(fetches.len(), 0);
    }
}
