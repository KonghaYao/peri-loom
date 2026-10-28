//! Reproducible identities for isolated mechanism tests. No production symbol or
//! environment hook exists. The guard is thread-bound and restores nested state.

use std::{cell::RefCell, marker::PhantomData, rc::Rc};

struct State {
    seed: u64,
    operation: u64,
    sequence: u64,
}
thread_local! { static STATE: RefCell<Option<State>> = const { RefCell::new(None) }; }

pub(crate) struct Scope {
    previous: Option<State>,
    _local: PhantomData<Rc<()>>,
}

impl Scope {
    pub(crate) fn new(seed: u64, operation: u64) -> Self {
        let previous = STATE.with(|state| {
            state.replace(Some(State {
                seed,
                operation,
                sequence: 0,
            }))
        });
        Self {
            previous,
            _local: PhantomData,
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        STATE.with(|state| {
            state.replace(self.previous.take());
        });
    }
}

pub(super) fn now_ms() -> Option<u64> {
    STATE.with(|state| state.borrow().as_ref().map(|_| 1_700_000_000_000))
}

pub(super) fn unique_id() -> Option<String> {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let state = state.as_mut()?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"peri-loom mechanism fixture identity v1");
        hash.update(&state.seed.to_le_bytes());
        hash.update(&state.operation.to_le_bytes());
        hash.update(&state.sequence.to_le_bytes());
        state.sequence = state
            .sequence
            .checked_add(1)
            .expect("fixture identity exhausted");
        Some(hash.finalize().to_hex().to_string())
    })
}

#[test]
fn deterministic_guard_is_scoped_seeded_and_does_not_cross_threads() {
    assert!(unique_id().is_none());
    let first;
    let second;
    {
        let _outer = Scope::new(7, 0);
        first = unique_id().unwrap();
        {
            let _nested = Scope::new(7, 0);
            assert_eq!(unique_id().unwrap(), first);
            second = unique_id().unwrap();
            assert!(std::thread::spawn(unique_id).join().unwrap().is_none());
        }
        assert_eq!(unique_id().unwrap(), second);
        let _different = Scope::new(8, 0);
        assert_ne!(unique_id().unwrap(), first);
    }
    assert!(unique_id().is_none());
    assert!(now_ms().is_none());
}
