//! Per-request lease validation window.
//!
//! One worker request used to spend four `session/validate` round trips on the
//! same unchanged binding: the engine's statement path validates, the database
//! image open validates, and each store accessor validates again. Those calls
//! are advisory pre-checks; the fence that actually protects the commit lives on
//! the authority, which re-checks the binding on every operation it serves and
//! publishes through the head CAS.
//!
//! Within one request the first full lease validation reaches the authority and
//! every later one is served from this window. Measured effect: one committing
//! execute drops from four `session/validate` RPCs to one, a point query stays
//! at one.
//!
//! Fencing is unchanged. A lease that dies mid-request still cannot publish:
//! the authority rejects the binding/epoch on the operation itself and the head
//! CAS stays the final fence. The local checks (`validate_local`) are untouched
//! and keep running on every store access.
use std::cell::Cell;

thread_local! {
    static OPEN: Cell<bool> = const { Cell::new(false) };
    static SATISFIED: Cell<bool> = const { Cell::new(false) };
}

/// Scopes the window to one worker request. Dropping restores the previous
/// state so nested or pooled reuse cannot leak a satisfied window.
pub(crate) struct LeaseWindow {
    previous_open: bool,
    previous_satisfied: bool,
}

impl Drop for LeaseWindow {
    fn drop(&mut self) {
        OPEN.with(|state| state.set(self.previous_open));
        SATISFIED.with(|state| state.set(self.previous_satisfied));
    }
}

/// Opens a fresh window for the calling request.
pub(crate) fn open() -> LeaseWindow {
    LeaseWindow {
        previous_open: OPEN.with(|state| state.replace(true)),
        previous_satisfied: SATISFIED.with(|state| state.replace(false)),
    }
}

/// Whether this request already performed a full lease validation. Outside a
/// window this is always false, so validations on a pooled connection thread
/// never inherit another request's window.
pub(crate) fn satisfied() -> bool {
    OPEN.with(Cell::get) && SATISFIED.with(Cell::get)
}

/// Records that the authority confirmed the lease for this request. Only called
/// after a successful validation, never optimistically, and only inside a
/// window: a validation performed with no window open must not suppress the
/// next request's validation.
pub(crate) fn mark_satisfied() {
    if OPEN.with(Cell::get) {
        SATISFIED.with(|state| state.set(true));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_is_scoped_and_never_leaks_a_satisfied_state() {
        assert!(!satisfied());
        {
            let _window = open();
            assert!(!satisfied());
            mark_satisfied();
            assert!(satisfied());
        }
        assert!(!satisfied(), "window state leaked past its scope");
    }

    /// A validation that happens with no window open must not suppress the next
    /// request's validation on the same (pooled) connection thread.
    #[test]
    fn validation_without_a_window_does_not_suppress_the_next_one() {
        mark_satisfied();
        assert!(!satisfied(), "a windowless validation opened a window");
        let _window = open();
        assert!(
            !satisfied(),
            "a windowless validation leaked into the next request"
        );
    }

    #[test]
    fn nested_window_restores_the_outer_state() {
        let _outer = open();
        mark_satisfied();
        {
            let _inner = open();
            assert!(!satisfied(), "inner window inherited outer validation");
        }
        assert!(satisfied(), "inner window clobbered the outer validation");
    }
}
