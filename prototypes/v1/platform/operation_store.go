package platform

import (
	"errors"
	"os"
	"path/filepath"
)

func openOperationDisk(directory string, registry *durableFile, state *State) (*operationJournal, error) {
	path := filepath.Join(directory, "operations", "journal")
	if state.OperationStorageVersion >= 1 {
		if _, err := os.Lstat(path + ".lock"); err != nil {
			return nil, errors.New("required operation storage missing")
		}
	}
	journal, err := openOperationJournal(filepath.Join(directory, "operations"))
	if err != nil {
		return nil, err
	}
	if (state.OperationStorageVersion == completionJournalVersion) != (journal.completion != nil) {
		journal.close()
		return nil, errors.New("completion format transition incomplete; restore the consistent pre-migration copy")
	}
	if journal.completion != nil && journal.completion.Seal != state.CompletionGenesis {
		journal.close()
		return nil, errors.New("completion genesis does not match registry")
	}
	ok := false
	defer func() {
		if !ok {
			journal.close()
		}
	}()
	if state.OperationStorageVersion == 0 {
		// No operation-only requests may run until the registry records that this
		// log is mandatory. A crashed migration still uses the registry.
		candidate := *state
		candidate.OperationStorageVersion = 1
		candidate.Revision++
		if err := validateState(candidate, state); err != nil {
			return nil, err
		}
		if err := registry.commit(candidate.Revision, candidate); err != nil {
			return nil, err
		}
		*state = candidate
	}
	// The registry payload is the recovered base view; the log only carries the
	// deltas appended after that revision, so a log that trails the registry is
	// already contained in the payload.
	journal.applyView(state.Revision, state)
	if err := validateState(*state, nil); err != nil {
		return nil, err
	}
	if journal.completion != nil && len(journal.records) > 0 && journal.records[0].Payload.Revision <= journal.archivedRevision {
		// Recovery has proved every overlapping byte against the immutable
		// archives. Finish an interrupted archive/snapshot/truncate cycle before
		// accepting more entries; otherwise the same prefix could be archived
		// again under another range on the next compaction.
		if err := journal.compact(*state); err != nil {
			return nil, err
		}
	}
	ok = true
	return journal, nil
}

// registryParentOperations rebuilds the operation view recovery would join into a
// registry payload. Between registry commits the log is the only writer of
// operations, so the persisted payload alone is not the parent a successor has to
// be legal against: an operation etched into that payload can settle and be evicted
// afterwards, and validating against the stale copy would reject a valid commit and
// poison the registry.
func registryParentOperations(journal *operationJournal) func(*State) {
	return func(parent *State) {
		journal.mutex.Lock()
		defer journal.mutex.Unlock()
		if journal.closed {
			return
		}
		journal.applyView(parent.Revision, parent)
	}
}

// operationPendingEdit is an edit that is already applied to the live view while its delta
// is only ordered in the journal. The view lock owns it: submit and settle are both called
// with that lock held, so the queue order and the view order stay the same order.
type operationPendingEdit struct {
	commit *operationCommit
	revert func()
}

// settleOperations confirms every edit that is applied to the view but not yet durable.
// The caller holds the view lock. It waits for the journal's own barrier and nothing else -
// the committer never takes the view lock - so no reader can deadlock against a flush. A
// failed barrier rolls its edits back in reverse order and poisons the store, because an
// uncertain append must never leave a confirmed-looking record behind.
func (store *stateStore) settleOperations() error {
	pending := store.unconfirmed
	if len(pending) == 0 {
		return nil
	}
	store.unconfirmed = nil
	var failure error
	for _, entry := range pending {
		if err := store.operations.await(entry.commit); err != nil {
			if failure == nil {
				failure = err
			}
			continue
		}
	}
	if failure == nil {
		return nil
	}
	// Only an append that failed is rolled back. An append whose barrier returned is durable,
	// and undoing it in the view would misreport what the log already holds; the store stays
	// fail-closed either way, and a failed append can leave a later key at an earlier value
	// only until the store is reopened.
	for index := len(pending) - 1; index >= 0; index-- {
		entry := pending[index]
		if entry.commit.err == nil || entry.commit.applied {
			continue
		}
		entry.commit.applied = true
		entry.revert()
	}
	// A shutdown refusal proves the append never happened; only an uncertain append leaves
	// persistence in doubt and poisons the store.
	if !errors.Is(failure, ErrStateClosed) {
		store.disk.poisoned = true
	}
	return failure
}

// compactOperationsLocked materializes the view once the log passed its append budget. The
// caller holds the view lock and has settled every in-flight commit, so the view it passes
// is the confirmed view and the journal can check that it still owns that revision.
func (store *stateStore) compactOperationsLocked() error {
	if len(store.unconfirmed) != 0 || !store.operations.compactionDue() {
		return nil
	}
	if err := store.operations.compactConfirmed(store.state); err != nil {
		store.operations.poison()
		return err
	}
	return nil
}

// readOperations visits the committed operation view without copying the rest of the
// registry. The callback must not retain or mutate the state. A visitor must not observe a
// record whose append was never confirmed, so the read settles first and reports the same
// uncertainty that append had.
func (store *stateStore) readOperations(visit func(State) error) error {
	defer store.lockState()()
	if err := store.disk.available(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("operation read requires a visitor")
	}
	if err := store.settleOperations(); err != nil {
		return err
	}
	return visit(store.state)
}

// updateOperations applies one edit and returns only after its appended delta is durable.
// The edit is applied under the view lock, so no reader observes an unconfirmed record; the
// barrier itself runs outside that lock, so a flush no longer blocks authentication and the
// other narrow reads that never look at operations.
func (store *stateStore) updateOperations(change func(*operationEdit) error) error {
	return store.updateOperationsAudit(change, nil, false)
}

func (store *stateStore) updateOperationsAudit(change func(*operationEdit) error, event *AuditEvent, reserved bool) error {
	unlock := store.lockState()
	if err := store.disk.available(); err != nil {
		unlock()
		return err
	}
	// The shutdown checkpoint names the revision that materialized the operation
	// view; an operation committed afterwards would make that name a lie, so the
	// write is refused loudly instead of silently invalidating the payload a
	// launcher reads.
	if store.checkpointed {
		unlock()
		return ErrStateFrozen
	}
	if change == nil && event == nil {
		unlock()
		return errors.New("operation mutation required")
	}
	if store.state.OperationStorageVersion != 1 && store.state.OperationStorageVersion != completionJournalVersion {
		unlock()
		return errors.New("operation storage identity unavailable")
	}
	// The edit is built on the view as it stands, including edits whose barrier is still
	// outstanding. That is safe because the queue keeps them in submit order and a failure
	// reverts them in reverse order, and it is what lets concurrent appends share one
	// barrier: waiting here would serialize every commit behind the oldest one.
	if store.operationsIndex == nil {
		store.operationsIndex = newOperationIndex(store.state.Operations)
		store.cost.observeIndexRebuild(len(store.operationsIndex.records))
	}
	edit := newOperationEdit(&store.state, store.operationsIndex)
	if change != nil {
		if err := change(edit); err != nil {
			edit.revert()
			unlock()
			return err
		}
	}
	if err := edit.settleRetention(); err != nil {
		edit.revert()
		unlock()
		return err
	}
	if err := edit.validate(); err != nil {
		edit.revert()
		unlock()
		return err
	}
	delta := edit.delta()
	if event == nil && delta.empty() && store.state.OperationSequence == edit.priorSequence && store.state.OperationEvictions == edit.evictions {
		// A mutation that changed nothing needs no barrier at all.
		unlock()
		return nil
	}
	commit, err := store.operations.submitAudit(store.state, delta, event, reserved)
	if err != nil {
		// A full queue is a refusal this change introduced before any write, so it poisons
		// nothing; every other registration refusal keeps the base behaviour of failing the
		// store closed.
		if !errors.Is(err, ErrOperationQueueFull) {
			store.disk.poisoned = true
		}
		edit.revert()
		unlock()
		return err
	}
	store.unconfirmed = append(store.unconfirmed, operationPendingEdit{commit: commit, revert: edit.revert})
	store.cost.observeCommit()
	unlock()
	<-commit.settled
	unlock = store.lockState()
	// Another waiter may have settled this commit before us; settling again is what makes
	// the outcome definite either way.
	settleErr := store.settleOperations()
	if settleErr == nil {
		if store.operations.completion != nil {
			store.scheduleCompletionMaintenanceLocked()
		} else {
			settleErr = store.compactOperationsLocked()
		}
	}
	poisoned := store.disk.poisoned
	unlock()
	if settleErr != nil {
		return settleErr
	}
	if commit.err != nil {
		return commit.err
	}
	if poisoned {
		// This delta is durable, but the log is now uncertain for another commit, so the
		// caller must not treat the registration as usable.
		return ErrStateUncertain
	}
	return nil
}
