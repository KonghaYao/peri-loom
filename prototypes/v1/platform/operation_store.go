package platform

import (
	"errors"
	"os"
	"path/filepath"
)

func openOperationDisk(directory string, registry *durableFile, state *State) (*operationJournal, error) {
	path := filepath.Join(directory, "operations", "journal")
	if state.OperationStorageVersion == 1 {
		if _, err := os.Lstat(path + ".lock"); err != nil {
			return nil, errors.New("required operation storage missing")
		}
	}
	journal, err := openOperationJournal(filepath.Join(directory, "operations"))
	if err != nil {
		return nil, err
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

// readOperations visits the committed operation view without copying the rest
// of the registry. The callback must not retain or mutate the state.
func (store *stateStore) readOperations(visit func(State) error) error {
	defer store.lockState()()
	if err := store.disk.available(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("operation read requires a visitor")
	}
	return visit(store.state)
}

func (store *stateStore) updateOperations(change func(*operationEdit) error) error {
	defer store.lockState()()
	if err := store.disk.available(); err != nil {
		return err
	}
	if change == nil {
		return errors.New("operation mutation required")
	}
	if store.state.OperationStorageVersion != 1 {
		return errors.New("operation storage identity unavailable")
	}
	if store.operationsIndex == nil {
		store.operationsIndex = newOperationIndex(store.state.Operations)
		store.cost.observeIndexRebuild(len(store.operationsIndex.records))
	}
	edit := newOperationEdit(&store.state, store.operationsIndex)
	if err := change(edit); err != nil {
		edit.revert()
		return err
	}
	if err := edit.settleRetention(); err != nil {
		edit.revert()
		return err
	}
	if err := edit.validate(); err != nil {
		edit.revert()
		return err
	}
	delta := edit.delta()
	if !delta.empty() || store.state.OperationSequence != edit.priorSequence || store.state.OperationEvictions != edit.evictions {
		if err := store.operations.appendOperations(store.state, delta); err != nil {
			store.disk.poisoned = true
			edit.revert()
			return err
		}
	}
	store.cost.observeCommit()
	return nil
}
