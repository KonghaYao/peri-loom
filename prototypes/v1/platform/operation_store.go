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
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	if err := store.disk.available(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("operation read requires a visitor")
	}
	return visit(store.state)
}

func (store *stateStore) updateOperations(change func(*State) error) error {
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	if err := store.disk.available(); err != nil {
		return err
	}
	if change == nil {
		return errors.New("operation mutation required")
	}
	// The private operation mutation may read catalog maps, but only changes
	// operation records and counters. Copy that bounded portion of the State.
	candidate := store.state
	candidate.Operations = make(map[string]OperationRecord, len(store.state.Operations))
	for key, record := range store.state.Operations {
		candidate.Operations[key] = record
	}
	if err := change(&candidate); err != nil {
		return err
	}
	if candidate.Revision != store.state.Revision || candidate.OperationStorageVersion != 1 {
		return errors.New("operation mutation changed registry identity")
	}
	if err := validateOperations(candidate, &store.state); err != nil {
		return err
	}
	delta := operationDeltaBetween(store.state.Operations, candidate.Operations)
	if !delta.empty() || candidate.OperationSequence != store.state.OperationSequence || candidate.OperationEvictions != store.state.OperationEvictions {
		// One append carries the whole commit; the registry rewrite is reserved for
		// catalog changes, so an operation-only commit never pays for the bounded
		// set it did not change.
		if err := store.operations.appendOperations(candidate, delta); err != nil {
			store.disk.poisoned = true
			return err
		}
	}
	store.state = candidate
	return nil
}
