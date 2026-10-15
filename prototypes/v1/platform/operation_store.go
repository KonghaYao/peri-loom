package platform

import (
	"errors"
	"os"
	"path/filepath"
)

type durableOperations struct {
	Version      uint32                     `json:"version"`
	BaseRevision uint64                     `json:"base_revision"`
	Records      map[string]OperationRecord `json:"records"`
	Sequence     uint64                     `json:"sequence"`
	Evictions    uint64                     `json:"evictions"`
}

func operationsFromState(state State) durableOperations {
	return durableOperations{
		Version: 1, BaseRevision: state.Revision, Records: state.Operations,
		Sequence: state.OperationSequence, Evictions: state.OperationEvictions,
	}
}

func applyDurableOperations(state *State, operations durableOperations) {
	state.Operations = operations.Records
	state.OperationSequence = operations.Sequence
	state.OperationEvictions = operations.Evictions
}

func openOperationDisk(directory string, registry *durableFile, state *State) (*durableFile, error) {
	path := filepath.Join(directory, "operations", "state.json")
	if state.OperationStorageVersion == 1 {
		if _, err := os.Lstat(path + ".lock"); err != nil {
			return nil, errors.New("required operation storage missing")
		}
	}
	disk, err := openDurable(path, operationsFromState(*state))
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			disk.close()
		}
	}()
	var current durableOperations
	if err := strictDecode(disk.payload, &current); err != nil {
		return nil, err
	}
	if current.Version != 1 || current.BaseRevision > state.Revision {
		return nil, errors.New("operation storage revision mismatch")
	}
	if state.OperationStorageVersion == 0 {
		// No operation-only requests may run until the registry records that
		// this sidecar is mandatory. A crashed migration still uses the registry.
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
	if current.BaseRevision < state.Revision {
		// A registry commit won the race with a crash before its sidecar
		// checkpoint. The registry contains the complete operation view.
		if err := disk.commit(disk.revision+1, operationsFromState(*state)); err != nil {
			return nil, err
		}
		current = operationsFromState(*state)
	} else if state.OperationStorageVersion == 1 {
		applyDurableOperations(state, current)
	}
	if err := validateState(*state, nil); err != nil {
		return nil, err
	}
	ok = true
	return disk, nil
}

func (store *stateStore) commitOperations(state State) error {
	store.operations.mutex.Lock()
	defer store.operations.mutex.Unlock()
	if err := store.operations.commit(store.operations.revision+1, operationsFromState(state)); err != nil {
		store.disk.poisoned = true
		return err
	}
	return nil
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
	if err := store.commitOperations(candidate); err != nil {
		return err
	}
	store.state = candidate
	return nil
}
