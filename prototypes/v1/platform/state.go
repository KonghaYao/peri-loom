package platform

import (
	"encoding/json"
	"errors"
	"path/filepath"

	controller "peri-loom/controller"
)

type stateStore struct {
	disk        *durableFile
	state       State
	digestIndex map[string]string
}

func OpenStateStore(directory string) (StateStore, error) {
	if directory == "" {
		return nil, errors.New("private state directory required")
	}
	initial := State{Version: 1, Organizations: map[string]Organization{}, Projects: map[string]Project{}, Databases: map[string]Database{}, APIKeys: map[string]APIKey{}, Keys: map[string]KeyDescriptor{}}
	disk, err := openDurable(filepath.Join(directory, "state.json"), initial)
	if err != nil {
		return nil, err
	}
	var state State
	if err := strictDecode(disk.payload, &state); err != nil {
		disk.close()
		return nil, err
	}
	if state.Revision != disk.revision {
		disk.close()
		return nil, errors.New("state revision mismatch")
	}
	if err := validateState(state, nil); err != nil {
		disk.close()
		return nil, err
	}
	store := &stateStore{disk: disk, state: state, digestIndex: make(map[string]string, len(state.APIKeys))}
	for id, key := range state.APIKeys {
		store.digestIndex[key.Digest] = id
	}
	known, _ := operationCounts(state)
	if known > operationKnownRetention {
		if err := updateOperationRecords(store, nil); err != nil {
			disk.close()
			return nil, err
		}
	}
	for _, record := range state.Operations {
		if operationNeedsRecovery(record) {
			if err := store.Update(func(candidate *State) error {
				for key, pending := range candidate.Operations {
					if operationNeedsRecovery(pending) {
						openedSession := pending.State == "terminal"
						pending.State = "quarantined"
						pending.MayHaveCommitted = true
						if pending.Stage == "close_session" || openedSession {
							pending.CleanupOutcome = "unknown"
						} else {
							pending.BusinessOutcome = "unknown"
						}
						candidate.Operations[key] = pending
					}
				}
				return nil
			}); err != nil {
				disk.close()
				return nil, err
			}
			break
		}
	}
	return store, nil
}

func (store *stateStore) Snapshot() (State, error) {
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	if err := store.disk.available(); err != nil {
		return State{}, err
	}
	return cloneState(store.state)
}

func (store *stateStore) credentialByDigest(digest string) (APIKey, bool, error) {
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	if err := store.disk.available(); err != nil {
		return APIKey{}, false, err
	}
	id, found := store.digestIndex[digest]
	if !found {
		return APIKey{}, false, nil
	}
	key, found := store.state.APIKeys[id]
	return key, found, nil
}

func (store *stateStore) authorizationState(keyID, projectID string, database controller.DatabaseID) (State, error) {
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	if err := store.disk.available(); err != nil {
		return State{}, err
	}
	view := State{
		APIKeys: map[string]APIKey{}, Projects: map[string]Project{},
		Databases: map[string]Database{}, Keys: map[string]KeyDescriptor{},
	}
	if key, found := store.state.APIKeys[keyID]; found {
		view.APIKeys[keyID] = key
	}
	if project, found := store.state.Projects[projectID]; found {
		view.Projects[projectID] = project
	}
	if record, found := store.state.Databases[database.Key()]; found {
		view.Databases[database.Key()] = record
		if project, found := store.state.Projects[record.Project]; found {
			view.Projects[record.Project] = project
		}
	}
	if descriptor, found := store.state.Keys[namespaceKey(database.Namespace)]; found {
		descriptor.Envelope = nil
		view.Keys[namespaceKey(database.Namespace)] = descriptor
	}
	return view, nil
}

func readAuthorizationState(store StateStore, keyID, projectID string, database controller.DatabaseID) (State, error) {
	if narrow, ok := store.(interface {
		authorizationState(string, string, controller.DatabaseID) (State, error)
	}); ok {
		return narrow.authorizationState(keyID, projectID, database)
	}
	return store.Snapshot()
}

func (store *stateStore) Update(mutation func(*State) error) error {
	store.disk.mutex.Lock()
	defer store.disk.mutex.Unlock()
	if err := store.disk.available(); err != nil {
		return err
	}
	if mutation == nil {
		return errors.New("state mutation required")
	}
	candidate, err := cloneState(store.state)
	if err != nil {
		return err
	}
	if err := mutation(&candidate); err != nil {
		return err
	}
	if candidate.Version != 1 || candidate.Revision != store.state.Revision || candidate.Revision == ^uint64(0) {
		return errors.New("state revision/version cannot be changed or exhausted")
	}
	candidate.Revision++
	committed, err := cloneState(candidate)
	if err != nil {
		return err
	}
	if err := validateState(committed, &store.state); err != nil {
		return err
	}
	if err := store.disk.commit(committed.Revision, committed); err != nil {
		return err
	}
	if len(committed.APIKeys) != len(store.state.APIKeys) {
		for id, key := range committed.APIKeys {
			if _, exists := store.state.APIKeys[id]; !exists {
				store.digestIndex[key.Digest] = id
			}
		}
	}
	store.state = committed
	return nil
}

func (store *stateStore) Close() error { return store.disk.close() }

func cloneState(state State) (State, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return State{}, errors.New("invalid state JSON")
	}
	var cloned State
	err = json.Unmarshal(encoded, &cloned)
	return cloned, err
}
