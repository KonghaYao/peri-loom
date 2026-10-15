package controller

import "errors"

// ReadRouting returns one placement and its selected worker from the same
// verified store revision. Callers still validate the placement's binding.
func ReadRouting(store MetadataStore, database DatabaseID) (Placement, Worker, bool, error) {
	if store == nil || database.Validate() != nil {
		return Placement{}, Worker{}, false, errors.New("invalid routing lookup")
	}
	if narrow, ok := store.(interface {
		RoutingView(DatabaseID) (Placement, Worker, bool, error)
	}); ok {
		return narrow.RoutingView(database)
	}
	metadata, err := store.Snapshot()
	if err != nil {
		return Placement{}, Worker{}, false, err
	}
	placement, found := metadata.Placements[database.Key()]
	return placement, metadata.Workers[placement.DesiredWorker], found, nil
}

func (store *diskStore) RoutingView(database DatabaseID) (Placement, Worker, bool, error) {
	if err := database.Validate(); err != nil {
		return Placement{}, Worker{}, false, err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := store.available(); err != nil {
		return Placement{}, Worker{}, false, err
	}
	placement, found := store.metadata.Placements[database.Key()]
	if !found {
		return Placement{}, Worker{}, false, nil
	}
	// Grant and PendingCommand are mutable fields in otherwise value records.
	if placement.Grant != nil {
		grant := *placement.Grant
		placement.Grant = &grant
	}
	placement.PendingCommand = append([]byte(nil), placement.PendingCommand...)
	return placement, store.applyHeartbeat(store.metadata.Workers[placement.DesiredWorker]), true, nil
}

func readWorker(store MetadataStore, identifier string) (Worker, bool, error) {
	if narrow, ok := store.(interface {
		workerView(string) (Worker, bool, error)
	}); ok {
		return narrow.workerView(identifier)
	}
	metadata, err := store.Snapshot()
	if err != nil {
		return Worker{}, false, err
	}
	worker, found := metadata.Workers[identifier]
	return worker, found, nil
}

func (store *diskStore) workerView(identifier string) (Worker, bool, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := store.available(); err != nil {
		return Worker{}, false, err
	}
	worker, found := store.metadata.Workers[identifier]
	if found {
		worker = store.applyHeartbeat(worker)
	}
	return worker, found, nil
}

func readWorkers(store MetadataStore) (map[string]Worker, error) {
	if narrow, ok := store.(interface {
		workersView() (map[string]Worker, error)
	}); ok {
		return narrow.workersView()
	}
	metadata, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	return metadata.Workers, nil
}

func (store *diskStore) workersView() (map[string]Worker, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := store.available(); err != nil {
		return nil, err
	}
	workers := make(map[string]Worker, len(store.metadata.Workers))
	for identifier, worker := range store.metadata.Workers {
		workers[identifier] = store.applyHeartbeat(worker)
	}
	return workers, nil
}
