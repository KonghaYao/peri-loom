package controller

import "errors"

// Heartbeats are observations. Epochs, placements, worker identity and drain
// intent remain in the durable metadata; a new process requires fresh probes.
type heartbeatObservation struct {
	address         string
	load            int
	lastHeartbeatMS int64
	sampledAtMS     int64
	healthy         bool
}

func (store *diskStore) applyHeartbeat(worker Worker) Worker {
	if observed, found := store.heartbeats[worker.ID]; found && observed.address == worker.Address {
		worker.Load = observed.load
		worker.LastHeartbeatMS = observed.lastHeartbeatMS
		worker.Healthy = observed.healthy
	}
	return worker
}

func (store *diskStore) applyHeartbeats(workers map[string]Worker) {
	for identifier, worker := range workers {
		workers[identifier] = store.applyHeartbeat(worker)
	}
}

func (store *diskStore) refreshHeartbeatObservations() {
	for identifier, observed := range store.heartbeats {
		worker, found := store.metadata.Workers[identifier]
		if !found || worker.Address != observed.address {
			delete(store.heartbeats, identifier)
			continue
		}
		observed.load = worker.Load
		observed.lastHeartbeatMS = worker.LastHeartbeatMS
		observed.healthy = worker.Healthy
		store.heartbeats[identifier] = observed
	}
}

func (store *diskStore) observeWorkerHeartbeat(identifier, address string, valid bool, load int, observedMS int64) error {
	return store.observeHeartbeat(identifier, address, valid, load, observedMS, false)
}

func (store *diskStore) heartbeatWorker(identifier string, load int, observedMS int64) error {
	return store.observeHeartbeat(identifier, "", true, load, observedMS, true)
}

func (store *diskStore) observeHeartbeat(identifier, address string, valid bool, load int, observedMS int64, required bool) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if err := store.available(); err != nil {
		return err
	}
	worker, found := store.metadata.Workers[identifier]
	if !found {
		if required {
			return errors.New("worker not registered")
		}
		return nil
	}
	if address != "" && worker.Address != address {
		return nil
	}
	if observedMS < 0 || valid && load < 0 {
		return errors.New("invalid worker heartbeat")
	}
	observed := store.heartbeats[identifier]
	if observed.address != worker.Address {
		observed = heartbeatObservation{address: worker.Address, load: worker.Load, lastHeartbeatMS: worker.LastHeartbeatMS, healthy: worker.Healthy}
	}
	if observedMS < observed.sampledAtMS {
		if required {
			return errors.New("worker heartbeat cannot decrease")
		}
		return nil
	}
	if observedMS < observed.lastHeartbeatMS {
		if valid {
			return errors.New("worker heartbeat cannot decrease")
		}
		return nil
	}
	if valid {
		observed.load = load
		observed.lastHeartbeatMS = observedMS
	}
	observed.healthy = valid
	observed.sampledAtMS = observedMS
	store.heartbeats[identifier] = observed
	return nil
}
