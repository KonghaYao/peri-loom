package controller

import (
	"errors"
	"math/bits"
	"net"
	"net/url"
	"strconv"
)

var ErrNoEligibleWorker = errors.New("no eligible worker")

func validateWorker(worker Worker) error {
	if !componentPattern.MatchString(worker.ID) || worker.Capacity <= 0 || worker.Load < 0 || worker.LastHeartbeatMS < 0 {
		return errors.New("invalid worker identity, capacity, load or heartbeat")
	}
	if worker.Region != "" && !componentPattern.MatchString(worker.Region) {
		return errors.New("invalid worker region")
	}
	address, err := url.Parse(worker.Address)
	if err != nil || address.Scheme != "http" || address.User != nil || address.RawQuery != "" || address.ForceQuery || address.Fragment != "" || address.RawPath != "" || (address.Path != "" && address.Path != "/") {
		return errors.New("worker address must be a plain loopback HTTP endpoint")
	}
	host := address.Hostname()
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("worker address must use literal loopback IP")
	}
	port, err := strconv.Atoi(address.Port())
	if err != nil || port < 1 || port > 65535 {
		return errors.New("worker endpoint port required")
	}
	return nil
}

func RegisterWorker(store MetadataStore, worker Worker) error {
	if err := validateWorker(worker); err != nil {
		return err
	}
	return store.Update(func(metadata *Metadata) error {
		if previous, exists := metadata.Workers[worker.ID]; exists {
			if previous.Address != worker.Address || previous.Region != worker.Region {
				return errors.New("registered worker endpoint and region are immutable")
			}
			worker.Load = previous.Load
			worker.Healthy = previous.Healthy
			worker.LastHeartbeatMS = previous.LastHeartbeatMS
			worker.Draining = previous.Draining
		} else {
			worker.Load = 0
			worker.Healthy = false
			worker.LastHeartbeatMS = 0
		}
		metadata.Workers[worker.ID] = worker
		return nil
	})
}

func HeartbeatWorker(store MetadataStore, id string, load int, nowMS int64) error {
	if !componentPattern.MatchString(id) || load < 0 || nowMS < 0 {
		return errors.New("invalid worker heartbeat")
	}
	if observer, ok := store.(interface {
		heartbeatWorker(string, int, int64) error
	}); ok {
		return observer.heartbeatWorker(id, load, nowMS)
	}
	return store.Update(func(metadata *Metadata) error {
		worker, exists := metadata.Workers[id]
		if !exists {
			return errors.New("worker not registered")
		}
		if nowMS < worker.LastHeartbeatMS {
			return errors.New("worker heartbeat cannot decrease")
		}
		worker.Load = load
		worker.LastHeartbeatMS = nowMS
		worker.Healthy = true
		metadata.Workers[id] = worker
		return nil
	})
}

func SetWorkerDraining(store MetadataStore, id string, draining bool) error {
	if !componentPattern.MatchString(id) {
		return errors.New("invalid worker identifier")
	}
	return store.Update(func(metadata *Metadata) error {
		worker, exists := metadata.Workers[id]
		if !exists {
			return errors.New("worker not registered")
		}
		worker.Draining = draining
		metadata.Workers[id] = worker
		return nil
	})
}

func SelectWorker(metadata Metadata, region, exclude string, nowMS, timeoutMS int64) (Worker, error) {
	if nowMS < 0 || timeoutMS <= 0 || (region != "" && !componentPattern.MatchString(region)) || (exclude != "" && !componentPattern.MatchString(exclude)) {
		return Worker{}, errors.New("invalid worker selection time bounds")
	}
	var selected Worker
	found := false
	for key, worker := range metadata.Workers {
		if key != worker.ID || validateWorker(worker) != nil || worker.ID == exclude || (region != "" && worker.Region != region) {
			continue
		}
		if !worker.Healthy || worker.Draining || worker.Load >= worker.Capacity || worker.LastHeartbeatMS > nowMS || nowMS-worker.LastHeartbeatMS >= timeoutMS {
			continue
		}
		if !found || lessLoaded(worker, selected) {
			selected = worker
			found = true
		}
	}
	if !found {
		return Worker{}, ErrNoEligibleWorker
	}
	return selected, nil
}

func lessLoaded(candidate, current Worker) bool {
	leftHigh, leftLow := bits.Mul64(uint64(candidate.Load), uint64(current.Capacity))
	rightHigh, rightLow := bits.Mul64(uint64(current.Load), uint64(candidate.Capacity))
	if leftHigh != rightHigh {
		return leftHigh < rightHigh
	}
	if leftLow != rightLow {
		return leftLow < rightLow
	}
	return candidate.ID < current.ID
}
