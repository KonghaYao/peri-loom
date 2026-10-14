package controller

import (
	"context"
	"errors"
	"sort"
	"time"
)

func (controller *Controller) selectWorker(region, exclude string) (Worker, error) {
	metadata, err := controller.store.Snapshot()
	if err != nil {
		return Worker{}, err
	}
	return SelectWorker(metadata, region, exclude, time.Now().UnixMilli(), controller.cfg.HeartbeatTimeout.Milliseconds())
}

func (controller *Controller) eligibleWorker(identifier, region string) (Worker, error) {
	metadata, err := controller.store.Snapshot()
	if err != nil {
		return Worker{}, err
	}
	worker, exists := metadata.Workers[identifier]
	if !exists {
		return Worker{}, errors.New("worker missing")
	}
	metadata.Workers = map[string]Worker{identifier: worker}
	return SelectWorker(metadata, region, "", time.Now().UnixMilli(), controller.cfg.HeartbeatTimeout.Milliseconds())
}

func (controller *Controller) lookupWorker(identifier string) (Worker, error) {
	metadata, err := controller.store.Snapshot()
	if err != nil {
		return Worker{}, err
	}
	worker, exists := metadata.Workers[identifier]
	if !exists {
		return Worker{}, errors.New("worker missing")
	}
	return worker, nil
}

func (controller *Controller) pollWorkers(ctx context.Context) error {
	metadata, err := controller.store.Snapshot()
	if err != nil {
		return err
	}
	identifiers := make([]string, 0, len(metadata.Workers))
	for identifier := range metadata.Workers {
		identifiers = append(identifiers, identifier)
	}
	sort.Strings(identifiers)
	for _, identifier := range identifiers {
		if err := ctx.Err(); err != nil {
			return err
		}
		worker := metadata.Workers[identifier]
		requestID, err := RandomID()
		if err != nil {
			return err
		}
		var report workerReport
		rpcErr := controller.transport.Worker(ctx, worker, requestID, workerAction{Command: "get_worker_heartbeat"}, &report)
		valid := rpcErr == nil && report.WorkerID == identifier && report.Resources.LoadedDatabases >= 0 && report.Resources.LoadingDatabases >= 0
		load := report.Resources.LoadedDatabases + report.Resources.LoadingDatabases
		if load < 0 {
			valid = false
		}
		if err := controller.store.Update(func(current *Metadata) error {
			registered, exists := current.Workers[identifier]
			if !exists || registered.Address != worker.Address {
				return nil
			}
			registered.Healthy = valid
			if valid {
				registered.Load = load
				registered.LastHeartbeatMS = time.Now().UnixMilli()
			}
			current.Workers[identifier] = registered
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) applyWorker(ctx context.Context, placement *Placement) error {
	worker, err := controller.lookupWorker(placement.DesiredWorker)
	if err != nil {
		return err
	}
	actual, err := controller.workerState(ctx, worker, placement.Database)
	if err != nil {
		return err
	}
	if serving(actual, *placement) {
		placement.Phase = "active"
		clearPending(placement)
		return controller.save(*placement)
	}
	if placement.Grant == nil {
		return errors.New("grant missing")
	}
	if placement.Phase == "renew_worker_pending" {
		if actual.Binding == nil || !sameLease(*actual.Binding, placement.Grant.Binding) || actual.Binding.RenewalSequence >= placement.Grant.Binding.RenewalSequence || actual.Mode != placement.Role || actual.RemainingTTLMS == 0 {
			return controller.planGrant(ctx, placement)
		}
		if err := controller.sendWorker(ctx, worker, placement.PendingID, workerAction{Command: "renew_lease", Lease: placement.Grant}); err != nil {
			return err
		}
	} else {
		if actual.Epoch >= placement.Epoch && actual.Mode != "detached" {
			if actual.Mode == "acquiring" && actual.Binding != nil && *actual.Binding == placement.Grant.Binding {
				return errors.New("worker acquisition still in progress")
			}
			return controller.planGrant(ctx, placement)
		}
		if actual.Epoch > 0 && actual.Mode != "detached" {
			identifier, err := RandomID()
			if err != nil {
				return err
			}
			if err := controller.sendWorker(ctx, worker, identifier, workerAction{Command: "revoke_database", Database: &placement.Database, ExpectedEpoch: &actual.Epoch}); err != nil {
				return err
			}
		}
		if err := controller.sendWorker(ctx, worker, placement.PendingID, workerAction{Command: "acquire_database", Lease: placement.Grant, Role: placement.Role}); err != nil {
			return err
		}
	}
	actual, err = controller.workerState(ctx, worker, placement.Database)
	if err != nil {
		return err
	}
	if !serving(actual, *placement) {
		return errors.New("worker did not confirm exact ownership")
	}
	placement.Phase = "active"
	clearPending(placement)
	return controller.save(*placement)
}

func (controller *Controller) drain(ctx context.Context, placement *Placement) error {
	if placement.Grant == nil {
		return errors.New("drain grant missing")
	}
	worker, err := controller.lookupWorker(placement.Grant.Binding.WorkerID)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, controller.cfg.HeartbeatTimeout)
	defer cancel()
	actual, err := controller.workerState(bounded, worker, placement.Database)
	if err != nil {
		return err
	}
	if actual.Epoch != placement.Grant.Binding.Epoch {
		return errors.New("drain ownership changed")
	}
	if actual.Mode != "detached" {
		if actual.Binding == nil || *actual.Binding != placement.Grant.Binding || (actual.Mode != placement.Role && actual.Mode != "draining") {
			return errors.New("drain binding not serving")
		}
		identifier, err := RandomID()
		if err != nil {
			return err
		}
		epoch := placement.Grant.Binding.Epoch
		if err := controller.sendWorker(bounded, worker, identifier, workerAction{Command: "drain_database", Database: &placement.Database, ExpectedEpoch: &epoch}); err != nil {
			return err
		}
		for {
			actual, err = controller.workerState(bounded, worker, placement.Database)
			if err != nil {
				return err
			}
			if actual.Mode == "detached" && actual.Epoch == epoch {
				break
			}
			if actual.Binding == nil || *actual.Binding != placement.Grant.Binding || actual.Mode != "draining" {
				return errors.New("drain ownership changed before native detach")
			}
			timer := time.NewTimer(controller.cfg.Interval)
			select {
			case <-bounded.Done():
				timer.Stop()
				return bounded.Err()
			case <-timer.C:
			}
		}
	}
	if placement.Phase == "suspend_drain" {
		return controller.planRevoke(placement)
	}
	return controller.planGrant(ctx, placement)
}
