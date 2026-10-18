package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

func (controller *Controller) selectWorker(region, exclude string) (Worker, error) {
	workers, err := readWorkers(controller.store)
	if err != nil {
		return Worker{}, err
	}
	return SelectWorker(Metadata{Workers: workers}, region, exclude, time.Now().UnixMilli(), controller.cfg.HeartbeatTimeout.Milliseconds())
}

func (controller *Controller) eligibleWorker(identifier, region string) (Worker, error) {
	worker, exists, err := readWorker(controller.store, identifier)
	if err != nil {
		return Worker{}, err
	}
	if !exists {
		return Worker{}, errors.New("worker missing")
	}
	return SelectWorker(Metadata{Workers: map[string]Worker{identifier: worker}}, region, "", time.Now().UnixMilli(), controller.cfg.HeartbeatTimeout.Milliseconds())
}

func (controller *Controller) lookupWorker(identifier string) (Worker, error) {
	worker, exists, err := readWorker(controller.store, identifier)
	if err != nil {
		return Worker{}, err
	}
	if !exists {
		return Worker{}, errors.New("worker missing")
	}
	return worker, nil
}

func (controller *Controller) pollWorkers(ctx context.Context) error {
	return controller.pollWorkersSelected(ctx, nil)
}

func (controller *Controller) pollWorkersSelected(ctx context.Context, selected []string) error {
	var workersView map[string]Worker
	if selected == nil {
		all, err := readWorkers(controller.store)
		if err != nil {
			return err
		}
		workersView = all
	} else {
		workersView = make(map[string]Worker, len(selected))
		for _, identifier := range selected {
			worker, found, err := readWorker(controller.store, identifier)
			if err != nil {
				return err
			}
			if found {
				workersView[identifier] = worker
			}
		}
	}
	identifiers := make([]string, 0, len(workersView))
	for identifier := range workersView {
		identifiers = append(identifiers, identifier)
	}
	sort.Strings(identifiers)
	if len(identifiers) == 0 {
		return ctx.Err()
	}
	const maxConcurrentHeartbeats = 16
	workers := min(len(identifiers), maxConcurrentHeartbeats)
	jobs := make(chan string)
	failures := make(chan error, len(identifiers))
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for identifier := range jobs {
				if err := controller.pollWorker(ctx, identifier, workersView[identifier]); err != nil {
					failures <- err
				}
			}
		}()
	}
	for _, identifier := range identifiers {
		if err := ctx.Err(); err != nil {
			break
		}
		jobs <- identifier
	}
	close(jobs)
	group.Wait()
	close(failures)
	var all []error
	for err := range failures {
		all = append(all, err)
	}
	return errors.Join(append(all, ctx.Err())...)
}

func (controller *Controller) workerFresh(worker Worker, nowMS int64) bool {
	return worker.Healthy && nowMS >= worker.LastHeartbeatMS && nowMS-worker.LastHeartbeatMS < controller.cfg.HeartbeatTimeout.Milliseconds()
}

func (controller *Controller) heartbeatProbeLimit() time.Duration {
	limit := min(controller.cfg.HeartbeatTimeout/2, 2*time.Second)
	if limit < time.Millisecond {
		limit = time.Millisecond
	}
	return limit
}

// refreshStaleWorker re-observes one worker whose heartbeat aged out during a
// reconcile pass. A pass probes each worker at most once, so a genuinely dead
// worker costs one bounded probe instead of one per placement.
func (controller *Controller) refreshStaleWorker(ctx context.Context, pass *reconcilePass, identifier string) error {
	if pass != nil {
		if pass.probed[identifier] {
			return nil
		}
		pass.probed[identifier] = true
	}
	log.Printf("控制面心跳观测过期：worker=%s，单次重探上限=%dms", identifier, controller.heartbeatProbeLimit().Milliseconds())
	return controller.pollWorkersSelected(ctx, []string{identifier})
}

func (controller *Controller) pollWorker(ctx context.Context, identifier string, worker Worker) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	requestID, err := RandomID()
	if err != nil {
		return err
	}
	probe, cancel := context.WithTimeout(ctx, controller.heartbeatProbeLimit())
	defer cancel()
	var report workerReport
	rpcErr := controller.transport.Worker(probe, worker, requestID, workerAction{Command: "get_worker_heartbeat"}, &report)
	valid := rpcErr == nil && report.WorkerID == identifier && report.Resources.LoadedDatabases >= 0 && report.Resources.LoadingDatabases >= 0 && report.Resources.ResidentCount >= 0 && report.Resources.QuarantinedCount >= 0
	// A database the worker cannot describe is still a database it holds: it is
	// named under failures and left out of the reported database list, so its
	// absence is not evidence of release. Counting those entries as residency
	// keeps the reported load from understating the worker, and the worker stays
	// healthy because it can still serve every database it did describe.
	load := report.Resources.LoadedDatabases + report.Resources.LoadingDatabases + len(report.Failures)
	if load < 0 {
		valid = false
	}
	observedMS := time.Now().UnixMilli()
	if err := ctx.Err(); err != nil {
		return err
	}
	if observer, ok := controller.store.(interface {
		observeWorkerHeartbeat(string, string, bool, int, int64) error
	}); ok {
		return observer.observeWorkerHeartbeat(identifier, worker.Address, valid, load, observedMS)
	}
	return controller.store.Update(func(current *Metadata) error {
		registered, exists := current.Workers[identifier]
		if !exists || registered.Address != worker.Address {
			return nil
		}
		registered.Healthy = valid
		if valid {
			registered.Load = load
			registered.LastHeartbeatMS = observedMS
		}
		current.Workers[identifier] = registered
		return nil
	})
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
		return fmt.Errorf("drain worker state %s: %w", worker.ID, err)
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
			return fmt.Errorf("drain command %s: %w", worker.ID, err)
		}
		for {
			actual, err = controller.workerState(bounded, worker, placement.Database)
			if err != nil {
				return fmt.Errorf("drain worker poll %s: %w", worker.ID, err)
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
				return fmt.Errorf("drain deadline %s mode=%s: %w", worker.ID, actual.Mode, bounded.Err())
			case <-timer.C:
			}
		}
	}
	if placement.Phase == "suspend_drain" {
		return controller.planRevoke(placement)
	}
	return controller.planGrant(ctx, placement)
}
