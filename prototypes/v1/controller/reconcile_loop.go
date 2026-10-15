package controller

import (
	"context"
	"errors"
	"sort"
	"time"
)

func (controller *Controller) Reconcile(ctx context.Context) error {
	release, err := controller.enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := controller.pollWorkers(ctx); err != nil {
		return err
	}
	metadata, err := controller.store.Snapshot()
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(metadata.Placements))
	for key := range metadata.Placements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var failures []error
	for _, key := range keys {
		placement := metadata.Placements[key]
		err = nil
		if err := ctx.Err(); err != nil {
			return err
		}
		if placement.Phase != "active" && placement.Phase != "suspended" {
			if placement.Phase == "acquire_pending" || placement.Phase == "renew_worker_pending" {
				worker, exists := metadata.Workers[placement.DesiredWorker]
				if !exists || !worker.Healthy {
					var replacement Worker
					replacement, err = controller.selectWorker(placement.Region, placement.DesiredWorker)
					if err == nil {
						placement.DesiredWorker = replacement.ID
						err = controller.planGrant(ctx, &placement)
					}
				}
			}
			if err == nil {
				_, err = controller.converge(ctx, placement)
			}
		} else if placement.Phase == "active" {
			err = controller.reconcileActive(ctx, &placement)
		}
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (controller *Controller) reconcileActive(ctx context.Context, placement *Placement) error {
	actual, err := controller.authorityState(ctx, placement.Database)
	if err != nil {
		return err
	}
	worker, exists, err := readWorker(controller.store, placement.DesiredWorker)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	fresh := exists && worker.Healthy && now >= worker.LastHeartbeatMS && now-worker.LastHeartbeatMS < controller.cfg.HeartbeatTimeout.Milliseconds()
	if !fresh {
		selected, err := controller.selectWorker(placement.Region, placement.DesiredWorker)
		if err != nil {
			return err
		}
		placement.DesiredWorker = selected.ID
		if err := controller.planGrant(ctx, placement); err != nil {
			return err
		}
		_, err = controller.converge(ctx, *placement)
		return err
	}
	if worker.Draining {
		selected, err := controller.selectWorker(placement.Region, worker.ID)
		if err != nil {
			return err
		}
		placement.DesiredWorker = selected.ID
		placement.Phase = "migrate_drain"
		if err := controller.save(*placement); err != nil {
			return err
		}
		_, err = controller.converge(ctx, *placement)
		return err
	}
	if placement.Grant == nil || actual.Binding == nil || *actual.Binding != placement.Grant.Binding || actual.RemainingTTLMS == 0 {
		if err := controller.planGrant(ctx, placement); err != nil {
			return err
		}
		_, err = controller.converge(ctx, *placement)
		return err
	}
	local, err := controller.workerState(ctx, worker, placement.Database)
	if err != nil {
		return err
	}
	if !serving(local, *placement) {
		if err := controller.planGrant(ctx, placement); err != nil {
			return err
		}
	} else if actual.RemainingTTLMS <= placement.TTLMS/2 || local.RemainingTTLMS <= placement.TTLMS/2 {
		if err := controller.planRenew(placement); err != nil {
			return err
		}
	} else {
		return nil
	}
	_, err = controller.converge(ctx, *placement)
	return err
}

func (controller *Controller) Run(ctx context.Context) error {
	ticker := time.NewTicker(controller.cfg.Interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := controller.Reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if _, err := controller.store.Snapshot(); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
