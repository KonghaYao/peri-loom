package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"
)

const reconcileRetryDelay = 100 * time.Millisecond

// maxWorkerReprobesPerPass bounds how often one pass may re-observe a worker.
// A 10s management pass at the default 5s heartbeat needs at most two refreshes;
// four leaves headroom for shorter configured windows while staying a small
// constant, so a slow pass cannot turn into O(placements) RPCs per worker.
const maxWorkerReprobesPerPass = 4

// reconcilePass carries observations that are only valid for one pass. A pass
// can outlive the heartbeat window while it reacquires many databases, so it
// re-probes a worker when the refreshed observation expires again, and stops
// after a failed probe or the per-worker bound.
type reconcilePass struct {
	workers map[string]*workerObservation
}

type workerObservation struct {
	reprobes int
	failed   bool
}

func newReconcilePass() *reconcilePass {
	return &reconcilePass{workers: make(map[string]*workerObservation)}
}

func (controller *Controller) Reconcile(ctx context.Context) (result error) {
	passStarted := time.Now()
	defer func() {
		if elapsed := time.Since(passStarted); elapsed >= controller.cfg.HeartbeatTimeout {
			log.Printf("控制面 reconcile pass 结束：elapsed=%dms err=%v", elapsed.Milliseconds(), result)
		}
	}()
	release, err := controller.enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := controller.pollWorkers(ctx); err != nil {
		return err
	}
	pass := newReconcilePass()
	metadata, err := readReconcileView(controller.store)
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
			err = controller.reconcileActive(ctx, &placement, pass)
		}
		if err != nil {
			// The caller sees one joined failure; naming the placement keeps the
			// private log able to say which database could not converge.
			failures = append(failures, fmt.Errorf("%s: %w", key, err))
		}
	}
	return errors.Join(failures...)
}

// ReconcileWithin retries a desired-state pass until the deadline. Every pass
// carries that deadline into its context, so an authority or worker RPC cannot
// outlive the budget. The deadline bounds those RPCs and the operation-gate
// wait; context-free local metadata reads are not interruptible, so this is not
// a strict wall-clock bound, and a pass that already converged may return
// success just after the boundary. Reconcile never dispatches SQL, so a retry
// cannot duplicate an admitted mutation; the durable placement and grant
// records make every pass idempotent.
func (controller *Controller) ReconcileWithin(ctx context.Context, until time.Time) error {
	var last error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return last
			}
			return err
		}
		if !time.Now().Before(until) {
			// The budget was consumed before this pass started; do not dispatch
			// another one just to observe its canceled context.
			if last != nil {
				log.Printf("控制面 reconcile 重试预算耗尽：attempts=%d 底层错误=%v", attempt-1, last)
				return last
			}
			return context.DeadlineExceeded
		}
		passCtx, cancel := context.WithDeadline(ctx, until)
		err := controller.Reconcile(passCtx)
		cancel()
		if err == nil {
			if attempt > 1 {
				log.Printf("控制面 reconcile 重试后完成：attempts=%d", attempt)
			}
			return nil
		}
		last = err
		if ctx.Err() != nil {
			return last
		}
		remaining := time.Until(until)
		if remaining <= 0 {
			log.Printf("控制面 reconcile 重试预算耗尽：attempts=%d 底层错误=%v", attempt, last)
			return last
		}
		log.Printf("控制面 reconcile 未完成，准备重试：attempt=%d 剩余预算=%dms 底层错误=%v", attempt, remaining.Milliseconds(), last)
		timer := time.NewTimer(min(reconcileRetryDelay, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return last
		case <-timer.C:
		}
	}
}

// RefreshWorkers reestablishes live worker observations before a restarted
// service advertises ready for previously active placements.
func (controller *Controller) RefreshWorkers(ctx context.Context, identifiers ...string) error {
	release, err := controller.enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	return controller.pollWorkersSelected(ctx, identifiers)
}

func (controller *Controller) reconcileActive(ctx context.Context, placement *Placement, pass *reconcilePass) error {
	actual, err := controller.authorityState(ctx, placement.Database)
	if err != nil {
		return err
	}
	worker, exists, err := readWorker(controller.store, placement.DesiredWorker)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	if exists && !controller.workerFresh(worker, now) {
		// The observation may have aged out only because this pass is still
		// reacquiring earlier placements. Probe the desired worker once before
		// treating it as gone; a worker that really stopped still fails the
		// selection below, and the probe itself is deadline-bounded.
		if err := controller.refreshStaleWorker(ctx, pass, placement.DesiredWorker); err != nil {
			return err
		}
		worker, exists, err = readWorker(controller.store, placement.DesiredWorker)
		if err != nil {
			return err
		}
		now = time.Now().UnixMilli()
	}
	if !controller.workerFresh(worker, now) {
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
