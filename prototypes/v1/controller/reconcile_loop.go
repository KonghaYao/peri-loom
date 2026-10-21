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

const reconcileRetryDelay = 100 * time.Millisecond

// maxWorkerReprobesPerPass bounds how often one pass may re-observe a worker.
// A 20s management pass at the default 5s heartbeat can refresh three times
// before the deadline; four leaves headroom for the boundary and for shorter
// configured windows while staying a small constant, so a slow pass cannot turn
// into O(placements) RPCs per worker.
const maxWorkerReprobesPerPass = 4

// reconcilePass carries observations that are only valid for one pass. A pass
// can outlive the heartbeat window while it reacquires many databases, so it
// re-probes a worker when the refreshed observation expires again, and stops
// after a failed probe or the per-worker bound.
type reconcilePass struct {
	mutex   sync.Mutex
	workers map[string]*workerObservation
}

// workerObservation is locked while its probe runs, so concurrent placement
// work for the same worker counts one reprobe per stale observation instead of
// multiplying probes across databases. Different workers never share the lock.
type workerObservation struct {
	mutex    sync.Mutex
	reprobes int
	failed   bool
}

func newReconcilePass() *reconcilePass {
	return &reconcilePass{workers: make(map[string]*workerObservation)}
}

func (pass *reconcilePass) observation(identifier string) *workerObservation {
	pass.mutex.Lock()
	defer pass.mutex.Unlock()
	observation := pass.workers[identifier]
	if observation == nil {
		observation = &workerObservation{}
		pass.workers[identifier] = observation
	}
	return observation
}

// reconcileJob classes order dispatch, not completion. Renewals whose hint has
// passed come first, then active placements without a hint (fresh process or
// newly observed), then active placements with a future hint, and finally
// pending transitions such as create, migrate and suspend.
type reconcileJob struct {
	key      string
	database DatabaseID
	class    int
	due      time.Time
}

const (
	reconcileRenewalOverdue = iota
	reconcileActiveUnknown
	reconcileActiveScheduled
	reconcileTransition
)

func (controller *Controller) reconcileJobs(metadata Metadata) []reconcileJob {
	keys := make([]string, 0, len(metadata.Placements))
	for key := range metadata.Placements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	now := time.Now()
	jobs := make([]reconcileJob, 0, len(keys))
	for _, key := range keys {
		placement := metadata.Placements[key]
		job := reconcileJob{key: key, database: placement.Database, class: reconcileTransition}
		if placement.Phase == "active" {
			job.class = reconcileActiveUnknown
			if deadline, known := controller.renewalDeadline(key); known {
				job.due = deadline
				if deadline.After(now) {
					job.class = reconcileActiveScheduled
				} else {
					job.class = reconcileRenewalOverdue
				}
			}
		}
		jobs = append(jobs, job)
	}
	sort.SliceStable(jobs, func(left, right int) bool {
		if jobs[left].class != jobs[right].class {
			return jobs[left].class < jobs[right].class
		}
		if jobs[left].class == reconcileRenewalOverdue || jobs[left].class == reconcileActiveScheduled {
			if !jobs[left].due.Equal(jobs[right].due) {
				return jobs[left].due.Before(jobs[right].due)
			}
		}
		return jobs[left].key < jobs[right].key
	})
	return jobs
}

func (controller *Controller) Reconcile(ctx context.Context) (result error) {
	passStarted := time.Now()
	defer func() {
		if elapsed := time.Since(passStarted); elapsed >= controller.cfg.HeartbeatTimeout {
			log.Printf("控制面 reconcile pass 结束：elapsed=%dms err=%v", elapsed.Milliseconds(), result)
		}
	}()
	if err := controller.pollWorkers(ctx); err != nil {
		return err
	}
	pass := newReconcilePass()
	metadata, err := readReconcileView(controller.store)
	if err != nil {
		return err
	}
	return controller.reconcilePlacements(ctx, controller.reconcileJobs(metadata), pass)
}

func (controller *Controller) reconcilePlacements(ctx context.Context, jobs []reconcileJob, pass *reconcilePass) error {
	if len(jobs) == 0 {
		return ctx.Err()
	}
	failures := make([]error, len(jobs))
	indices := make(chan int)
	workers := min(len(jobs), controller.cfg.ReconcileConcurrency)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range indices {
				failures[index] = controller.reconcilePlacement(ctx, jobs[index], pass)
			}
		}()
	}
dispatch:
	for index := range jobs {
		select {
		case indices <- index:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(indices)
	group.Wait()
	var joined []error
	for index, err := range failures {
		if err != nil {
			// The caller sees one joined failure; naming the placement keeps the
			// private log able to say which database could not converge.
			joined = append(joined, fmt.Errorf("%s: %w", jobs[index].key, err))
		}
	}
	if err := ctx.Err(); err != nil {
		// Keep the placement failures observed before cancellation; the caller
		// needs both the deadline and the underlying RPC errors.
		joined = append(joined, err)
	}
	return errors.Join(joined...)
}

func (controller *Controller) reconcilePlacement(ctx context.Context, job reconcileJob, pass *reconcilePass) error {
	releaseLane, err := controller.enterDatabase(ctx, job.key)
	if err != nil {
		return err
	}
	defer releaseLane()
	releaseSlot, err := controller.enterReconcileSlot(ctx)
	if err != nil {
		return err
	}
	defer releaseSlot()
	// The pass view can age while earlier work runs, so the durable placement
	// is read again under the lane. Without this, a management call that
	// completed in between would be overwritten with the pass's old copy.
	placement, _, exists, err := ReadRouting(controller.store, job.database)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if placement.Database.Key() != job.key {
		return errors.New("placement identity changed")
	}
	switch {
	case placement.Phase == "active":
		err = controller.reconcileActive(ctx, &placement, pass)
	case placement.Phase == "suspended", placement.Phase == "idle":
		// Both are settled and outside the active schedule. Idle is normally
		// filtered out of the pass view; a stale view can still name it, and
		// renewing it would be the exact leak S02 closes.
		controller.clearRenewalDeadline(job.key)
	case placement.Phase == "idle_drain":
		_, err = controller.converge(ctx, placement)
	default:
		if placement.Phase == "acquire_pending" || placement.Phase == "renew_worker_pending" {
			worker, found, readErr := readWorker(controller.store, placement.DesiredWorker)
			if readErr != nil {
				return readErr
			}
			if !found || !worker.Healthy {
				replacement, selectErr := controller.selectWorker(placement.Region, placement.DesiredWorker)
				if selectErr == nil {
					placement.DesiredWorker = replacement.ID
					selectErr = controller.planGrant(ctx, &placement)
				}
				if selectErr != nil {
					return selectErr
				}
			}
		}
		_, err = controller.converge(ctx, placement)
	}
	if err != nil {
		controller.recordRenewalDeadline(job.key, time.Now())
	}
	return err
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
	return controller.pollWorkersSelected(ctx, identifiers)
}

func (controller *Controller) reconcileActive(ctx context.Context, placement *Placement, pass *reconcilePass) error {
	key := placement.Database.Key()
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
		controller.recordRenewalDeadline(key, time.Now())
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
		controller.recordRenewalDeadline(key, time.Now())
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
		controller.recordRenewalDeadline(key, time.Now())
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
	// The worker retires an instance after its own idle window, but its
	// ownership context can keep reporting the role until the lease moves. An
	// explicit non-resident answer is the worker saying no SQL can be served:
	// release the lease instead of renewing an instance that is gone.
	if local.Resident != nil && !*local.Resident && placement.Phase == "active" && idleReleaseSafe(actual, *placement) {
		return controller.settleIdle(ctx, *placement, actual, worker, local)
	}
	if !serving(local, *placement) {
		controller.recordRenewalDeadline(key, time.Now())
		if err := controller.planGrant(ctx, placement); err != nil {
			return err
		}
	} else {
		remaining := min(actual.RemainingTTLMS, local.RemainingTTLMS)
		controller.recordRenewalDeadline(key, renewalDue(remaining))
		if remaining > placement.TTLMS/2 {
			return nil
		}
		if err := controller.planRenew(placement); err != nil {
			return err
		}
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
