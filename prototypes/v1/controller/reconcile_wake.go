package controller

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// activationRPCTimeoutKey lets one wake widen the per-call deadline without
// changing the controller-wide control deadline used by reconcile. A cold load
// inside acquire_database takes longer than any renewal, and the activation
// context still carries the outer budget.
type activationRPCTimeoutKey struct{}

func (controller *Controller) rpcTimeout(ctx context.Context) time.Duration {
	if timeout, ok := ctx.Value(activationRPCTimeoutKey{}).(time.Duration); ok {
		return timeout
	}
	return controller.cfg.ControlRPCTimeout
}

// wakeCall is the single-flight record of one database activation. Waiters that
// time out leave without cancelling it: the durable intents are already
// ordered, and a cancelled activation would only hand the same work to the
// next reconcile pass.
type wakeCall struct {
	mutex       sync.Mutex
	operationID string
	waiters     int
	status      ActivationStatus
	err         error
	done        chan struct{}
}

func (call *wakeCall) join() ActivationStatus {
	call.mutex.Lock()
	defer call.mutex.Unlock()
	call.waiters++
	status := call.status
	status.Waiters = call.waiters
	return status
}

func (call *wakeCall) snapshot() ActivationStatus {
	call.mutex.Lock()
	defer call.mutex.Unlock()
	status := call.status
	status.Waiters = call.waiters
	return status
}

func (call *wakeCall) finish(status ActivationStatus, err error) {
	call.mutex.Lock()
	call.status = status
	call.err = err
	call.mutex.Unlock()
	close(call.done)
}

// Wake merges every request for one database onto one activation and returns
// when that activation settles or the caller's budget ends. The activation
// keeps running after a caller gives up, so a cancelled wait changes neither
// the epoch nor the durable intent sequence.
func (controller *Controller) Wake(ctx context.Context, database DatabaseID) (ActivationStatus, error) {
	if err := database.Validate(); err != nil {
		return ActivationStatus{}, err
	}
	key := database.Key()
	controller.wakeMutex.Lock()
	if existing := controller.wakes[key]; existing != nil {
		status := existing.join()
		controller.wakeMutex.Unlock()
		return controller.awaitWake(ctx, existing, status)
	}
	operationID, err := RandomID()
	if err != nil {
		controller.wakeMutex.Unlock()
		return ActivationStatus{}, err
	}
	call := &wakeCall{
		operationID: operationID,
		waiters:     1,
		status:      ActivationStatus{Database: database, State: ActivationWaking, OperationID: operationID, Waiters: 1},
		done:        make(chan struct{}),
	}
	controller.wakes[key] = call
	controller.wakeMutex.Unlock()
	go controller.runWake(call, database)
	return controller.awaitWake(ctx, call, call.snapshot())
}

func (controller *Controller) awaitWake(ctx context.Context, call *wakeCall, status ActivationStatus) (ActivationStatus, error) {
	select {
	case <-call.done:
		call.mutex.Lock()
		defer call.mutex.Unlock()
		status = call.status
		status.Waiters = call.waiters
		return status, call.err
	case <-ctx.Done():
		return status, ctx.Err()
	}
}

func (controller *Controller) runWake(call *wakeCall, database DatabaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), controller.cfg.ActivationTimeout)
	// One RPC may consume the whole activation budget; the outer deadline is
	// what bounds it, and the acquire cold load must not be cut to a renewal
	// deadline.
	ctx = context.WithValue(ctx, activationRPCTimeoutKey{}, controller.cfg.ActivationTimeout)
	defer cancel()
	placement, err := controller.wake(ctx, database)
	status := ActivationStatus{Database: database, State: placement.ActivationState(), OperationID: call.operationID, Epoch: placement.Epoch}
	if status.State == "" {
		status.State = ActivationFailed
	}
	if errors.Is(err, ErrActivationRefused) {
		status.State = ActivationRefused
	} else if err != nil && status.State == ActivationWaking {
		// A still-durable waking intent is reported as such; reconcile owns its
		// completion after the activation budget ends.
		status.State = ActivationWaking
	}
	call.finish(status, err)
	controller.wakeMutex.Lock()
	if controller.wakes[database.Key()] == call {
		delete(controller.wakes, database.Key())
	}
	controller.wakeMutex.Unlock()
}

func (controller *Controller) wake(ctx context.Context, database DatabaseID) (Placement, error) {
	release, err := controller.enterDatabaseOperation(ctx, database.Key())
	if err != nil {
		return Placement{}, err
	}
	defer release()
	placement, _, exists, err := ReadRouting(controller.store, database)
	if err != nil {
		return placement, err
	}
	if !exists {
		return placement, ErrPlacementMissing
	}
	switch placement.Phase {
	case "active":
		return placement, nil
	case "idle", "idle_drain", "revoke_pending", "suspended",
		"grant_intent", "grant_pending", "acquire_pending":
		if placement.SuspendReason != SuspendReasonIdle || placement.WakePolicy != WakePolicyAuto {
			return placement, ErrActivationRefused
		}
		return controller.activateLocked(ctx, placement)
	default:
		// A migration, a legacy record without a reason, or an initial
		// provisioning is not a request-triggered activation.
		return placement, ErrActivationRefused
	}
}

// activateLocked settles any durable transition, then grants a new epoch and
// acquires it on a worker. The caller holds the database lane, so a management
// Resume and an automatic wake cannot interleave their grants.
func (controller *Controller) activateLocked(ctx context.Context, placement Placement) (Placement, error) {
	placement, err := controller.converge(ctx, placement)
	if err != nil {
		return placement, err
	}
	if placement.Phase == "active" {
		return placement, nil
	}
	if placement.Phase != "idle" && placement.Phase != "suspended" {
		return placement, errors.New("activation is still in progress")
	}
	if err := controller.pollWorkers(ctx); err != nil {
		return placement, err
	}
	worker, err := controller.selectWorker(placement.Region, "")
	if err != nil {
		return placement, err
	}
	placement.DesiredWorker = worker.ID
	placement.DesiredState = "active"
	if err := controller.planGrant(ctx, &placement); err != nil {
		return placement, err
	}
	return controller.converge(ctx, placement)
}

// idleReleaseSafe requires the authority to agree that the idle worker held
// this placement's lease. A different binding means the database moved and the
// stale worker observation must not release the new owner's ownership.
func idleReleaseSafe(authority Actual, placement Placement) bool {
	if authority.Binding == nil {
		return true
	}
	return placement.Grant != nil && *authority.Binding == placement.Grant.Binding
}

// settleIdle closes the worker's local scope, revokes the authority lease and
// only then marks the placement idle. The worker has already dropped the
// instance, so the drain step has nothing left to wait for.
func (controller *Controller) settleIdle(ctx context.Context, placement Placement, authority Actual, worker Worker, local Actual) error {
	key := placement.Database.Key()
	placement.SuspendReason = SuspendReasonIdle
	placement.WakePolicy = WakePolicyAuto
	if placement.IdleSinceMS == 0 {
		placement.IdleSinceMS = time.Now().UnixMilli()
	}
	controller.clearRenewalDeadline(key)
	if local.Mode != "detached" && local.Mode != "revoked" && local.Epoch > 0 {
		identifier, err := RandomID()
		if err != nil {
			return err
		}
		epoch := local.Epoch
		if err := controller.sendWorker(ctx, worker, identifier, workerAction{Command: "revoke_database", Database: &placement.Database, ExpectedEpoch: &epoch}); err != nil {
			// The authority revoke below is the fencing authority. A worker that
			// cannot confirm its local revoke is logged, not trusted with a
			// lease renewal.
			log.Printf("控制面闲置释放：worker 本地撤销未确认 database=%s worker=%s err=%v", key, worker.ID, err)
		}
	}
	if authority.Binding == nil {
		placement.Phase = "idle"
		clearPending(&placement)
		return controller.save(placement)
	}
	placement.Phase = "idle_drain"
	if err := controller.save(placement); err != nil {
		return err
	}
	_, err := controller.converge(ctx, placement)
	return err
}
