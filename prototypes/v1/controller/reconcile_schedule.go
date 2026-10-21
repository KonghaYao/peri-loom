package controller

import (
	"context"
	"time"
)

// databaseLane serializes placement mutations for one database. Lanes are
// independent, so a control or data RPC for a slow database cannot delay
// another database's reconcile or renewal work. The controller-wide mutex
// below only guards lane lookup and retirement; it is never held across an RPC
// or a metadata commit.
type databaseLane struct {
	token chan struct{}
	refs  int
}

func newDatabaseLane() *databaseLane {
	return &databaseLane{token: make(chan struct{}, 1)}
}

func (controller *Controller) enterDatabase(ctx context.Context, key string) (func(), error) {
	controller.lanesMutex.Lock()
	lane := controller.lanes[key]
	if lane == nil {
		lane = newDatabaseLane()
		controller.lanes[key] = lane
	}
	lane.refs++
	controller.lanesMutex.Unlock()
	select {
	case lane.token <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-lane.token
			controller.retireLane(key, lane)
			return nil, err
		}
		return func() {
			<-lane.token
			controller.retireLane(key, lane)
		}, nil
	case <-ctx.Done():
		controller.retireLane(key, lane)
		return nil, ctx.Err()
	}
}

func (controller *Controller) retireLane(key string, lane *databaseLane) {
	controller.lanesMutex.Lock()
	defer controller.lanesMutex.Unlock()
	lane.refs--
	if lane.refs == 0 && controller.lanes[key] == lane {
		delete(controller.lanes, key)
	}
}

func (controller *Controller) enterSlot(ctx context.Context, slots chan struct{}) (func(), error) {
	select {
	case slots <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-slots
			return nil, err
		}
		return func() { <-slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// enterDatabaseOperation serializes one management operation for the database
// and then charges it to the shared management budget. Acquiring the lane first
// keeps queued calls for the same database from consuming slots; slot holders
// never wait for a lane they do not already own, so this order cannot cycle.
func (controller *Controller) enterDatabaseOperation(ctx context.Context, key string) (func(), error) {
	releaseLane, err := controller.enterDatabase(ctx, key)
	if err != nil {
		return nil, err
	}
	releaseSlot, err := controller.enterSlot(ctx, controller.operations)
	if err != nil {
		releaseLane()
		return nil, err
	}
	return func() {
		releaseSlot()
		releaseLane()
	}, nil
}

func (controller *Controller) enterReconcileSlot(ctx context.Context) (func(), error) {
	return controller.enterSlot(ctx, controller.reconciliations)
}

// Renewal deadlines are scheduling hints, not durable facts: they rank active
// databases within a pass. A restart starts with no hints, which makes every
// active placement an immediate observation candidate.
func (controller *Controller) recordRenewalDeadline(key string, deadline time.Time) {
	controller.scheduleMutex.Lock()
	controller.renewals[key] = deadline
	controller.scheduleMutex.Unlock()
}

func (controller *Controller) clearRenewalDeadline(key string) {
	controller.scheduleMutex.Lock()
	delete(controller.renewals, key)
	controller.scheduleMutex.Unlock()
}

func (controller *Controller) renewalDeadline(key string) (time.Time, bool) {
	controller.scheduleMutex.Lock()
	defer controller.scheduleMutex.Unlock()
	deadline, exists := controller.renewals[key]
	return deadline, exists
}

// renewalDue converts an observed remaining TTL into the next scheduling hint.
// The maximum lease bounds the conversion so an unvalidated authority value
// cannot overflow the duration.
func renewalDue(remainingMS uint64) time.Time {
	if remainingMS > 60000 {
		remainingMS = 60000
	}
	return time.Now().Add(time.Duration(remainingMS) * time.Millisecond / 2)
}

// recordSettledPlacement refreshes the hint after converge installs or revokes
// a grant. The observation path records a tighter deadline from the remaining
// TTLs, so only actual phase changes update the hint here.
func (controller *Controller) recordSettledPlacement(placement Placement, previous string) {
	key := placement.Database.Key()
	if placement.Phase != "active" {
		controller.clearRenewalDeadline(key)
		return
	}
	if previous == "active" {
		return
	}
	ttl := time.Duration(placement.TTLMS) * time.Millisecond
	controller.recordRenewalDeadline(key, time.Now().Add(ttl/2))
}
