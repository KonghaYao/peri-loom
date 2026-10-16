package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

type WorkerStopVerifier interface {
	VerifyOwnedWorkerStopped(context.Context, string) error
}

func (management *Management) retireLocal(ctx context.Context, principal Principal, input managementRequest) (any, error) {
	if input.Database.Validate() != nil || management.config.Identity.Authorize(principal, input.Database, "manage") != nil {
		return nil, errors.New("forbidden")
	}
	if management.config.StopProof == nil || management.config.Resources == nil {
		return nil, errors.New("verified owned worker stop proof unavailable")
	}
	unlock := lockOperationNamespace(input.Database.Namespace)
	defer unlock()
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	workers := map[string]bool{}
	decisions := map[string]OperationStatus{}
	for _, record := range state.Operations {
		if record.Database.Namespace == input.Database.Namespace && record.State == "in_flight" {
			return nil, errors.New("local operation still active; retain isolation")
		}
		if record.Database.Namespace == input.Database.Namespace && record.State == "quarantined" {
			if record.Decision != "retain_isolation_new_database_required" {
				return nil, errors.New("retain isolation decision required")
			}
			workers[record.Worker] = true
			if previous, exists := decisions[record.Worker]; !exists || record.OperationID < previous.OperationID {
				decisions[record.Worker] = record.OperationStatus
			}
		}
	}
	if len(workers) == 0 {
		return nil, ErrDatabaseQuarantined
	}
	for worker := range workers {
		if err := management.config.StopProof.VerifyOwnedWorkerStopped(ctx, worker); err != nil {
			return nil, errors.New("owned worker stop not proven")
		}
	}
	if err := management.config.State.Update(func(state *State) error {
		for key, record := range state.Operations {
			if record.Database.Namespace == input.Database.Namespace && record.State == "in_flight" {
				return errors.New("local operation still active; retain isolation")
			}
			if record.Database.Namespace == input.Database.Namespace && workers[record.Worker] && operationNeedsRecovery(record) {
				record.State, record.CleanupOutcome = "quarantined", "unknown"
				record.Decision, record.DecisionRequestID = decisions[record.Worker].Decision, decisions[record.Worker].DecisionRequestID
			}
			if record.Database.Namespace == input.Database.Namespace && record.State == "quarantined" && !record.LocalResourcesRetired {
				if !workers[record.Worker] {
					return errors.New("isolation changed during proof")
				}
				record.RetirementPending = true
				state.Operations[key] = record
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := management.config.Resources.retireLocalResources(ctx, input.Database.Namespace, workers, input.RequestID); err != nil {
		return nil, err
	}
	return management.operations(principal, input, "/v1/databases/operations")
}

func (gateway *Gateway) retireLocalResources(ctx context.Context, namespace controller.Namespace, workers map[string]bool, requestID string) error {
	gateway.operationMu.Lock()
	defer gateway.operationMu.Unlock()
	state, err := gateway.config.State.Snapshot()
	if err != nil {
		return err
	}
	gateway.mu.Lock()
	sessions := []*gatewaySession{}
	for _, session := range gateway.sessions {
		if session.database.Namespace == namespace && workers[session.worker.ID] {
			sessions = append(sessions, session)
		}
	}
	gateway.mu.Unlock()
	locked := []*gatewaySession{}
	defer func() {
		for _, session := range locked {
			session.mu.Unlock()
		}
	}()
	for _, session := range sessions {
		if ctx.Err() != nil || !session.mu.TryLock() {
			return errors.New("local session still active; retain isolation")
		}
		locked = append(locked, session)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, session := range sessions {
		gateway.removeSession(session)
	}
	gateway.mu.Lock()
	released := map[string]*admissionPermit{}
	for key, record := range state.Operations {
		if record.Database.Namespace == namespace && (record.RetirementPending || record.LocalResourcesRetired) && workers[record.Worker] {
			if permit := gateway.operationHolds[key]; permit != nil {
				released[key] = permit
				releaseOperationPermit(permit)
				delete(gateway.operationHolds, key)
			}
		}
	}
	gateway.mu.Unlock()
	err = gateway.config.State.Update(func(candidate *State) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		for key, record := range candidate.Operations {
			if record.Database.Namespace == namespace && record.RetirementPending && workers[record.Worker] {
				record.LocalResourcesRetired, record.RetirementRequestID, record.RetirementPending = true, requestID, false
				candidate.Operations[key] = record
			}
		}
		return nil
	})
	if err != nil {
		gateway.mu.Lock()
		for _, session := range sessions {
			session.closed = false
			gateway.sessions[session.public] = session
			gateway.admission.mu.Lock()
			gateway.admission.projects[session.project].sessions++
			gateway.admission.mu.Unlock()
		}
		for key, permit := range released {
			permit.gate.mu.Lock()
			project := permit.gate.projects[permit.project]
			project.unknown++
			// The lane is restored only when the permit still held it: an
			// unresolved close hands the session-close lane back on purpose.
			if !permit.laneReleased {
				if permit.control {
					project.control++
				} else {
					project.active++
					if permit.owner {
						project.ownerActive++
					}
				}
			}
			if permit.session && !permit.keepSession {
				project.sessions++
			}
			permit.retained = true
			permit.finished = false
			permit.gate.mu.Unlock()
			gateway.operationHolds[key] = permit
		}
		gateway.mu.Unlock()
		return err
	}
	return nil
}
