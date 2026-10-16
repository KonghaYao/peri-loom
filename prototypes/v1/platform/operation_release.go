package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

// IsolationFence is what the data plane attests about one branch right now. The
// platform never constructs storage identity itself: it only accepts the authority's
// own report, so the fencing argument rests on the component that owns the epochs.
type IsolationFence struct {
	Mode  string
	Epoch uint64
	// Bound is set when the authority still reports a live binding for the branch.
	Bound bool
}

// fencedAuthorityMode lists the ownership modes that prove no worker holds the
// branch. Everything else (writer, read_only, acquiring, draining) is still an
// owner and must keep the namespace isolated.
func fencedAuthorityMode(mode string) bool {
	return mode == "detached" || mode == "revoked"
}

func (fence IsolationFence) fenced() bool { return !fence.Bound && fencedAuthorityMode(fence.Mode) }

// IsolationFenceVerifier reads the authority's attestation for one database. The
// release path depends on it, and a nil verifier refuses instead of assuming.
type IsolationFenceVerifier interface {
	VerifyIsolationFence(context.Context, controller.DatabaseID) (IsolationFence, error)
}

type authorityFenceAction struct {
	Command  string                `json:"command"`
	Database controller.DatabaseID `json:"database"`
}

// authorityIsolationFence asks the authority for its own database state. It is the
// same read the placement reconciler already uses, so no new data-plane interface is
// required for the Go side to prove fencing.
type authorityIsolationFence struct {
	transport controller.ControlTransport
}

func (fence authorityIsolationFence) VerifyIsolationFence(ctx context.Context, database controller.DatabaseID) (IsolationFence, error) {
	if fence.transport == nil || database.Validate() != nil {
		return IsolationFence{}, errors.New("authority fence unavailable")
	}
	identifier, err := controller.RandomID()
	if err != nil {
		return IsolationFence{}, errors.New("authority fence unavailable")
	}
	var actual controller.Actual
	if err := fence.transport.Authority(ctx, identifier, authorityFenceAction{Command: "get_database_state", Database: database}, &actual); err != nil {
		return IsolationFence{}, errors.New("authority fence unavailable")
	}
	if actual.Database != database || actual.Binding != nil && (actual.Binding.Database != database || actual.Binding.Epoch != actual.Epoch) {
		return IsolationFence{}, errors.New("authority fence identity mismatch")
	}
	return IsolationFence{Mode: actual.Mode, Epoch: actual.Epoch, Bound: actual.Binding != nil}, nil
}

// Distinguishable refusals, so an operator can tell which proof is missing.
var (
	errReleaseNotRetired       = errors.New("isolation release requires completed local retirement")
	errReleaseProofMissing     = errors.New("isolation release requires owned worker stop proof")
	errReleaseFenceUnavailable = errors.New("isolation release requires data plane fencing evidence")
)

// releaseIsolation ends a durable quarantine for one database after every proof is
// present: each affected worker's owned stop, completed local retirement, and the
// authority attesting that no worker holds the branch. What it means to the user:
// the platform will serve that database again on a fresh epoch, and whatever the
// unknown operation left behind stays in the data. It does not claim the old data is
// intact, and it never releases on a timeout, a restart, or a partial proof.
func (management *Management) releaseIsolation(ctx context.Context, principal Principal, input managementRequest) (any, error) {
	if input.Database.Validate() != nil || management.config.Identity.Authorize(principal, input.Database, "manage") != nil {
		return nil, errManagementForbidden
	}
	unlock := lockOperationNamespace(input.Database.Namespace)
	defer unlock()
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	workers := map[string]bool{}
	pending := false
	for _, record := range state.Operations {
		if record.Database.Namespace != input.Database.Namespace || record.State != "quarantined" {
			continue
		}
		if record.ReleaseRequestID != "" {
			continue
		}
		if record.Decision != "retain_isolation_new_database_required" || !record.LocalResourcesRetired || record.RetirementPending {
			pending = true
			continue
		}
		workers[record.Worker] = true
	}
	if pending {
		return nil, errReleaseNotRetired
	}
	if len(workers) == 0 {
		// Nothing left to release: either the namespace carries no quarantine at all,
		// or every quarantine was already released. Both are terminal, and neither
		// authorizes a new one.
		if _, isolated := isolatedOperation(state, input.Database, false); isolated {
			return nil, errReleaseNotRetired
		}
		return management.operations(principal, input, "/v1/databases/operations")
	}
	if management.config.StopProof == nil {
		return nil, errReleaseProofMissing
	}
	if management.config.Fence == nil {
		return nil, errReleaseFenceUnavailable
	}
	for worker := range workers {
		if err := management.config.StopProof.VerifyOwnedWorkerStopped(ctx, worker); err != nil {
			return nil, errReleaseProofMissing
		}
	}
	fence, err := management.config.Fence.VerifyIsolationFence(ctx, input.Database)
	if err != nil || !fence.fenced() {
		return nil, errReleaseFenceUnavailable
	}
	released := 0
	if err := management.config.State.Update(func(candidate *State) error {
		for key, record := range candidate.Operations {
			if record.Database.Namespace != input.Database.Namespace || record.State != "quarantined" || record.ReleaseRequestID != "" {
				continue
			}
			if !workers[record.Worker] || !record.LocalResourcesRetired {
				return errReleaseNotRetired
			}
			record.ReleaseRequestID, record.ReleaseAuthorityMode, record.ReleaseEpoch = input.RequestID, fence.Mode, fence.Epoch
			candidate.Operations[key] = record
			released++
		}
		if released == 0 {
			return errReleaseNotRetired
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return management.operations(principal, input, "/v1/databases/operations")
}
