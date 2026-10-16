package platform

import (
	"context"
	"errors"
	"fmt"
	"sort"

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
	errReleaseScopeMismatch    = errors.New("isolation release requires the named database to carry the quarantine it ends")
)

// serviceableBranch reports whether the platform could serve this database again
// the moment the namespace isolation ends. A branch that was never activated, or
// that is no longer active, cannot serve again without a fresh activation, and a
// fresh activation is a new grant on a new epoch: it needs no fencing proof from
// this release.
func serviceableBranch(database Database) bool {
	return database.Encrypted && database.State == "active" && database.RootIncarnation != "" && database.BranchIncarnation > 0
}

// releaseProofScope is every branch the release must prove fenced before it may end
// the isolation of a namespace. The gate it opens is namespace-wide, so the proof
// scope is the namespace and not just the branch that happens to carry a quarantine
// record: a sibling branch that keeps its owner would resume serving on the very
// epoch the unproven write happened in.
func releaseProofScope(state State, input controller.DatabaseID) []controller.DatabaseID {
	targets := map[string]controller.DatabaseID{}
	for _, record := range state.Operations {
		if record.Database.Namespace == input.Namespace && record.State == "quarantined" && record.ReleaseRequestID == "" {
			targets[record.Database.Key()] = record.Database
		}
	}
	for _, database := range state.Databases {
		if database.ID.Namespace == input.Namespace && serviceableBranch(database) {
			targets[database.ID.Key()] = database.ID
		}
	}
	ordered := make([]string, 0, len(targets))
	for key := range targets {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	scope := make([]controller.DatabaseID, 0, len(ordered))
	for _, key := range ordered {
		scope = append(scope, targets[key])
	}
	return scope
}

// releaseIsolation ends a durable quarantine for one namespace after every proof is
// present: each affected worker's owned stop, completed local retirement, and the
// authority attesting that no worker holds any branch the release puts back in
// service. What it means to the user: the platform will serve that namespace again
// on fresh epochs, and whatever the unknown operation left behind stays in the data.
// It does not claim the old data is intact, and it never releases on a timeout, a
// restart, or a partial proof.
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
	named := false
	for _, record := range state.Operations {
		if record.Database.Namespace != input.Database.Namespace || record.State != "quarantined" {
			continue
		}
		if record.ReleaseRequestID != "" {
			continue
		}
		if record.Decision != "retain_isolation_new_database_required" || !record.LocalResourcesRetired || record.RetirementPending {
			return nil, errReleaseNotRetired
		}
		if record.Database == input.Database {
			named = true
		}
		workers[record.Worker] = true
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
	if !named {
		// The request names the database it releases. A namespace release that names a
		// branch carrying none of the quarantines would record that request id as the
		// proof of branches the operator never asked about, and read as a release of the
		// database that is still isolated.
		return nil, errReleaseScopeMismatch
	}
	scope := releaseProofScope(state, input.Database)
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
	// One attestation per branch, never one branch's attestation reused for the rest:
	// each epoch is the epoch of the branch the record names. The reads run while the
	// namespace lock is held and the quarantine is still recorded, so no activation or
	// rebind of this namespace can be granted between a proof and the durability write
	// that relies on it.
	proofs := make(map[string]IsolationFence, len(scope))
	for _, database := range scope {
		fence, err := management.config.Fence.VerifyIsolationFence(ctx, database)
		if err != nil || !fence.fenced() {
			return nil, fmt.Errorf("%w: %s", errReleaseFenceUnavailable, database.Key())
		}
		proofs[database.Key()] = fence
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
			fence, proven := proofs[record.Database.Key()]
			if !proven {
				// A quarantine appeared after the proof scope was read. Nothing was proven
				// about its branch, so the release refuses instead of recording a proof it
				// does not hold.
				return errReleaseFenceUnavailable
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
