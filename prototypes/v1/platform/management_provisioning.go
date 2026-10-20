package platform

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strings"

	controller "peri-loom/controller"
)

const (
	provisioningReserved         = "reserved"
	provisioningAuthorityApplied = "authority_applied"
	provisioningPlacementReady   = "placement_ready"
	provisioningActive           = "active"
	provisioningFailed           = "failed"
	provisioningUncertain        = "uncertain"

	// Failed attempts are evidence for request-id reuse, not resources. Keeping a
	// bounded tail prevents an invalid client loop from growing the registry while
	// every non-failed record still guards a real branch identity.
	provisioningFailureRetention = 1024
)

var errProvisioningPayloadMismatch = errors.New("provisioning request id reused with different payload")

const (
	// authority_rejected means the platform proved no branch was created,
	// either through an explicit refusal or an absent catalog entry after an
	// ambiguous acknowledgement.
	provisioningErrorAuthorityRejected  = "authority_rejected"
	provisioningErrorAuthorityUncertain = "authority_uncertain"
	provisioningErrorPlacement          = "placement_failed"
	provisioningErrorActivation         = "activation_failed"
)

// ProvisioningStatus is the public readback of one fork/clone workflow. The
// durable record is the authority for request-id reuse; RetryAllowed is derived
// from the stage and never replaces it.
type ProvisioningStatus struct {
	ProvisioningRecord
	RetryAllowed bool `json:"retry_allowed"`
}

type forkPlan struct {
	operation string
	requestID string
	digest    string
	parent    controller.DatabaseID
	child     controller.DatabaseID
	project   string
	source    string
	snapshot  string
	name      string
	role      string
}

// newForkPlan performs every check the platform can decide without reading or
// changing durable state. A request rejected here must not reserve a descriptor
// or reach the authority.
func newForkPlan(input managementRequest, path string) (forkPlan, error) {
	if err := input.Database.Validate(); err != nil {
		return forkPlan{}, errors.New("invalid source database")
	}
	if !labelPattern.MatchString(input.Name) {
		return forkPlan{}, errors.New("invalid branch name")
	}
	role := input.Role
	if role == "" {
		role = "writer"
	}
	if role != "writer" && role != "read_only" {
		return forkPlan{}, errors.New("invalid branch role")
	}
	plan := forkPlan{requestID: input.RequestID, parent: input.Database, source: input.Database.Branch, name: input.Name, role: role}
	switch path {
	case "/v1/databases/branch":
		plan.operation = "branch"
	case "/v1/databases/clone":
		plan.operation = "clone"
		plan.source = ""
		plan.snapshot = input.Snapshot
		if !labelPattern.MatchString(plan.snapshot) {
			return forkPlan{}, errors.New("invalid snapshot name")
		}
	default:
		return forkPlan{}, errors.New("unknown fork operation")
	}
	plan.child = input.Database
	plan.child.Branch = input.Name
	if plan.child.Key() == plan.parent.Key() {
		return forkPlan{}, errors.New("branch identity already reserved")
	}
	return plan, nil
}

func (plan forkPlan) canonicalDigest() string {
	// The digest covers every field that changes the authority command or the
	// resulting identity. NUL separates fields so no combination can collide with
	// another concatenation.
	canonical := strings.Join([]string{
		"provisioning_v1", plan.operation,
		plan.child.Namespace.Tenant, plan.child.Namespace.Database,
		plan.project, plan.source, plan.snapshot, plan.name, plan.role,
	}, "\x00")
	return "provisioning_v1:" + digestHex([]byte(canonical))
}

func (plan forkPlan) authorityOperation() map[string]any {
	if plan.operation == "clone" {
		return map[string]any{"operation": "clone", "snapshot": plan.snapshot, "name": plan.name}
	}
	return map[string]any{"operation": "branch", "source": plan.source, "name": plan.name}
}

func validProvisioningStage(stage string) bool {
	switch stage {
	case provisioningReserved, provisioningAuthorityApplied, provisioningPlacementReady, provisioningActive, provisioningFailed, provisioningUncertain:
		return true
	}
	return false
}

// authorityRefs is the catalog subset needed to resolve a fork whose
// acknowledgement was lost. Branch and clone mutate this catalog
// synchronously, so the branch list is an equivalent receipt: present means
// committed, absent means the command never executed.
type authorityRefs struct {
	Revision  uint64                     `json:"revision"`
	Branches  map[string]json.RawMessage `json:"branches"`
	Snapshots map[string]json.RawMessage `json:"snapshots"`
}

func (management *Management) forkAuthorityApplied(ctx context.Context, plan forkPlan) (applied, known bool) {
	// A fresh request id is required: the receipt ledger refuses the same id
	// with a different payload, which is exactly what a probe would look like
	// after the fork command committed.
	requestID, err := controller.RandomID()
	if err != nil {
		return false, false
	}
	var refs authorityRefs
	if err := management.config.Authority.Authority(ctx, requestID, map[string]any{
		"command": "maintenance", "request_id": requestID, "database": plan.parent,
		"operation": map[string]any{"operation": "refs"},
	}, &refs); err != nil {
		return false, false
	}
	if refs.Branches == nil {
		return false, false
	}
	_, applied = refs.Branches[plan.name]
	return applied, true
}

// reserveFork atomically records the request identity and reserves the child
// descriptor before any authority call. Replaying the same request id and
// digest continues where the durable record stopped; a different digest is
// refused before quota or identity can be touched.
func (management *Management) reserveFork(plan forkPlan) (ProvisioningRecord, error) {
	var record ProvisioningRecord
	err := management.config.State.Update(func(state *State) error {
		source, exists := state.Databases[plan.parent.Key()]
		if !exists || source.State == "deleted" || source.Project != plan.project {
			return errors.New("source database missing")
		}
		project, exists := state.Projects[plan.project]
		if !exists {
			return errors.New("project missing")
		}
		if state.Provisionings == nil {
			state.Provisionings = make(map[string]ProvisioningRecord)
		}
		existing, hasRecord := state.Provisionings[plan.requestID]
		child, hasChild := state.Databases[plan.child.Key()]
		if hasRecord {
			if existing.Digest != plan.digest || existing.Database != plan.child || existing.Project != plan.project || existing.Operation != plan.operation || existing.Source != plan.source || existing.Snapshot != plan.snapshot || existing.Name != plan.name || existing.Role != plan.role {
				return errProvisioningPayloadMismatch
			}
			if existing.Stage == provisioningActive {
				if !hasChild || child.State == "deleted" {
					return errors.New("database identity unavailable")
				}
				record = existing
				return nil
			}
			if hasChild {
				if child.OperationID != plan.requestID || child.Project != plan.project || child.State == "deleted" {
					return errors.New("branch identity already reserved")
				}
			} else if err := reserveForkDescriptor(state, plan, source, project); err != nil {
				return err
			}
			if existing.Stage == provisioningFailed {
				existing.Stage = provisioningReserved
				existing.LastError = ""
				state.Provisionings[plan.requestID] = existing
			}
			record = existing
			return nil
		}
		if hasChild {
			if child.OperationID != plan.requestID || child.Project != plan.project || child.State == "deleted" {
				return errors.New("branch identity already reserved")
			}
			// A descriptor written before provisioning records existed is adopted
			// by the same request identity. If it already left provisioning the old
			// workflow completed, so the record becomes terminal without replaying
			// the authority command. The authority command itself is idempotent by
			// request id for the still-provisioning case.
		} else if err := reserveForkDescriptor(state, plan, source, project); err != nil {
			return err
		}
		pruneFailedProvisionings(state, plan.requestID)
		record = newProvisioningRecord(plan, state.Revision)
		if hasChild && child.State != "provisioning" {
			record.Stage = provisioningActive
		}
		state.Provisionings[plan.requestID] = record
		return nil
	})
	return record, err
}

func newProvisioningRecord(plan forkPlan, revision uint64) ProvisioningRecord {
	return ProvisioningRecord{
		RequestID: plan.requestID, Digest: plan.digest, Operation: plan.operation,
		Database: plan.child, Project: plan.project, Source: plan.source,
		Snapshot: plan.snapshot, Name: plan.name, Role: plan.role,
		Stage: provisioningReserved, Revision: revision,
	}
}

func reserveForkDescriptor(state *State, plan forkPlan, source Database, project Project) error {
	count := 0
	for _, database := range state.Databases {
		if database.Project == project.ID && database.State != "deleted" {
			count++
		}
	}
	if count >= project.Quotas.MaxDatabases {
		return errDatabaseQuotaExceeded
	}
	// The child gets a fresh incarnation binding at activation. Copying the
	// parent's would claim the wrong root while the branch is still provisioning.
	state.Databases[plan.child.Key()] = Database{ID: plan.child, Project: plan.project, State: "provisioning", Encrypted: source.Encrypted, OperationID: plan.requestID}
	return nil
}

// pruneFailedProvisionings keeps the failed-record tail bounded. Active and
// unresolved records are never evicted because they are the durable proof that
// a request id belongs to one payload.
func pruneFailedProvisionings(state *State, keep string) {
	failed := make([]ProvisioningRecord, 0)
	for id, record := range state.Provisionings {
		if id != keep && record.Stage == provisioningFailed {
			failed = append(failed, record)
		}
	}
	if len(failed) < provisioningFailureRetention {
		return
	}
	sort.Slice(failed, func(first, second int) bool { return failed[first].Revision < failed[second].Revision })
	for _, record := range failed[:len(failed)-provisioningFailureRetention+1] {
		delete(state.Provisionings, record.RequestID)
	}
}

func (management *Management) setProvisioningStage(plan forkPlan, stage, lastError string) error {
	return management.config.State.Update(func(state *State) error {
		record, exists := state.Provisionings[plan.requestID]
		if !exists || record.Digest != plan.digest {
			return errors.New("provisioning record missing or changed")
		}
		record.Stage, record.LastError = stage, lastError
		state.Provisionings[plan.requestID] = record
		return nil
	})
}

// recordProvisioningFailure persists the classified outcome. release is true
// only when the failure proves the authority did not execute: the reservation
// then returns to the quota pool and the descriptor is withdrawn. An uncertain
// failure keeps both, because a branch may exist and a guessed rollback would
// make the platform lie about it.
func (management *Management) recordProvisioningFailure(plan forkPlan, stage, code string, release bool) {
	err := management.config.State.Update(func(state *State) error {
		record, exists := state.Provisionings[plan.requestID]
		if !exists || record.Digest != plan.digest {
			return errors.New("provisioning record missing or changed")
		}
		record.Stage, record.LastError = stage, code
		state.Provisionings[plan.requestID] = record
		if release {
			if child, exists := state.Databases[plan.child.Key()]; exists && child.OperationID == plan.requestID && child.State == "provisioning" {
				delete(state.Databases, plan.child.Key())
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("fork 状态记录失败：request_id=%s 阶段=%s 错误=%v", plan.requestID, code, err)
	}
}

// provisioningNotExecuted separates a refusal that proves no mutation from a
// transport failure that may have committed. controller.RPCError.Rejected is
// set only for a 4xx or an explicit precondition refusal; an ambiguous failure
// carries neither proof.
func provisioningNotExecuted(err error) bool {
	var rejected *controller.RPCError
	return errors.As(err, &rejected) && rejected.Rejected && !rejected.Ambiguous
}
