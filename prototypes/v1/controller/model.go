package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

type Namespace struct {
	Tenant   string `json:"tenant"`
	Database string `json:"database"`
}

type DatabaseID struct {
	Namespace Namespace `json:"namespace"`
	Branch    string    `json:"branch"`
}

var componentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func (database DatabaseID) Validate() error {
	if !componentPattern.MatchString(database.Namespace.Tenant) || !componentPattern.MatchString(database.Namespace.Database) || !componentPattern.MatchString(database.Branch) {
		return fmt.Errorf("invalid database identifier")
	}
	return nil
}

func (database DatabaseID) Key() string {
	return database.Namespace.Tenant + "/" + database.Namespace.Database + "/" + database.Branch
}

type Binding struct {
	Database          DatabaseID `json:"database"`
	RootIncarnation   string     `json:"root_incarnation"`
	AuthorityTerm     uint64     `json:"authority_term"`
	BranchIncarnation uint64     `json:"branch_incarnation"`
	Epoch             uint64     `json:"epoch"`
	WorkerID          string     `json:"worker_id"`
	LeaseID           string     `json:"lease_id"`
	GrantID           string     `json:"grant_id"`
	RenewalSequence   uint64     `json:"renewal_sequence"`
	Role              string     `json:"role"`
}

type Grant struct {
	Binding        Binding `json:"binding"`
	RemainingTTLMS uint64  `json:"remaining_ttl_ms"`
}

type Actual struct {
	Database       DatabaseID `json:"database"`
	Mode           string     `json:"mode"`
	Binding        *Binding   `json:"binding"`
	Epoch          uint64     `json:"epoch"`
	RemainingTTLMS uint64     `json:"remaining_ttl_ms"`
	// Resident is present only in the worker's answer. The authority has no
	// instance to describe, so nil means "not observed" rather than "absent".
	Resident *bool `json:"resident,omitempty"`
}

type Worker struct {
	ID              string `json:"id"`
	Address         string `json:"address"`
	Region          string `json:"region"`
	Capacity        int    `json:"capacity"`
	Load            int    `json:"load"`
	Healthy         bool   `json:"healthy"`
	LastHeartbeatMS int64  `json:"last_heartbeat_ms"`
	Draining        bool   `json:"draining"`
}

type Placement struct {
	Database       DatabaseID      `json:"database"`
	DesiredWorker  string          `json:"desired_worker"`
	Region         string          `json:"region,omitempty"`
	Epoch          uint64          `json:"epoch"`
	LeaseID        string          `json:"lease_id"`
	TTLMS          uint64          `json:"ttl_ms"`
	DesiredState   string          `json:"desired_state"`
	Role           string          `json:"role"`
	Phase          string          `json:"phase"`
	PendingID      string          `json:"pending_id,omitempty"`
	PendingCommand json.RawMessage `json:"pending_command,omitempty"`
	Grant          *Grant          `json:"grant,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	// SuspendReason separates an administrator's decision from an idle release.
	// Without it a request could not tell a database it may wake from one an
	// operator deliberately stopped.
	SuspendReason string `json:"suspend_reason,omitempty"`
	// WakePolicy is the durable answer to "may a request activate this
	// database?". An administrator suspension and a failed activation are
	// manual; only an idle release is automatic.
	WakePolicy string `json:"wake_policy,omitempty"`
	// IdleSinceMS is when the worker reported no resident instance. It is an
	// operator-facing fact and never an input to fencing.
	IdleSinceMS int64 `json:"idle_since_ms,omitempty"`
}

const (
	SuspendReasonAdmin   = "admin"
	SuspendReasonIdle    = "idle"
	SuspendReasonFailure = "failure"

	WakePolicyAuto   = "auto"
	WakePolicyManual = "manual"

	// ActivationIdle, ActivationWaking, ActivationActive and ActivationDraining
	// are the externally visible lifecycle derived from the durable phase. The
	// phase keeps the protocol detail (which durable intent exists); this is the
	// single name a request or an operator acts on.
	ActivationIdle      = "idle"
	ActivationWaking    = "waking"
	ActivationProvision = "provisioning"
	ActivationActive    = "active"
	ActivationDraining  = "draining"
	ActivationSuspended = "suspended"
	ActivationRefused   = "refused"
	ActivationFailed    = "failed"
)

// ActivationState collapses the durable phases into the S02 lifecycle. The
// phase stays the protocol truth: Wake derives its state from here instead of
// persisting a second copy that could disagree after a crash.
func (placement Placement) ActivationState() string {
	switch placement.Phase {
	case "idle":
		return ActivationIdle
	case "active", "renew_intent", "renew_pending", "renew_worker_pending":
		return ActivationActive
	case "grant_intent", "grant_pending", "acquire_pending":
		if placement.SuspendReason == SuspendReasonIdle {
			return ActivationWaking
		}
		return ActivationProvision
	case "migrate_drain", "suspend_drain", "idle_drain", "revoke_pending":
		return ActivationDraining
	case "suspended":
		return ActivationSuspended
	default:
		return ""
	}
}

// AutoWakeable reports whether a SQL request may activate this placement. It is
// deliberately narrow: an administrator suspension, a failed activation, and a
// legacy suspended record without a reason stay manual.
func (placement Placement) AutoWakeable() bool {
	if placement.WakePolicy != WakePolicyAuto || placement.SuspendReason != SuspendReasonIdle {
		return false
	}
	switch placement.Phase {
	case "idle", "idle_drain", "revoke_pending", "suspended",
		"grant_intent", "grant_pending", "acquire_pending":
		return true
	}
	return false
}

// Transient reports whether an unroutable placement is expected to become
// routable through the controller's own convergence, without an operator.
func (placement Placement) Transient() bool {
	switch placement.Phase {
	case "grant_intent", "grant_pending", "acquire_pending", "migrate_drain", "idle_drain":
		return true
	case "idle", "revoke_pending":
		return placement.AutoWakeable()
	}
	return false
}

// ActivationStatus is the durable identity of one wake attempt. Waiters that
// leave the budget keep the same operation ID to inspect or retry the open,
// and they never resend SQL: activation itself dispatches no statement.
type ActivationStatus struct {
	Database    DatabaseID `json:"database"`
	State       string     `json:"state"`
	OperationID string     `json:"operation_id"`
	Epoch       uint64     `json:"epoch,omitempty"`
	Waiters     int        `json:"waiters,omitempty"`
}

// ErrActivationRefused is returned for a placement whose wake policy forbids a
// request-driven activation. It is a policy answer, not a retryable failure.
var ErrActivationRefused = errors.New("database activation refused")

type Metadata struct {
	Version    uint32               `json:"version"`
	Revision   uint64               `json:"revision"`
	Epoch      uint64               `json:"epoch"`
	Workers    map[string]Worker    `json:"workers"`
	Placements map[string]Placement `json:"placements"`
}

type MetadataStore interface {
	Snapshot() (Metadata, error)
	Update(func(*Metadata) error) error
	Close() error
}

type ControlTransport interface {
	Authority(context.Context, string, any, any) error
	Worker(context.Context, Worker, string, any, any) error
}

type ReconcileConfig struct {
	LeaseTTL         time.Duration
	HeartbeatTimeout time.Duration
	Interval         time.Duration
	// MaxConcurrentOperations bounds management operations that hold one
	// database lane (place, migrate, suspend, resume). Reconcile uses its own
	// pool so a slow management call cannot starve lease renewal.
	MaxConcurrentOperations int
	// ReconcileConcurrency bounds per-database reconcile work across passes.
	ReconcileConcurrency int
	// ControlRPCTimeout bounds one authority or worker control call. A call
	// that exceeds it is left to the next reconcile pass; ambiguous mutators
	// are never retried inside the call.
	ControlRPCTimeout time.Duration
	// ActivationTimeout bounds one request-driven wake, including the cold
	// load the worker performs while acquiring. It is longer than a control
	// call because the caller is allowed to keep waiting in the background
	// after its HTTP request stopped waiting.
	ActivationTimeout time.Duration
}
