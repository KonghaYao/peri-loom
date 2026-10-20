package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	controller "peri-loom/controller"
)

type Quotas struct {
	MaxDatabases      int `json:"max_databases"`
	MaxSessions       int `json:"max_sessions"`
	MaxConcurrent     int `json:"max_concurrent"`
	RequestsPerSecond int `json:"requests_per_second"`
	MaxSQLBytes       int `json:"max_sql_bytes"`
	MaxResultBytes    int `json:"max_result_bytes"`
}

type Organization struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Project struct {
	ID           string `json:"id"`
	Organization string `json:"organization"`
	Tenant       string `json:"tenant"`
	Region       string `json:"region"`
	Quotas       Quotas `json:"quotas"`
}

type Database struct {
	ID                controller.DatabaseID `json:"id"`
	Project           string                `json:"project"`
	State             string                `json:"state"`
	Encrypted         bool                  `json:"encrypted"`
	OperationID       string                `json:"operation_id"`
	RootIncarnation   string                `json:"root_incarnation,omitempty"`
	BranchIncarnation uint64                `json:"branch_incarnation,omitempty"`
}

// ProvisioningRecord is the durable identity of one fork/clone workflow. The
// stage advances only after the matching effect is durable, so a retry can tell
// which effects it still has to perform without replaying a possibly committed
// authority command. LastError holds a stable classification, never backend
// error text.
type ProvisioningRecord struct {
	RequestID string                `json:"request_id"`
	Digest    string                `json:"digest"`
	Operation string                `json:"operation"`
	Database  controller.DatabaseID `json:"database"`
	Project   string                `json:"project"`
	Source    string                `json:"source,omitempty"`
	Snapshot  string                `json:"snapshot,omitempty"`
	Name      string                `json:"name"`
	Role      string                `json:"role"`
	Stage     string                `json:"stage"`
	LastError string                `json:"last_error,omitempty"`
	// Revision is the registry revision at reservation time. It orders failed
	// records for bounded pruning; all other stages are retained indefinitely.
	Revision uint64 `json:"revision,omitempty"`
}

type APIKey struct {
	ID             string `json:"id"`
	Subject        string `json:"subject"`
	Project        string `json:"project"`
	Role           string `json:"role"`
	Digest         string `json:"digest"`
	Revoked        bool   `json:"revoked"`
	ServiceAccount bool   `json:"service_account"`
}

type Principal struct {
	KeyID             string                 `json:"key_id"`
	Subject           string                 `json:"subject"`
	Project           string                 `json:"project"`
	Role              string                 `json:"role"`
	Database          *controller.DatabaseID `json:"database,omitempty"`
	ExpiresMS         int64                  `json:"expires_ms,omitempty"`
	RootIncarnation   string                 `json:"root_incarnation,omitempty"`
	BranchIncarnation uint64                 `json:"branch_incarnation,omitempty"`
	DomainID          string                 `json:"domain_id,omitempty"`
}

type KeyDescriptor struct {
	ID              string               `json:"id"`
	ProvisionID     string               `json:"provision_id"`
	Revision        uint64               `json:"revision"`
	Namespace       controller.Namespace `json:"namespace"`
	RootIncarnation string               `json:"root_incarnation"`
	DomainID        string               `json:"domain_id"`
	DEKID           string               `json:"dek_id"`
	Cipher          string               `json:"cipher"`
	Envelope        json.RawMessage      `json:"envelope"`
	State           string               `json:"state"`
}

type State struct {
	Version                 uint32                        `json:"version"`
	Revision                uint64                        `json:"revision"`
	OperationStorageVersion uint32                        `json:"operation_storage_version,omitempty"`
	Organizations           map[string]Organization       `json:"organizations"`
	Projects                map[string]Project            `json:"projects"`
	Databases               map[string]Database           `json:"databases"`
	APIKeys                 map[string]APIKey             `json:"api_keys"`
	Keys                    map[string]KeyDescriptor      `json:"keys"`
	Operations              map[string]OperationRecord    `json:"operations,omitempty"`
	Provisionings           map[string]ProvisioningRecord `json:"provisionings,omitempty"`
	OperationSequence       uint64                        `json:"operation_sequence,omitempty"`
	OperationEvictions      uint64                        `json:"operation_evictions,omitempty"`
	// OperationsCheckpoint is the revision whose registry commit materialized the
	// complete operation view into this payload. Only the shutdown checkpoint
	// writes it, so a payload whose checkpoint equals its revision is a final
	// view a reader may trust without the operation sidecar; absent or older than
	// the revision means the payload may trail that sidecar.
	OperationsCheckpoint uint64 `json:"operations_checkpoint,omitempty"`
	OwnedRunGeneration   string `json:"owned_run_generation,omitempty"`
	OwnedRunBinding      string `json:"owned_run_binding,omitempty"`
}

type StateStore interface {
	Snapshot() (State, error)
	Update(func(*State) error) error
	Close() error
}

type Identity interface {
	Authenticate(string) (Principal, error)
	Authorize(Principal, controller.DatabaseID, string) error
	IssueToken(Principal, controller.DatabaseID, string, int64) (string, error)
}

type KMS interface {
	Seal([]byte, []byte) (json.RawMessage, error)
	Open(json.RawMessage, []byte) ([]byte, error)
}

type LeaseValidator interface {
	Validate(context.Context, controller.Binding, string) (controller.Grant, error)
}

type KeyBroker interface {
	http.Handler
	Provision(context.Context, controller.Namespace, string) (KeyDescriptor, error)
	Rewrap(context.Context, controller.Namespace) error
}

type AuditEvent struct {
	RequestID   string `json:"request_id"`
	Subject     string `json:"subject"`
	Project     string `json:"project"`
	Database    string `json:"database,omitempty"`
	Action      string `json:"action"`
	Outcome     string `json:"outcome"`
	TimestampMS int64  `json:"timestamp_ms"`
}

type Audit interface {
	Record(AuditEvent) error
	Close() error
}

// AuditClass names the admission budget an audit record is charged to. The
// budgets are separate so an unauthenticated storm cannot consume the capacity
// a dispatched request needs to settle its own record.
type AuditClass int

const (
	// AuditClassRequest is an authenticated request that holds no completion slot.
	AuditClassRequest AuditClass = iota
	// AuditClassRejection is traffic refused before dispatch, including requests
	// that never authenticated.
	AuditClassRejection
	// AuditClassCompletion is the record of a request whose capacity was reserved
	// before dispatch.
	AuditClassCompletion
)

// AuditAdmission is the bounded form of Audit. A sink that does not implement it
// is still recorded through Record; only a sink that does can reserve the
// capacity that makes a completion record survive queue pressure.
type AuditAdmission interface {
	// ReserveCompletion holds capacity for one completion record before SQL is
	// dispatched. A nil error means the slot is reserved until Record or Release.
	ReserveCompletion() (AuditReservation, error)
	// RecordClass records one event charged to the given class. A completion
	// record must go through its reservation instead.
	RecordClass(AuditEvent, AuditClass) error
	// AuditStatus reports queue depth, limits, refusals and fsync cost.
	AuditStatus() map[string]any
}

// AuditReservation is one completion slot. Record consumes it and blocks until
// the record is durable; Release returns it unused. Release after Record is a
// no-op, so either path may clean up unconditionally.
type AuditReservation interface {
	Record(AuditEvent) error
	Release()
}

type SQLUpstream interface {
	Call(context.Context, controller.Worker, string, any, any) error
}

type SessionWriterState struct {
	Session  string             `json:"session"`
	Binding  controller.Binding `json:"binding"`
	Owner    bool               `json:"owner"`
	Sequence uint64             `json:"sequence"`
}

type WriterSQLUpstream interface {
	CallWriter(context.Context, controller.Worker, string, any, any) (*SessionWriterState, error)
	WriterProfile(context.Context, controller.Worker, string) error
}

type GatewayConfig struct {
	State           StateStore
	Placements      controller.MetadataStore
	Controller      *controller.Controller
	Identity        Identity
	Upstream        SQLUpstream
	Audit           Audit
	WriterAdmission bool
	OpenObserver    *SQLOpenObserver
	RequestTimeout  time.Duration
	// WakeWaitBudget bounds how long an open waits for an idle database to
	// activate before it publishes a structured loading answer. Zero derives
	// a budget from RequestTimeout.
	WakeWaitBudget time.Duration
}
