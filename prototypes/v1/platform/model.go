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
	Version            uint32                     `json:"version"`
	Revision           uint64                     `json:"revision"`
	Organizations      map[string]Organization    `json:"organizations"`
	Projects           map[string]Project         `json:"projects"`
	Databases          map[string]Database        `json:"databases"`
	APIKeys            map[string]APIKey          `json:"api_keys"`
	Keys               map[string]KeyDescriptor   `json:"keys"`
	Operations         map[string]OperationRecord `json:"operations,omitempty"`
	OperationSequence  uint64                     `json:"operation_sequence,omitempty"`
	OperationEvictions uint64                     `json:"operation_evictions,omitempty"`
	OwnedRunGeneration string                     `json:"owned_run_generation,omitempty"`
	OwnedRunBinding    string                     `json:"owned_run_binding,omitempty"`
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
}
