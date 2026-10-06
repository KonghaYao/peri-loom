package sdk

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
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

var label = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var component = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func (database DatabaseID) Validate() error {
	if !component.MatchString(database.Namespace.Tenant) || !component.MatchString(database.Namespace.Database) || !component.MatchString(database.Branch) {
		return errors.New("invalid database identifier")
	}
	return nil
}

type Options struct {
	Timeout          time.Duration
	MaxRequestBytes  int64
	MaxResponseBytes int64
	RootCAs          *x509.CertPool
}

type Error struct {
	Code               string
	RequestID          string
	OutcomeUnknown     bool
	ExecutionCompleted bool
	MayHaveCommitted   bool
}

func (failure *Error) Error() string {
	return "peri: " + failure.Code + " (request " + failure.RequestID + ")"
}

func RandomID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", errors.New("request identifier unavailable")
	}
	return hex.EncodeToString(bytes[:]), nil
}

type ManagementRequest struct {
	RequestID      string          `json:"request_id"`
	Database       *DatabaseID     `json:"database,omitempty"`
	Project        string          `json:"project,omitempty"`
	Region         string          `json:"region,omitempty"`
	Role           string          `json:"role,omitempty"`
	Name           string          `json:"name,omitempty"`
	Snapshot       string          `json:"snapshot,omitempty"`
	WorkerID       string          `json:"worker_id,omitempty"`
	Sequence       uint64          `json:"sequence,omitempty"`
	TimestampMS    uint64          `json:"timestamp_ms,omitempty"`
	SafetyWindowMS uint64          `json:"safety_window_ms,omitempty"`
	Organization   string          `json:"organization,omitempty"`
	ID             string          `json:"id,omitempty"`
	Subject        string          `json:"subject,omitempty"`
	ServiceAccount bool            `json:"service_account,omitempty"`
	ExpiresMS      int64           `json:"expires_ms,omitempty"`
	Quotas         json.RawMessage `json:"quotas,omitempty"`
}

type envelope struct {
	Version   int             `json:"version"`
	RequestID string          `json:"request_id"`
	OK        *bool           `json:"ok"`
	Result    json.RawMessage `json:"result"`
	Error     *struct {
		Code               string `json:"code"`
		ExecutionCompleted bool   `json:"execution_completed,omitempty"`
		MayHaveCommitted   bool   `json:"may_have_committed,omitempty"`
	} `json:"error"`
}

func safeCode(code string) string {
	switch code {
	case "sql_error":
		return code
	case "outcome_unknown", "session_moved", "session_unusable", "invalid_request", "invalid_session", "unauthenticated", "unauthorized", "forbidden", "quota_exceeded", "request_limit", "result_limit", "sql_limit", "session_busy", "upstream_rejected", "upstream_unavailable", "placement_unavailable", "metadata_unavailable", "audit_unavailable", "gateway_closing", "operation_incomplete", "token_binding_mismatch", "readonly_placement_required":
		return code
	default:
		return "remote_error"
	}
}
