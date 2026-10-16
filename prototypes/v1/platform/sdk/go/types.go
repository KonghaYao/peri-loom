package sdk

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"peri-loom/platform/sqlcontract"
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
	// ConnectionReuse 显式启用外部连接复用（默认 false：逐请求新建连接）。
	// 复用只改变连接存活期；传输层失败仍以 outcome_unknown 结束，绝不自动重发。
	ConnectionReuse bool
	Timeout          time.Duration
	MaxRequestBytes  int64
	MaxResponseBytes int64
	RootCAs          *x509.CertPool
}

type Error struct {
	Code               string
	Message            string
	RequestID          string
	OutcomeUnknown     bool
	ExecutionCompleted bool
	MayHaveCommitted   bool
	OperationID        string
	Stage              string
	BusinessOutcome    string
	CleanupOutcome     string
	ExecutionProven    bool
	Ambiguous          bool
	Rejected           bool
	RetryAllowed       bool
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
	OperationID    string          `json:"operation_id,omitempty"`
	Decision       string          `json:"decision,omitempty"`
	Quotas         json.RawMessage `json:"quotas,omitempty"`
}

type envelope struct {
	Version   int                  `json:"version"`
	RequestID string               `json:"request_id"`
	OK        *bool                `json:"ok"`
	Result    json.RawMessage      `json:"result"`
	Error     *sqlcontract.Failure `json:"error"`
}

// safeCode keeps the published vocabulary from reaching a client as free text:
// a code the platform does not publish becomes remote_error instead of being
// echoed. The vocabulary and the evidence rule both live in sqlcontract, so a
// code added to the platform cannot fall out of this SDK's table.
func safeCode(failure sqlcontract.Failure) string {
	if failure.Class() == sqlcontract.Unrecognized {
		return "remote_error"
	}
	return failure.Code()
}
