package stress

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type Failure struct {
	Class              string `json:"class"`
	Code               string `json:"code"`
	HTTPStatus         int    `json:"http_status,omitempty"`
	Unknown            bool   `json:"unknown"`
	TimedOut           bool   `json:"timed_out"`
	ExecutionCompleted bool   `json:"execution_completed"`
	MayHaveCommitted   bool   `json:"may_have_committed"`
}

func (failure *Failure) Error() string { return failure.Class + ": " + failure.Code }

type IDs struct {
	Prefix   string
	sequence atomic.Uint64
}

func (ids *IDs) Next() string { return fmt.Sprintf("stress_%s_%012d", ids.Prefix, ids.sequence.Add(1)) }

type DataBudget struct {
	Maximum int64
	used    atomic.Int64
}

func (budget *DataBudget) Charge(bytes int64) bool {
	for {
		prior := budget.used.Load()
		if bytes < 0 || bytes > budget.Maximum-prior {
			return false
		}
		if budget.used.CompareAndSwap(prior, prior+bytes) {
			return true
		}
	}
}

type Client struct {
	endpoint    string
	key         string
	http        *http.Client
	ids         *IDs
	budget      *DataBudget
	maxResponse int64
	artifacts   *Artifacts
}

type wireRequest struct {
	Database  *DatabaseID `json:"database,omitempty"`
	Session   string      `json:"session,omitempty"`
	SQL       string      `json:"sql,omitempty"`
	RequestID string      `json:"request_id"`
}

type wireError struct {
	Code               string `json:"code"`
	ExecutionCompleted bool   `json:"execution_completed,omitempty"`
	MayHaveCommitted   bool   `json:"may_have_committed,omitempty"`
}

type wireResponse struct {
	Version   int             `json:"version"`
	RequestID string          `json:"request_id"`
	OK        *bool           `json:"ok"`
	Result    json.RawMessage `json:"result"`
	Error     *wireError      `json:"error"`
}

type Session struct {
	client   *Client
	ID       string
	poisoned bool
	closed   bool
}

const maxCABytes = 1 << 20

func readBoundedCA(path string) ([]byte, error) {
	invalid := errors.New("CA file unavailable or invalid")
	descriptor, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, invalid
	}
	certificate := os.NewFile(uintptr(descriptor), "bounded CA")
	if certificate == nil {
		_ = syscall.Close(descriptor)
		return nil, invalid
	}
	defer certificate.Close()
	info, err := certificate.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxCABytes {
		return nil, invalid
	}
	body, err := io.ReadAll(io.LimitReader(certificate, maxCABytes+1))
	if err != nil || len(body) > maxCABytes {
		return nil, invalid
	}
	return body, nil
}

func NewClient(endpoint, key, caFile string, settings Settings, ids *IDs, budget *DataBudget) (*Client, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	if !validKey(key) || ids == nil || budget == nil {
		return nil, errors.New("invalid client configuration")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := readBoundedCA(caFile)
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file unavailable or invalid")
		}
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		DisableKeepAlives: true, ForceAttemptHTTP2: false, ResponseHeaderTimeout: settings.RequestTimeout,
		MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: 8}
	return &Client{endpoint: strings.TrimSuffix(endpoint, "/"), key: key, ids: ids, budget: budget,
		maxResponse: settings.MaxResponseBytes, http: &http.Client{Timeout: settings.RequestTimeout, Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (client *Client) call(ctx context.Context, method, path, requestID string, input any) (result json.RawMessage, failure *Failure) {
	if client.artifacts != nil && client.artifacts.failed.Load() {
		return nil, &Failure{Class: "artifact_error", Code: "artifact_recording_unavailable"}
	}
	started := time.Now()
	status, responseBytes := 0, 0
	defer func() {
		if client.artifacts != nil {
			_ = client.artifacts.Record(map[string]any{"type": "gateway_rpc", "request_id": requestID, "method": method, "path": path,
				"started_at_observed": started.UTC().Format(time.RFC3339Nano), "finished_at_observed": time.Now().UTC().Format(time.RFC3339Nano), "elapsed_ns": time.Since(started).Nanoseconds(),
				"http_status": status, "response_bytes": responseBytes, "envelope_ok": failure == nil, "failure": failure})
		}
	}()
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
	}
	if err != nil || len(body) > 512<<10 || !client.budget.Charge(int64(len(body))) {
		return nil, &Failure{Class: "client_limit", Code: "request_budget"}
	}
	var dispatched atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { dispatched.Store(true) }}
	request, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), method, client.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, &Failure{Class: "client_error", Code: "invalid_request"}
	}
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+client.key)
	response, err := client.http.Do(request)
	if err != nil {
		timedOut := timeoutError(err)
		class := "transport_error"
		if dispatched.Load() {
			class = "unknown"
		}
		if timedOut {
			class = "timeout"
		}
		return nil, &Failure{Class: class, Code: "request_failed", Unknown: dispatched.Load(), TimedOut: timedOut}
	}
	defer response.Body.Close()
	status = response.StatusCode
	if response.ContentLength > client.maxResponse {
		return nil, &Failure{Class: "unknown", Code: "response_budget", Unknown: true, HTTPStatus: response.StatusCode}
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, client.maxResponse+1))
	responseBytes = len(encoded)
	if err != nil || int64(len(encoded)) > client.maxResponse {
		class, timedOut := "unknown", timeoutError(err)
		if timedOut {
			class = "timeout"
		}
		return nil, &Failure{Class: class, Code: "response_unavailable", Unknown: true, TimedOut: timedOut, HTTPStatus: response.StatusCode}
	}
	var reply wireResponse
	if decodeStrict(encoded, &reply) != nil || reply.Version != 1 || reply.OK == nil || requestID != "" && reply.RequestID != requestID {
		return nil, &Failure{Class: "unknown", Code: "invalid_envelope", Unknown: true, HTTPStatus: response.StatusCode}
	}
	if !*reply.OK {
		if reply.Error == nil || !bytes.Equal(bytes.TrimSpace(reply.Result), []byte("null")) {
			return nil, &Failure{Class: "unknown", Code: "invalid_failure", Unknown: true, HTTPStatus: response.StatusCode}
		}
		return nil, decodeError(response.StatusCode, *reply.Error)
	}
	if reply.Error != nil || response.StatusCode != 200 || len(reply.Result) == 0 || bytes.Equal(bytes.TrimSpace(reply.Result), []byte("null")) {
		return nil, &Failure{Class: "unknown", Code: "invalid_success", Unknown: true, HTTPStatus: response.StatusCode}
	}
	return reply.Result, nil
}

func timeoutError(err error) bool {
	var network net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &network) && network.Timeout()
}

func decodeError(status int, reply wireError) *Failure {
	if reply.Code == "sql_error" {
		failure := &Failure{Class: "unknown", Code: "sql_error", HTTPStatus: status, Unknown: true,
			ExecutionCompleted: reply.ExecutionCompleted, MayHaveCommitted: reply.MayHaveCommitted}
		if status == http.StatusBadRequest && reply.ExecutionCompleted && reply.MayHaveCommitted {
			failure.Class, failure.Unknown = "sql_error", false
		}
		return failure
	}
	failure := classify(status, reply.Code)
	failure.ExecutionCompleted, failure.MayHaveCommitted = reply.ExecutionCompleted, reply.MayHaveCommitted
	return failure
}

func classify(status int, code string) *Failure {
	known := map[string]string{"quota_exceeded": "admission", "sql_limit": "request_limit", "request_limit": "request_limit", "result_limit": "result_limit",
		"unauthenticated": "authorization", "forbidden": "authorization", "invalid_session": "session_error", "session_busy": "admission",
		"placement_unavailable": "placement", "token_binding_mismatch": "authorization", "readonly_placement_required": "authorization",
		"upstream_unavailable": "server_error", "upstream_rejected": "server_error", "gateway_closing": "admission",
		"metadata_unavailable": "server_error", "invalid_request": "request_error", "operation_incomplete": "unknown",
		"outcome_unknown": "unknown", "audit_unavailable": "unknown", "not_found": "unsupported", "unauthorized": "authorization", "method_not_allowed": "unsupported"}
	class, found := known[code]
	if !found {
		class, code = "unknown", "unrecognized_server_code"
	}
	return &Failure{Class: class, Code: code, HTTPStatus: status, Unknown: class == "unknown"}
}

func (client *Client) Open(ctx context.Context, database DatabaseID) (*Session, *Failure) {
	identifier := client.ids.Next()
	body, failure := client.call(ctx, "POST", "/v1/sql/open", identifier, wireRequest{Database: &database, RequestID: identifier})
	if failure != nil {
		return nil, failure
	}
	var result struct {
		Session  string `json:"session"`
		Readonly *bool  `json:"readonly"`
	}
	if decodeStrict(body, &result) != nil || len(result.Session) != 32 || result.Readonly == nil {
		return nil, &Failure{Class: "unknown", Code: "invalid_session_result", Unknown: true}
	}
	for _, character := range result.Session {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return nil, &Failure{Class: "unknown", Code: "invalid_session_result", Unknown: true}
		}
	}
	return &Session{client: client, ID: result.Session}, nil
}

func (session *Session) SQL(ctx context.Context, query bool, sql string) ([][]any, string, *Failure) {
	identifier := session.client.ids.Next()
	if session.poisoned || session.closed {
		return nil, identifier, &Failure{Class: "session_error", Code: "unusable_session", Unknown: session.poisoned}
	}
	if len(sql) == 0 || len(sql) > 64<<10 {
		return nil, identifier, &Failure{Class: "client_limit", Code: "sql_budget"}
	}
	action := "execute"
	if query {
		action = "query"
	}
	body, failure := session.client.call(ctx, "POST", "/v1/sql/"+action, identifier, wireRequest{Session: session.ID, SQL: sql, RequestID: identifier})
	if failure != nil {
		session.poisoned = session.poisoned || failure.Unknown
		return nil, identifier, failure
	}
	if query {
		var rows [][]any
		if decodeStrict(body, &rows) == nil {
			return rows, identifier, nil
		}
	} else {
		var result struct {
			Executed bool `json:"executed"`
		}
		if decodeStrict(body, &result) == nil && result.Executed {
			return nil, identifier, nil
		}
	}
	session.poisoned = true
	return nil, identifier, &Failure{Class: "unknown", Code: "invalid_sql_result", Unknown: true}
}

func (session *Session) Close(ctx context.Context) *Failure {
	if session.poisoned {
		return &Failure{Class: "unknown", Code: "unresolved_session_not_closed", Unknown: true}
	}
	if session.closed {
		return nil
	}
	identifier := session.client.ids.Next()
	body, failure := session.client.call(ctx, "POST", "/v1/sql/close", identifier, wireRequest{Session: session.ID, RequestID: identifier})
	if failure != nil {
		session.poisoned = failure.Unknown
		return failure
	}
	var result struct {
		Closed bool `json:"closed"`
	}
	if decodeStrict(body, &result) != nil || !result.Closed {
		session.poisoned = true
		return &Failure{Class: "unknown", Code: "invalid_close_result", Unknown: true}
	}
	session.closed = true
	return nil
}

func (client *Client) Management(ctx context.Context, action string, database DatabaseID, worker string) (json.RawMessage, *Failure) {
	identifier := client.ids.Next()
	input := map[string]any{"database": database, "request_id": identifier}
	if worker != "" {
		input["worker_id"] = worker
	}
	return client.call(ctx, "POST", "/v1/databases/"+action, identifier, input)
}

func (client *Client) Metrics(ctx context.Context) (json.RawMessage, *Failure) {
	return client.call(ctx, "GET", "/v1/metrics", "", nil)
}
