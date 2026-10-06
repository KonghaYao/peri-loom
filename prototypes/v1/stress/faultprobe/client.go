package faultprobe

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type Failure struct {
	Code               string `json:"code"`
	Unknown            bool   `json:"unknown"`
	HTTPStatus         int    `json:"http_status,omitempty"`
	ExecutionCompleted bool   `json:"execution_completed"`
	MayHaveCommitted   bool   `json:"may_have_committed"`
}

func (failure *Failure) Error() string { return failure.Code }

type Client struct {
	endpoint, key, prefix string
	http                  *http.Client
	sequence              atomic.Uint64
	requests              atomic.Uint64
	log                   *Log
}

type Session struct {
	client   *Client
	id       string
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

func newClient(connection Connection, key, prefix string, config Config, log *Log) (*Client, error) {
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if connection.CAFile != "" {
		pem, err := readBoundedCA(connection.CAFile)
		if err != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file unavailable or invalid")
		}
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, MaxConnsPerHost: 4, MaxResponseHeaderBytes: 16 << 10, ResponseHeaderTimeout: config.RequestTimeout, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	return &Client{endpoint: connection.Endpoint, key: key, prefix: prefix, log: log, http: &http.Client{Transport: transport, Timeout: config.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (client *Client) call(ctx context.Context, path string, input map[string]any) (result json.RawMessage, failure *Failure) {
	identifier := fmt.Sprintf("fp_%s_%06d", client.prefix, client.sequence.Add(1))
	input["request_id"] = identifier
	encoded, err := json.Marshal(input)
	if err != nil || len(encoded) > 128<<10 || client.requests.Add(1) > 512 {
		return nil, &Failure{Code: "request_bound"}
	}
	started := time.Now()
	if client.log != nil {
		if client.log.Record(map[string]any{"type": "rpc_intent", "request_id": identifier, "path": path, "observed_at": started.UTC().Format(time.RFC3339Nano)}) != nil {
			return nil, &Failure{Code: "evidence_unavailable"}
		}
		defer func() {
			if client.log.Record(map[string]any{"type": "rpc_result", "request_id": identifier, "path": path, "elapsed_ns": time.Since(started).Nanoseconds(), "result": result, "failure": failure}) != nil && failure == nil {
				failure = &Failure{Code: "evidence_after_dispatch_failed", Unknown: true}
			}
		}()
	}
	request, err := http.NewRequestWithContext(ctx, "POST", client.endpoint+path, bytes.NewReader(encoded))
	if err != nil {
		return nil, &Failure{Code: "request_invalid"}
	}
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+client.key)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		return nil, &Failure{Code: "reply_unavailable", Unknown: true}
	}
	defer response.Body.Close()
	if response.ContentLength > 1<<20 {
		return nil, &Failure{Code: "response_bound", Unknown: true}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return nil, &Failure{Code: "response_unavailable_or_bound", Unknown: true}
	}
	var envelope struct {
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
	if strict(body, &envelope) != nil || envelope.Version != 1 || envelope.RequestID != identifier || envelope.OK == nil {
		return nil, &Failure{Code: "invalid_envelope", Unknown: true}
	}
	if !*envelope.OK {
		if envelope.Error == nil || !bytes.Equal(bytes.TrimSpace(envelope.Result), []byte("null")) {
			return nil, &Failure{Code: "invalid_failure", Unknown: true}
		}
		failure = &Failure{Code: "unrecognized_error", Unknown: true, HTTPStatus: response.StatusCode, ExecutionCompleted: envelope.Error.ExecutionCompleted, MayHaveCommitted: envelope.Error.MayHaveCommitted}
		switch envelope.Error.Code {
		case "sql_error":
			failure.Code = "sql_error"
			failure.Unknown = !(response.StatusCode == 400 && failure.ExecutionCompleted && failure.MayHaveCommitted)
		case "quota_exceeded", "forbidden", "unauthenticated", "sql_limit", "request_limit", "invalid_request", "invalid_session", "not_found", "method_not_allowed":
			failure.Code = envelope.Error.Code
			failure.Unknown = false
		case "outcome_unknown", "operation_incomplete", "audit_unavailable":
			failure.Code = envelope.Error.Code
		}
		return nil, failure
	}
	if envelope.Error != nil || response.StatusCode != 200 || len(envelope.Result) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Result), []byte("null")) {
		return nil, &Failure{Code: "invalid_success", Unknown: true}
	}
	return envelope.Result, nil
}

func (client *Client) Open(ctx context.Context, database DatabaseID) (*Session, *Failure) {
	body, failure := client.call(ctx, "/v1/sql/open", map[string]any{"database": database})
	if failure != nil {
		return nil, failure
	}
	var result struct {
		Session  string `json:"session"`
		Readonly *bool  `json:"readonly"`
	}
	if strict(body, &result) != nil || len(result.Session) != 32 || result.Readonly == nil || *result.Readonly {
		return nil, &Failure{Code: "invalid_open", Unknown: true}
	}
	for _, character := range result.Session {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return nil, &Failure{Code: "invalid_session_id", Unknown: true}
		}
	}
	return &Session{client: client, id: result.Session}, nil
}

func (session *Session) SQL(ctx context.Context, query bool, sql string) ([][]any, *Failure) {
	if session.poisoned {
		return nil, &Failure{Code: "poisoned_session", Unknown: true}
	}
	if session.closed {
		return nil, &Failure{Code: "closed_session"}
	}
	if len(sql) == 0 || len(sql) > 64<<10 {
		return nil, &Failure{Code: "sql_bound"}
	}
	action := "execute"
	if query {
		action = "query"
	}
	body, failure := session.client.call(ctx, "/v1/sql/"+action, map[string]any{"session": session.id, "sql": sql})
	if failure != nil {
		session.poisoned = failure.Unknown
		return nil, failure
	}
	if query {
		var rows [][]any
		if strict(body, &rows) == nil {
			return rows, nil
		}
	} else {
		var executed struct {
			Executed bool `json:"executed"`
		}
		if strict(body, &executed) == nil && executed.Executed {
			return nil, nil
		}
	}
	session.poisoned = true
	return nil, &Failure{Code: "invalid_sql_result", Unknown: true}
}

func (session *Session) Close(ctx context.Context) *Failure {
	if session.poisoned {
		return &Failure{Code: "poisoned_session_not_closed", Unknown: true}
	}
	if session.closed {
		return nil
	}
	body, failure := session.client.call(ctx, "/v1/sql/close", map[string]any{"session": session.id})
	if failure != nil {
		session.poisoned = failure.Unknown
		return failure
	}
	var result struct {
		Closed bool `json:"closed"`
	}
	if strict(body, &result) != nil || !result.Closed {
		session.poisoned = true
		return &Failure{Code: "invalid_close", Unknown: true}
	}
	session.closed = true
	return nil
}
