package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	controller "peri-loom/controller"
)

const sqlWireLimit = 32 * 1024 * 1024

type SQLRPCError struct {
	Ambiguous          bool
	Rejected           bool
	StatusCode         int
	ExecutionCompleted bool
	cause              error
}

func (failure *SQLRPCError) Error() string { return "SQL upstream request failed" }
func (failure *SQLRPCError) Unwrap() error { return failure.cause }

type sqlHTTPUpstream struct {
	tokens map[string]string
	client *http.Client
}

func NewSQLUpstream(workerSQLTokens map[string]string) (SQLUpstream, error) {
	tokens := make(map[string]string, len(workerSQLTokens))
	seen := make(map[string]bool)
	for worker, token := range workerSQLTokens {
		if !sqlLabel(worker) || !sqlToken(token) || seen[token] {
			return nil, &SQLRPCError{}
		}
		seen[token] = true
		tokens[worker] = token
	}
	return &sqlHTTPUpstream{tokens: tokens, client: &http.Client{
		Timeout:       30 * time.Second,
		Transport:     &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, DisableKeepAlives: true, ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 16384},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (upstream *sqlHTTPUpstream) Call(ctx context.Context, worker controller.Worker, requestID string, command any, output any) error {
	_, err := upstream.callVersion(ctx, worker, requestID, command, output, 1)
	return err
}

func (upstream *sqlHTTPUpstream) CallWriter(ctx context.Context, worker controller.Worker, requestID string, command any, output any) (*SessionWriterState, error) {
	var state *SessionWriterState
	err := upstream.callVersionInto(ctx, worker, requestID, command, output, 2, &state)
	return state, err
}

func (upstream *sqlHTTPUpstream) WriterProfile(ctx context.Context, worker controller.Worker, requestID string) error {
	var state *SessionWriterState
	return upstream.callVersionInto(ctx, worker, requestID, map[string]any{"command": "writer_profile_status"}, nil, 2, &state)
}

func (upstream *sqlHTTPUpstream) callVersion(ctx context.Context, worker controller.Worker, requestID string, command any, output any, version uint32) (*SessionWriterState, error) {
	var state *SessionWriterState
	err := upstream.callVersionInto(ctx, worker, requestID, command, output, version, &state)
	return state, err
}

func (upstream *sqlHTTPUpstream) callVersionInto(ctx context.Context, worker controller.Worker, requestID string, command any, output any, version uint32, state **SessionWriterState) error {
	token, found := upstream.tokens[worker.ID]
	endpoint, err := sqlEndpoint(worker.Address)
	if !found || err != nil || ctx == nil || !sqlLabel(requestID) || !sqlOutput(output) {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	encoded, err := json.Marshal(command)
	var fields map[string]json.RawMessage
	var operation string
	if err != nil || json.Unmarshal(encoded, &fields) != nil || json.Unmarshal(fields["command"], &operation) != nil {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	switch operation {
	case "open_session", "execute", "query", "close_session":
		if version == 2 && !writerCommandValid(operation, encoded, fields) {
			return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
		}
	case "writer_profile_status":
		if version != 2 || len(fields) != 1 {
			return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
		}
	case "checkpoint", "compact", "diagnostics":
		var session string
		if version != 1 || len(fields) != 2 || json.Unmarshal(fields["session"], &session) != nil || !upstreamSessionID(session) || sqlJSONValue(ctx, json.NewDecoder(bytes.NewReader(encoded)), 0) != nil {
			return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
		}
	default:
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{cause: ctx.Err()})
	}
	body, err := json.Marshal(struct {
		Version   uint32          `json:"version"`
		RequestID string          `json:"request_id"`
		Command   json.RawMessage `json:"command"`
	}{version, requestID, encoded})
	if err != nil || len(body) > sqlWireLimit {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	var dispatched atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { dispatched.Store(true) }}
	request, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := upstream.client.Do(request)
	if err != nil {
		cause := ctx.Err()
		if cause == nil && errors.Is(err, context.DeadlineExceeded) {
			cause = context.DeadlineExceeded
		}
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenTransportFailure, &SQLRPCError{Ambiguous: dispatched.Load(), cause: cause})
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		rejected := response.StatusCode >= 400 && response.StatusCode < 500
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenHTTPStatus, &SQLRPCError{Ambiguous: !rejected, Rejected: rejected, StatusCode: response.StatusCode})
	}
	if response.ContentLength > sqlWireLimit {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenBody, &SQLRPCError{Ambiguous: true})
	}
	body, err = io.ReadAll(io.LimitReader(response.Body, sqlWireLimit+1))
	if err != nil || len(body) > sqlWireLimit {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenBody, &SQLRPCError{Ambiguous: true, cause: ctx.Err()})
	}
	if version == 2 {
		if operation == "writer_profile_status" {
			return sqlDecodeWriterProfile(ctx, body, requestID)
		}
		var err error
		*state, err = sqlDecodeWriter(ctx, body, requestID, output)
		return err
	}
	return sqlDecode(ctx, body, requestID, output)
}

func sqlEndpoint(address string) (string, error) {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(address, "#") || parsed.RawPath != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", &SQLRPCError{}
	}
	addressIP := net.ParseIP(parsed.Hostname())
	if addressIP == nil || !addressIP.IsLoopback() || strings.HasSuffix(parsed.Host, ":") {
		return "", &SQLRPCError{}
	}
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return "", &SQLRPCError{}
		}
	}
	parsed.Path = "/sql"
	return parsed.String(), nil
}

func sqlLabel(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, character := range []byte(value) {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.') {
			return false
		}
	}
	return true
}

func sqlToken(value string) bool {
	if len(value) < 32 || len(value) > 256 {
		return false
	}
	for _, character := range []byte(value) {
		if character < 33 || character > 126 {
			return false
		}
	}
	return true
}
