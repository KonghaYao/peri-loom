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
	"unicode/utf8"

	controller "peri-loom/controller"
	"peri-loom/platform/sqlcontract"
)

const sqlWireLimit = 32 * 1024 * 1024

// sqlAdmissionRejectionLimit bounds the one non-200 body the platform reads. A
// refusal before dispatch is a handful of fields; anything larger is not it.
const sqlAdmissionRejectionLimit = 4 * 1024

type SQLRPCError struct {
	Ambiguous  bool
	StatusCode int
	// evidence is the worker's own SQL evidence, decoded by sqlcontract: the
	// statement entered execution and settled, or it never entered execution. It
	// is unexported and only the two constructors below fill it, which is what
	// keeps an HTTP status, a dropped socket or an elapsed deadline from ever
	// claiming what happened to a statement.
	evidence sqlcontract.Evidence
	// workerFailure is the failure text a worker reported for a statement. It is
	// classified for the client contract and logged, never echoed to the caller.
	workerFailure string
	cause         error
}

func (failure *SQLRPCError) Error() string { return "SQL upstream request failed" }
func (failure *SQLRPCError) Unwrap() error { return failure.cause }

// sqlWorkerFailureFromReply is the only constructor for the execution state the
// platform reports about a statement, and sql_upstream_codec.go is its only
// caller. A present "execution_completed" states the worker's own verdict; an
// absent one proves nothing and stays ambiguous. The worker answers every SQL
// reply with HTTP 200 (prototypes/v1/src/worker_rpc/server.rs), so a reply the
// platform could not decode has no evidence to offer and never reaches here.
func sqlWorkerFailureFromReply(present, completed bool, message string) *SQLRPCError {
	if !present {
		return &SQLRPCError{Ambiguous: true, workerFailure: message}
	}
	rejected := !completed
	failure := &SQLRPCError{evidence: sqlcontract.EvidenceFromDirections(completed, rejected, false)}
	failure.workerFailure = message
	return failure
}

// sqlRecordedEvidence restores the evidence a durable operation record kept, so a
// later request against an isolated session is answered from the same proof that
// was recorded. The record is written from a SQLRPCError that already carried it.
func sqlRecordedEvidence(record OperationStatus) *SQLRPCError {
	return &SQLRPCError{Ambiguous: record.Ambiguous, evidence: sqlcontract.EvidenceFromDirections(record.ExecutionCompleted, record.Rejected, record.MayHaveCommitted)}
}

type sqlHTTPUpstream struct {
	tokens  map[string]string
	client  *http.Client
	timeout time.Duration
}

func NewSQLUpstream(workerSQLTokens map[string]string) (SQLUpstream, error) {
	return NewSQLUpstreamWithTimeout(workerSQLTokens, 30*time.Second)
}

func NewSQLUpstreamWithTimeout(workerSQLTokens map[string]string, timeout time.Duration) (SQLUpstream, error) {
	if timeout < 30*time.Second || timeout > 5*time.Minute {
		return nil, &SQLRPCError{}
	}
	tokens := make(map[string]string, len(workerSQLTokens))
	seen := make(map[string]bool)
	for worker, token := range workerSQLTokens {
		if !sqlLabel(worker) || !sqlToken(token) || seen[token] {
			return nil, &SQLRPCError{}
		}
		seen[token] = true
		tokens[worker] = token
	}
	return &sqlHTTPUpstream{tokens: tokens, timeout: timeout, client: &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:                  nil,
			DialContext:            (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ResponseHeaderTimeout:  timeout,
			MaxResponseHeaderBytes: 16384,
			// Connections are pooled. The worker now keeps its side open for its own
			// idle window and answers with "keep-alive", so dispatches do reuse sockets
			// and the TIME_WAIT churn is gone. Idle connections stay bounded, and a
			// request that lands on a socket the worker is closing fails as an ambiguous
			// upstream failure rather than being retried, because a retry could
			// double-apply a commit. This client idle timeout still matches the worker's
			// window, so a request can be handed a socket the worker is about to close;
			// that race is a known open defect (closing it means retiring connections
			// here strictly before the worker does, which trades reuse for reconnects on
			// bursty traffic) and is deliberately not papered over by replaying.
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     5 * time.Second,
		},
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

// sqlCommandBytes returns the exact body of one worker command. A frozen command
// carries the bytes its durable digest covers, so the wire body and the record
// cannot drift apart through a second encoding. The remaining callers - the
// writer probe and hand-built fixtures - authorize nothing and carry no digest,
// so they keep the generic encoding.
func sqlCommandBytes(command any) ([]byte, error) {
	if frozen, ok := command.(frozenCommand); ok {
		return frozen.bytes(), nil
	}
	return json.Marshal(command)
}

func (upstream *sqlHTTPUpstream) callVersionInto(ctx context.Context, worker controller.Worker, requestID string, command any, output any, version uint32, state **SessionWriterState) error {
	token, found := upstream.tokens[worker.ID]
	endpoint, err := sqlEndpoint(worker.Address)
	if !found || err != nil || ctx == nil || !sqlLabel(requestID) || !sqlOutput(output) {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	encoded, err := sqlCommandBytes(command)
	var fields map[string]json.RawMessage
	var operation string
	if err != nil || json.Unmarshal(encoded, &fields) != nil || json.Unmarshal(fields["command"], &operation) != nil {
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	switch operation {
	case "open_session", "execute", "query", "statement", "describe", "close_session", "open_read_cursor", "fetch_read_cursor", "close_read_cursor", "open_write_cursor", "fetch_write_cursor", "close_write_cursor":
		if version == 2 && !writerCommandValid(operation, encoded, fields) {
			return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
		}
	case "writer_profile_status":
		if version != 2 || len(fields) != 1 {
			return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
		}
	case "checkpoint", "compact", "diagnostics", "session_autocommit":
		var session string
		if version != 1 || len(fields) != 2 || json.Unmarshal(fields["session"], &session) != nil || !upstreamSessionID(session) || sqlJSONValue(ctx, json.NewDecoder(bytes.NewReader(encoded)), 0) != nil {
			return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
		}
	default:
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenRequestValidation, &SQLRPCError{})
	}
	ctx, cancel := context.WithTimeout(ctx, upstream.timeout)
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
		// The worker answers every SQL reply with 200 and everything else with a
		// bare body, so a status alone proves only that the request reached the
		// worker. The one exception is the refusal it encodes before dispatch:
		// that body says the request never entered execution or any queue, and it
		// is accepted only in its complete, strictly decoded form and only with the
		// status the worker reserves for it. Every other status, a missing or
		// extra field, an unreadable or oversized body stays unproven, because no
		// body may prove non-execution through a status the worker never used for
		// it. This classification explains the request it answers and nothing else:
		// it releases no earlier unknown and authorises no replay.
		if rejection := admissionRejection(ctx, response); rejection != nil {
			return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenAdmissionRejected, rejection)
		}
		return observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenHTTPStatus, &SQLRPCError{Ambiguous: true, StatusCode: response.StatusCode})
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

// admissionRejection reads the one non-200 body that carries admission evidence:
// the refusal a worker sends when it turned a request away before dispatch
// (prototypes/v1/src/worker_rpc/protocol.rs AdmissionRejection, encoded with
// deny_unknown_fields, hence the strict decode here). The request never entered
// execution or any queue, so a refusal is proof it did not run — and only then:
// a partial body, an unknown field, a different status or an unreadable reply
// proves nothing and keeps the ambiguous classification.
func admissionRejection(ctx context.Context, response *http.Response) *SQLRPCError {
	if response.StatusCode != http.StatusTooManyRequests || ctx == nil || ctx.Err() != nil {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, sqlAdmissionRejectionLimit+1))
	if err != nil || len(body) > sqlAdmissionRejectionLimit || !utf8.Valid(body) || ctx.Err() != nil {
		return nil
	}
	var rejection struct {
		Error              string `json:"error"`
		ExecutionCompleted *bool  `json:"execution_completed"`
		Admission          string `json:"admission"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if decoder.Decode(&rejection) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil
	}
	if rejection.Error == "" || rejection.ExecutionCompleted == nil || *rejection.ExecutionCompleted || rejection.Admission != "not_admitted" {
		return nil
	}
	// The reason is classified by the same table as a worker failure text, so the
	// caller sees the cause class instead of a bare status, and the proof that the
	// statement never entered execution travels with it.
	failure := &SQLRPCError{StatusCode: response.StatusCode, evidence: sqlcontract.EvidenceFromDirections(false, true, false)}
	failure.workerFailure = rejection.Error
	return failure
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
