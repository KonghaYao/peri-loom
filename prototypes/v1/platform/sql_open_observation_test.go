package platform

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func sqlOpenTestObserver(mode string) *SQLOpenObserver {
	if mode == "off" {
		return nil
	}
	observer := NewSQLOpenObserver()
	if mode == "exhausted" {
		observer.attempts.Store(sqlOpenObservationLimit)
	}
	if mode == "full" {
		ctx := bindSQLOpenObservation(context.Background(), observer, "previous_call")
		for count := 0; count < sqlOpenObservationLimit; count++ {
			observeSQLOpen(ctx, "previous_call", sqlOpenGateway, sqlOpenReply, nil, "outcome_unknown")
		}
	}
	return observer
}

func sqlOpenTestRecords(observer *SQLOpenObserver) []SQLOpenObservation {
	var records []SQLOpenObservation
	for {
		record, found := observer.TryReceive()
		if !found {
			return records
		}
		records = append(records, record)
	}
}

func TestSQLOpenObservationDefaultContextAndPrivacy(test *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if bindSQLOpenObservation(parent, nil, "public_open") != parent || bindSQLOpenObservation(nil, NewSQLOpenObserver(), "public_open") != nil {
		test.Fatal("default-off or nil context changed")
	}
	if allocations := testing.AllocsPerRun(100, func() { bindSQLOpenObservation(parent, nil, "public_open") }); allocations != 0 {
		test.Fatal("default-off observation allocates")
	}
	observer := NewSQLOpenObserver()
	ctx := bindSQLOpenObservation(parent, observer, "public_open")
	deadline, _ := parent.Deadline()
	if observed, ok := ctx.Deadline(); !ok || !observed.Equal(deadline) {
		test.Fatal("observation changed deadline")
	}
	secret := "credential_body_SQL_namespace_session_binding_private"
	failure := &SQLRPCError{Ambiguous: true, cause: errors.New(secret)}
	if observeSQLOpenError(ctx, "public_open", sqlOpenTransport, sqlOpenTransportFailure, failure) != failure {
		test.Fatal("observation replaced original error")
	}
	records := sqlOpenTestRecords(observer)
	if len(records) != 1 || records[0].CallSequence != 1 || records[0].RequestID != "public_open" || !records[0].Ambiguous || !records[0].RPCError {
		test.Fatal("incorrect immutable observation")
	}
	encoded, _ := json.Marshal(records)
	if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "cause") || strings.Contains(string(encoded), "SQL upstream request failed") {
		test.Fatal("observation leaked private data")
	}
	var fields []map[string]any
	_ = json.Unmarshal(encoded, &fields)
	if len(fields[0]) != 8 {
		test.Fatal("unexpected diagnostic fields")
	}
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		test.Fatal("observation changed cancellation")
	}
}

func TestSQLOpenObservationConcurrentReusedIDAndIsolation(test *testing.T) {
	observer := NewSQLOpenObserver()
	start := make(chan struct{})
	var group sync.WaitGroup
	for count := 0; count < 32; count++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			ctx := bindSQLOpenObservation(context.Background(), observer, "reused_public_id")
			observeSQLOpen(ctx, "reused_public_id", sqlOpenCodecV1, sqlOpenStrictDecode, nil, "")
		}()
	}
	close(start)
	group.Wait()
	records := sqlOpenTestRecords(observer)
	sequences := map[uint64]bool{}
	for _, record := range records {
		if sequences[record.CallSequence] || record.CallSequence == 0 || record.RequestID != "reused_public_id" {
			test.Fatal("reused public ID confused concurrent attempts")
		}
		sequences[record.CallSequence] = true
	}
	if len(sequences) != 32 {
		test.Fatal("concurrent observation lost an in-budget attempt")
	}
	fresh := NewSQLOpenObserver()
	ctx := bindSQLOpenObservation(context.Background(), fresh, "reused_public_id")
	observeSQLOpen(ctx, "reused_public_id", sqlOpenGateway, sqlOpenReply, nil, "outcome_unknown")
	if _, found := observer.TryReceive(); found {
		test.Fatal("fresh observation reached old stream")
	}
	if record, found := fresh.TryReceive(); !found || record.CallSequence != 1 {
		test.Fatal("fresh observer did not isolate its attempt sequence")
	}
}

func TestSQLOpenObservationBoundsAndInvalidCorrelation(test *testing.T) {
	observer := NewSQLOpenObserver()
	parent := context.Background()
	for _, identifier := range []string{"", "invalid\nheader", strings.Repeat("a", 129)} {
		if bindSQLOpenObservation(parent, observer, identifier) != parent {
			test.Fatal("invalid ID acquired an observation binding")
		}
	}
	ctx := bindSQLOpenObservation(parent, observer, "public_open")
	observeSQLOpen(ctx, "wrong_id", sqlOpenCodecV1, sqlOpenStrictDecode, nil, "")
	observeSQLOpen(ctx, "public_open", "private_stage", sqlOpenReply, nil, "")
	observeSQLOpen(ctx, "public_open", sqlOpenGateway, "private_reason", nil, "")
	if _, found := observer.TryReceive(); found {
		test.Fatal("uncorrelated or nonclosed observation emitted")
	}
	for count := 0; count < 65; count++ {
		observeSQLOpen(ctx, "public_open", sqlOpenGateway, sqlOpenReply, nil, "private_classification")
	}
	records := sqlOpenTestRecords(observer)
	if len(records) != 64 || records[0].Classification != "" {
		test.Fatal("event limit or closed classification violated")
	}
	observeSQLOpen(ctx, "public_open", sqlOpenGateway, sqlOpenReply, nil, "")
	if _, found := observer.TryReceive(); found {
		test.Fatal("draining queue renewed total event budget")
	}
	for count := 1; count < 64; count++ {
		if bindSQLOpenObservation(parent, observer, "public_open") == parent {
			test.Fatal("in-budget attempt refused binding")
		}
	}
	if bindSQLOpenObservation(parent, observer, "public_open") != parent {
		test.Fatal("exhausted attempt cap still observed")
	}
	observer.attempts.Store(^uint64(0))
	if bindSQLOpenObservation(parent, observer, "public_open") != parent || observer.attempts.Load() != ^uint64(0) {
		test.Fatal("checked sequence overflow wrapped")
	}
}

func TestSQLOpenObservationDecoderResultsUnchanged(test *testing.T) {
	scenarios := []struct {
		name      string
		body      string
		version   uint32
		ambiguous bool
		completed bool
		success   bool
	}{
		{"success", `{"version":1,"request_id":"public_open","ok":true,"result":null,"error":null}`, 1, false, false, true},
		{"wrong_echo", `{"version":1,"request_id":"private_echo","ok":true,"result":null,"error":null}`, 1, true, false, false},
		{"negative", `{"version":1,"request_id":"public_open","ok":false,"result":null,"error":"private_body_token_SQL"}`, 1, true, false, false},
		{"terminal", `{"version":1,"request_id":"public_open","ok":false,"result":null,"error":"private_body_token_SQL","execution_completed":true}`, 1, false, true, false},
		{"missing_settled_state", `{"version":2,"request_id":"public_open","ok":true,"result":null,"error":null}`, 2, true, false, false},
		{"terminal_missing_state", `{"version":2,"request_id":"public_open","ok":false,"result":null,"error":"private_body_token_SQL","execution_completed":true}`, 2, true, false, false},
	}
	for _, scenario := range scenarios {
		for _, mode := range []string{"off", "on", "full", "exhausted"} {
			test.Run(scenario.name+"/"+mode, func(test *testing.T) {
				observer := sqlOpenTestObserver(mode)
				ctx := bindSQLOpenObservation(context.Background(), observer, "public_open")
				var err error
				if scenario.version == 2 {
					_, err = sqlDecodeWriter(ctx, []byte(scenario.body), "public_open", nil)
				} else {
					err = sqlDecode(ctx, []byte(scenario.body), "public_open", nil)
				}
				if scenario.success {
					if err != nil {
						test.Fatal("successful decoder result changed")
					}
				} else {
					failure := sqlTestFailure(test, err)
					if failure.Ambiguous != scenario.ambiguous || failure.evidence.Executed() != scenario.completed || failure.evidence.Rejected() || failure.StatusCode != 0 || failure.cause != nil || failure.Error() != "SQL upstream request failed" {
						test.Fatal("original decoder flags/error changed")
					}
				}
				records := sqlOpenTestRecords(observer)
				for _, record := range records {
					encoded, _ := json.Marshal(record)
					if strings.Contains(string(encoded), "private_") || mode == "full" && record.RequestID != "previous_call" {
						test.Fatal("reply payload or new event escaped its closed budget")
					}
				}
				if mode == "on" && !scenario.success && len(records) == 0 {
					test.Fatal("in-budget decoder failure not observed")
				}
				if mode == "on" && strings.Contains(scenario.name, "missing") && records[len(records)-1].Reason != string(sqlOpenWriterPresence) {
					test.Fatal("settled/presence decision not observed exactly")
				}
			})
		}
	}
}

type sqlOpenTestTransport func(*http.Request) (*http.Response, error)

func (transport sqlOpenTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestSQLOpenObservationOriginalTransportUnchanged(test *testing.T) {
	for _, scenario := range []string{"transport", "body", "http_status", "strict_decode"} {
		test.Run(scenario, func(test *testing.T) {
			upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
			if err != nil {
				test.Fatal(err)
			}
			httpUpstream := upstream.(*sqlHTTPUpstream)
			originalClient := httpUpstream.client
			var calls atomic.Int32
			var requestBodies []string
			httpUpstream.client.Transport = sqlOpenTestTransport(func(request *http.Request) (*http.Response, error) {
				calls.Add(1)
				body, _ := io.ReadAll(request.Body)
				requestBodies = append(requestBodies, string(body))
				if request.GetBody != nil || request.Method != "POST" || request.URL.Path != "/sql" {
					test.Error("original nonreplay transport shape changed")
				}
				if scenario == "transport" {
					httptrace.ContextClientTrace(request.Context()).GotConn(httptrace.GotConnInfo{})
					return nil, errors.New("private_transport_token_body")
				}
				response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"request_id":"private_untrusted_echo"}`))}
				if scenario == "http_status" {
					response.StatusCode = 403
				}
				if scenario == "body" {
					response.ContentLength = sqlWireLimit + 1
				}
				return response, nil
			})
			for _, mode := range []string{"off", "on", "full", "exhausted"} {
				observer := sqlOpenTestObserver(mode)
				ctx := bindSQLOpenObservation(context.Background(), observer, "public_open")
				err := upstream.Call(ctx, controller.Worker{ID: "worker-a", Address: "http://127.0.0.1:1"}, "public_open", map[string]any{"command": "open_session"}, nil)
				failure := sqlTestFailure(test, err)
				// No branch of this transport may claim execution state: the status
				// code, the body and the socket prove nothing about the statement.
				if !failure.Ambiguous || failure.evidence.Rejected() || failure.evidence.Executed() || failure.cause != nil || httpUpstream.client != originalClient {
					test.Fatal("observation changed original transport outcome/client")
				}
				if mode == "on" {
					records := sqlOpenTestRecords(observer)
					if len(records) != 1 || records[0].Reason != scenario || records[0].RequestID != "public_open" {
						test.Fatal("transport branch correlation changed")
					}
				}
			}
			if calls.Load() != 4 {
				test.Fatal("observation added RPC or retry")
			}
			for _, body := range requestBodies {
				if body != requestBodies[0] {
					test.Fatal("observation changed original request bytes")
				}
			}
		})
	}
}

type sqlOpenProbeUpstream struct {
	*gatewayWriterUpstream
	profileBound bool
	negativeOpen bool
}

func (upstream *sqlOpenProbeUpstream) WriterProfile(ctx context.Context, worker controller.Worker, requestID string) error {
	_, upstream.profileBound = ctx.Value(sqlOpenObservationKey{}).(sqlOpenObservationBinding)
	observeSQLOpen(ctx, "public_open", sqlOpenTransport, sqlOpenTransportFailure, nil, "")
	return upstream.gatewayWriterUpstream.WriterProfile(ctx, worker, requestID)
}

func (upstream *sqlOpenProbeUpstream) CallWriter(ctx context.Context, worker controller.Worker, requestID string, command any, output any) (*SessionWriterState, error) {
	if !upstream.negativeOpen {
		return upstream.gatewayWriterUpstream.CallWriter(ctx, worker, requestID, command, output)
	}
	upstream.mu.Lock()
	upstream.writerCalls++
	upstream.mu.Unlock()
	return nil, upstream.base.Call(ctx, worker, requestID, command, output)
}

func TestSQLOpenObservationGatewayClassificationAndProbeExclusion(test *testing.T) {
	for _, scenario := range []string{"success", "openreply", "writerbinding", "unknown", "terminal"} {
		for _, mode := range []string{"off", "on", "full", "exhausted"} {
			test.Run(scenario+"/"+mode, func(test *testing.T) {
				fixture, writer := newWriterGatewayFixture(test)
				observer := sqlOpenTestObserver(mode)
				fixture.gateway.config.OpenObserver = observer
				probe := &sqlOpenProbeUpstream{gatewayWriterUpstream: writer}
				fixture.gateway.config.Upstream = probe
				expectedStatus, expectedCode := 200, ""
				if scenario == "openreply" {
					fixture.upstream.setHook(func(context.Context, controller.Worker, map[string]any) (any, error) {
						return map[string]any{"session": "private_session", "readonly": false}, nil
					})
				}
				if scenario == "writerbinding" {
					writer.hook = func(_ map[string]any, state *SessionWriterState, err error) (*SessionWriterState, error) {
						state.Binding.Epoch++
						return state, err
					}
				}
				if scenario == "unknown" || scenario == "terminal" {
					probe.negativeOpen = true
					fixture.upstream.setHook(func(context.Context, controller.Worker, map[string]any) (any, error) {
						return nil, testRPCError(scenario == "terminal", false, SQLRPCError{Ambiguous: scenario == "unknown", cause: errors.New("private_token_SQL_namespace")})
					})
				}
				if scenario != "success" {
					expectedStatus, expectedCode = 502, "outcome_unknown"
				}
				if scenario == "terminal" {
					expectedStatus, expectedCode = 400, "sql_error"
				}
				response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first, "request_id": "public_open"})
				var reply struct {
					Error struct{ Code string } `json:"error"`
				}
				_ = json.Unmarshal(response.Body.Bytes(), &reply)
				if response.Code != expectedStatus || reply.Error.Code != expectedCode || probe.profileBound || writer.profileCalls != 1 || writer.writerCalls != 1 || fixture.upstream.count("open_session") != 1 || fixture.upstream.count("close_session") != 0 {
					test.Fatal("original Gateway classification or probe/open/cleanup count changed")
				}
				unknown := fixture.gateway.Metrics()["unknown_held_permits"].(int)
				if (unknown != 0) != (expectedCode == "outcome_unknown") {
					test.Fatal("unknown permit retention changed")
				}
				if mode == "on" {
					records := sqlOpenTestRecords(observer)
					if scenario == "success" && len(records) != 0 || scenario != "success" && (len(records) != 1 || records[0].Stage != "gateway_open" || records[0].CallSequence != 1 || records[0].Classification != expectedCode) {
						test.Fatal("profile probe leaked into Open trace or final decision missing")
					}
				}
			})
		}
	}
}
