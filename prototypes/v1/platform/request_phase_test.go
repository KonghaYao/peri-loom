package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func requestPhaseObserverFixture(test *testing.T) (*gatewayFixture, *requestPhaseObserver) {
	test.Helper()
	fixture := newGatewayFixture(test)
	observer := NewSQLOpenObserver()
	observer.phases = newRequestPhaseObserver()
	fixture.gateway.config.OpenObserver = observer
	return fixture, observer.phases
}

// requestPhaseObserverFixtureAll keeps every request. Tests that assert the span vocabulary
// use it because a fixture answer is usually faster than the production threshold; the slow
// filter and its defaults have their own directed tests.
func requestPhaseObserverFixtureAll(test *testing.T) (*gatewayFixture, *requestPhaseObserver) {
	test.Helper()
	fixture, observer := requestPhaseObserverFixture(test)
	observer.slowThreshold = 0
	return fixture, observer
}

func requestPhaseRecords(test *testing.T, observer *requestPhaseObserver) []requestPhaseRecord {
	test.Helper()
	var records []requestPhaseRecord
	for {
		record, found := observer.TryReceive()
		if !found {
			return records
		}
		records = append(records, record)
	}
}

func requestPhaseAction(records []requestPhaseRecord, action string) (requestPhaseRecord, bool) {
	for _, record := range records {
		if record.Action == action {
			return record, true
		}
	}
	return requestPhaseRecord{}, false
}

// With the profile off, nothing is bound, nothing is recorded, and the request path keeps the
// context it was given.
func TestRequestPhaseDefaultOff(test *testing.T) {
	ctx := context.Background()
	bound, trace := beginRequestPhases(ctx, nil, "synthetic_request", "query", time.Now())
	if bound != ctx || trace != nil {
		test.Fatal("a nil observer still bound a trace")
	}
	if _, found := bound.Value(requestPhaseKey{}).(*requestPhaseTrace); found {
		test.Fatal("a disabled profile put a value in the request context")
	}
	leave := requestPhaseSpan(ctx, requestPhaseWorker)
	if leave == nil {
		test.Fatal("the disabled span helper returned nothing to call")
	}
	leave()
	// A fixture that only wants the open observer has no phase sink, so the gateway cannot
	// record phases by accident.
	if NewSQLOpenObserver().requestPhases() != nil || (*SQLOpenObserver)(nil).requestPhases() != nil {
		test.Fatal("the open observer handed out a phase observer it was never given")
	}
	fixture := newGatewayFixture(test)
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": "", "sql": "SELECT 1"})
	if response.Code == 0 {
		test.Fatal("a request without the profile did not answer")
	}
}

// One read request reports its own segments: the admission check, the worker call and
// the durable audit barrier are separated, and no sidecar terminal write is charged to
// the request.
func TestRequestPhaseRecordsReadSegments(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
	if response.Code != 200 {
		test.Fatalf("query failed: %d %s", response.Code, response.Body.String())
	}
	record, found := requestPhaseAction(requestPhaseRecords(test, observer), "query")
	if !found {
		test.Fatal("the query left no phase record")
	}
	if record.RequestID == "" || record.Outcome != "ok" || record.Status != 200 || record.TotalNS == 0 {
		test.Fatalf("phase record header is unusable: %+v", record)
	}
	seen := map[requestPhase]uint64{}
	for _, segment := range record.Spans {
		if _, duplicate := seen[segment.Phase]; duplicate {
			test.Fatalf("a segment was reported twice: %s", segment.Phase)
		}
		seen[segment.Phase] = segment.NS
	}
	for _, phase := range []requestPhase{requestPhaseAuth, requestPhaseIntent, requestPhaseWorker, requestPhaseTerminal, requestPhaseAudit, requestPhaseRespond} {
		if _, reported := seen[phase]; !reported {
			test.Fatalf("the read path did not report its %s segment: %+v", phase, record.Spans)
		}
	}
	// A read query pays no sidecar terminal write: its audit row is the durable
	// completion barrier. The admission check still reports an intent segment, and the
	// terminal segment may only carry the call that skipped the record.
	if seen[requestPhaseIntent] == 0 {
		test.Fatal("the read admission check was not reported")
	}
	if seen[requestPhaseTerminal] > 1_000_000 {
		test.Fatalf("a read still paid a sidecar terminal write: %d ns", seen[requestPhaseTerminal])
	}
	if seen[requestPhaseAudit] == 0 {
		test.Fatal("the read durable audit barrier was not reported")
	}
	if seen[requestPhaseWorker] == 0 {
		test.Fatal("the worker call was reported with no time")
	}
}

// A denied request is still attributed: the failure path keeps its own segments.
func TestRequestPhaseRecordsFailureSegments(test *testing.T) {
	fixture, observer := requestPhaseObserverFixture(test)
	response := gatewayHTTP(test, fixture.gateway, "invalid-private-credential", "/v1/sql/query", map[string]any{"session": "forged", "sql": "SELECT 1"})
	if response.Code != 401 {
		test.Fatalf("an invalid credential answered %d", response.Code)
	}
	record, found := requestPhaseAction(requestPhaseRecords(test, observer), "query")
	if !found || record.Outcome != "unauthenticated" || record.Status != 401 {
		test.Fatalf("the denial left no usable record: %+v", record)
	}
	seen := map[requestPhase]uint64{}
	for _, segment := range record.Spans {
		seen[segment.Phase] = segment.NS
	}
	if _, reported := seen[requestPhaseEntry]; !reported {
		test.Fatal("the denial reported no entry segment")
	}
	if _, reported := seen[requestPhaseRespond]; !reported {
		test.Fatal("the denial reported no respond segment")
	}
	if _, reported := seen[requestPhaseWorker]; reported {
		test.Fatal("a denied request claimed a worker call")
	}
}

// The record is self-consistent: segments are a partition of what was accounted for, the
// accounted and unaccounted parts add up to the total, and no phase claims more than the
// request took.
func TestRequestPhaseRecordIsConsistent(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	var group sync.WaitGroup
	for index := 0; index < 4; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT 1"})
		}()
	}
	group.Wait()
	records := requestPhaseRecords(test, observer)
	if len(records) < 4 {
		test.Fatalf("records = %d, want at least one per request", len(records))
	}
	requests := map[string]bool{}
	for _, record := range records {
		var accounted uint64
		for _, segment := range record.Spans {
			accounted += segment.NS
			if segment.NS > record.TotalNS {
				test.Fatalf("segment %s exceeded the request: %d > %d", segment.Phase, segment.NS, record.TotalNS)
			}
		}
		if accounted != record.AccountedNS {
			test.Fatalf("accounted = %d, segments sum to %d", record.AccountedNS, accounted)
		}
		if record.AccountedNS+record.UnaccountedNS != record.TotalNS {
			test.Fatalf("total %d != accounted %d + unaccounted %d", record.TotalNS, record.AccountedNS, record.UnaccountedNS)
		}
		// Every request has its own trace: a record answers for exactly one request id.
		if record.RequestID == "" || requests[record.RequestID] {
			test.Fatalf("request %q was recorded twice or without an id", record.RequestID)
		}
		requests[record.RequestID] = true
	}
}

// The stream carries no credential and no SQL text, only the vocabulary it documents.
func TestRequestPhaseRecordIsPrivate(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	secret := "private-phase-sql-secret-value"
	session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT '" + secret + "'"})
	records := requestPhaseRecords(test, observer)
	if len(records) == 0 {
		test.Fatal("no record to inspect")
	}
	for _, record := range records {
		encoded, err := json.Marshal(record)
		if err != nil {
			test.Fatal(err)
		}
		line := string(encoded)
		for _, forbidden := range []string{secret, fixture.tokenA, "Bearer ", "SELECT", "plk1_", session, fixture.first.Namespace.Tenant} {
			if strings.Contains(line, forbidden) {
				test.Fatalf("the phase record leaked %q: %s", forbidden, line)
			}
		}
	}
}

// A fast successful request is sampled out before it reaches the queue: the bound is global,
// and warmup traffic must not consume it before the measurement stalls.
func TestRequestPhaseFiltersFastSuccess(test *testing.T) {
	observer := newRequestPhaseObserver()
	ctx, trace := beginRequestPhases(context.Background(), observer, "synthetic_fast", "query", time.Now())
	requestPhaseSpan(ctx, requestPhaseAuth)()
	trace.finish("synthetic_fast", "ok", 200)
	if record, found := observer.TryReceive(); found {
		test.Fatalf("a fast success was still recorded: %+v", record)
	}
	if got := observer.filtered.Load(); got != 1 {
		test.Fatalf("filtered = %d, want 1", got)
	}
	// A sampled-out request must not consume the global bound either.
	if got := observer.sequence.Load(); got != 0 {
		test.Fatalf("a filtered request consumed the bounded sequence: %d", got)
	}
}

// A slow measurement statement is exactly the evidence this profile exists for, so it is kept
// even though its outcome is ordinary.
func TestRequestPhaseKeepsSlowMeasurement(test *testing.T) {
	observer := newRequestPhaseObserver()
	ctx, trace := beginRequestPhases(context.Background(), observer, "synthetic_measure", "statement", time.Now().Add(-2*time.Second))
	requestPhaseSpan(ctx, requestPhaseWorker)()
	trace.finish("synthetic_measure", "ok", 200)
	record, found := observer.TryReceive()
	if !found {
		test.Fatal("a slow measurement statement was filtered out")
	}
	if record.Action != "statement" || record.Outcome != "ok" || record.Status != 200 || record.TotalNS < uint64(requestPhaseSlowThreshold) {
		test.Fatalf("the slow measurement record is unusable: %+v", record)
	}
	if got := observer.filtered.Load(); got != 0 {
		test.Fatalf("a kept request incremented filtered: %d", got)
	}
}

// Failures are kept regardless of duration: a fast Unknown must not disappear.
func TestRequestPhaseKeepsFastFailures(test *testing.T) {
	cases := []struct {
		name    string
		outcome string
		status  int
	}{
		{name: "error", outcome: "unauthenticated", status: 401},
		{name: "unknown", outcome: "outcome_unknown", status: 500},
		{name: "non_2xx", outcome: "ok", status: 503},
	}
	for _, item := range cases {
		test.Run(item.name, func(test *testing.T) {
			observer := newRequestPhaseObserver()
			ctx, trace := beginRequestPhases(context.Background(), observer, "synthetic_failure", "query", time.Now())
			requestPhaseSpan(ctx, requestPhaseRespond)()
			trace.finish("synthetic_failure", item.outcome, item.status)
			record, found := observer.TryReceive()
			if !found {
				test.Fatal("a fast failure was filtered out")
			}
			if record.Outcome != item.outcome || record.Status != item.status {
				test.Fatalf("failure record changed: %+v", record)
			}
			if got := observer.filtered.Load(); got != 0 {
				test.Fatalf("a kept failure incremented filtered: %d", got)
			}
		})
	}
}

// The closing line carries the number of requests the slow filter removed, so a complete
// stream is not mistaken for a capture of every request.
func TestRequestPhaseSinkReportsFiltered(test *testing.T) {
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	observer := newRequestPhaseObserver()
	sink, err := phaseSink(context.Background(), observer, directory)
	if err != nil {
		test.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		_, trace := beginRequestPhases(context.Background(), observer, "synthetic_fast", "query", time.Now())
		trace.finish("synthetic_fast", "ok", 200)
	}
	_, slow := beginRequestPhases(context.Background(), observer, "synthetic_slow", "statement", time.Now().Add(-time.Second))
	slow.finish("synthetic_slow", "ok", 200)
	if !sink.stop() {
		test.Fatal("the sink did not stop")
	}
	encoded, err := os.ReadFile(filepath.Join(directory, requestPhaseFileName))
	if err != nil {
		test.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(encoded)), "\n")
	if len(lines) != 2 {
		test.Fatalf("phase stream lines = %d, want one slow record and the closing line: %s", len(lines), encoded)
	}
	var closing serviceRequestPhaseClose
	if err := json.Unmarshal([]byte(lines[1]), &closing); err != nil || !closing.Complete || closing.Filtered != 2 {
		test.Fatalf("the closing line did not report the filtered requests: %v %s", err, lines[1])
	}
}

// The closing line still distinguishes a full stream from a bounded one at the global limit.
func TestRequestPhaseClosingLineStaysBounded(test *testing.T) {
	observer := newRequestPhaseObserver()
	observer.filtered.Store(7)
	sink := &serviceRequestPhaseSink{observer: observer, stream: "synthetic"}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	sink.finish(func() {}, encoder, requestPhaseLimit-1, nil)
	var complete serviceRequestPhaseClose
	if err := json.Unmarshal(output.Bytes(), &complete); err != nil || !complete.Complete || complete.Outcome != "stopped" || complete.Filtered != 7 {
		test.Fatalf("the complete closing line is wrong: %v %s", err, output.String())
	}
	output.Reset()
	sink.finish(func() {}, encoder, requestPhaseLimit, nil)
	var bounded serviceRequestPhaseClose
	if err := json.Unmarshal(output.Bytes(), &bounded); err != nil || bounded.Complete || bounded.Outcome != "bounded" || bounded.Filtered != 7 {
		test.Fatalf("the bounded closing line is wrong: %v %s", err, output.String())
	}
}

// The sink is exclusive and private, and it states whether it captured the whole stream.
func TestRequestPhaseSinkIsExclusiveAndComplete(test *testing.T) {
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	observer := newRequestPhaseObserver()
	sink, err := phaseSink(context.Background(), observer, directory)
	if err != nil {
		test.Fatal(err)
	}
	observer.record(requestPhaseRecord{RequestID: "synthetic_request", Action: "query", Outcome: "ok", Status: 200, TotalNS: 10, AccountedNS: 10, Spans: []requestPhaseSegment{{Phase: requestPhaseAuth, NS: 10}}})
	path := filepath.Join(directory, requestPhaseFileName)
	if _, err := phaseSink(context.Background(), observer, directory); err == nil {
		test.Fatal("a second sink replaced or appended the first")
	}
	if !sink.stop() {
		test.Fatal("the sink did not stop")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		test.Fatal("the phase stream is not a private regular file")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(encoded)), "\n")
	if len(lines) != 2 {
		test.Fatalf("phase stream lines = %d, want the record and the closing line: %s", len(lines), encoded)
	}
	var record serviceRequestPhaseRecord
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil || record.RequestID != "synthetic_request" || record.Action != "query" || len(record.Spans) != 1 {
		test.Fatalf("phase record did not survive the sink: %v %s", err, lines[0])
	}
	var closing serviceRequestPhaseClose
	if err := json.Unmarshal([]byte(lines[1]), &closing); err != nil || closing.Action != "observer" || !closing.Complete || closing.Filtered != 0 {
		test.Fatalf("the closing line did not state completeness and filtering: %v %s", err, lines[1])
	}
	// A sink that was never opened is not a failure, so the service can stop unconditionally.
	var absent *serviceRequestPhaseSink
	if !absent.stop() {
		test.Fatal("a disabled sink reported an incomplete stop")
	}
}

// The disabled path is a nil check: it allocates nothing and cannot slow a request down.
func BenchmarkRequestPhaseDisabled(benchmark *testing.B) {
	ctx := context.Background()
	started := time.Now()
	benchmark.ReportAllocs()
	for index := 0; index < benchmark.N; index++ {
		bound, trace := beginRequestPhases(ctx, nil, "synthetic_request", "query", started)
		leave := requestPhaseSpan(bound, requestPhaseWorker)
		leave()
		trace.finish("synthetic_request", "ok", 200)
	}
}

func BenchmarkRequestPhaseEnabled(benchmark *testing.B) {
	observer := newRequestPhaseObserver()
	ctx, trace := beginRequestPhases(context.Background(), observer, "synthetic_request", "query", time.Now())
	benchmark.ReportAllocs()
	for index := 0; index < benchmark.N; index++ {
		requestPhaseSpan(ctx, requestPhaseAuth)()
		requestPhaseSpan(ctx, requestPhaseWorker)()
	}
	trace.finish("synthetic_request", "ok", 200)
}
