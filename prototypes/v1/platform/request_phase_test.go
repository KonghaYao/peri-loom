package platform

import (
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

// One read request reports its own segments: durable intent and terminal writes are separated
// from the worker call, and the reported parts never exceed the request.
func TestRequestPhaseRecordsReadSegments(test *testing.T) {
	fixture, observer := requestPhaseObserverFixture(test)
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
	// A read query registers a durable intent and then its terminal record, so both segments
	// must be present and neither may be folded into the other.
	if seen[requestPhaseIntent] == 0 || seen[requestPhaseTerminal] == 0 {
		test.Fatalf("a durable write was reported with no time: intent=%d terminal=%d", seen[requestPhaseIntent], seen[requestPhaseTerminal])
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
	fixture, observer := requestPhaseObserverFixture(test)
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
	fixture, observer := requestPhaseObserverFixture(test)
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
	var closing serviceRequestPhaseRecord
	if err := json.Unmarshal([]byte(lines[1]), &closing); err != nil || closing.Action != "observer" || !closing.Complete {
		test.Fatalf("the closing line did not state completeness: %v %s", err, lines[1])
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
