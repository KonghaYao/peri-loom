package stress

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type phaseDrainTransport struct {
	allStarted chan struct{}
	started    atomic.Int64
	opened     atomic.Int64
	closed     atomic.Int64
	mode       string
	peerDelay  time.Duration
	onStarted  func()
	peerRows   any
}

func phaseDrainReply(identifier string, status int, result any, failure any) *http.Response {
	encoded, _ := json.Marshal(map[string]any{"version": 1, "request_id": identifier, "ok": failure == nil, "result": result, "error": failure})
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(encoded)))}
}

func (transport *phaseDrainTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var input wireRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		return nil, err
	}
	if request.URL.Path == "/v1/sql/open" {
		return phaseDrainReply(input.RequestID, 200, map[string]any{"session": fmt.Sprintf("%032x", transport.opened.Add(1)), "readonly": false}, nil), nil
	}
	if request.URL.Path == "/v1/sql/close" {
		transport.closed.Add(1)
		return phaseDrainReply(input.RequestID, 200, map[string]any{"closed": true}, nil), nil
	}
	if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.GotConn != nil {
		trace.GotConn(httptrace.GotConnInfo{})
	}
	ordinal := transport.started.Add(1)
	if ordinal == 3 {
		if transport.onStarted != nil {
			transport.onStarted()
		}
		close(transport.allStarted)
	}
	select {
	case <-transport.allStarted:
	case <-request.Context().Done():
		return nil, request.Context().Err()
	}
	if transport.mode == "force_cancel" {
		<-request.Context().Done()
		return nil, request.Context().Err()
	}
	if ordinal == 1 && transport.mode == "lost_commit" {
		return nil, io.ErrUnexpectedEOF
	}
	if ordinal == 1 && transport.mode == "artifact_failure" {
		return phaseDrainReply(input.RequestID, 200, map[string]any{"executed": true}, nil), nil
	}
	if ordinal == 1 && transport.mode == "incorrect_rows" {
		return phaseDrainReply(input.RequestID, 200, [][]string{{"wrong"}}, nil), nil
	}
	if ordinal == 1 {
		return phaseDrainReply(input.RequestID, 400, nil, map[string]any{"code": "sql_error", "execution_completed": true, "may_have_committed": true}), nil
	}
	select {
	case <-request.Context().Done():
		return nil, request.Context().Err()
	case <-time.After(transport.peerDelay):
		if transport.mode == "incorrect_rows" {
			return phaseDrainReply(input.RequestID, 200, transport.peerRows, nil), nil
		}
		return phaseDrainReply(input.RequestID, 200, map[string]any{"executed": true}, nil), nil
	}
}

func phaseDrainFixture(t *testing.T, transport *phaseDrainTransport) (*Fixture, *Artifacts, *IDs, Settings) {
	t.Helper()
	settings := DefaultSettings()
	settings.OfferedRate = 1000
	settings.RequestTimeout = 200 * time.Millisecond
	client := unitClient(t, "http://127.0.0.1:1", settings)
	client.http.Transport = transport
	artifacts, err := NewArtifacts(filepath.Join(t.TempDir(), "run"), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	client.artifacts = artifacts
	fixture := &Fixture{Client: client, Model: NewModel("stress_drain", 1000, 1), Target: Target{Project: "stress_drain", Database: DatabaseID{Namespace: Namespace{Tenant: "local", Database: "stress_drain"}, Branch: "main"}}}
	return fixture, artifacts, client.ids, settings
}

func phaseDrainRecords(t *testing.T, artifacts *Artifacts) []OperationRecord {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join(artifacts.directory, "operations.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var records []OperationRecord
	for _, line := range strings.Split(strings.TrimSpace(string(encoded)), "\n") {
		var record OperationRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record.Type == "operation" {
			records = append(records, record)
		}
	}
	return records
}

func TestPhaseKnownSQLFailureSoftStopsAndDrainsAdmittedPeers(t *testing.T) {
	transport := &phaseDrainTransport{allStarted: make(chan struct{}), peerDelay: 30 * time.Millisecond}
	fixture, artifacts, identifiers, settings := phaseDrainFixture(t, transport)
	result := runPhase(context.Background(), "sustained_wal", "sustained_wal", "smoke", 3, time.Second, []*Fixture{fixture}, settings, artifacts, identifiers)
	if result.Status != "FAIL" || result.Reason != "sql_error" || result.Errors["sql_error"] != 1 || result.Acked != 2 || result.Unknown != 0 || result.CleanupUnknown != 0 {
		t.Fatalf("known failure must remain FAIL without canceling peers: %+v", result)
	}
	if result.Attempted != 3 || result.Completed != 3 || transport.started.Load() != 3 || transport.closed.Load() != 3 || len(fixture.Acked) != 2 {
		t.Fatalf("dispatch/drain/ledger counts changed: %+v", result)
	}
	if result.Offered != result.Attempted+result.ClientAdmissionDropped+result.QueueExpired || result.ElapsedNS >= int64(settings.RequestTimeout+100*time.Millisecond) {
		t.Fatal("invalid offered denominator or unbounded drain")
	}
	records := phaseDrainRecords(t, artifacts)
	if len(records) != 3 || records[0].Failure == nil || records[0].Failure.Code != "sql_error" {
		t.Fatal("first error evidence lost or reordered")
	}
	seen := map[string]bool{}
	var expected []Mutation
	for _, record := range records {
		if len(record.RequestIDs) != 1 || seen[record.RequestIDs[0]] {
			t.Fatal("duplicate SQL request or replay")
		}
		seen[record.RequestIDs[0]] = true
		if record.Acked {
			if !record.Correct || record.Failure != nil || len(record.Mutations) != 1 {
				t.Fatal("ACK oracle or mutation record missing")
			}
			expected = append(expected, record.Mutations...)
		} else if len(record.Mutations) != 0 {
			t.Fatal("failed write entered acknowledged ledger")
		}
	}
	for index, mutation := range fixture.Acked {
		if mutation != expected[index] {
			t.Fatal("ledger differs from ordered ACK-only evidence")
		}
	}
}

func TestPhaseLostCommitRemainsUnknownWithoutReplay(t *testing.T) {
	transport := &phaseDrainTransport{allStarted: make(chan struct{}), mode: "lost_commit", peerDelay: 30 * time.Millisecond}
	fixture, artifacts, identifiers, settings := phaseDrainFixture(t, transport)
	result := runPhase(context.Background(), "sustained_wal", "sustained_wal", "smoke", 3, time.Second, []*Fixture{fixture}, settings, artifacts, identifiers)
	if result.Status != "UNRESOLVED" || result.Unknown != 1 || result.CleanupUnknown != 1 || result.Acked != 2 || result.Attempted != 3 {
		t.Fatalf("lost COMMIT certainty changed: %+v", result)
	}
	if transport.started.Load() != 3 || transport.closed.Load() != 2 || len(fixture.Acked) != 2 {
		t.Fatal("unknown COMMIT was replayed, closed, or included in ACK ledger")
	}
	records := phaseDrainRecords(t, artifacts)
	if len(records) != 3 || records[0].Failure == nil || !records[0].Failure.Unknown || len(records[0].Mutations) != 0 {
		t.Fatal("lost COMMIT first evidence missing")
	}
	seen := map[string]bool{}
	for _, record := range records {
		if len(record.RequestIDs) != 1 || seen[record.RequestIDs[0]] {
			t.Fatal("SQL replay after unknown")
		}
		seen[record.RequestIDs[0]] = true
	}
}

func TestPhaseParentForceCancelStillPoisonsInflight(t *testing.T) {
	transport := &phaseDrainTransport{allStarted: make(chan struct{}), mode: "force_cancel"}
	fixture, artifacts, identifiers, settings := phaseDrainFixture(t, transport)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport.onStarted = cancel
	result := runPhase(parent, "sustained_wal", "sustained_wal", "smoke", 3, time.Second, []*Fixture{fixture}, settings, artifacts, identifiers)
	if result.Status != "UNRESOLVED" || result.Unknown != 3 || result.CleanupUnknown != 3 || result.Acked != 0 || result.Attempted != 3 {
		t.Fatalf("hard cancellation was softened: %+v", result)
	}
	if len(fixture.Acked) != 0 || transport.closed.Load() != 0 || transport.started.Load() != 3 {
		t.Fatal("canceled request entered ledger or poisoned session was closed")
	}
}

func TestPhaseKnownErrorDrainKeepsOriginalFiniteDeadlines(t *testing.T) {
	transport := &phaseDrainTransport{allStarted: make(chan struct{}), peerDelay: time.Second}
	fixture, artifacts, identifiers, settings := phaseDrainFixture(t, transport)
	settings.RequestTimeout = 40 * time.Millisecond
	fixture.Client.http.Timeout = settings.RequestTimeout
	result := runPhase(context.Background(), "sustained_wal", "sustained_wal", "smoke", 3, 2*time.Second, []*Fixture{fixture}, settings, artifacts, identifiers)
	if result.Status != "UNRESOLVED" || result.Errors["sql_error"] != 1 || result.Unknown != 2 || result.Timeouts != 2 || result.Acked != 0 {
		t.Fatalf("original deadline or error certainty lost: %+v", result)
	}
	if result.ElapsedNS >= int64(time.Second) || transport.started.Load() != 3 || len(fixture.Acked) != 0 {
		t.Fatal("drain extended deadline or dispatched queued jobs")
	}
}

func TestPhaseArtifactFailureStillHardCancelsPeers(t *testing.T) {
	transport := &phaseDrainTransport{allStarted: make(chan struct{}), mode: "artifact_failure", peerDelay: time.Second}
	fixture, artifacts, identifiers, settings := phaseDrainFixture(t, transport)
	transport.onStarted = func() { _ = artifacts.Close() }
	result := runPhase(context.Background(), "sustained_wal", "sustained_wal", "smoke", 3, time.Second, []*Fixture{fixture}, settings, artifacts, identifiers)
	if result.Status != "UNRESOLVED" || result.Unknown != 2 || result.Acked != 1 || !artifacts.failed.Load() {
		t.Fatalf("artifact safety did not hard cancel: %+v", result)
	}
	if result.ElapsedNS >= int64(time.Second) || transport.started.Load() != 3 || len(fixture.Acked) != 1 {
		t.Fatal("artifact failure allowed continued dispatch or false ledger ACK")
	}
}

func TestPhaseIncorrectRowsRemainFailureWhilePeersDrain(t *testing.T) {
	transport := &phaseDrainTransport{allStarted: make(chan struct{}), mode: "incorrect_rows", peerDelay: 30 * time.Millisecond}
	fixture, artifacts, identifiers, settings := phaseDrainFixture(t, transport)
	transport.peerRows = [][]string{{fixture.Model.Large}}
	result := runPhase(context.Background(), "large_values", "large_values", "smoke", 3, time.Second, []*Fixture{fixture}, settings, artifacts, identifiers)
	if result.Status != "FAIL" || result.Reason != "operation_rows_or_index_plan_mismatch" || result.CorrectnessFailures != 1 || result.Acked != 3 || result.Unknown != 0 || result.CleanupUnknown != 0 {
		t.Fatalf("oracle mismatch was masked or canceled peers: %+v", result)
	}
	if len(fixture.Acked) != 0 || transport.started.Load() != 3 || transport.closed.Load() != 3 {
		t.Fatal("read oracle failure dispatched more work or changed write ledger")
	}
	records := phaseDrainRecords(t, artifacts)
	if len(records) != 3 || !records[0].Acked || records[0].Correct || records[0].Failure != nil {
		t.Fatal("ACK versus correctness distinction lost")
	}
}
