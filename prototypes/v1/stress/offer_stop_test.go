package stress

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type offerStopTransport struct {
	started     atomic.Int64
	inflight    atomic.Int64
	opened      atomic.Int64
	closed      atomic.Int64
	canceled    atomic.Int64
	twoStarted  chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func (transport *offerStopTransport) releasePeers() {
	transport.releaseOnce.Do(func() { close(transport.release) })
}

func (transport *offerStopTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var input wireRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		return nil, err
	}
	switch request.URL.Path {
	case "/v1/sql/open":
		return phaseDrainReply(input.RequestID, 200, map[string]any{"session": fmt.Sprintf("%032x", transport.opened.Add(1)), "readonly": false}, nil), nil
	case "/v1/sql/close":
		transport.closed.Add(1)
		return phaseDrainReply(input.RequestID, 200, map[string]bool{"closed": true}, nil), nil
	}
	if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.GotConn != nil {
		trace.GotConn(httptrace.GotConnInfo{})
	}
	transport.inflight.Add(1)
	defer transport.inflight.Add(-1)
	if transport.started.Add(1) == 2 {
		close(transport.twoStarted)
	}
	select {
	case <-request.Context().Done():
		transport.canceled.Add(1)
		return nil, request.Context().Err()
	case <-transport.release:
		if err := request.Context().Err(); err != nil {
			transport.canceled.Add(1)
			return nil, err
		}
		return phaseDrainReply(input.RequestID, 200, map[string]bool{"executed": true}, nil), nil
	}
}

func offerStopFixture(test *testing.T) (*Fixture, *Artifacts, *IDs, Settings, *offerStopTransport, chan struct{}) {
	test.Helper()
	fixture, artifacts, identifiers, settings := phaseDrainFixture(test, &phaseDrainTransport{})
	transport := &offerStopTransport{twoStarted: make(chan struct{}), release: make(chan struct{})}
	fixture.Client.http.Transport = transport
	settings.RequestTimeout = 2 * time.Second
	stop := make(chan struct{})
	settings.OfferStop = stop
	test.Cleanup(transport.releasePeers)
	return fixture, artifacts, identifiers, settings, transport, stop
}

func offerStopAwaitTwo(test *testing.T, transport *offerStopTransport) {
	test.Helper()
	select {
	case <-transport.twoStarted:
		if transport.inflight.Load() != 2 || transport.started.Load() != 2 {
			test.Fatal("expected two actually dispatched SQL requests before offer stop")
		}
	case <-time.After(time.Second):
		test.Fatal("two requests did not reach the fake HTTP transport")
	}
}

func offerStopAssertDrain(test *testing.T, result PhaseResult, fixture *Fixture, artifacts *Artifacts, transport *offerStopTransport) {
	test.Helper()
	if result.Status != "FAIL" || result.Reason != "operator_stop_offers" {
		test.Fatalf("operator stop falsely completed or lost its reason: %+v", result)
	}
	if result.Attempted != 2 || result.Completed != 2 || result.Acked != 2 || result.Unknown != 0 || result.CleanupUnknown != 0 || result.Timeouts != 0 || result.CorrectnessFailures != 0 || len(result.Errors) != 0 {
		test.Fatalf("offer stop canceled or lost an admitted request: %+v", result)
	}
	if transport.started.Load() != 2 || transport.canceled.Load() != 0 || transport.inflight.Load() != 0 || transport.opened.Load() != 2 || transport.closed.Load() != 2 {
		test.Fatal("new SQL admitted, existing request canceled, or cleanup ACK lost")
	}
	if result.PlannedOffers != result.Offered+result.MissedOffers || result.Offered != result.Attempted+result.ClientAdmissionDropped+result.QueueExpired || result.QueueExpired < 0 || result.ClientAdmissionDropped < 0 {
		test.Fatalf("offer/admission/queue accounting denominator changed: %+v", result)
	}
	if result.ElapsedNS <= 0 || result.OfferingElapsedNS < 0 || result.OfferingElapsedNS > result.ElapsedNS || result.AchievedRate != float64(result.Acked)*1e9/float64(result.ElapsedNS) {
		test.Fatal("ACK rate no longer uses the actual elapsed time including drain")
	}
	if result.PerProjectAcked[fixture.Target.Project] != 2 || result.PerProjectOffered[fixture.Target.Project] != result.Offered {
		test.Fatal("per-project accounting dropped naturally completed ACKs")
	}
	records := phaseDrainRecords(test, artifacts)
	if len(records) != 2 || len(fixture.Acked) != 2 {
		test.Fatal("natural ACK evidence or mutation ledger missing")
	}
	identifiers := map[string]bool{}
	for _, record := range records {
		if !record.Acked || !record.Correct || record.Failure != nil || len(record.RequestIDs) != 1 || len(record.Mutations) != 1 {
			test.Fatal("operator stop rewrote an admitted operation outcome")
		}
		if identifiers[record.RequestIDs[0]] {
			test.Fatal("logical SQL request retried")
		}
		identifiers[record.RequestIDs[0]] = true
	}
}

func TestPhaseOfferStopDrainsAdmittedSQLWithoutCancelOrThirdDispatch(test *testing.T) {
	fixture, artifacts, identifiers, settings, transport, stop := offerStopFixture(test)
	parent, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	finished := make(chan PhaseResult, 1)
	go func() {
		finished <- runPhase(parent, "sustained_wal", "sustained_wal", "smoke", 2, time.Second, []*Fixture{fixture}, settings, artifacts, identifiers)
	}()
	offerStopAwaitTwo(test, transport)
	close(stop)
	transport.releasePeers()
	select {
	case result := <-finished:
		offerStopAssertDrain(test, result, fixture, artifacts, transport)
		if parent.Err() != nil {
			test.Fatal("soft offer stop canceled the parent context")
		}
	case <-time.After(3 * time.Second):
		test.Fatal("operator-stopped phase did not finish after admitted peers ACKed")
	}
}

func TestPhaseOfferStopPreclosedChannelDoesNotDispatchSQL(test *testing.T) {
	fixture, artifacts, identifiers, settings, transport, stop := offerStopFixture(test)
	close(stop)
	transport.releasePeers()
	result := runPhase(context.Background(), "sustained_wal", "sustained_wal", "smoke", 2, 20*time.Millisecond, []*Fixture{fixture}, settings, artifacts, identifiers)
	if result.Status != "FAIL" || result.Reason != "operator_stop_offers" || result.Attempted != 0 || result.Completed != 0 || result.Acked != 0 || result.Unknown != 0 || result.CleanupUnknown != 0 || transport.started.Load() != 0 {
		test.Fatalf("preclosed offer-stop channel admitted SQL or pretended completion: %+v", result)
	}
	if transport.opened.Load() != transport.closed.Load() {
		test.Fatal("preclosed stop leaked a session")
	}
}

func TestScenarioOfferStopDoesNotStartLaterPhasesOrConcurrency(test *testing.T) {
	fixture, artifacts, identifiers, settings, transport, stop := offerStopFixture(test)
	settings.Concurrency = []int{2, 4}
	settings.Warmup, settings.Smoke, settings.Measure = time.Second, time.Second, time.Second
	runner := SerialRunner{Settings: settings}
	parent, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	finished := make(chan ScenarioResult, 1)
	go func() {
		finished <- runner.scenario(parent, Scenario{Name: "sustained_wal", Phase: "sql", Capability: "SUPPORTED"}, []*Fixture{fixture}, nil, artifacts, identifiers)
	}()
	offerStopAwaitTwo(test, transport)
	close(stop)
	transport.releasePeers()
	select {
	case result := <-finished:
		if result.Status != "FAIL" || result.Reason != "operator_stop_offers" || len(result.Phases) != 1 || result.Phases[0].Phase != "warmup" || len(result.Correctness) != 0 {
			test.Fatalf("runner continued later phases or falsely verified stopped workload: %+v", result)
		}
		offerStopAssertDrain(test, result.Phases[0], fixture, artifacts, transport)
	case <-time.After(3 * time.Second):
		test.Fatal("scenario did not return after stopped warmup drained")
	}
}

func TestOfferStopSettingsJSONExcludesRuntimeChannel(test *testing.T) {
	settings := DefaultSettings()
	baseline, err := json.Marshal(settings)
	if err != nil {
		test.Fatal(err)
	}
	for _, closed := range []bool{false, true} {
		stop := make(chan struct{})
		if closed {
			close(stop)
		}
		settings.OfferStop = stop
		encoded, err := json.Marshal(settings)
		if err != nil || string(encoded) != string(baseline) {
			test.Fatalf("runtime offer-stop channel leaked into report settings: error=%v", err)
		}
	}
}
