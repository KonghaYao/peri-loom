package stress

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type capacityTransport struct {
	queries []string
	failAt  int
	code    string
	status  int
	cancel  context.CancelFunc
}

func (transport *capacityTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var input wireRequest
	if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
		return nil, err
	}
	transport.queries = append(transport.queries, input.SQL)
	status := 200
	result := any(map[string]any{"executed": true})
	var failure any
	if strings.HasSuffix(request.URL.Path, "/checkpoint") {
		result = map[string]any{"checkpointed": true}
	}
	if transport.failAt == len(transport.queries) {
		status = transport.status
		result = nil
		failure = map[string]any{"code": transport.code}
	}
	if transport.cancel != nil && len(transport.queries) == 2 {
		transport.cancel()
	}
	encoded, err := json.Marshal(map[string]any{"version": 1, "request_id": input.RequestID, "ok": failure == nil, "result": result, "error": failure})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(encoded)))}, nil
}

func capacitySession(transport *capacityTransport) (*Session, *Fixture) {
	client := &Client{endpoint: "http://127.0.0.1:1", key: testKey, http: &http.Client{Transport: transport},
		ids: &IDs{Prefix: "capacity_unit"}, budget: &DataBudget{Maximum: 1 << 20}, maxResponse: 1 << 20}
	fixture := &Fixture{Client: client, Model: NewModel("capacity", 1000, 1)}
	return &Session{client: client, ID: "opaque_fixture"}, fixture
}

func TestCapacityHeldWriterCommitRollbackAndReservedCleanup(t *testing.T) {
	for _, sequence := range []int{0, 1} {
		transport := &capacityTransport{}
		session, fixture := capacitySession(transport)
		task := fixture.Model.Task("sustained_wal", "unit_operation", rand.New(rand.NewSource(1)))
		_, identifiers, mutations, failure := capacityWorkflow(context.Background(), session, fixture, task, "held_writer", sequence, time.Second)
		if failure != nil || len(identifiers) != 3 {
			t.Fatalf("workflow not acknowledged: %v", failure)
		}
		terminal := "COMMIT"
		if sequence == 1 {
			terminal = "ROLLBACK"
		}
		if transport.queries[0] != "BEGIN IMMEDIATE" || transport.queries[2] != terminal || strings.Contains(transport.queries[1], "BEGIN") || strings.Contains(transport.queries[1], "COMMIT") {
			t.Fatal("held transaction was not split into actual begin/write/terminal RPCs")
		}
		if (sequence == 0 && len(mutations) != 1) || (sequence == 1 && len(mutations) != 0) {
			t.Fatal("rolled-back writes must not enter acknowledged durable ledger")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	transport := &capacityTransport{cancel: cancel}
	session, fixture := capacitySession(transport)
	task := fixture.Model.Task("sustained_wal", "canceled_operation", rand.New(rand.NewSource(1)))
	_, _, mutations, failure := capacityWorkflow(ctx, session, fixture, task, "held_writer", 0, time.Second)
	if failure == nil || failure.Unknown || len(mutations) != 0 || transport.queries[2] != "ROLLBACK" {
		t.Fatal("determinate cancellation must use independent bounded rollback cleanup")
	}
}

func TestCapacityUnknownDoesNotRollbackOrCloseOrReplay(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		transport := &capacityTransport{failAt: failAt, code: "outcome_unknown", status: 500}
		session, fixture := capacitySession(transport)
		task := fixture.Model.Task("sustained_wal", "unknown_operation", rand.New(rand.NewSource(1)))
		_, _, _, failure := capacityWorkflow(context.Background(), session, fixture, task, "held_writer", 0, time.Second)
		if failure == nil || !failure.Unknown || !session.poisoned || len(transport.queries) != failAt {
			t.Fatal("Unknown must retain unresolved session without extra SQL")
		}
		if session.Close(context.Background()) == nil || len(transport.queries) != failAt {
			t.Fatal("poisoned Close must remain local and issue no API")
		}
	}
}

func TestCapacityIOUsesActualManagementCheckpointAndStopsIncompleteWrite(t *testing.T) {
	transport := &capacityTransport{}
	session, fixture := capacitySession(transport)
	artifacts, err := NewArtifacts(filepath.Join(t.TempDir(), "io"), 1<<20, []string{testKey})
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	fixture.Artifacts = artifacts
	fixture.Management = fixture.Client
	task := fixture.Model.Task("sustained_wal", "io_operation", rand.New(rand.NewSource(1)))
	_, _, mutations, failure := capacityWorkflow(context.Background(), session, fixture, task, "io_checkpoint", 0, time.Second)
	if failure != nil || len(mutations) != 1 || len(transport.queries) != 2 {
		t.Fatal("IO competition requires actual write and checkpoint, not a request flood")
	}
	transport = &capacityTransport{failAt: 2, code: "quota_exceeded", status: 429}
	session, fixture = capacitySession(transport)
	fixture.Artifacts = artifacts
	fixture.Management = fixture.Client
	_, _, _, failure = capacityWorkflow(context.Background(), session, fixture, task, "io_checkpoint", 0, time.Second)
	if failure == nil || !failure.Unknown || !session.poisoned {
		t.Fatal("acknowledged write with incomplete checkpoint is unresolved workflow, never replayed")
	}
}

func TestCapacityDemandAndDatabaseAllocation(t *testing.T) {
	fixtures := []*Fixture{{Target: Target{Project: "primary", Database: DatabaseID{Branch: "first"}}},
		{Target: Target{Project: "primary", Database: DatabaseID{Branch: "second"}}},
		{Target: Target{Project: "noise", Database: DatabaseID{Branch: "third"}}}}
	selected := capacityPhaseFixtures(fixtures, 4)
	projects := []string{}
	for _, fixture := range selected {
		projects = append(projects, fixture.Target.Project)
	}
	if !reflect.DeepEqual(projects, []string{"primary", "primary", "noise", "noise"}) {
		t.Fatal("equal projects must receive equal session allocation while both primary DBs remain active")
	}
	for _, demand := range []int{0, 10} {
		primaryOffers := 0
		for sequence := 0; sequence < 100; sequence++ {
			slot := capacitySlot(sequence, 4, selected, demand)
			if selected[slot].Target.Project == "primary" {
				primaryOffers++
			}
		}
		expected := 50
		if demand == 10 {
			expected = 10
		}
		if primaryOffers != expected {
			t.Fatal("offered victim fraction is not frozen deterministic demand")
		}
	}
}

func TestCapacityIsolatedDemandMatchesCombinedAndNoRoundingExtraOffer(t *testing.T) {
	for _, concurrency := range []int{2, 4, 8} {
		for _, phase := range []string{"isolated_primary_", "isolated_noise_"} {
			if capacityPhaseSlots(concurrency, phase, true) != concurrency/2 {
				t.Fatal("isolated tenant slots must match combined half, not borrow the other tenant's slots")
			}
		}
		if capacityPhaseSlots(concurrency, "combined_", true) != concurrency || capacityPhaseSlots(concurrency, "isolated_primary_", false) != concurrency {
			t.Fatal("combined/single total slots changed")
		}
	}
	settings := DefaultSettings()
	settings.OfferedRate = 5
	settings.VictimEvery = 10
	primary := capacityPhaseSettings(settings, "isolated_primary_", true)
	noise := capacityPhaseSettings(settings, "isolated_noise_", true)
	if primary.Validate() != nil || noise.Validate() != nil || plannedOffers(primary, 30*time.Second) != 15 || plannedOffers(noise, 30*time.Second) != 135 {
		t.Fatal("fractional low-rate baseline must match combined per-project offered demand")
	}
	if offerSchedule(noise, 135) != 30*time.Second || offerSchedule(primary, 15) != 30*time.Second {
		t.Fatal("rounded fractional period must not emit an extra offer at the deadline")
	}
	settings = DefaultSettings()
	settings.OfferedRate = 25
	for sequence := 0; sequence < 100; sequence++ {
		if offerSchedule(settings, sequence) != time.Duration(sequence)*40*time.Millisecond {
			t.Fatal("unweighted legacy scheduler changed")
		}
	}
}
