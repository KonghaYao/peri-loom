package platform

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	controller "peri-loom/controller"
)

type releaseTestStopProof struct {
	failures map[string]error
	calls    []string
}

func (proof *releaseTestStopProof) VerifyOwnedWorkerStopped(_ context.Context, worker string) error {
	proof.calls = append(proof.calls, worker)
	return proof.failures[worker]
}

type releaseTestFence struct {
	fence IsolationFence
	err   error
	calls int
}

func (verifier *releaseTestFence) VerifyIsolationFence(context.Context, controller.DatabaseID) (IsolationFence, error) {
	verifier.calls++
	return verifier.fence, verifier.err
}

type releaseTestIdentity struct{}

func (releaseTestIdentity) Authenticate(string) (Principal, error) {
	return Principal{Subject: "operator", Project: "alpha", Role: "admin"}, nil
}
func (releaseTestIdentity) Authorize(Principal, controller.DatabaseID, string) error { return nil }
func (releaseTestIdentity) IssueToken(Principal, controller.DatabaseID, string, int64) (string, error) {
	return "", errors.New("unused")
}

type releaseFixture struct {
	management *Management
	store      *stateStore
	directory  string
	proof      *releaseTestStopProof
	fence      *releaseTestFence
}

// quarantineForRelease drives one operation to a durable quarantine whose local
// retirement already completed: the only state the release may start from.
func quarantineDecision(test *testing.T, store StateStore, database controller.DatabaseID, identifier string) {
	test.Helper()
	if err := beginOperation(store, database, identifier, "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(store, database, identifier, testRPCError(true, false, SQLRPCError{Ambiguous: true}), false); err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		key := operationKey(database, identifier)
		record, exists := state.Operations[key]
		if !exists {
			return errors.New("fixture quarantine missing")
		}
		record.Decision, record.DecisionRequestID = "retain_isolation_new_database_required", "retain_"+identifier
		state.Operations[key] = record
		return nil
	}); err != nil {
		test.Fatal(err)
	}
}

func quarantineForRelease(test *testing.T, store StateStore, database controller.DatabaseID, identifier string) {
	test.Helper()
	quarantineDecision(test, store, database, identifier)
	if err := store.Update(func(state *State) error {
		key := operationKey(database, identifier)
		record := state.Operations[key]
		record.RetirementPending = true
		state.Operations[key] = record
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		key := operationKey(database, identifier)
		record := state.Operations[key]
		record.LocalResourcesRetired, record.RetirementRequestID, record.RetirementPending = true, "retire_"+identifier, false
		state.Operations[key] = record
		return nil
	}); err != nil {
		test.Fatal(err)
	}
}

func releaseManagementFixture(test *testing.T) *releaseFixture {
	return releaseManagementFixtureMode(test, true)
}

// releaseManagementFixtureMode builds the durable quarantine either fully retired
// (the only legal starting point for a release) or decided but not yet retired.
func releaseManagementFixtureMode(test *testing.T, retired bool) *releaseFixture {
	test.Helper()
	service := operationDurableStore(test)
	store := service.(*stateStore)
	if retired {
		quarantineForRelease(test, store, securityDatabase(), "unknown")
	} else {
		quarantineDecision(test, store, securityDatabase(), "unknown")
	}
	proof := &releaseTestStopProof{failures: map[string]error{}}
	fence := &releaseTestFence{fence: IsolationFence{Mode: "detached", Epoch: 12}}
	management := &Management{config: ManagementConfig{State: store, Identity: releaseTestIdentity{}, StopProof: proof, Fence: fence}}
	return &releaseFixture{management: management, store: store, directory: filepath.Dir(store.disk.path), proof: proof, fence: fence}
}

func (fixture *releaseFixture) release(test *testing.T, requestID string) error {
	test.Helper()
	_, err := fixture.management.releaseIsolation(context.Background(), Principal{Subject: "operator", Project: "alpha", Role: "admin"}, managementRequest{RequestID: requestID, Database: securityDatabase()})
	return err
}

func (fixture *releaseFixture) record(test *testing.T) OperationRecord {
	test.Helper()
	state, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	return state.Operations[operationKey(securityDatabase(), "unknown")]
}

func (fixture *releaseFixture) isolated(test *testing.T) bool {
	test.Helper()
	state, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	_, found := isolatedOperation(state, securityDatabase(), false)
	return found
}

// Every proof is mandatory: a missing worker stop, a missing or negative data plane
// attestation, and an unfinished retirement each refuse with their own reason.
func TestIsolationReleaseRefusesIncompleteProof(test *testing.T) {
	for _, scenario := range []string{"no_stop_verifier", "stop_not_proven", "no_fence_verifier", "fence_unavailable", "fence_serving_writer", "fence_acquiring", "fence_bound_binding", "not_retired"} {
		test.Run(scenario, func(test *testing.T) {
			fixture := releaseManagementFixtureMode(test, scenario != "not_retired")
			expected := error(errReleaseFenceUnavailable)
			switch scenario {
			case "no_stop_verifier":
				fixture.management.config.StopProof = nil
				expected = errReleaseProofMissing
			case "stop_not_proven":
				fixture.proof.failures["worker-a"] = errors.New("owned native worker has not exited")
				expected = errReleaseProofMissing
			case "no_fence_verifier":
				fixture.management.config.Fence = nil
			case "fence_unavailable":
				fixture.fence.err = errors.New("authority fence unavailable")
			case "fence_serving_writer":
				fixture.fence.fence = IsolationFence{Mode: "writer", Epoch: 12, Bound: true}
			case "fence_acquiring":
				fixture.fence.fence = IsolationFence{Mode: "acquiring", Epoch: 12}
			case "fence_bound_binding":
				fixture.fence.fence = IsolationFence{Mode: "detached", Epoch: 12, Bound: true}
			case "not_retired":
				expected = errReleaseNotRetired
			}
			if err := fixture.release(test, "release_incomplete"); !errors.Is(err, expected) {
				test.Fatalf("%s refused with %v, want %v", scenario, err, expected)
			}
			if record := fixture.record(test); record.ReleaseRequestID != "" {
				test.Fatal("refused release mutated the durable record")
			}
			if !fixture.isolated(test) {
				test.Fatal("refused release cleared namespace isolation")
			}
		})
	}
}

// A restart never releases anything, and a release without proofs is refused exactly
// as it was before the restart.
func TestIsolationReleaseNeverHappensOnRestart(test *testing.T) {
	fixture := releaseManagementFixture(test)
	if err := fixture.store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(fixture.directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	fixture.store = reopened.(*stateStore)
	fixture.management.config.State = fixture.store
	if record := fixture.record(test); record.ReleaseRequestID != "" || record.State != "quarantined" || !record.MayHaveCommitted {
		test.Fatal("restart changed the durable quarantine")
	}
	if !fixture.isolated(test) {
		test.Fatal("restart cleared namespace isolation")
	}
	fixture.management.config.StopProof, fixture.management.config.Fence = nil, nil
	if err := fixture.release(test, "release_after_restart"); !errors.Is(err, errReleaseProofMissing) {
		test.Fatalf("restart-only release accepted: %v", err)
	}
	if !fixture.isolated(test) {
		test.Fatal("restart-only release cleared namespace isolation")
	}
}

// With both proofs present the release advances, is idempotent, survives a restart,
// and leaves the operation truthful: still quarantined and still may-have-committed.
func TestIsolationReleaseAdvancesWithProof(test *testing.T) {
	fixture := releaseManagementFixture(test)
	if err := fixture.release(test, "release_proven"); err != nil {
		test.Fatalf("proven release refused: %v", err)
	}
	record := fixture.record(test)
	if record.ReleaseRequestID != "release_proven" || record.ReleaseAuthorityMode != "detached" || record.ReleaseEpoch != 12 {
		test.Fatalf("release proof not recorded: %+v", record.OperationStatus)
	}
	if record.State != "quarantined" || !record.MayHaveCommitted || !record.ExecutionCompleted {
		test.Fatal("release rewrote unknown semantics")
	}
	if len(fixture.proof.calls) != 1 || fixture.proof.calls[0] != "worker-a" || fixture.fence.calls != 1 {
		test.Fatalf("release did not collect every proof exactly once: %v %d", fixture.proof.calls, fixture.fence.calls)
	}
	if fixture.isolated(test) {
		test.Fatal("proven release kept namespace isolation")
	}
	proofCalls, fenceCalls := len(fixture.proof.calls), fixture.fence.calls
	if err := fixture.release(test, "release_repeat"); err != nil {
		test.Fatalf("idempotent release refused: %v", err)
	}
	if len(fixture.proof.calls) != proofCalls || fixture.fence.calls != fenceCalls {
		test.Fatal("repeat release re-collected proofs instead of reading durable state")
	}
	if err := fixture.store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenStateStore(fixture.directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	fixture.store = reopened.(*stateStore)
	fixture.management.config.State = fixture.store
	if record := fixture.record(test); record.ReleaseRequestID != "release_proven" {
		test.Fatal("restart lost the recorded release")
	}
	if fixture.isolated(test) {
		test.Fatal("released namespace isolated again after restart")
	}
	if err := fixture.release(test, "release_after_restart"); err != nil {
		test.Fatalf("release after restart refused: %v", err)
	}
}

// A release ends one quarantine, not the isolation rule: the next unknown operation
// on the same database blocks admission again.
func TestIsolationReleaseDoesNotWeakenLaterUnknowns(test *testing.T) {
	fixture := releaseManagementFixture(test)
	if err := fixture.release(test, "release_proven"); err != nil {
		test.Fatal(err)
	}
	database := securityDatabase()
	if err := beginOperation(fixture.store, database, "second_unknown", "gateway", "execute", "tool_rpc", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if err := finishOperation(fixture.store, database, "second_unknown", &SQLRPCError{Ambiguous: true}, false); err != nil {
		test.Fatal(err)
	}
	if !fixture.isolated(test) {
		test.Fatal("a later unknown did not isolate the namespace")
	}
	if err := beginOperation(fixture.store, database, "blocked", "gateway", "execute", "tool_rpc", "worker-a", "", nil); !errors.Is(err, ErrDatabaseQuarantined) {
		test.Fatalf("admission accepted a new operation under a fresh unknown: %v", err)
	}
}

// The durable shape refuses unsafe release records, so no other path can fake one.
func TestIsolationReleaseRejectsUnsafeRecords(test *testing.T) {
	fixture := releaseManagementFixture(test)
	mark := func(store *stateStore, change func(*OperationRecord)) error {
		return store.Update(func(state *State) error {
			key := operationKey(securityDatabase(), "unknown")
			record := state.Operations[key]
			change(&record)
			state.Operations[key] = record
			return nil
		})
	}
	// A release requires a serving authority mode that proves fencing.
	if err := mark(fixture.store, func(record *OperationRecord) {
		record.ReleaseRequestID, record.ReleaseAuthorityMode = "release_proven", "writer"
	}); err == nil {
		test.Fatal("release with a serving authority mode accepted")
	}
	// The legal first record.
	if err := mark(fixture.store, func(record *OperationRecord) {
		record.ReleaseRequestID, record.ReleaseAuthorityMode, record.ReleaseEpoch = "release_proven", "detached", 12
	}); err != nil {
		test.Fatal(err)
	}
	if err := mark(fixture.store, func(record *OperationRecord) { record.ReleaseEpoch = 13 }); err == nil {
		test.Fatal("a recorded release was rewritten")
	}
	if err := mark(fixture.store, func(record *OperationRecord) { record.ReleaseRequestID = "" }); err == nil {
		test.Fatal("a recorded release was dropped")
	}
	if err := mark(fixture.store, func(record *OperationRecord) { record.State = "terminal" }); err == nil {
		test.Fatal("a released quarantine left the quarantine state")
	}
	if record := fixture.record(test); record.ReleaseAuthorityMode != "detached" {
		test.Fatal("fixture release record lost its evidence")
	}
	// A quarantine that has not completed local retirement cannot carry a release.
	unretired := releaseManagementFixtureMode(test, false)
	if err := mark(unretired.store, func(record *OperationRecord) {
		record.ReleaseRequestID, record.ReleaseAuthorityMode = "release_proven", "detached"
	}); err == nil {
		test.Fatal("release on an unretired quarantine accepted")
	}
}

// The refusals stay distinguishable at the API boundary, so an operator can tell
// which proof is missing.
func TestIsolationReleaseRefusalCodesAreDistinct(test *testing.T) {
	management, fixture, _, _ := nativeDiagnosticManagement(test)
	management.config.Identity, management.config.Audit = fixture.identity, fixture.audit
	_, token, err := NewAPIKey(fixture.state, "operator", "project-a", "admin", false)
	if err != nil {
		test.Fatal(err)
	}
	quarantineForRelease(test, fixture.state, fixture.first, "unknown")
	proof := &releaseTestStopProof{failures: map[string]error{}}
	management.config.StopProof = proof
	management.config.Fence = &releaseTestFence{fence: IsolationFence{Mode: "detached", Epoch: 4}}
	code := func(test *testing.T) string {
		test.Helper()
		response := operationHTTP(test, management, token, "/v1/databases/isolation/release", managementRequest{RequestID: "release_code", Database: fixture.first})
		var reply struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(response.Body.Bytes(), &reply)
		if response.Code != 409 {
			test.Fatalf("refusal status %d: %s", response.Code, response.Body.String())
		}
		return reply.Error.Code
	}
	management.config.StopProof = nil
	if observed := code(test); observed != "release_stop_proof_missing" {
		test.Fatalf("stop proof code %q", observed)
	}
	management.config.StopProof = proof
	fence := management.config.Fence.(*releaseTestFence)
	fence.err = errors.New("authority fence unavailable")
	if observed := code(test); observed != "release_fencing_proof_unavailable" {
		test.Fatalf("fencing code %q", observed)
	}
	fence.err = nil
	if err := fixture.state.Update(func(state *State) error {
		key := operationKey(fixture.first, "unknown")
		record := state.Operations[key]
		record.RetirementPending = true
		state.Operations[key] = record
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	if observed := code(test); observed != "release_not_retired" {
		test.Fatalf("retirement code %q", observed)
	}
}
