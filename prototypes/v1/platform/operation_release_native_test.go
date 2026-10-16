package platform

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// nativeReleaseFence reads what the data plane attests for the fixture database.
func nativeReleaseFence(test *testing.T, fixture *p6VerifyFixture, transport controller.ControlTransport) (IsolationFence, error) {
	test.Helper()
	identifier, err := controller.RandomID()
	if err != nil {
		return IsolationFence{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var actual controller.Actual
	if err := transport.Authority(ctx, identifier, authorityFenceAction{Command: "get_database_state", Database: fixture.database}, &actual); err != nil {
		return IsolationFence{}, err
	}
	if actual.Database != fixture.database {
		return IsolationFence{}, errNativeFenceIdentity
	}
	return IsolationFence{Mode: actual.Mode, Epoch: actual.Epoch, Bound: actual.Binding != nil}, nil
}

var errNativeFenceIdentity = fencesIdentityError{}

type fencesIdentityError struct{}

func (fencesIdentityError) Error() string { return "authority returned a different database" }

// nativeWaitForFence polls until the data plane reports the wanted state.
func nativeWaitForFence(test *testing.T, fixture *p6VerifyFixture, transport controller.ControlTransport, deadline time.Duration, wanted func(IsolationFence) bool) IsolationFence {
	test.Helper()
	limit := time.Now().Add(deadline)
	var last IsolationFence
	var saw error
	for {
		fence, err := nativeReleaseFence(test, fixture, transport)
		if err == nil {
			last = fence
			if wanted(fence) {
				return fence
			}
		} else {
			saw = err
		}
		if time.Now().After(limit) {
			test.Fatalf("authority never reached the wanted state: mode=%s bound=%v epoch=%d err=%v", last.Mode, last.Bound, last.Epoch, saw)
		}
		time.Sleep(2 * time.Second)
	}
}

func nativeReleaseCode(test *testing.T, fixture *p6VerifyFixture, requestID string) (int, string, string) {
	test.Helper()
	status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/isolation/release", requestID, map[string]any{"database": fixture.database})
	var failure struct {
		Code string `json:"code"`
	}
	if raw, exists := reply["error"]; exists {
		_ = json.Unmarshal(raw, &failure)
	}
	var result json.RawMessage
	if raw, exists := reply["result"]; exists {
		result = raw
	}
	return status, failure.Code, string(result)
}

// nativeDecideEveryQuarantine records the retain decision on every quarantine of the
// fixture database, which is what retirement and the release both require.
func nativeDecideEveryQuarantine(test *testing.T, fixture *p6VerifyFixture, known string) map[string]bool {
	test.Helper()
	status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations", "release_list", map[string]any{"database": fixture.database})
	if status != 200 {
		test.Fatalf("operation list unavailable: %d %s", status, reply)
	}
	var recovered struct {
		Operations []OperationStatus `json:"operations"`
	}
	if err := json.Unmarshal(reply["result"], &recovered); err != nil {
		test.Fatal("operation list undecodable")
	}
	decided := map[string]bool{}
	quarantinedWorkers := map[string]bool{}
	for _, record := range recovered.Operations {
		if record.State != "quarantined" {
			continue
		}
		quarantinedWorkers[record.Worker] = true
		if record.Decision != "" {
			continue
		}
		decided[record.OperationID] = true
		if status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations/decision", "retain_"+record.OperationID, map[string]any{"database": fixture.database, "operation_id": record.OperationID, "decision": "retain_isolation_new_database_required"}); status != 200 {
			test.Fatalf("retain decision for %s rejected: %d %s", record.OperationID, status, reply)
		}
	}
	if !decided[known] {
		test.Fatalf("the unknown operation %s was not among the decided quarantines: %v", known, decided)
	}
	return quarantinedWorkers
}

// nativeOfflineManagement builds the same management the platform runs, against the
// stopped platform's own directories, so retirement and the release can be driven with
// the real owned-stop proof and the real authority connection.
func nativeOfflineManagement(test *testing.T, fixture *p6VerifyFixture, transport controller.ControlTransport) (*Management, func()) {
	test.Helper()
	var store StateStore
	var err error
	for attempt := 0; ; attempt++ {
		store, err = OpenStateStore(fixture.config.StateDirectory)
		if err == nil {
			break
		}
		// The platform releases the private state lock as it exits, so a stop that has
		// returned can still race this open. Wait for it instead of failing.
		if !errors.Is(err, ErrStateLocked) || attempt > 60 {
			test.Fatalf("offline state open failed: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	signing, err := os.ReadFile(fixture.config.SigningKeyFile)
	if err != nil {
		store.Close()
		test.Fatal(err)
	}
	identity, err := NewIdentity(store, signing)
	if err != nil {
		store.Close()
		test.Fatal(err)
	}
	placements, err := controller.OpenStore(fixture.config.MetadataDirectory)
	if err != nil {
		store.Close()
		test.Fatal(err)
	}
	upstream, err := NewSQLUpstream(fixture.config.WorkerSQLTokens)
	if err != nil {
		placements.Close()
		store.Close()
		test.Fatal(err)
	}
	audit, err := OpenAudit(fixture.config.AuditPath)
	if err != nil {
		placements.Close()
		store.Close()
		test.Fatal(err)
	}
	resources, err := NewGateway(GatewayConfig{State: store, Placements: placements, Identity: identity, Upstream: upstream, Audit: audit})
	if err != nil {
		audit.Close()
		placements.Close()
		store.Close()
		test.Fatal(err)
	}
	management := &Management{config: ManagementConfig{
		State: store, Identity: identity, Resources: resources, Audit: audit,
		StopProof: unknownOwnedProcessStopVerifier{workers: fixture.workers},
		Fence:     authorityIsolationFence{transport: transport},
	}}
	return management, func() {
		audit.Close()
		placements.Close()
		store.Close()
	}
}

// End to end on the real stack: unknown → decision → retire → stop serving (the
// authority revokes the branch) → release with both proofs → the database serves
// again on a new epoch. The two negatives run inside the same flow: before the owned
// worker exits the release has no stop proof, and after the exit but before the
// revoke it has no fencing evidence.
func TestIsolationReleaseNativeEndToEnd(test *testing.T) {
	proxy := &unknownResponseLossProxy{counts: map[string]int{}}
	fixture := p6VerifyNewRouted(test, Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 4096}, func(worker, target string) string { return proxy.route(test, worker, target) })
	transport, err := controller.NewTransport(fixture.config.Controller.AuthorityURL, fixture.config.Controller.AuthorityToken, fixture.config.WorkerAuthorityTokens)
	if err != nil {
		test.Fatal(err)
	}
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE release_probe(id INTEGER PRIMARY KEY, marker TEXT NOT NULL)")
	serving, err := nativeReleaseFence(test, fixture, transport)
	if err != nil || !serving.Bound || serving.Mode != "writer" || serving.Epoch == 0 {
		test.Fatalf("fixture branch is not serving: %+v %v", serving, err)
	}

	// Durable Unknown on a committing write.
	proxy.mutex.Lock()
	proxy.target = "execute"
	proxy.counts = map[string]int{}
	proxy.mutex.Unlock()
	status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/execute", "release_unknown", map[string]any{"session": session, "sql": "INSERT INTO release_probe VALUES(1,'unknown')"})
	var failure struct {
		Code        string `json:"code"`
		OperationID string `json:"operation_id"`
	}
	if raw, exists := reply["error"]; !exists || json.Unmarshal(raw, &failure) != nil || status != 502 || failure.Code != "outcome_unknown" || failure.OperationID == "" {
		test.Fatalf("unknown not durably recorded: status=%d error=%s", status, reply["error"])
	}
	proxy.mutex.Lock()
	proxy.target = ""
	proxy.mutex.Unlock()
	// Before local retirement completes there is nothing to release, and the platform
	// says exactly that. The owned stop proof itself is enforced by retirement (it
	// cannot complete while the worker is alive) and by the unit proof matrix.
	if status, code, _ := nativeReleaseCode(test, fixture, "release_before_retire"); status == 200 {
		test.Fatal("release advanced before local retirement")
	} else if code != "release_not_retired" {
		test.Fatalf("unretired isolation refused with %q, want release_not_retired", code)
	}
	// A restart lets the platform's own recovery turn the still-open session's pending
	// record into a quarantine; decisions and retirement both need that settled first.
	fixture.platform.stop(test, false)
	fixture.startPlatform()
	quarantinedWorkers := nativeDecideEveryQuarantine(test, fixture, failure.OperationID)

	// Retire through the real owned-stop proof: this stops the worker that could have
	// committed the unknown. The helper reads the state store directly, so the
	// platform is stopped first and started again afterwards, which also proves the
	// release still works across a restart.
	if len(quarantinedWorkers) != 1 {
		test.Fatalf("expected exactly one owning worker for the quarantine, saw %v", quarantinedWorkers)
	}
	worker := ""
	for candidate := range quarantinedWorkers {
		worker = candidate
	}
	if _, exists := fixture.workers[worker]; !exists {
		test.Fatalf("quarantined worker %q is not an owned fixture worker", worker)
	}
	// Retirement needs the real owned-stop proof, and the platform holds the private
	// state lock while it runs, so both are driven against a cleanly stopped platform.
	// One offline session covers the retirement and the fencing refusal that follows it.
	fixture.workers[worker].stop(test, false)
	fixture.platform.stop(test, false)
	offline, cleanup := nativeOfflineManagement(test, fixture, transport)
	retire := operationHTTP(test, offline, fixture.credential, "/v1/databases/operations/retire-local", managementRequest{Database: fixture.database, RequestID: "release_retire"})
	if retire.Code != 200 {
		cleanup()
		test.Fatalf("retirement after the proven worker exit failed: %d %s", retire.Code, retire.Body.String())
	}
	// The stop proof holds now, but the branch is still owned by the data plane: the
	// release must refuse for exactly that missing proof.
	owned := operationHTTP(test, offline, fixture.credential, "/v1/databases/isolation/release", managementRequest{Database: fixture.database, RequestID: "release_before_revoke"})
	ownedBody := owned.Body.String()
	cleanup()
	if owned.Code == 200 {
		fixture.startPlatform()
		test.Fatal("release advanced without fencing evidence")
	}
	if !strings.Contains(ownedBody, `"code":"release_fencing_proof_unavailable"`) {
		fixture.startPlatform()
		test.Fatalf("still-owned branch refused with %s, want release_fencing_proof_unavailable", ownedBody)
	}
	fixture.startPlatform()
	if status, _ := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/open", "open_before_release", map[string]any{"database": fixture.database}); status == 200 {
		test.Fatal("retired isolation served the database again without a release, or a restart cleared it")
	}

	// The sanctioned stop-serving path: the drain detaches the worker and records the
	// revoke, so the authority stops reporting an owner.
	if status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/suspend", "release_suspend", map[string]any{"database": fixture.database}); status != 200 && status != 409 {
		test.Fatalf("suspend refused outright: %d %s", status, reply)
	}
	fenced := nativeWaitForFence(test, fixture, transport, 150*time.Second, func(fence IsolationFence) bool { return fence.fenced() })

	// Both proofs are present now: the release advances and is auditable.
	fixture.platform.stop(test, false)
	offline, cleanup = nativeOfflineManagement(test, fixture, transport)
	release := operationHTTP(test, offline, fixture.credential, "/v1/databases/isolation/release", managementRequest{Database: fixture.database, RequestID: "release_proven"})
	body := release.Body.String()
	if release.Code != 200 || !strings.Contains(body, `"release_request_id":"release_proven"`) || !strings.Contains(body, `"state":"quarantined"`) || !strings.Contains(body, `"may_have_committed":true`) || !strings.Contains(body, `"release_authority_mode":"`+fenced.Mode+`"`) {
		cleanup()
		test.Fatalf("proven release not recorded: %d %s", release.Code, body)
	}
	repeat := operationHTTP(test, offline, fixture.credential, "/v1/databases/isolation/release", managementRequest{Database: fixture.database, RequestID: "release_repeat"})
	if repeat.Code != 200 {
		cleanup()
		test.Fatalf("idempotent release refused: %d %s", repeat.Code, repeat.Body.String())
	}
	cleanup()
	fixture.startPlatform()

	// Serving again requires the fenced epoch, and the released database works.
	if status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/resume", "release_resume", map[string]any{"database": fixture.database}); status != 200 && status != 409 {
		test.Fatalf("resume refused outright: %d %s", status, reply)
	}
	resumed := nativeWaitForFence(test, fixture, transport, 150*time.Second, func(fence IsolationFence) bool { return fence.Bound && fence.Mode == "writer" })
	if resumed.Epoch <= serving.Epoch {
		test.Fatalf("released branch did not move to a new epoch: %d -> %d (fenced at %d)", serving.Epoch, resumed.Epoch, fenced.Epoch)
	}
	fresh := fixture.open(fixture.credential, fixture.database)
	fixture.sql(fresh, "execute", "INSERT INTO release_probe VALUES(2,'after-release')")
	fixture.sql(fresh, "query", "SELECT COUNT(*) FROM release_probe")
	if status, _ := fixture.call(fixture.credential, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"}); status == 200 {
		test.Fatal("pre-unknown session served the released database without a new epoch")
	}
	test.Logf("release end-to-end: serving_epoch=%d fenced_mode=%s fenced_epoch=%d resumed_epoch=%d unknown_operation=%s worker=%s", serving.Epoch, fenced.Mode, fenced.Epoch, resumed.Epoch, failure.OperationID, worker)
}
