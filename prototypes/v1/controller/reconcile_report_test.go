package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// reportTransport injects ownership facts into the heartbeats the fake worker
// answers, so a test can drive the exact report the Rust worker sends without
// rebuilding the whole transport.
type reportTransport struct {
	ControlTransport
	failures   []ownershipFailure
	resident   reporterResidency
	unmodelled bool
}

type reporterResidency struct {
	count        int
	oldestAgeMS  uint64
	quarantined  int
	quarantineMS uint64
	resourceFail bool
}

func (transport reportTransport) Worker(ctx context.Context, worker Worker, requestID string, input, output any) error {
	var action workerAction
	if err := decodeWorkerAction(input, &action); err != nil {
		return err
	}
	if err := transport.ControlTransport.Worker(ctx, worker, requestID, input, output); err != nil {
		return err
	}
	if action.Command != "get_worker_heartbeat" && action.Command != "get_worker_state" {
		return nil
	}
	report, ok := output.(*workerReport)
	if !ok {
		return nil
	}
	report.Failures = append(report.Failures, transport.failures...)
	report.Resources.ResidentCount = transport.resident.count
	report.Resources.OldestResidentAgeMS = transport.resident.oldestAgeMS
	report.Resources.QuarantinedCount = transport.resident.quarantined
	report.Resources.OldestQuarantinedAgeMS = transport.resident.quarantineMS
	if transport.resident.resourceFail {
		for index := range report.Resources.Databases {
			reason := "resource sample unavailable"
			report.Resources.Databases[index].ResourceError = &reason
		}
	}
	return nil
}

func decodeWorkerAction(input any, action *workerAction) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, action)
}

// workerOwnershipWire is the result body a Rust worker sends for
// get_worker_heartbeat: the databases it can describe, the failures it cannot,
// and the residency report of the part it described.
func workerOwnershipWire(identifier string, database DatabaseID, failure ownershipFailure) string {
	return fmt.Sprintf(`{"worker_id":%q,"databases":[{"database":{"namespace":{"tenant":%q,"database":%q},"branch":%q},"mode":"writer","binding":null,"epoch":3,"remaining_ttl_ms":4000}],"failures":[{"database":{"namespace":{"tenant":%q,"database":%q},"branch":%q},"error":%q}],"resources":{"loaded_databases":2,"loading_databases":1,"active_sessions":4,"resident_count":5,"oldest_resident_age_ms":9000,"quarantined_count":1,"oldest_quarantined_age_ms":7000,"databases":[{"identifier":{"namespace":{"tenant":%q,"database":%q},"branch":%q},"epoch":3,"lifecycle":"open","active_sessions":2,"idle_ms":500,"resources":null,"resource_error":"nothing else to report"}]}}`,
		identifier,
		database.Namespace.Tenant, database.Namespace.Database, database.Branch,
		failure.Database.Namespace.Tenant, failure.Database.Namespace.Database, failure.Database.Branch, failure.Error,
		database.Namespace.Tenant, database.Namespace.Database, database.Branch)
}

func transportReplyWith(requestID string, result json.RawMessage) []byte {
	return []byte(fmt.Sprintf(`{"version":1,"request_id":%q,"ok":true,"result":%s,"error":null}`, requestID, result))
}

// The ownership report a worker sends carries its failures and the residency
// account of the part it could describe. A heartbeat the platform cannot decode
// makes the worker unusable, so every field of that report is modelled.
func TestWorkerReportDecodesOwnershipFailuresAndResidency(test *testing.T) {
	reported := DatabaseID{Namespace: Namespace{Tenant: "tenant-a", Database: "db-a"}, Branch: "main"}
	failed := DatabaseID{Namespace: Namespace{Tenant: "tenant-a", Database: "db-b"}, Branch: "main"}
	body := transportReplyWith("report-1", json.RawMessage(workerOwnershipWire("worker-a", reported, ownershipFailure{Database: failed, Error: "ownership registry poisoned"})))
	var report workerReport
	if err := decodeRPCResponse(body, "report-1", &report, false); err != nil {
		test.Fatalf("ownership report with failures was rejected: %v", err)
	}
	if report.WorkerID != "worker-a" || len(report.Databases) != 1 || report.Databases[0].Database != reported {
		test.Fatalf("reported databases lost: %+v", report)
	}
	if len(report.Failures) != 1 || report.Failures[0].Database != failed || report.Failures[0].Error == "" {
		test.Fatalf("worker failures lost: %+v", report.Failures)
	}
	resources := report.Resources
	if resources.LoadedDatabases != 2 || resources.LoadingDatabases != 1 || resources.ActiveSessions != 4 ||
		resources.ResidentCount != 5 || resources.OldestResidentAgeMS != 9000 || resources.QuarantinedCount != 1 || resources.OldestQuarantinedAgeMS != 7000 {
		test.Fatalf("residency account lost: %+v", resources)
	}
	if len(resources.Databases) != 1 || resources.Databases[0].Identifier != reported || resources.Databases[0].Lifecycle != "open" || resources.Databases[0].IdleMS == nil || *resources.Databases[0].IdleMS != 500 || resources.Databases[0].ResourceError == nil {
		test.Fatalf("per-database report lost: %+v", resources.Databases)
	}
	// A field the platform does not model stays a rejected report: the strict
	// decode is the worker's anti-forgery guard, not a convenience.
	unmodelled := strings.Replace(workerOwnershipWire("worker-a", reported, ownershipFailure{Database: failed, Error: "poisoned"}), `"resources":{`, `"resident_bytes":1,"resources":{`, 1)
	var ignored workerReport
	if err := decodeRPCResponse(transportReplyWith("report-2", json.RawMessage(unmodelled)), "report-2", &ignored, false); err == nil {
		test.Fatal("unmodelled ownership field was accepted")
	}
}

// A database the worker cannot describe is missing from its database list, and
// that absence is not evidence of release: it is counted as residency so the
// reported load never understates the worker, and the worker stays healthy
// because it can still serve everything it did describe.
func TestWorkerHeartbeatCountsUnreportableDatabaseAsResidency(test *testing.T) {
	fixture := newReconcileFixture(test)
	unreportable := DatabaseID{Namespace: Namespace{Tenant: "tenant-a", Database: "db-unreportable"}, Branch: "main"}
	transport := reportTransport{ControlTransport: fixture.fake, failures: []ownershipFailure{{Database: unreportable, Error: "ownership registry poisoned"}}, resident: reporterResidency{count: 3, oldestAgeMS: 1200, quarantined: 2, quarantineMS: 800}}
	control, err := NewController(fixture.store, transport, fixture.config)
	if err != nil {
		test.Fatal(err)
	}
	if err := control.pollWorkers(context.Background()); err != nil {
		test.Fatal(err)
	}
	metadata, err := fixture.store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	worker := metadata.Workers["worker-a"]
	if !worker.Healthy {
		test.Fatal("a worker that cannot describe one database stopped being reachable")
	}
	if worker.Load != 1 {
		test.Fatalf("unreportable database was dropped from residency: load=%d", worker.Load)
	}
	// Nothing in the controller derives a database state from that absence: the
	// only per-database answer is the worker's own get_database_state reply.
	if _, exists := metadata.Placements[unreportable.Key()]; exists {
		test.Fatal("heartbeat created placement state for an unreportable database")
	}
	if actual, err := control.workerState(context.Background(), worker, unreportable); err != nil || actual.Mode != "detached" {
		test.Fatalf("per-database answer was not asked of the worker: %+v %v", actual, err)
	}
	if fixture.fake.calls["get_database_state"] != 1 {
		test.Fatalf("controller inferred database state instead of asking: %+v", fixture.fake.calls)
	}
}
