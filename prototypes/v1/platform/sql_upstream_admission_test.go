package platform

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	controller "peri-loom/controller"
)

// sqlAdmissionTestRefusal renders the body a worker returns when it turned a
// request away before dispatch (prototypes/v1/src/worker_rpc/protocol.rs
// AdmissionRejection). This body is the only non-200 reply that carries
// admission evidence, so it is the only non-200 body the platform reads.
func sqlAdmissionTestRefusal(reason string) string {
	encoded, err := json.Marshal(map[string]any{"error": reason, "execution_completed": false, "admission": "not_admitted"})
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func TestSQLUpstreamAdmissionRefusalIsProvenAndBounded(test *testing.T) {
	refusal := sqlAdmissionTestRefusal("worker execution lane saturated")
	scenarios := []struct {
		name         string
		status       int
		body         string
		refused      bool
		answerCode   string
		answerStatus int
		observed     sqlOpenObservationReason
	}{
		// The three refusals the worker encodes before dispatch, each a complete
		// body: the statement never entered execution or any queue.
		{"lane_saturated", 429, sqlAdmissionTestRefusal("worker execution lane saturated"), true, "sql_unavailable", 503, sqlOpenAdmissionRejected},
		{"worker_saturated", 429, sqlAdmissionTestRefusal("worker saturated"), true, "sql_unavailable", 503, sqlOpenAdmissionRejected},
		{"connection_limit", 429, sqlAdmissionTestRefusal("worker connection limit reached"), true, "sql_unavailable", 503, sqlOpenAdmissionRejected},
		// An unmodelled reason keeps the proof the body carries: the statement did
		// not run, only the cause class is unknown.
		{"unmodelled_reason", 429, sqlAdmissionTestRefusal("worker refused an operator"), true, "upstream_rejected", 502, sqlOpenAdmissionRejected},
		// Counter-examples: every incomplete, contradictory or differently encoded
		// body proves nothing about the statement and stays ambiguous.
		{"missing_execution_completed", 429, `{"error":"worker saturated","admission":"not_admitted"}`, false, "", 0, sqlOpenHTTPStatus},
		{"missing_admission", 429, `{"error":"worker saturated","execution_completed":false}`, false, "", 0, sqlOpenHTTPStatus},
		{"execution_completed_true", 429, `{"error":"worker saturated","execution_completed":true,"admission":"not_admitted"}`, false, "", 0, sqlOpenHTTPStatus},
		{"admission_admitted", 429, `{"error":"worker saturated","execution_completed":false,"admission":"admitted"}`, false, "", 0, sqlOpenHTTPStatus},
		{"extra_field", 429, `{"error":"worker saturated","execution_completed":false,"admission":"not_admitted","worker_id":"worker-a"}`, false, "", 0, sqlOpenHTTPStatus},
		{"empty_error", 429, `{"error":"","execution_completed":false,"admission":"not_admitted"}`, false, "", 0, sqlOpenHTTPStatus},
		{"trailing_document", 429, refusal + refusal, false, "", 0, sqlOpenHTTPStatus},
		{"unparseable", 429, `{"error":`, false, "", 0, sqlOpenHTTPStatus},
		{"oversized_body", 429, sqlAdmissionTestRefusal(strings.Repeat("s", sqlAdmissionRejectionLimit)), false, "", 0, sqlOpenHTTPStatus},
		{"status_forbidden", 403, refusal, false, "", 0, sqlOpenHTTPStatus},
		{"status_unavailable", 503, refusal, false, "", 0, sqlOpenHTTPStatus},
		{"status_server_error", 500, refusal, false, "", 0, sqlOpenHTTPStatus},
	}
	for _, scenario := range scenarios {
		test.Run(scenario.name, func(test *testing.T) {
			upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
			if err != nil {
				test.Fatal(err)
			}
			httpUpstream := upstream.(*sqlHTTPUpstream)
			var calls atomic.Int32
			httpUpstream.client.Transport = sqlOpenTestTransport(func(request *http.Request) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: scenario.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(scenario.body))}, nil
			})
			observer := NewSQLOpenObserver()
			ctx := bindSQLOpenObservation(context.Background(), observer, "public_open")
			err = upstream.Call(ctx, controller.Worker{ID: "worker-a", Address: "http://127.0.0.1:1"}, "public_open", map[string]any{"command": "open_session"}, nil)
			failure := sqlTestFailure(test, err)
			if failure.StatusCode != scenario.status {
				test.Fatalf("status changed: %d", failure.StatusCode)
			}
			if failure.Ambiguous == scenario.refused || failure.evidence.Rejected() != scenario.refused || failure.evidence.Executed() {
				test.Fatalf("admission proof changed: ambiguous=%t rejected=%t executed=%t", failure.Ambiguous, failure.evidence.Rejected(), failure.evidence.Executed())
			}
			// Only the accepted body may carry worker text: an unproven reply must not
			// put a caller-controlled string into the classification table either.
			if scenario.refused != (failure.workerFailure != "") {
				test.Fatalf("worker text accepted from a body that proved nothing: %q", failure.workerFailure)
			}
			worker, found := sqlWorkerFailure(failure)
			if found != scenario.refused {
				test.Fatalf("platform answer changed: found=%t", found)
			}
			record := OperationRecord{}
			if scenario.refused {
				if worker.code != scenario.answerCode || worker.status != scenario.answerStatus || worker.executed || !worker.rejected {
					test.Fatalf("refusal answered as %d/%s executed=%t rejected=%t", worker.status, worker.code, worker.executed, worker.rejected)
				}
				// The refusal must not become a durable unknown: no quarantine, no
				// possible commit, and nothing that needs a recovery proof later.
				applyOperationOutcome(&record, gatewayResult{status: worker.status, code: worker.code, failure: &worker, rpc: failure, dispatched: true}, "open")
				if record.State != "terminal" || record.BusinessOutcome != "not_executed" || record.Ambiguous || !record.Rejected || record.ExecutionCompleted || record.MayHaveCommitted || record.ExecutionProven {
					test.Fatalf("admission refusal recorded as unresolved: %+v", record)
				}
			} else {
				applyOperationOutcome(&record, gatewayResult{code: "outcome_unknown", rpc: failure, dispatched: true}, "open")
				if record.State != "quarantined" || !record.Ambiguous || record.Rejected || record.ExecutionCompleted || !record.MayHaveCommitted {
					test.Fatalf("unproven reply did not keep the unknown: %+v", record)
				}
			}
			records := sqlOpenTestRecords(observer)
			if len(records) != 1 || records[0].Reason != string(scenario.observed) || records[0].RequestID != "public_open" {
				test.Fatalf("observation changed: %+v", records)
			}
			if calls.Load() != 1 {
				test.Fatalf("admission classification added an RPC or a replay")
			}
		})
	}
}
