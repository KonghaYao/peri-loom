package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

// TestHranaSettledFailureKeepsThePipelineAlive pins the Hrana adapter on the same
// evidence contract the Go, TypeScript, browser and console clients read. A
// failure the platform proved was a step error before the mapping change split
// the failure codes, so a proven outcome must not turn into "SQL outcome unknown"
// merely because the platform answers it with a different code or status. The
// adapter decides from the reply's own execution evidence, so a code added later
// cannot become fatal by itself.
func TestHranaSettledFailureKeepsThePipelineAlive(test *testing.T) {
	scenarios := []struct {
		name     string
		text     string
		code     string
		executed bool
	}{
		{"compute budget", "SQL execution settled: VM step budget exceeded", "sql_budget_exceeded", true},
		{"result row budget", "SQL execution settled: result row budget exceeded", "result_limit", true},
		{"duration budget", "SQL execution settled: writer admission deadline: query duration budget exceeded", "sql_timeout", true},
		{"result byte budget", "SQL execution settled: result byte budget exceeded", "result_limit", true},
		{"writer conflict", "SQL execution not entered: writer admission conflict", "sql_conflict", false},
		{"admission refusal", "SQL execution not entered: writer admission queue full", "sql_unavailable", false},
	}
	for _, scenario := range scenarios {
		test.Run(scenario.name, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
				switch command["command"] {
				case "statement":
					return nil, testRPCError(scenario.executed, !scenario.executed, SQLRPCError{workerFailure: scenario.text})
				case "describe":
					// The statement is a write, so the read-only fallback stays off
					// and the adapter reports the failure it already has.
					return map[string]any{"params": []any{}, "cols": []any{}, "is_explain": false, "is_readonly": false}, nil
				}
				return nil, nil
			})
			body := `{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO proof VALUES(1)"}},{"type":"close"}]}`
			request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
			if response.Code != 200 || !strings.Contains(response.Body.String(), `"code":"`+scenario.code+`"`) {
				test.Fatalf("settled failure aborted the pipeline: %d %s", response.Code, response.Body.String())
			}
			// The pipeline continued past the failed step, so the trailing close ran.
			if fixture.upstream.count("close_session") != 1 {
				test.Fatalf("pipeline stopped after a proven failure: %s", response.Body.String())
			}
		})
	}
}

// TestHranaUnprovenFailureStillAborts keeps the other side of the rule: a reply
// that proves nothing about the statement must still stop the pipeline, so the
// evidence-driven decision cannot be widened into accepting an unknown outcome.
func TestHranaUnprovenFailureStillAborts(test *testing.T) {
	fixture := newGatewayFixture(test)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "statement" {
			return nil, &SQLRPCError{Ambiguous: true}
		}
		return nil, nil
	})
	body := `{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO proof VALUES(1)"}},{"type":"close"}]}`
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	// The refusal is still fatal to the pipeline, but it now carries the same
	// structured outcome the Go/TypeScript clients decode instead of a bare
	// transport line an SDK cannot classify.
	failureBody := response.Body.String()
	if response.Code != http.StatusBadGateway || !strings.Contains(failureBody, `"code":"outcome_unknown"`) ||
		!strings.Contains(failureBody, `"outcome_unknown":true`) || !strings.Contains(failureBody, `"retry_allowed":false`) ||
		!strings.Contains(failureBody, `"possible_commit":true`) {
		test.Fatalf("unproven outcome was answered as a step error: %d %s", response.Code, failureBody)
	}
	if fixture.upstream.count("close_session") != 0 {
		test.Fatal("pipeline continued after an unproven outcome")
	}
}
