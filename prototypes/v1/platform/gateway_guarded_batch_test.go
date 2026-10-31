package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// guardedBatchReadResult is one read-only statement result in the shape the
// worker publishes: the five statement fields plus the three engine counters.
func guardedBatchReadResult(value any) map[string]any {
	return map[string]any{
		"cols":               []any{map[string]any{"name": "value", "decltype": "INTEGER"}},
		"rows":               [][]any{{value}},
		"affected_row_count": 0,
		"last_insert_rowid":  nil,
		"autocommit":         true,
		"rows_read":          1,
		"rows_written":       0,
		"query_duration_ms":  0.25,
	}
}

func guardedBatchWriteResult() map[string]any {
	return map[string]any{
		"cols":               []any{},
		"rows":               []any{},
		"affected_row_count": 1,
		"last_insert_rowid":  1,
		"autocommit":         true,
		"rows_read":          0,
		"rows_written":       1,
		"query_duration_ms":  0.25,
	}
}

// guardedBatchCapability answers the discovery probe positively.
func guardedBatchCapability() map[string]any {
	return map[string]any{"readonly_batch_protocol": 1}
}

// guardedBatchHook models a worker that parses the block command. serve decides
// every entry so a test can model a stop, an error or a budget boundary. The
// single-statement commands answer the same way the default fixture does.
func guardedBatchHook(serve func(statements []map[string]any) ([]any, int)) func(context.Context, controller.Worker, map[string]any) (any, error) {
	return func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return guardedBatchCapability(), nil
		case "guarded_readonly_batch":
			raw, _ := command["statements"].([]any)
			statements := make([]map[string]any, len(raw))
			for index, value := range raw {
				statements[index], _ = value.(map[string]any)
			}
			items, next := serve(statements)
			return map[string]any{"items": items, "next": next}, nil
		case "guarded_readonly_statement":
			return guardedBatchReadResult(1), nil
		case "statement":
			return guardedBatchWriteResult(), nil
		case "describe":
			return testDescribeVerdict(""), nil
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	}
}

// guardedBatchQuota makes the fixture project accept the aggregate result bytes
// of a pipeline test. The default quota is sized for one statement.
func guardedBatchQuota(test *testing.T, fixture *gatewayFixture, resultBytes int, concurrency int) {
	test.Helper()
	if err := fixture.state.Update(func(state *State) error {
		for key, project := range state.Projects {
			project.Quotas.MaxResultBytes = resultBytes
			project.Quotas.MaxConcurrent = concurrency
			state.Projects[key] = project
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
}

// guardedBatchServesReads answers every entry of the block with one read result.
func guardedBatchServesReads(statements []map[string]any) ([]any, int) {
	items := make([]any, len(statements))
	for index := range statements {
		items[index] = map[string]any{"index": index, "status": "ok", "result": guardedBatchReadResult(1)}
	}
	return items, len(statements)
}

// guardedBatchServesReadsUntilWrite answers reads until the first write, then
// returns the worker's refusal for that entry and stops.
func guardedBatchServesReadsUntilWrite() func([]map[string]any) ([]any, int) {
	return func(statements []map[string]any) ([]any, int) {
		items := make([]any, 0, len(statements))
		for index, statement := range statements {
			sql, _ := statement["sql"].(string)
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
				items = append(items, map[string]any{"index": index, "status": "ok", "result": guardedBatchReadResult(1)})
				continue
			}
			items = append(items, map[string]any{"index": index, "status": "requires_intent", "reason": "engine_writable"})
			break
		}
		return items, len(items)
	}
}

func guardedBatchPipelineBody(count int, sql string) []any {
	requests := make([]any, 0, count+1)
	for index := 0; index < count; index++ {
		requests = append(requests, map[string]any{
			"type": "execute",
			"stmt": map[string]any{"sql": sql, "want_rows": true},
		})
	}
	requests = append(requests, map[string]any{"type": "close"})
	return requests
}

func runGuardedBatchPipeline(test *testing.T, handler http.Handler, fixture *gatewayFixture, requests []any) *httptest.ResponseRecorder {
	test.Helper()
	request := hranaPipelineRequest(test, context.Background(), fixture.tokenA, fixture.first, requests)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// hranaPipelineDocument decodes a successful pipeline answer and returns its
// steps; a failed pipeline returns its failure object.
func hranaPipelineDocument(test *testing.T, response *httptest.ResponseRecorder) (map[string]any, []any) {
	test.Helper()
	if response.Code != http.StatusOK {
		return nil, nil
	}
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(response.Body.Bytes()))
	decoder.UseNumber()
	if decoder.Decode(&document) != nil {
		test.Fatalf("invalid pipeline document: %s", response.Body.String())
	}
	results, ok := document["results"].([]any)
	if !ok {
		test.Fatalf("pipeline document has no results: %s", response.Body.String())
	}
	return document, results
}

// normalizeStepDurations replaces the worker-measured duration in every execute
// result so two runs can be compared without that noise.
func normalizeStepDurations(results []any) {
	for _, raw := range results {
		step, _ := raw.(map[string]any)
		response, _ := step["response"].(map[string]any)
		if response["type"] != "execute" {
			continue
		}
		result, _ := response["result"].(map[string]any)
		if result == nil {
			continue
		}
		if _, found := result["query_duration_ms"]; found {
			result["query_duration_ms"] = "duration"
		}
	}
}

func guardedBatchCalls(upstream *gatewayTestUpstream) [][]map[string]any {
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	var blocks [][]map[string]any
	for _, call := range upstream.calls {
		if call.command["command"] != "guarded_readonly_batch" {
			continue
		}
		raw, _ := call.command["statements"].([]any)
		statements := make([]map[string]any, len(raw))
		for index, value := range raw {
			statements[index], _ = value.(map[string]any)
		}
		blocks = append(blocks, statements)
	}
	return blocks
}

func guardedBatchAuditOutcomes(audit *gatewayTestAudit, action string) []string {
	audit.mu.Lock()
	defer audit.mu.Unlock()
	var outcomes []string
	for _, event := range audit.events {
		if event.Action == action {
			outcomes = append(outcomes, event.Outcome)
		}
	}
	return outcomes
}

// guardedBatchSingleModeHook answers like an older worker: no capability and no
// block command, only the single guarded statement.
func guardedBatchSingleModeHook() func(context.Context, controller.Worker, map[string]any) (any, error) {
	return func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "guarded_readonly_statement":
			return guardedBatchReadResult(1), nil
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("private old worker failure")
	}
}

// guardedBatchJournalFixture builds a gateway whose audit is the completion
// journal on a real state store, so the journal's sync and append counts can be
// compared between the block and single-statement paths.
func guardedBatchJournalFixture(test *testing.T) (*stateStore, *Gateway, *gatewayTestUpstream, string) {
	test.Helper()
	state, err := OpenStateStore(test.TempDir())
	if err != nil {
		test.Fatal(err)
	}
	store := state.(*stateStore)
	test.Cleanup(func() { _ = store.Close() })
	// The migration requires a fresh store, so the journal is enabled before the
	// tenant state exists.
	auditPath := filepath.Join(test.TempDir(), "completion.jsonl")
	service, err := OpenCompletionAudit(store, auditPath, true)
	if err != nil {
		test.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		state.Organizations["org"] = Organization{ID: "org", Name: "test"}
		state.Projects["alpha"] = Project{ID: "alpha", Organization: "org", Tenant: "tenant_a", Quotas: Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 1 << 20}}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	activeSecurityDatabase(test, store, securityKMS(test))
	_, credential, err := NewAPIKey(store, "alice", "alpha", "sql", false)
	if err != nil {
		test.Fatal(err)
	}
	identity, err := NewIdentity(store, bytes.Repeat([]byte{17}, 32))
	if err != nil {
		test.Fatal(err)
	}
	database := securityDatabase()
	binding := securityBinding()
	worker := controller.Worker{ID: binding.WorkerID, Address: "http://127.0.0.1:1234", Healthy: true, Capacity: 8, Load: 1, LastHeartbeatMS: time.Now().UnixMilli()}
	placements := &gatewayTestPlacements{metadata: controller.Metadata{Version: 1, Workers: map[string]controller.Worker{worker.ID: worker}, Placements: map[string]controller.Placement{}}}
	placements.metadata.Placements[database.Key()] = controller.Placement{Database: database, DesiredWorker: worker.ID, Epoch: 10, LeaseID: binding.LeaseID, Role: "writer", DesiredState: "active", Phase: "active", Grant: &controller.Grant{Binding: binding, RemainingTTLMS: 10000}}
	upstream := &gatewayTestUpstream{}
	gateway, err := NewGateway(GatewayConfig{State: store, Placements: placements, Identity: identity, Upstream: upstream, Audit: service})
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gateway.Close(ctx)
		_ = service.Close()
	})
	return store, gateway, upstream, credential
}

// An older worker rejects both private commands before it reads a session. Only
// the exact body and status prove non-execution; every other refusal stays
// ambiguous.
func TestSQLUpstreamClassifiesUnsupportedBlockCommands(t *testing.T) {
	token := sqlTestToken(t)
	var body atomic.Value
	body.Store(unsupportedWorkerCommandBody)
	status := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(body.Load().(string)))
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": token})
	if err != nil {
		t.Fatal(err)
	}
	worker := controller.Worker{ID: "worker-a", Address: server.URL}
	commands := []map[string]any{
		{"command": "capabilities"},
		{
			"command":          "guarded_readonly_batch",
			"session":          strings.Repeat("a", 64),
			"statements":       []map[string]any{{"sql": "SELECT 1", "args": []sqlArgument{}, "named_args": []namedSQLArgument{}, "want_rows": true}},
			"max_result_bytes": 4096,
		},
	}
	status = http.StatusBadRequest
	for _, command := range commands {
		var reply json.RawMessage
		callErr := upstream.Call(context.Background(), worker, "unsupported_"+sqlTestToken(t), command, &reply)
		failure := sqlTestFailure(t, callErr)
		if !failure.unsupportedCommand || failure.Ambiguous || failure.evidence.Proven() {
			t.Fatalf("%s refusal classified as %+v", command["command"], failure)
		}
	}
	for name, refusal := range map[string]struct {
		status int
		body   string
	}{
		"other body":        {http.StatusBadRequest, `{"error":"invalid HTTP request"}`},
		"admission refusal": {http.StatusTooManyRequests, unsupportedWorkerCommandBody},
	} {
		status = refusal.status
		body.Store(refusal.body)
		var reply json.RawMessage
		callErr := upstream.Call(context.Background(), worker, "unsupported_"+sqlTestToken(t), commands[0], &reply)
		failure := sqlTestFailure(t, callErr)
		if failure.unsupportedCommand || !failure.Ambiguous {
			t.Fatalf("%s was accepted as non-execution: %+v", name, failure)
		}
	}
}

// A block command changes no client-visible byte: the same pipeline over a
// worker without the capability answers the identical document once the
// worker-measured duration is normalized.
func TestGuardedBatchWireMatchesTheSingleStatementPipeline(t *testing.T) {
	run := func(batch bool) []byte {
		fixture := newGatewayFixture(t)
		guardedBatchQuota(t, fixture, 1<<20, 2)
		if batch {
			fixture.upstream.setHook(guardedBatchHook(guardedBatchServesReads))
		} else {
			fixture.upstream.setHook(guardedBatchSingleModeHook())
		}
		response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(20, "SELECT 1"))
		document, results := hranaPipelineDocument(t, response)
		normalizeStepDurations(results)
		document["results"] = results
		encoded, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	batched, single := run(true), run(false)
	if !bytes.Equal(batched, single) {
		t.Fatalf("block and single pipelines answered different documents:\n%s\n%s", batched, single)
	}
}

// The block keeps the pipeline's durability points: the same audit rows, the
// same audit barriers and the same operation-journal cost as one statement per
// entry.
func TestGuardedBatchKeepsAuditAndJournalCosts(t *testing.T) {
	runFileAudit := func(batch bool) (int, uint64, []string) {
		fixture, service, path := gatewayFileAuditFixture(t)
		guardedBatchQuota(t, fixture, 1<<20, 2)
		audit := service.(*fileAudit)
		if batch {
			fixture.upstream.setHook(guardedBatchHook(guardedBatchServesReads))
		} else {
			fixture.upstream.setHook(guardedBatchSingleModeHook())
		}
		response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(20, "SELECT 1"))
		if response.Code != http.StatusOK {
			t.Fatalf("pipeline failed: %d %s", response.Code, response.Body.String())
		}
		events := auditChainEvents(t, path)
		actions := make([]string, len(events))
		for index, event := range events {
			actions[index] = event.Action
		}
		return len(events), audit.syncCount.Load(), actions
	}
	batchedEvents, batchedSyncs, batchedActions := runFileAudit(true)
	singleEvents, singleSyncs, singleActions := runFileAudit(false)
	if batchedEvents != singleEvents || batchedEvents != 22 {
		t.Fatalf("audit rows batch=%d single=%d, want 22", batchedEvents, singleEvents)
	}
	// The documented B1 shape: one open barrier, three statement batches of
	// 8/8/4 and one close barrier.
	if batchedSyncs != singleSyncs || batchedSyncs != 5 {
		t.Fatalf("audit barriers batch=%d single=%d, want 5", batchedSyncs, singleSyncs)
	}
	if strings.Join(batchedActions, ",") != strings.Join(singleActions, ",") ||
		strings.Join(batchedActions, ",") != "open,"+strings.Repeat("statement,", 20)+"close" {
		t.Fatalf("audit order changed: batch=%v single=%v", batchedActions, singleActions)
	}

	runJournal := func(batch bool) (uint64, uint64) {
		store, gateway, upstream, credential := guardedBatchJournalFixture(t)
		if batch {
			upstream.setHook(guardedBatchHook(guardedBatchServesReads))
		} else {
			upstream.setHook(guardedBatchSingleModeHook())
		}
		before := store.operations.stats()
		request := hranaPipelineRequest(t, context.Background(), credential, securityDatabase(), guardedBatchPipelineBody(20, "SELECT 1"))
		response := httptest.NewRecorder()
		newHranaHTTP(gateway).ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("journal pipeline failed: %d %s", response.Code, response.Body.String())
		}
		after := store.operations.stats()
		return after["operation_journal_syncs_total"] - before["operation_journal_syncs_total"],
			after["operation_journal_appends_total"] - before["operation_journal_appends_total"]
	}
	batchedSyncsJournal, batchedAppends := runJournal(true)
	singleSyncsJournal, singleAppends := runJournal(false)
	// The durable record count is the invariant: a block adds no append. The
	// barrier count is a group-commit coalescing result and varies with how the
	// journal batches concurrent completions, so it is only bounded here.
	if batchedAppends != singleAppends {
		t.Fatalf("journal appends changed: batch=%d single=%d", batchedAppends, singleAppends)
	}
	for name, syncs := range map[string]uint64{"batch": batchedSyncsJournal, "single": singleSyncsJournal} {
		if syncs == 0 || syncs > batchedAppends {
			t.Fatalf("%s journal syncs=%d outside (0, appends=%d]", name, syncs, batchedAppends)
		}
	}
}

// The B1 shape: twenty reads and a close are answered by three blocks of 8, 8
// and 4 statements, while every entry keeps its own audit row and result.
func TestGuardedBatchPipelineUsesThreeBlocksAndKeepsEveryResult(t *testing.T) {
	fixture := newGatewayFixture(t)
	guardedBatchQuota(t, fixture, 1<<20, 2)
	fixture.upstream.setHook(guardedBatchHook(guardedBatchServesReads))
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(20, "SELECT 1"))
	_, results := hranaPipelineDocument(t, response)
	if len(results) != 21 {
		t.Fatalf("results=%d, want 20 executes and a close: %s", len(results), response.Body.String())
	}
	normalizeStepDurations(results)
	for index, raw := range results[:20] {
		step := raw.(map[string]any)
		response, _ := step["response"].(map[string]any)
		result, _ := response["result"].(map[string]any)
		if step["type"] != "ok" || response["type"] != "execute" || hranaIntegerCell(t, result) != "1" {
			t.Fatalf("execute %d did not return its own result: %v", index, step)
		}
	}
	blocks := guardedBatchCalls(fixture.upstream)
	if len(blocks) != 3 || len(blocks[0]) != 8 || len(blocks[1]) != 8 || len(blocks[2]) != 4 {
		t.Fatalf("block sizes %v, want 8/8/4", blockSizes(blocks))
	}
	if statements := fixture.upstream.count("guarded_readonly_statement"); statements != 0 {
		t.Fatalf("%d entries fell back to single statements", statements)
	}
	if outcomes := guardedBatchAuditOutcomes(fixture.audit, "statement"); len(outcomes) != 20 {
		t.Fatalf("statement audit rows=%d, want 20: %v", len(outcomes), outcomes)
	}
	for index, outcome := range guardedBatchAuditOutcomes(fixture.audit, "statement") {
		if outcome != "ok" {
			t.Fatalf("statement audit row %d = %q", index, outcome)
		}
	}
	if reserved, admitted := auditCompletionStatusFromAudit(fixture.gateway.config.Audit); reserved != 0 || admitted != 0 {
		t.Fatalf("blocks retained audit capacity: reserved=%d admitted=%d", reserved, admitted)
	}
}

func blockSizes(blocks [][]map[string]any) []int {
	sizes := make([]int, len(blocks))
	for index, block := range blocks {
		sizes[index] = len(block)
	}
	return sizes
}

// hranaIntegerCell returns the integer text of the single cell of a result.
func hranaIntegerCell(test *testing.T, result map[string]any) string {
	test.Helper()
	rows, _ := result["rows"].([]any)
	if len(rows) != 1 {
		test.Fatalf("result has %d rows", len(rows))
	}
	row, _ := rows[0].([]any)
	if len(row) != 1 {
		test.Fatalf("row has %d cells", len(row))
	}
	cell, _ := row[0].(map[string]any)
	if cell["type"] != "integer" {
		test.Fatalf("cell is not an integer: %v", cell)
	}
	return fmt.Sprint(cell["value"])
}

func auditCompletionStatusFromAudit(sink Audit) (int, int) {
	admission, ok := sink.(AuditAdmission)
	if !ok {
		return 0, 0
	}
	return auditCompletionStatus(admission.AuditStatus())
}

// The same pipeline over a worker that does not answer the capability probe runs
// one statement per entry and produces the same steps.
func TestGuardedBatchFallsBackWhenTheWorkerLacksTheCapability(t *testing.T) {
	fixture := newGatewayFixture(t)
	guardedBatchQuota(t, fixture, 1<<20, 2)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "guarded_readonly_statement":
			return guardedBatchReadResult(1), nil
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("private old worker failure")
	})
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(20, "SELECT 1"))
	_, results := hranaPipelineDocument(t, response)
	if len(results) != 21 {
		t.Fatalf("results=%d: %s", len(results), response.Body.String())
	}
	if blocks := guardedBatchCalls(fixture.upstream); len(blocks) != 0 {
		t.Fatalf("old worker received %d block commands", len(blocks))
	}
	if statements := fixture.upstream.count("guarded_readonly_statement"); statements != 20 {
		t.Fatalf("single statements=%d, want 20", statements)
	}
	// A second pipeline reuses the negative cache: no probe and no block.
	before := fixture.upstream.count("capabilities")
	if response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(2, "SELECT 1")); response.Code != http.StatusOK {
		t.Fatalf("second pipeline failed: %d %s", response.Code, response.Body.String())
	}
	if after := fixture.upstream.count("capabilities"); after != before {
		t.Fatalf("negative capability cache asked %d more times", after-before)
	}
}

// A read, a write and a read: the block decides the first two entries, the
// refusal sends the write through the committing path, and the third entry runs
// on its own.
func TestGuardedBatchStopsAtAWriteAndKeepsPerEntryDispatch(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(guardedBatchHook(guardedBatchServesReadsUntilWrite()))
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 1", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO records(id) VALUES (1)", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 2", "want_rows": true}},
		map[string]any{"type": "close"},
	})
	_, results := hranaPipelineDocument(t, response)
	if len(results) != 4 {
		t.Fatalf("results=%d: %s", len(results), response.Body.String())
	}
	for index, raw := range results[:3] {
		step := raw.(map[string]any)
		if step["type"] != "ok" {
			t.Fatalf("step %d failed: %v", index, step)
		}
	}
	blocks := guardedBatchCalls(fixture.upstream)
	if len(blocks) != 1 || len(blocks[0]) != 3 {
		t.Fatalf("blocks=%v, want one block of 3", blockSizes(blocks))
	}
	// The write must be its own committing statement; the third read must not be
	// answered from the block because the block stopped at the write.
	if statements := fixture.upstream.count("statement"); statements != 1 {
		t.Fatalf("committing statements=%d, want 1", statements)
	}
	if statements := fixture.upstream.count("guarded_readonly_statement"); statements != 1 {
		t.Fatalf("single guarded statements=%d, want 1", statements)
	}
	if outcomes := guardedBatchAuditOutcomes(fixture.audit, "statement"); len(outcomes) != 3 {
		t.Fatalf("statement audit rows=%d, want 3: %v", len(outcomes), outcomes)
	}
}

// A block entry whose statement failed keeps the failure of that entry, the
// pipeline continues, and the next entry runs on its own.
func TestGuardedBatchReportsASettledEntryFailureAndContinues(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(guardedBatchHook(func(statements []map[string]any) ([]any, int) {
		items := []any{
			map[string]any{"index": 0, "status": "ok", "result": guardedBatchReadResult(1)},
			map[string]any{"index": 1, "status": "error", "error": "SQL execution settled: private constraint details", "execution_completed": true},
		}
		return items, len(items)
	}))
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 1", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "INSERT INTO records(id) VALUES (1)", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 3", "want_rows": true}},
		map[string]any{"type": "close"},
	})
	_, results := hranaPipelineDocument(t, response)
	if len(results) != 4 {
		t.Fatalf("results=%d: %s", len(results), response.Body.String())
	}
	if step := results[0].(map[string]any); step["type"] != "ok" {
		t.Fatalf("first entry failed: %v", step)
	}
	failed, _ := results[1].(map[string]any)
	errorObject, _ := failed["error"].(map[string]any)
	if failed["type"] != "error" || errorObject["code"] != "sql_error" {
		t.Fatalf("settled entry failure did not become a step error: %v", failed)
	}
	if step := results[2].(map[string]any); step["type"] != "ok" {
		t.Fatalf("pipeline did not continue after a settled failure: %v", step)
	}
	if statement := fixture.upstream.count("guarded_readonly_statement"); statement != 1 {
		t.Fatalf("the entry after the block stop ran %d single statements", statement)
	}
}

// An ambiguous block poisons the session, isolates the namespace, writes one
// durable record for the block digest and never replays an entry.
func TestGuardedBatchAmbiguousOutcomeIsolatesAndIsNotReplayed(t *testing.T) {
	fixture, _, _ := gatewayFileAuditFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return guardedBatchCapability(), nil
		case "guarded_readonly_batch":
			return nil, &SQLRPCError{Ambiguous: true}
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(4, "SELECT 1"))
	if response.Code == http.StatusOK || !strings.Contains(response.Body.String(), "outcome_unknown") {
		t.Fatalf("ambiguous block answered %d: %s", response.Code, response.Body.String())
	}
	blocks := guardedBatchCalls(fixture.upstream)
	if len(blocks) != 1 {
		t.Fatalf("ambiguous block reached the worker %d times", len(blocks))
	}
	if statements := fixture.upstream.count("guarded_readonly_statement") + fixture.upstream.count("statement"); statements != 0 {
		t.Fatalf("ambiguous block replayed %d entries", statements)
	}
	state, err := fixture.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var records []OperationRecord
	for _, record := range state.Operations {
		if record.Digest == commandDigest(transmittedCommands(fixture.upstream, "guarded_readonly_batch")[0]) {
			records = append(records, record)
		}
	}
	if len(records) != 1 || records[0].BusinessOutcome != "unknown" || !records[0].MayHaveCommitted {
		t.Fatalf("block digest records=%+v, want one unknown record", records)
	}
	// The unresolved block isolates the namespace for the next request.
	opened := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if opened.Code != 409 || gatewayCode(t, opened) != "database_isolated" {
		t.Fatalf("isolated namespace admitted a new request: %d %s", opened.Code, opened.Body.String())
	}
}

// The worker stops a block at the result budget; the entries it did not reach
// run on their own and the pipeline budget still fails the same way.
func TestGuardedBatchResultBudgetCrossingFallsBackAndThePipelineLimitStillApplies(t *testing.T) {
	fixture := newGatewayFixture(t)
	large := func(size int) map[string]any {
		result := guardedBatchReadResult(1)
		result["rows"] = [][]any{{strings.Repeat("x", size)}}
		result["cols"] = []any{map[string]any{"name": "value", "decltype": "TEXT"}}
		return result
	}
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return guardedBatchCapability(), nil
		case "guarded_readonly_batch":
			// The first entry alone crossed the remaining budget.
			return map[string]any{"items": []any{map[string]any{"index": 0, "status": "ok", "result": large(3000)}}, "next": 1}, nil
		case "guarded_readonly_statement":
			return large(1000), nil
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, []any{
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 1", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 2", "want_rows": true}},
		map[string]any{"type": "execute", "stmt": map[string]any{"sql": "SELECT 3", "want_rows": true}},
	})
	if response.Code != http.StatusRequestEntityTooLarge || !strings.Contains(response.Body.String(), "pipeline_result_limit") {
		t.Fatalf("pipeline result budget answered %d: %s", response.Code, response.Body.String())
	}
	if blocks := guardedBatchCalls(fixture.upstream); len(blocks) != 1 {
		t.Fatalf("blocks=%d", len(blocks))
	}
	// Only the first entry after the block ran; the third never started.
	if statements := fixture.upstream.count("guarded_readonly_statement"); statements != 1 {
		t.Fatalf("fallback statements=%d, want 1", statements)
	}
}

// A timeout on the block command answers exactly like a timeout on a single
// statement and releases every completion slot the block reserved.
func TestGuardedBatchTimeoutReleasesReservedSlots(t *testing.T) {
	fixture, service, _ := gatewayFileAuditFixture(t)
	audit := service.(*fileAudit)
	fixture.upstream.setHook(func(ctx context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return guardedBatchCapability(), nil
		case "guarded_readonly_batch":
			<-ctx.Done()
			return nil, ctx.Err()
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	handler.timeout = 100 * time.Millisecond
	response := runGuardedBatchPipeline(t, handler, fixture, guardedBatchPipelineBody(6, "SELECT 1"))
	if response.Code != http.StatusGatewayTimeout || !strings.Contains(response.Body.String(), "pipeline_timeout") {
		t.Fatalf("timed-out block answered %d: %s", response.Code, response.Body.String())
	}
	if blocks := guardedBatchCalls(fixture.upstream); len(blocks) != 1 {
		t.Fatalf("blocks=%d", len(blocks))
	}
	if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
		t.Fatalf("timeout leaked audit capacity: reserved=%d admitted=%d", reserved, admitted)
	}
}

// When the audit cannot hold a block's completion rows the pipeline does not
// send a block at all: the entries fall back to the single-statement path, where
// the same admission answers each one until capacity really is exhausted.
func TestGuardedBatchAuditCapacityFallsBackBeforeDispatch(t *testing.T) {
	fixture, service, _ := gatewayFileAuditFixture(t)
	guardedBatchQuota(t, fixture, 1<<20, 2)
	audit := service.(*fileAudit)
	held := make([]AuditReservation, 0, auditCompletionMaximum)
	for {
		reservation, err := audit.ReserveCompletion()
		if errors.Is(err, ErrAuditOverloaded) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, reservation)
		if len(held) > auditCompletionMaximum {
			t.Fatal("completion admission never refused")
		}
	}
	// Four slots are left: fewer than the eight a block reserves, enough for the
	// four statements the single path can carry into one audit batch.
	for len(held) > auditCompletionMaximum-4 {
		held[len(held)-1].Release()
		held = held[:len(held)-1]
	}
	fixture.upstream.setHook(guardedBatchHook(guardedBatchServesReads))
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(20, "SELECT 1"))
	if response.Code == http.StatusOK || !strings.Contains(response.Body.String(), "audit_unavailable") {
		t.Fatalf("exhausted audit answered %d: %s", response.Code, response.Body.String())
	}
	if blocks := guardedBatchCalls(fixture.upstream); len(blocks) != 0 {
		t.Fatalf("a block was sent without completion capacity: %v", blockSizes(blocks))
	}
	if statements := fixture.upstream.count("guarded_readonly_statement"); statements != 4 {
		t.Fatalf("single statements=%d, want the four admitted slots", statements)
	}
	for _, reservation := range held {
		reservation.Release()
	}
	if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
		t.Fatalf("audit retained capacity: reserved=%d admitted=%d", reserved, admitted)
	}
}

// A worker that answered the probe but now refuses the block command proves the
// command never ran; the entry falls back to a single statement and the refusal
// is remembered without poisoning anything.
func TestGuardedBatchVersionSkewFallsBackWithoutPoisoning(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "capabilities":
			return guardedBatchCapability(), nil
		case "guarded_readonly_batch":
			return nil, &SQLRPCError{StatusCode: http.StatusBadRequest, unsupportedCommand: true}
		case "guarded_readonly_statement":
			return guardedBatchReadResult(1), nil
		case "open_session":
			return map[string]any{"session": strings.Repeat("a", 64), "readonly": false}, nil
		case "close_session":
			return map[string]bool{"closed": true}, nil
		}
		return nil, errors.New("unknown fixture action")
	})
	response := runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(4, "SELECT 1"))
	_, results := hranaPipelineDocument(t, response)
	if len(results) != 5 {
		t.Fatalf("results=%d: %s", len(results), response.Body.String())
	}
	if blocks := guardedBatchCalls(fixture.upstream); len(blocks) != 1 {
		t.Fatalf("blocks=%d, want the refused one", len(blocks))
	}
	if statements := fixture.upstream.count("guarded_readonly_statement"); statements != 4 {
		t.Fatalf("fallback statements=%d, want 4", statements)
	}
	// The refusal is cached: the next pipeline sends no block.
	response = runGuardedBatchPipeline(t, newHranaHTTP(fixture.gateway), fixture, guardedBatchPipelineBody(2, "SELECT 1"))
	if response.Code != http.StatusOK {
		t.Fatalf("second pipeline failed: %d %s", response.Code, response.Body.String())
	}
	if blocks := guardedBatchCalls(fixture.upstream); len(blocks) != 1 {
		t.Fatalf("the refused command was retried: blocks=%d", len(blocks))
	}
}

func TestGuardedBatchCommandWireValidation(t *testing.T) {
	valid := `{"command":"guarded_readonly_batch","session":"` + strings.Repeat("a", 64) + `","statements":[{"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true}],"max_result_bytes":4096}`
	if !guardedBatchCommandValid("guarded_readonly_batch", []byte(valid)) {
		t.Fatal("valid block command rejected")
	}
	if !guardedBatchCommandValid("capabilities", []byte(`{"command":"capabilities"}`)) {
		t.Fatal("valid capability command rejected")
	}
	for name, body := range map[string]string{
		"unknown field":   `{"command":"capabilities","extra":1}`,
		"capabilities id": `{"command":"capabilities","session":"` + strings.Repeat("a", 64) + `"}`,
		"empty block":     `{"command":"guarded_readonly_batch","session":"` + strings.Repeat("a", 64) + `","statements":[],"max_result_bytes":1}`,
		"missing rows":    `{"command":"guarded_readonly_batch","session":"` + strings.Repeat("a", 64) + `","statements":[{"sql":"SELECT 1","args":[],"named_args":[]}],"max_result_bytes":1}`,
		"bad session":     `{"command":"guarded_readonly_batch","session":"short","statements":[{"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true}],"max_result_bytes":1}`,
	} {
		operation := "capabilities"
		if strings.HasPrefix(body, `{"command":"guarded_readonly_batch"`) {
			operation = "guarded_readonly_batch"
		}
		if guardedBatchCommandValid(operation, []byte(body)) {
			t.Fatalf("%s was accepted", name)
		}
	}
}

// Concurrent pipelines share the gateway and the audit. This is the coverage the
// race detector needs for the block cache, the pre-reserved slots and the audit
// collector.
func TestGuardedBatchConcurrentPipelines(t *testing.T) {
	fixture, service, _ := gatewayFileAuditFixture(t)
	guardedBatchQuota(t, fixture, 1<<20, 8)
	audit := service.(*fileAudit)
	fixture.upstream.setHook(guardedBatchHook(guardedBatchServesReads))
	handler := newHranaHTTP(fixture.gateway)
	const pipelines = 4
	var wait sync.WaitGroup
	wait.Add(pipelines)
	failures := make(chan string, pipelines)
	for index := 0; index < pipelines; index++ {
		go func() {
			defer wait.Done()
			response := runGuardedBatchPipeline(t, handler, fixture, guardedBatchPipelineBody(12, "SELECT 1"))
			_, results := hranaPipelineDocument(t, response)
			if len(results) != 13 {
				failures <- fmt.Sprintf("pipeline answered %d: %s", response.Code, response.Body.String())
			}
		}()
	}
	wait.Wait()
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	if blocks := guardedBatchCalls(fixture.upstream); len(blocks) != 8 {
		t.Fatalf("blocks=%d, want 8 (two per pipeline)", len(blocks))
	}
	if reserved, admitted := auditCompletionStatus(audit.AuditStatus()); reserved != 0 || admitted != 0 {
		t.Fatalf("concurrent pipelines leaked audit capacity: reserved=%d admitted=%d", reserved, admitted)
	}
}
