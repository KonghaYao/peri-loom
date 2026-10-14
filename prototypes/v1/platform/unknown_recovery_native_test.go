package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

type unknownResponseLossProxy struct {
	mutex             sync.Mutex
	target            string
	targetSQL         string
	dropped           int
	nativeACKs        int
	counts            map[string]int
	requestID         string
	worker            string
	sessions          []string
	lastSession       string
	lastSessionWorker string
}

type unknownOwnedProcessStopVerifier struct{ workers map[string]*p6VerifyProcess }

func (verifier unknownOwnedProcessStopVerifier) VerifyOwnedWorkerStopped(ctx context.Context, worker string) error {
	process := verifier.workers[worker]
	if process == nil || ctx.Err() != nil {
		return errors.New("owned process identity not proven")
	}
	select {
	case <-process.finished:
		return nil
	default:
		return errors.New("owned native worker has not exited")
	}
}

func unknownNativeMetrics(test *testing.T, fixture *p6VerifyFixture) map[string]json.RawMessage {
	test.Helper()
	request, err := http.NewRequest(http.MethodGet, fixture.endpoint+"/v1/metrics", nil)
	if err != nil {
		test.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+fixture.config.BootstrapKey)
	response, err := fixture.client.Do(request)
	if err != nil {
		test.Fatal(err)
	}
	defer response.Body.Close()
	var reply struct {
		Result map[string]json.RawMessage `json:"result"`
	}
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&reply) != nil {
		test.Fatal("fresh admission metrics unavailable")
	}
	return reply.Result
}

func unknownNativeRetireOwnedWorker(test *testing.T, fixture *p6VerifyFixture, worker string, logs []*p6VerifyLog, nativeSessions []string) {
	test.Helper()
	store, err := OpenStateStore(fixture.config.StateDirectory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	signing, err := os.ReadFile(fixture.config.SigningKeyFile)
	if err != nil {
		test.Fatal(err)
	}
	identity, err := NewIdentity(store, signing)
	if err != nil {
		test.Fatal(err)
	}
	placements, err := controller.OpenStore(fixture.config.MetadataDirectory)
	if err != nil {
		test.Fatal(err)
	}
	defer placements.Close()
	upstream, err := NewSQLUpstream(fixture.config.WorkerSQLTokens)
	if err != nil {
		test.Fatal(err)
	}
	audit, err := OpenAudit(fixture.config.AuditPath)
	if err != nil {
		test.Fatal(err)
	}
	defer audit.Close()
	resources, err := NewGateway(GatewayConfig{State: store, Placements: placements, Identity: identity, Upstream: upstream, Audit: audit})
	if err != nil {
		test.Fatal(err)
	}
	management := &Management{config: ManagementConfig{State: store, Identity: identity, Resources: resources, Audit: audit, StopProof: unknownOwnedProcessStopVerifier{workers: fixture.workers}}}
	input := managementRequest{Database: fixture.database, RequestID: "retire_own_worker"}
	before := operationHTTP(test, management, fixture.credential, "/v1/databases/operations/retire-local", input)
	if before.Code == 200 || resources.Metrics()["unknown_held_permits"].(int) < 1 {
		test.Fatal("live worker or local resources retired without terminal proof")
	}
	fixture.workers[worker].stop(test, false)
	after := operationHTTP(test, management, fixture.credential, "/v1/databases/operations/retire-local", input)
	if after.Code != 200 || resources.Metrics()["unknown_held_permits"] != 0 || !bytes.Contains(after.Body.Bytes(), []byte(`"local_resources_retired":true`)) || !bytes.Contains(after.Body.Bytes(), []byte(`"state":"quarantined"`)) {
		test.Fatal("real owned worker exit did not retire only local resources")
	}
	state, err := store.Snapshot()
	if err != nil {
		test.Fatal(err)
	}
	for _, record := range state.Operations {
		if record.Database.Namespace == fixture.database.Namespace && record.State == "quarantined" {
			nativeSessions = append(nativeSessions, record.Session)
		}
	}
	for _, log := range logs {
		log.mutex.Lock()
		for _, session := range nativeSessions {
			if session != "" && bytes.Contains(log.data, []byte(session)) {
				log.mutex.Unlock()
				test.Fatal("native session leaked to public platform log")
			}
		}
		log.mutex.Unlock()
	}
}

func (proxy *unknownResponseLossProxy) route(test *testing.T, worker, target string) string {
	test.Helper()
	client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, Timeout: 35 * time.Second}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(io.LimitReader(request.Body, sqlWireLimit+1))
		if err != nil || len(body) > sqlWireLimit {
			http.Error(response, "bounded proxy request failed", 502)
			return
		}
		var envelope struct {
			RequestID string `json:"request_id"`
			Command   struct {
				Command string `json:"command"`
				SQL     string `json:"sql"`
			} `json:"command"`
		}
		_ = json.Unmarshal(body, &envelope)
		proxy.mutex.Lock()
		armed := request.URL.Path == "/sql" && proxy.target == envelope.Command.Command && (proxy.targetSQL == "" || proxy.targetSQL == envelope.Command.SQL) && proxy.dropped == 0
		if request.URL.Path == "/sql" {
			proxy.counts[envelope.Command.Command]++
		}
		if armed {
			proxy.dropped++
			proxy.requestID, proxy.worker = envelope.RequestID, worker
		}
		proxy.mutex.Unlock()
		forward, err := http.NewRequestWithContext(request.Context(), request.Method, target+request.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			http.Error(response, "bounded proxy forwarding failed", 502)
			return
		}
		forward.Header = request.Header.Clone()
		native, err := client.Do(forward)
		if err != nil {
			http.Error(response, "native forwarding failed", 502)
			return
		}
		defer native.Body.Close()
		payload, err := io.ReadAll(io.LimitReader(native.Body, sqlWireLimit+1))
		if err != nil || len(payload) > sqlWireLimit {
			http.Error(response, "bounded native response failed", 502)
			return
		}
		var acknowledged struct {
			OK     bool `json:"ok"`
			Result struct {
				Session      string `json:"session"`
				Checkpointed bool   `json:"checkpointed"`
				Closed       bool   `json:"closed"`
				Executed     bool   `json:"executed"`
			} `json:"result"`
		}
		decoded := json.Unmarshal(payload, &acknowledged) == nil
		if request.URL.Path == "/sql" && envelope.Command.Command == "open_session" && native.StatusCode == 200 && decoded && acknowledged.OK && upstreamSessionID(acknowledged.Result.Session) {
			proxy.mutex.Lock()
			proxy.lastSession, proxy.lastSessionWorker = acknowledged.Result.Session, worker
			proxy.sessions = append(proxy.sessions, acknowledged.Result.Session)
			proxy.mutex.Unlock()
		}
		if armed {
			if native.StatusCode == 200 && decoded && acknowledged.OK && (envelope.Command.Command == "open_session" && upstreamSessionID(acknowledged.Result.Session) || envelope.Command.Command == "checkpoint" && acknowledged.Result.Checkpointed || envelope.Command.Command == "close_session" && acknowledged.Result.Closed || envelope.Command.Command == "execute" && envelope.Command.SQL == "COMMIT" && acknowledged.Result.Executed) {
				proxy.mutex.Lock()
				proxy.nativeACKs++
				if acknowledged.Result.Session != "" {
					proxy.sessions = append(proxy.sessions, acknowledged.Result.Session)
				}
				proxy.mutex.Unlock()
			}
			connection, _, err := response.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		for key, values := range native.Header {
			response.Header()[key] = values
		}
		response.WriteHeader(native.StatusCode)
		_, _ = response.Write(payload)
	}))
	test.Cleanup(func() { server.Close(); client.CloseIdleConnections() })
	return server.URL
}

func unknownNativeCall(test *testing.T, fixture *p6VerifyFixture, token, path, requestID string, input map[string]any) (int, map[string]json.RawMessage) {
	test.Helper()
	input["request_id"] = requestID
	body, err := json.Marshal(input)
	if err != nil {
		test.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, fixture.endpoint+path, bytes.NewReader(body))
	if err != nil {
		test.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := fixture.client.Do(request)
	if err != nil {
		test.Fatalf("fresh management transport failed: %v", err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	var reply map[string]json.RawMessage
	if err != nil || json.Unmarshal(encoded, &reply) != nil {
		test.Fatal("fresh response envelope missing")
	}
	for _, secret := range fixture.secrets {
		if bytes.Contains(encoded, []byte(secret)) {
			test.Fatal("fresh response disclosed fixture credentials")
		}
	}
	return response.StatusCode, reply
}

func unknownCommitLedgerWitness(test *testing.T, fixture *p6VerifyFixture, proxy *unknownResponseLossProxy, nativeSession, sessionWorker, ledgerMarker, publicSession, requestID string) {
	test.Helper()
	upstream, err := NewSQLUpstream(fixture.config.WorkerSQLTokens)
	if err != nil {
		test.Fatal(err)
	}
	var reader controller.Worker
	for _, configured := range fixture.config.Controller.Workers {
		if configured.Worker.ID == sessionWorker {
			reader = configured.Worker
		}
	}
	if reader.ID == "" {
		test.Fatal("preexisting native reader identity unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var rows [][]json.RawMessage
	err = upstream.Call(ctx, reader, "independent_commit_ledger_read", map[string]any{"command": "query", "session": nativeSession, "sql": "SELECT id,marker FROM commit_ledger ORDER BY id"}, &rows)
	var observedMarker string
	if err != nil || len(rows) != 1 || len(rows[0]) != 2 || string(rows[0][0]) != "1" || json.Unmarshal(rows[0][1], &observedMarker) != nil || observedMarker != ledgerMarker {
		test.Fatal("independent preexisting native reader did not observe committed ledger row")
	}
	for _, correlation := range []string{requestID, "fresh_commit_other_correlation"} {
		status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/execute", correlation, map[string]any{"session": publicSession, "sql": "COMMIT"})
		var failure struct {
			Code string `json:"code"`
		}
		if status != 502 || json.Unmarshal(reply["error"], &failure) != nil || failure.Code != "outcome_unknown" {
			test.Fatal("poisoned COMMIT accepted retry with same or new correlation")
		}
	}
	status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/close", "commit_no_blind_close", map[string]any{"session": publicSession})
	var closedFailure struct {
		Code string `json:"code"`
	}
	if status != 502 || json.Unmarshal(reply["error"], &closedFailure) != nil || closedFailure.Code != "outcome_unknown" {
		test.Fatal("poisoned COMMIT accepted blind Close")
	}
	status, recordReply := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations", "commit_not_reclassified", map[string]any{"database": fixture.database})
	var records struct {
		Operations []OperationStatus `json:"operations"`
	}
	if status != 200 || json.Unmarshal(recordReply["result"], &records) != nil {
		test.Fatal("commit incident unavailable")
	}
	unknownRetained := false
	for _, record := range records.Operations {
		if record.RequestID == requestID {
			unknownRetained = record.State == "quarantined" && record.BusinessOutcome == "unknown" && !record.ExecutionProven && !record.RetryAllowed && record.MayHaveCommitted
		}
	}
	if !unknownRetained {
		test.Fatal("independent witness retroactively reclassified client Unknown")
	}
	proxy.mutex.Lock()
	valid := proxy.nativeACKs == 1 && proxy.dropped == 1 && proxy.counts["execute"] == 1 && proxy.counts["query"] == 1 && proxy.counts["close_session"] == 0 && proxy.counts["open_session"] == 0
	proxy.mutex.Unlock()
	if !valid {
		test.Fatal("commit witness retried mutation, blindly closed, or opened a new native session")
	}
}

func TestUnknownRecoveryNativeResponseLoss(test *testing.T) {
	for _, scenario := range []struct{ name, path, command, stage string }{
		{"open", "/v1/sql/open", "open_session", "open_session"},
		{"management-open", "/v1/databases/checkpoint", "open_session", "open_session"},
		{"checkpoint", "/v1/databases/checkpoint", "checkpoint", "tool_rpc"},
		{"close", "/v1/databases/checkpoint", "close_session", "close_session"},
		{"gateway-close", "/v1/sql/close", "close_session", "close_session"},
		{"commit", "/v1/sql/execute", "execute", "tool_rpc"},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			proxy := &unknownResponseLossProxy{counts: map[string]int{}}
			fixture := p6VerifyNewRouted(test, Quotas{MaxDatabases: 8, MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 1000, MaxSQLBytes: 4096, MaxResultBytes: 4096}, func(worker, target string) string { return proxy.route(test, worker, target) })
			publicLogs := []*p6VerifyLog{&fixture.platform.log}
			input := map[string]any{"database": fixture.database}
			ledgerMarker, nativeSession, sessionWorker, publicSession := "", "", "", ""
			if scenario.name == "gateway-close" {
				session := fixture.open(fixture.credential, fixture.database)
				fixture.sql(session, "execute", "CREATE TABLE fresh_proof(id INTEGER PRIMARY KEY)")
				input = map[string]any{"session": session}
			}
			if scenario.name == "commit" {
				publicSession = fixture.open(fixture.credential, fixture.database)
				preexistingReader := fixture.open(fixture.credential, fixture.database)
				ledgerMarker = p6VerifyToken(test)
				fixture.secrets = append(fixture.secrets, ledgerMarker)
				fixture.sql(publicSession, "execute", "CREATE TABLE commit_ledger(id INTEGER PRIMARY KEY,marker TEXT NOT NULL)")
				if rows := fixture.sql(preexistingReader, "query", "SELECT COUNT(*) FROM commit_ledger"); string(rows.Result) != "[[0]]" {
					test.Fatal("preexisting independent ledger reader not empty before transaction")
				}
				fixture.sql(publicSession, "execute", "BEGIN")
				fixture.sql(publicSession, "execute", "INSERT INTO commit_ledger VALUES(1,'"+ledgerMarker+"')")
				proxy.mutex.Lock()
				nativeSession, sessionWorker = proxy.lastSession, proxy.lastSessionWorker
				proxy.targetSQL = "COMMIT"
				proxy.mutex.Unlock()
				if !upstreamSessionID(nativeSession) || sessionWorker == "" {
					test.Fatal("fresh retained native session not witnessed")
				}
				fixture.secrets = append(fixture.secrets, nativeSession)
				input = map[string]any{"session": publicSession, "sql": "COMMIT"}
			}
			proxy.mutex.Lock()
			proxy.counts = map[string]int{}
			proxy.target = scenario.command
			proxy.mutex.Unlock()
			requestID := "fresh_" + strings.ReplaceAll(scenario.name, "-", "_")
			status, reply := unknownNativeCall(test, fixture, fixture.credential, scenario.path, requestID, input)
			var failure struct {
				Code            string `json:"code"`
				OperationID     string `json:"operation_id"`
				Stage           string `json:"stage"`
				RetryAllowed    bool   `json:"retry_allowed"`
				BusinessOutcome string `json:"business_outcome"`
				CleanupOutcome  string `json:"cleanup_outcome"`
			}
			if json.Unmarshal(reply["error"], &failure) != nil || status != 502 || string(reply["request_id"]) != `"`+requestID+`"` || failure.Code != "outcome_unknown" || failure.OperationID == "" || failure.OperationID == requestID || failure.Stage != scenario.stage || failure.RetryAllowed {
				test.Fatalf("fresh Unknown not accurately correlated: status=%d error=%s", status, reply["error"])
			}
			if scenario.name == "close" && (failure.BusinessOutcome != "completed" || failure.CleanupOutcome != "unknown") {
				test.Fatal("Checkpoint ACK erased by missing Close reply")
			}
			if scenario.name == "commit" {
				unknownCommitLedgerWitness(test, fixture, proxy, nativeSession, sessionWorker, ledgerMarker, publicSession, requestID)
			}
			proxy.mutex.Lock()
			if proxy.dropped != 1 || proxy.nativeACKs != 1 || proxy.counts[scenario.command] != 1 {
				proxy.mutex.Unlock()
				test.Fatal("fault did not consume exactly one real native ACK")
			}
			before := map[string]int{}
			for command, count := range proxy.counts {
				before[command] = count
			}
			worker, nativeRequestID := proxy.worker, proxy.requestID
			nativeSessions := append([]string(nil), proxy.sessions...)
			proxy.mutex.Unlock()
			for _, path := range []string{"/v1/databases/operations", "/v1/databases/status", "/v1/databases/schema", "/v1/databases/diagnostics"} {
				status, read := unknownNativeCall(test, fixture, fixture.credential, path, "read_"+strings.ReplaceAll(filepath.Base(path), "-", "_"), map[string]any{"database": fixture.database})
				if status != 200 || bytes.Contains(read["result"], []byte(`"session"`)) || bytes.Contains(read["result"], []byte(`"digest"`)) || !bytes.Contains(read["result"], []byte(requestID)) {
					test.Fatal("fresh record read missing or leaked native session")
				}
			}
			for _, path := range []string{"/v1/databases/checkpoint", "/v1/sql/open", "/v1/databases/operations/clear"} {
				status, _ := unknownNativeCall(test, fixture, fixture.credential, path, "blocked_"+strings.ReplaceAll(filepath.Base(path), "-", "_"), map[string]any{"database": fixture.database})
				if status == 200 {
					test.Fatal("fresh isolation admitted mutation or clear")
				}
			}
			status, _ = unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations/decision", "retain_decision", map[string]any{"database": fixture.database, "operation_id": failure.OperationID, "decision": "retain_isolation_new_database_required"})
			if status != 200 {
				test.Fatal("fresh retain-isolation decision failed")
			}
			fixture.platform.stop(test, false)
			fixture.startPlatform()
			publicLogs = append(publicLogs, &fixture.platform.log)
			status, read := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations", "after_restart", map[string]any{"database": fixture.database})
			if status != 200 || !bytes.Contains(read["result"], []byte(`"state":"quarantined"`)) || !bytes.Contains(read["result"], []byte("retain_isolation_new_database_required")) {
				test.Fatal("fresh restart erased durable danger or operator decision")
			}
			var recovered struct {
				Operations []OperationStatus `json:"operations"`
			}
			if json.Unmarshal(read["result"], &recovered) != nil {
				test.Fatal("fresh operation list unavailable")
			}
			for _, record := range recovered.Operations {
				if record.State == "quarantined" && record.Decision == "" {
					status, _ = unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations/decision", "retain_"+record.OperationID, map[string]any{"database": fixture.database, "operation_id": record.OperationID, "decision": "retain_isolation_new_database_required"})
					if status != 200 {
						test.Fatal("recovered live session retain decision failed")
					}
				}
			}
			status, _ = unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/open", "after_restart_open", map[string]any{"database": fixture.database})
			if status == 200 {
				test.Fatal("restart reopened unknown namespace")
			}
			metrics := unknownNativeMetrics(test, fixture)
			var held int
			if json.Unmarshal(metrics["unknown_held_permits"], &held) != nil || held < 1 {
				test.Fatal("restart silently recycled unknown native resource admission")
			}
			proxy.mutex.Lock()
			for command, count := range before {
				if proxy.counts[command] != count {
					proxy.mutex.Unlock()
					test.Fatal("safe reads, decisions, or restart dispatched another native RPC")
				}
			}
			for command, count := range proxy.counts {
				if before[command] != count {
					proxy.mutex.Unlock()
					test.Fatal("isolation dispatched new native command")
				}
			}
			proxy.mutex.Unlock()
			for _, file := range []string{fixture.config.AuditPath, filepath.Join(fixture.config.StateDirectory, "state.json")} {
				encoded, err := os.ReadFile(file)
				if err != nil {
					test.Fatal(err)
				}
				for _, secret := range fixture.secrets {
					if secret == nativeSession && file == filepath.Join(fixture.config.StateDirectory, "state.json") {
						continue
					}
					if bytes.Contains(encoded, []byte(secret)) {
						test.Fatal("credential persisted in fresh audit or state")
					}
				}
			}
			fixture.platform.stop(test, false)
			unknownNativeRetireOwnedWorker(test, fixture, worker, publicLogs, nativeSessions)
			fixture.startPlatform()
			metrics = unknownNativeMetrics(test, fixture)
			if string(metrics["unknown_held_permits"]) != "0" {
				test.Fatal("durably retired local resources restored as live")
			}
			status, _ = unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/open", "after_retirement_open", map[string]any{"database": fixture.database})
			if status == 200 {
				test.Fatal("owned worker exit falsely cleared namespace isolation")
			}
			fixture.platform.stop(test, false)
			test.Logf("TRUE_FAULT case=%s native_ack=1 dropped_response=1 worker=%s native_request_id=%s durable_reload=isolated safe_reads=no_native_open decision=new_database_required historical_cause=not_attributed owned_worker_stop=proven local_resources_retired=1 namespace_isolated=1", scenario.name, worker, nativeRequestID)
			if scenario.name == "commit" {
				test.Log("COMMIT_WITNESS native_ack=1 ledger_rows=1 retained_session_read=1 commit_replays=0 blind_close=0 new_sessions=0")
			}
		})
	}
}
