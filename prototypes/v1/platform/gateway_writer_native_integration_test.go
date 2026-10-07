package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func writerNativeProfileFixture(test *testing.T, quotas Quotas) *p6VerifyFixture {
	test.Helper()
	fixture := &p6VerifyFixture{test: test, directory: test.TempDir(), workers: map[string]*p6VerifyProcess{}, client: &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 35 * time.Second}}
	fixture.rust = p6VerifyRustBinary(test)
	fixture.binary = filepath.Join(fixture.directory, "peri-platform")
	buildArguments := []string{"build"}
	if os.Getenv("PERI_P6_VERIFY_RACE_CLI") == "1" {
		buildArguments = append(buildArguments, "-race")
		test.Log("actual GoCLI race instrumentation enabled")
	}
	buildArguments = append(buildArguments, "-o", fixture.binary, "./cmd/peri-platform")
	build := exec.Command("go", buildArguments...)
	build.Env = p6VerifyEnvironment(nil)
	if output, err := build.CombinedOutput(); err != nil {
		test.Fatalf("actual platform build: %v\n%s", err, output)
	}
	for _, directory := range []string{"authority", "metadata", "state", "audit", "kms", "cache-a", "cache-b"} {
		if err := os.Mkdir(filepath.Join(fixture.directory, directory), 0700); err != nil {
			test.Fatal(err)
		}
	}
	token := func() string {
		value := p6VerifyToken(test)
		fixture.secrets = append(fixture.secrets, value)
		return value
	}
	fixture.config = ServiceConfig{
		Controller:        controller.ServiceConfig{AuthorityToken: token(), APIToken: token()},
		MetadataDirectory: filepath.Join(fixture.directory, "metadata"), StateDirectory: filepath.Join(fixture.directory, "state"),
		AuditPath: filepath.Join(fixture.directory, "audit", "events.json"), KMSKeyring: filepath.Join(fixture.directory, "kms", "keyring.json"),
		SigningKeyFile: filepath.Join(fixture.directory, "signing.key"), BootstrapKey: token(), GatewayListen: "127.0.0.1:0", BrokerListen: p6VerifyAddress(test),
		InsecureLoopback: true, WriterAdmission: true, WorkerSQLTokens: map[string]string{}, WorkerKeyTokens: map[string]string{}, WorkerAuthorityTokens: map[string]string{},
	}
	if err := os.WriteFile(fixture.config.SigningKeyFile, []byte(token())[:32], 0600); err != nil {
		test.Fatal(err)
	}
	addresses := map[string]string{}
	for _, workerID := range []string{"worker-a", "worker-b"} {
		address := "http://" + p6VerifyAddress(test)
		addresses[workerID] = address
		fixture.config.Controller.Workers = append(fixture.config.Controller.Workers, controller.ConfiguredWorker{Worker: controller.Worker{ID: workerID, Address: address, Region: "local", Capacity: 32}, ControlToken: token()})
		fixture.config.WorkerSQLTokens[workerID] = token()
		fixture.config.WorkerKeyTokens[workerID] = token()
		fixture.config.WorkerAuthorityTokens[workerID] = token()
	}
	registry := filepath.Join(fixture.directory, "workers.json")
	p6VerifyPrivate(test, registry, fixture.config.WorkerAuthorityTokens)
	fixture.authority = p6VerifyStart(test, fixture.rust, []string{"authority", filepath.Join(fixture.directory, "objects"), "127.0.0.1:0", filepath.Join(fixture.directory, "authority")}, map[string]string{"PERI_AUTHORITY_CONTROL_TOKEN": fixture.config.Controller.AuthorityToken, "PERI_AUTHORITY_WORKERS_FILE": registry})
	fixture.config.Controller.AuthorityURL = "http://" + fixture.authority.ready["address"].(string)
	actualAuthorityURL := fixture.config.Controller.AuthorityURL
	if os.Getenv("PERI_P6_VERIFY_OBSERVE_AUTHORITY") == "1" {
		fixture.config.Controller.AuthorityURL = fixture.observeAuthority(actualAuthorityURL)
	}
	fixture.configPath = filepath.Join(fixture.directory, "platform.json")
	p6VerifyPrivate(test, fixture.configPath, fixture.config)
	fixture.startPlatform()
	for index, worker := range fixture.config.Controller.Workers {
		cache := filepath.Join(fixture.directory, fmt.Sprintf("cache-%c", 'a'+index))
		fixture.workers[worker.Worker.ID] = p6VerifyStart(test, fixture.rust, []string{"owned-worker", actualAuthorityURL, cache, worker.Worker.ID, addresses[worker.Worker.ID][len("http://"):]}, map[string]string{
			"PERI_WORKER_WRITER_PROFILE": "1", "PERI_WORKER_MAX_INFLIGHT": "21", "PERI_WORKER_CONTROL_RESERVE": "16",
			"PERI_WORKER_AUTHORITY_TOKEN": fixture.config.WorkerAuthorityTokens[worker.Worker.ID], "PERI_WORKER_CONTROL_TOKEN": worker.ControlToken,
			"PERI_WORKER_SQL_TOKEN": fixture.config.WorkerSQLTokens[worker.Worker.ID], "PERI_WORKER_KEY_TOKEN": fixture.config.WorkerKeyTokens[worker.Worker.ID], "PERI_KMS_ENDPOINT": "http://" + fixture.config.BrokerListen,
		})
	}
	test.Cleanup(func() {
		if test.Failed() {
			for name, process := range fixture.workers {
				test.Logf("%s diagnostics: %s", name, process.log.text(fixture.secrets))
			}
			test.Logf("authority diagnostics: %s", fixture.authority.log.text(fixture.secrets))
			test.Logf("platform diagnostics: %s", fixture.platform.log.text(fixture.secrets))
		}
		fixture.platform.stop(test, true)
		fixture.client.CloseIdleConnections()
	})
	fixture.must(fixture.config.BootstrapKey, "/v1/reconcile", map[string]any{})
	fixture.must(fixture.config.BootstrapKey, "/v1/organizations/create", map[string]any{"id": "verify", "name": "Independent verification"})
	fixture.must(fixture.config.BootstrapKey, "/v1/projects/create", map[string]any{"id": "primary", "organization": "verify", "region": "local", "quotas": quotas})
	key := fixture.must(fixture.config.BootstrapKey, "/v1/keys/create", map[string]any{"project": "primary", "subject": "verifier", "role": "admin", "service_account": true})
	var created struct {
		Credential string `json:"credential"`
	}
	if json.Unmarshal(key.Result, &created) != nil || created.Credential == "" {
		test.Fatal("API credential missing")
	}
	fixture.credential = created.Credential
	fixture.secrets = append(fixture.secrets, fixture.credential)
	fixture.database = controller.DatabaseID{Namespace: controller.Namespace{Tenant: "verify_primary", Database: "encrypted"}, Branch: "main"}
	fixture.must(fixture.credential, "/v1/databases/create", map[string]any{"project": "primary", "database": fixture.database, "role": "writer"})
	return fixture
}

type writerNativeReply struct {
	Version   uint32          `json:"version"`
	RequestID string          `json:"request_id"`
	OK        bool            `json:"ok"`
	Result    json.RawMessage `json:"result"`
	Error     struct {
		Code               string `json:"code"`
		ExecutionCompleted bool   `json:"execution_completed"`
		MayHaveCommitted   bool   `json:"may_have_committed"`
	} `json:"error"`
}

type writerNativeCompletion struct {
	status int
	reply  writerNativeReply
	err    error
}

func writerNativeRequest(fixture *p6VerifyFixture, action string, input map[string]any) writerNativeCompletion {
	identifier, err := controller.RandomID()
	if err != nil {
		return writerNativeCompletion{err: err}
	}
	input["request_id"] = identifier
	body, err := json.Marshal(input)
	if err != nil {
		return writerNativeCompletion{err: err}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, fixture.endpoint+"/v1/sql/"+action, bytes.NewReader(body))
	if err != nil {
		return writerNativeCompletion{err: err}
	}
	request.Header.Set("Authorization", "Bearer "+fixture.credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-ID", identifier)
	response, err := fixture.client.Do(request)
	if err != nil {
		return writerNativeCompletion{err: fmt.Errorf("native gateway transport failed")}
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	var reply writerNativeReply
	if err != nil || len(encoded) > 1024*1024 || json.Unmarshal(encoded, &reply) != nil || reply.Version != 1 || reply.RequestID != identifier {
		return writerNativeCompletion{err: fmt.Errorf("native gateway envelope invalid")}
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil {
		return writerNativeCompletion{err: fmt.Errorf("native gateway envelope invalid")}
	}
	for _, forbidden := range []string{"session_state", "binding", "owner", "sequence"} {
		if _, exists := fields[forbidden]; exists {
			return writerNativeCompletion{err: fmt.Errorf("private native state leaked")}
		}
	}
	return writerNativeCompletion{status: response.StatusCode, reply: reply}
}

func writerNativeMust(test *testing.T, fixture *p6VerifyFixture, action string, input map[string]any) writerNativeReply {
	test.Helper()
	result := writerNativeRequest(fixture, action, input)
	if result.err != nil || result.status != 200 || !result.reply.OK {
		test.Fatalf("native %s status=%d code=%s transport=%v", action, result.status, result.reply.Error.Code, result.err)
	}
	return result.reply
}

func writerNativeWaitGeneral(test *testing.T, fixture *p6VerifyFixture, worker controller.Worker, count int, finished <-chan writerNativeCompletion) {
	test.Helper()
	telemetry := &Telemetry{tokens: map[string]string{}, client: fixture.client}
	for _, configured := range fixture.config.Controller.Workers {
		telemetry.tokens[configured.Worker.ID] = configured.ControlToken
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case result := <-finished:
			test.Fatalf("contender completed before holder release: status=%d code=%s transport=%v", result.status, result.reply.Error.Code, result.err)
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		value, err := telemetry.workerMetrics(ctx, worker)
		cancel()
		if err != nil {
			test.Fatal("actual worker metrics unavailable")
		}
		encoded, err := json.Marshal(value)
		var metrics struct {
			WriterProfile bool `json:"writer_profile"`
			Lanes         struct {
				General int `json:"general_sql"`
			} `json:"rpc_lanes"`
		}
		if err != nil || json.Unmarshal(encoded, &metrics) != nil || !metrics.WriterProfile {
			test.Fatal("actual worker did not enable writer profile")
		}
		if metrics.Lanes.General == count {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	test.Fatal("actual worker general lanes did not reach expected occupancy")
}

func TestGatewayWriterNativeV2ReservedProgress(test *testing.T) {
	if os.Getenv("PERI_NATIVE_WRITER_PROFILE_TEST") != "1" {
		test.Skip("opt-in actual native process fixture; requires authorized execution and explicit PERI_RUST_BINARY")
	}
	binary := os.Getenv("PERI_RUST_BINARY")
	if binary == "" {
		test.Fatal("PERI_RUST_BINARY must explicitly identify the newly frozen compatible Rust executable")
	}
	content, err := os.ReadFile(binary)
	if err != nil {
		test.Fatal("explicit Rust executable unreadable")
	}
	test.Logf("actual Rust executable SHA256=%x", sha256.Sum256(content))
	fixture := writerNativeProfileFixture(test, Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 1000})
	if !fixture.config.WriterAdmission {
		test.Fatal("actual GoCLI writer_admission not configured")
	}
	placement := fixture.placement()
	var worker controller.Worker
	for _, configured := range fixture.config.Controller.Workers {
		if configured.Worker.ID == placement.DesiredWorker {
			worker = configured.Worker
		}
	}
	if worker.ID == "" {
		test.Fatal("actual writer placement worker missing")
	}
	owner := fixture.open(fixture.credential, fixture.database)
	idle := fixture.open(fixture.credential, fixture.database)
	waiters := []string{fixture.open(fixture.credential, fixture.database), fixture.open(fixture.credential, fixture.database), fixture.open(fixture.credential, fixture.database)}
	writerNativeMust(test, fixture, "execute", map[string]any{"session": owner, "sql": "CREATE TABLE reserve_proof(id INTEGER PRIMARY KEY); INSERT INTO reserve_proof VALUES(1)"})
	writerNativeMust(test, fixture, "execute", map[string]any{"session": owner, "sql": "BEGIN IMMEDIATE; INSERT INTO reserve_proof VALUES(2)"})
	finished := make(chan writerNativeCompletion, 3)
	for index, session := range waiters {
		go func(session string, identifier int) {
			finished <- writerNativeRequest(fixture, "execute", map[string]any{"session": session, "sql": fmt.Sprintf("BEGIN IMMEDIATE; INSERT INTO reserve_proof VALUES(%d); COMMIT", identifier)})
		}(session, index+10)
	}
	writerNativeWaitGeneral(test, fixture, worker, 3, finished)
	writerNativeMust(test, fixture, "close", map[string]any{"session": idle})
	writerNativeWaitGeneral(test, fixture, worker, 3, finished)
	failed := writerNativeRequest(fixture, "execute", map[string]any{"session": owner, "sql": "INSERT INTO reserve_proof VALUES(1)"})
	if failed.err != nil || failed.status != 400 || failed.reply.OK || failed.reply.Error.Code != "sql_error" || !failed.reply.Error.ExecutionCompleted || !failed.reply.Error.MayHaveCommitted {
		test.Fatalf("actual native constraint not settled: status=%d code=%s transport=%v", failed.status, failed.reply.Error.Code, failed.err)
	}
	writerNativeWaitGeneral(test, fixture, worker, 3, finished)
	time.Sleep(1100 * time.Millisecond)
	writerNativeMust(test, fixture, "execute", map[string]any{"session": owner, "sql": "COMMIT"})
	for count := 0; count < 3; count++ {
		select {
		case result := <-finished:
			if result.err != nil || result.status != 200 || !result.reply.OK {
				test.Fatalf("actual contender failed after COMMIT: status=%d code=%s transport=%v", result.status, result.reply.Error.Code, result.err)
			}
		case <-time.After(10 * time.Second):
			test.Fatal("actual contender did not finish after reserved holder COMMIT")
		}
	}
	writerNativeMust(test, fixture, "execute", map[string]any{"session": owner, "sql": "BEGIN IMMEDIATE; INSERT INTO reserve_proof VALUES(3)"})
	time.Sleep(1100 * time.Millisecond)
	failed = writerNativeRequest(fixture, "execute", map[string]any{"session": owner, "sql": "INSERT INTO reserve_proof VALUES(1)"})
	if failed.err != nil || failed.status != 400 || failed.reply.Error.Code != "sql_error" || !failed.reply.Error.ExecutionCompleted {
		test.Fatal("second actual constraint did not preserve reusable explicit transaction")
	}
	time.Sleep(1100 * time.Millisecond)
	writerNativeMust(test, fixture, "execute", map[string]any{"session": owner, "sql": "ROLLBACK"})
	rows := writerNativeMust(test, fixture, "query", map[string]any{"session": owner, "sql": "SELECT id FROM reserve_proof ORDER BY id"})
	if string(rows.Result) != "[[1],[2],[10],[11],[12]]" {
		test.Fatalf("actual COMMIT/constraint/ROLLBACK data mismatch: %s", rows.Result)
	}
	for _, session := range append(waiters, owner) {
		closed := writerNativeMust(test, fixture, "close", map[string]any{"session": session})
		var result struct {
			Closed bool `json:"closed"`
		}
		if json.Unmarshal(closed.Result, &result) != nil || !result.Closed {
			test.Fatal("actual native Close ACK missing")
		}
	}
	test.Log("actual GoCLI writer_admission=true + native private v2; C4 general3 blocked, idle Close ACK, constraint owner retained, reserved COMMIT, three contenders complete, explicit ROLLBACK and Close ACK; no SQL retry")
}
