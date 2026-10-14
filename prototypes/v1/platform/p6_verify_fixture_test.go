package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	controller "peri-loom/controller"
)

type p6VerifyReply struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  struct {
		Code               string `json:"code"`
		ExecutionCompleted bool   `json:"execution_completed"`
		MayHaveCommitted   bool   `json:"may_have_committed"`
	} `json:"error"`
}

type p6VerifyFixture struct {
	test       *testing.T
	directory  string
	rust       string
	binary     string
	configPath string
	config     ServiceConfig
	platform   *p6VerifyProcess
	authority  *p6VerifyProcess
	workers    map[string]*p6VerifyProcess
	endpoint   string
	credential string
	database   controller.DatabaseID
	client     *http.Client
	secrets    []string
	request    uint64
	observer   *p6VerifyAuthorityObserver
}

func p6VerifyAddress(test *testing.T) string {
	test.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		test.Fatal(err)
	}
	return address
}

func p6VerifyPrivate(test *testing.T, path string, value any) {
	test.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		test.Fatal(err)
	}
}

func p6VerifyRustBinary(test *testing.T) string {
	test.Helper()
	if binary := os.Getenv("PERI_RUST_BINARY"); binary != "" {
		info, err := os.Stat(binary)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			test.Fatal("PERI_RUST_BINARY must be an actual built executable")
		}
		return binary
	}
	directory, err := os.Getwd()
	if err != nil {
		test.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, ".git")); err == nil {
			binary := filepath.Join(directory, "prototypes", "v1", "target", "debug", "peri-loom-v1")
			if info, err := os.Stat(binary); err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
				return binary
			}
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}
	test.Fatal("build the actual P6 Rust binary and set PERI_RUST_BINARY; no helper skip or machine-specific fallback")
	return ""
}

func p6VerifyNew(test *testing.T) *p6VerifyFixture {
	return p6VerifyNewWithQuotas(test, Quotas{})
}

func p6VerifyNewWithQuotas(test *testing.T, quotas Quotas) *p6VerifyFixture {
	return p6VerifyNewRouted(test, quotas, nil)
}

func p6VerifyNewRouted(test *testing.T, quotas Quotas, route func(string, string) string) *p6VerifyFixture {
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
		InsecureLoopback: true, WorkerSQLTokens: map[string]string{}, WorkerKeyTokens: map[string]string{}, WorkerAuthorityTokens: map[string]string{},
	}
	if err := os.WriteFile(fixture.config.SigningKeyFile, []byte(token())[:32], 0600); err != nil {
		test.Fatal(err)
	}
	addresses := map[string]string{}
	for _, workerID := range []string{"worker-a", "worker-b"} {
		address := "http://" + p6VerifyAddress(test)
		addresses[workerID] = address
		if route != nil {
			address = route(workerID, address)
		}
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

func (fixture *p6VerifyFixture) startPlatform() {
	fixture.platform = p6VerifyStart(fixture.test, fixture.binary, []string{"-config", fixture.configPath}, nil)
	fixture.endpoint = "http://" + fixture.platform.ready["address"].(string)
}

func (fixture *p6VerifyFixture) call(token, path string, input map[string]any) (int, p6VerifyReply) {
	fixture.test.Helper()
	fixture.request++
	input["request_id"] = fmt.Sprintf("verify-%d", fixture.request)
	body, err := json.Marshal(input)
	if err != nil {
		fixture.test.Fatal(err)
	}
	request, err := http.NewRequest("POST", fixture.endpoint+path, bytes.NewReader(body))
	if err != nil {
		fixture.test.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := fixture.client.Do(request)
	if err != nil {
		fixture.test.Fatalf("HTTP %s: %v", path, err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 8*1024*1024))
	if err != nil {
		fixture.test.Fatal(err)
	}
	var reply p6VerifyReply
	if json.Unmarshal(encoded, &reply) != nil {
		fixture.test.Fatalf("invalid response envelope for %s status=%d", path, response.StatusCode)
	}
	return response.StatusCode, reply
}

func (fixture *p6VerifyFixture) must(token, path string, input map[string]any) p6VerifyReply {
	fixture.test.Helper()
	status, reply := fixture.call(token, path, input)
	if status != 200 || !reply.OK {
		if path == "/v1/databases/status" {
			fixture.captureStatusFailure()
		}
		fixture.test.Fatalf("%s status=%d code=%s", path, status, reply.Error.Code)
	}
	return reply
}

func (fixture *p6VerifyFixture) open(token string, database controller.DatabaseID) string {
	fixture.test.Helper()
	reply := fixture.must(token, "/v1/sql/open", map[string]any{"database": database})
	var result struct {
		Session string `json:"session"`
	}
	if json.Unmarshal(reply.Result, &result) != nil || result.Session == "" {
		fixture.test.Fatal("session missing")
	}
	return result.Session
}

func (fixture *p6VerifyFixture) sql(session, action, sql string) p6VerifyReply {
	fixture.test.Helper()
	return fixture.must(fixture.credential, "/v1/sql/"+action, map[string]any{"session": session, "sql": sql})
}
