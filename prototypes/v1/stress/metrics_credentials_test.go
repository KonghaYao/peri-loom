package stress

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const testMetricsKey = "private_metrics_admin_0123456789abcdef"
const testNoisyKey = "private_noisy_project_0123456789abcdef"

func metricsCredentialRunner(endpoint string) SerialRunner {
	config := testConfig()
	config.Endpoint = endpoint
	config.ManagementEndpoint = endpoint
	config.ManagementKey = config.APIKey
	config.MetricsKey = testMetricsKey
	return SerialRunner{Config: config, Settings: DefaultSettings()}
}

func metricsCredentialArtifacts(test *testing.T, runner SerialRunner) (*Artifacts, string) {
	test.Helper()
	directory := filepath.Join(test.TempDir(), "run")
	artifacts, err := NewArtifacts(directory, 1<<20, runner.credentials())
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = artifacts.Close() })
	return artifacts, filepath.Join(directory, "operations.jsonl")
}

func TestMetricsCredentialsValidation(test *testing.T) {
	runner := metricsCredentialRunner("http://127.0.0.1:1234")
	if err := runner.Config.Validate(); err != nil {
		test.Fatal(err)
	}
	for _, key := range []string{"short", "private\nmetrics", strings.Repeat("x", 4097)} {
		test.Run("invalid_key", func(test *testing.T) {
			config := runner.Config
			config.MetricsKey = key
			if config.Validate() == nil {
				test.Fatal("invalid metrics key accepted")
			}
		})
	}
	for _, endpoint := range []string{"", "http://example.com", "https://user:pass@example.com", "https://example.com?key=private", "https://example.com/path", "https://example.com#private"} {
		test.Run("invalid_endpoint", func(test *testing.T) {
			config := runner.Config
			config.ManagementEndpoint = endpoint
			if config.Validate() == nil {
				test.Fatal("invalid or implicit metrics endpoint accepted")
			}
		})
	}
}

func TestMetricsCredentialsPrivateConfigOptionalStrict(test *testing.T) {
	runner := metricsCredentialRunner("http://127.0.0.1:1234")
	connection := filepath.Join(test.TempDir(), "connection.json")
	encoded, err := json.Marshal(runner.Config)
	if err != nil || os.WriteFile(connection, encoded, 0600) != nil {
		test.Fatal("private config unavailable")
	}
	decoded, err := ReadConnection(connection)
	if err != nil || decoded.MetricsKey != testMetricsKey {
		test.Fatal("new strict config rejected explicit metrics key")
	}
	if err := os.Chmod(connection, 0644); err != nil {
		test.Fatal(err)
	}
	if _, err := ReadConnection(connection); err == nil {
		test.Fatal("public metrics credential file accepted")
	}
	runner.Config.MetricsKey = ""
	encoded, err = json.Marshal(runner.Config)
	if err != nil || strings.Contains(string(encoded), "metrics_key") {
		test.Fatal("empty metrics field was not omitted")
	}
	var oldShape struct {
		ManagementKey string `json:"management_key"`
	}
	if decodeStrict([]byte(`{"management_key":"project","metrics_key":"global"}`), &oldShape) == nil {
		test.Fatal("old strict shape unexpectedly accepts new field")
	}
}

func TestMetricsCredentialsRoutingGETOnlyAndProjectSQL(test *testing.T) {
	var mu sync.Mutex
	metricsCalls, projectCalls, noisyCalls, managementCalls := 0, 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		if request.URL.Path == "/v1/metrics" {
			body, err := io.ReadAll(request.Body)
			if request.Method != http.MethodGet || key != testMetricsKey || err != nil || len(body) != 0 {
				test.Error("metrics route used operational credentials or non-GET/body")
			}
			metricsCalls++
			respond(writer, "", map[string]any{"native": map[string]any{}, "echo": testMetricsKey}, "", 200)
			return
		}
		if key == testMetricsKey || request.Method != http.MethodPost {
			test.Error("metrics credential dispatched operational request")
		}
		var input wireRequest
		if json.NewDecoder(request.Body).Decode(&input) != nil {
			test.Error("invalid operational request")
		}
		if request.URL.Path == "/v1/databases/checkpoint" {
			if key != testKey {
				test.Error("management did not use main project key")
			}
			managementCalls++
			respond(writer, input.RequestID, map[string]bool{"checkpointed": true}, "", 200)
			return
		}
		if key == testKey {
			projectCalls++
		} else if key == testNoisyKey {
			noisyCalls++
		} else {
			test.Error("unexpected SQL credential")
		}
		switch request.URL.Path {
		case "/v1/sql/open":
			respond(writer, input.RequestID, map[string]any{"session": strings.Repeat("a", 32), "readonly": false}, "", 200)
		case "/v1/sql/query":
			respond(writer, input.RequestID, [][]int{{42}}, "", 200)
		case "/v1/sql/close":
			respond(writer, input.RequestID, map[string]bool{"closed": true}, "", 200)
		default:
			test.Error("unexpected operational route")
		}
	}))
	defer server.Close()
	runner := metricsCredentialRunner(server.URL)
	runner.Config.NoisyTenant = &Target{Endpoint: server.URL, Project: "noisy_unit", APIKey: testNoisyKey,
		Database: DatabaseID{Namespace: Namespace{Tenant: "noisy_unit", Database: "stress_noisy_unit"}, Branch: "main"}}
	ids, budget := &IDs{Prefix: "metrics"}, &DataBudget{Maximum: 1 << 20}
	fixtures, management, err := runner.clients("metrics", ids, budget, []Scenario{{Name: "noisy_tenant"}})
	if err != nil || len(fixtures) != 2 || management == nil {
		test.Fatal("operational clients unavailable")
	}
	for _, fixture := range fixtures {
		session, failure := fixture.Client.Open(context.Background(), fixture.Target.Database)
		if failure != nil {
			test.Fatal(failure)
		}
		if _, _, failure := session.SQL(context.Background(), true, "SELECT 42"); failure != nil {
			test.Fatal(failure)
		}
		if failure := session.Close(context.Background()); failure != nil {
			test.Fatal(failure)
		}
	}
	if _, failure := management.Management(context.Background(), "checkpoint", runner.Config.Database, ""); failure != nil {
		test.Fatal(failure)
	}
	artifacts, artifactPath := metricsCredentialArtifacts(test, runner)
	observer, err := runner.observer(artifacts, ids, budget)
	if err != nil || observer.Sample(context.Background()) != nil {
		test.Fatal("metrics observation unavailable")
	}
	mu.Lock()
	if metricsCalls != 1 || managementCalls != 1 || projectCalls != 3 || noisyCalls != 3 {
		test.Error("credential routing changed or request retried")
	}
	mu.Unlock()
	encoded, err := os.ReadFile(artifactPath)
	if err != nil || strings.Contains(string(encoded), testMetricsKey) || !strings.Contains(string(encoded), `"management_metrics_status":"available"`) {
		test.Fatal("metrics evidence absent or credential leaked")
	}
}

func TestMetricsCredentialsAbsentNeverFallsBack(test *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	runner := metricsCredentialRunner(server.URL)
	runner.Config.MetricsKey = ""
	artifacts, artifactPath := metricsCredentialArtifacts(test, runner)
	observer, err := runner.observer(artifacts, &IDs{Prefix: "absent"}, &DataBudget{Maximum: 1 << 20})
	if err != nil || observer.MetricsClient != nil || observer.Sample(context.Background()) != nil || calls.Load() != 0 {
		test.Fatal("missing metrics key fell back or produced typed-nil client")
	}
	encoded, err := os.ReadFile(artifactPath)
	if err != nil || !strings.Contains(string(encoded), `"management_metrics_status":"unavailable"`) || !strings.Contains(string(encoded), `"management_metrics":null`) {
		test.Fatal("missing metrics inferred as zero/available")
	}
}

func TestMetricsCredentialsNegativeReplyUnavailableNoRetry(test *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Method != http.MethodGet || request.URL.Path != "/v1/metrics" || request.Header.Get("Authorization") != "Bearer "+testMetricsKey {
			test.Error("negative metrics request changed credentials/path/method")
		}
		respond(writer, "", nil, "forbidden", http.StatusForbidden)
	}))
	defer server.Close()
	runner := metricsCredentialRunner(server.URL)
	artifacts, artifactPath := metricsCredentialArtifacts(test, runner)
	observer, err := runner.observer(artifacts, &IDs{Prefix: "denied"}, &DataBudget{Maximum: 1 << 20})
	if err != nil || observer.Sample(context.Background()) != nil || calls.Load() != 1 {
		test.Fatal("negative metrics request retried or changed sample semantics")
	}
	encoded, err := os.ReadFile(artifactPath)
	if err != nil || !strings.Contains(string(encoded), `"management_metrics":null`) || !strings.Contains(string(encoded), `"management_metrics_status":"unavailable"`) {
		test.Fatal("rejected metrics presented as available")
	}
}

func TestMetricsCredentialsRedactionAndOutputPathGuard(test *testing.T) {
	runner := metricsCredentialRunner("http://127.0.0.1:1234")
	runner.Config.NoisyTenant = &Target{Endpoint: runner.Config.Endpoint, Project: "noisy_unit", APIKey: testNoisyKey,
		Database: DatabaseID{Namespace: Namespace{Tenant: "noisy_unit", Database: "stress_noisy_unit"}, Branch: "main"}}
	encoded, err := json.Marshal(runner.Config.Redacted())
	if err != nil || !strings.Contains(string(encoded), `"metrics_key":"[REDACTED]"`) {
		test.Fatal("metrics config was not explicitly redacted")
	}
	artifacts, artifactPath := metricsCredentialArtifacts(test, runner)
	if artifacts.Record(map[string]any{"nested": runner.credentials()}) != nil {
		test.Fatal("credential artifact unavailable")
	}
	actual, err := os.ReadFile(artifactPath)
	if err != nil {
		test.Fatal(err)
	}
	for _, key := range runner.credentials() {
		if strings.Contains(string(encoded), key) || strings.Contains(string(actual), key) {
			test.Fatal("private credential leaked")
		}
		runner.OutputDir = filepath.Join(test.TempDir(), "output_"+key)
		if _, err := runner.Run(context.Background()); err == nil || err.Error() != "output path must not contain a credential" {
			test.Fatal("credential path passed pre-source/pre-dispatch guard")
		}
	}
}

func TestMetricsCredentialsInvalidCANeverDispatches(test *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	runner := metricsCredentialRunner(server.URL)
	runner.Config.CAFile = filepath.Join(test.TempDir(), "missing-ca.crt")
	if _, err := runner.metricsClient(&IDs{Prefix: "ca"}, &DataBudget{Maximum: 1 << 20}); err == nil || calls.Load() != 0 {
		test.Fatal("invalid CA bypassed or dispatched")
	}
}
