package stress

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func testConfig() ConnectionConfig {
	return ConnectionConfig{Target: Target{Endpoint: "http://127.0.0.1:1", Project: "project_unit", APIKey: testKey,
		Database: DatabaseID{Namespace: Namespace{Tenant: "tenant_unit", Database: "stress_unit"}, Branch: "main"}}, FixtureOnly: true, SourceRoot: "/isolated/source"}
}

func TestSerialRunnerRejectsDirtySourceBeforeGateAndOutput(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	if err := os.Mkdir(sourceRoot, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		command := exec.Command("git", append([]string{"-C", sourceRoot}, args...)...)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	git("init", "-q")
	tracked := filepath.Join(sourceRoot, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("clean"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "tracked.txt")
	git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture")

	config := testConfig()
	config.SourceRoot = sourceRoot
	output := filepath.Join(root, "output")
	runner := SerialRunner{Config: config, Settings: DefaultSettings(), OutputDir: output, GatePath: filepath.Join(root, "missing-gate.json")}
	checkNoOutput := func() {
		t.Helper()
		if _, err := os.Lstat(output); !os.IsNotExist(err) {
			t.Fatalf("output directory or lock created: %v", err)
		}
	}
	checkNoOutput()
	if path, err := runner.Run(context.Background()); path != "" || err == nil || err.Error() != "main joint PASS evidence file required" {
		t.Fatalf("clean source did not reach joint gate: path=%q err=%v", path, err)
	}
	checkNoOutput()
	if err := os.WriteFile(tracked, []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	if path, err := runner.Run(context.Background()); path != "" || err == nil || err.Error() != "load prohibited: source working tree is dirty" {
		t.Fatalf("dirty source did not stop before joint gate: path=%q err=%v", path, err)
	}
	checkNoOutput()
	if err := os.WriteFile(tracked, []byte("clean"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "untracked.txt"), []byte("untracked"), 0600); err != nil {
		t.Fatal(err)
	}
	if path, err := runner.Run(context.Background()); path != "" || err == nil || err.Error() != "load prohibited: source working tree is dirty" {
		t.Fatalf("untracked source did not stop before joint gate: path=%q err=%v", path, err)
	}
	checkNoOutput()
}

func TestPrivateConnectionFileAndFixtureBounds(t *testing.T) {
	config := testConfig()
	encoded, _ := json.Marshal(config)
	path := filepath.Join(t.TempDir(), "connection.json")
	if err := os.WriteFile(path, encoded, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadConnection(path); err == nil {
		t.Fatal("world-readable credential accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadConnection(path); err != nil {
		t.Fatal(err)
	}
	config.Database.Namespace.Database = "existing_user_database"
	if config.Validate() == nil {
		t.Fatal("non-stress fixture accepted")
	}
	config = testConfig()
	config.Processes = []Process{{"worker_a", 11}, {"worker_b", 12}, {"worker_c", 13}}
	if config.Validate() == nil {
		t.Fatal("worker topology exceeded initial budget")
	}
	config = testConfig()
	config.Endpoint = "http://untrusted.example"
	if config.Validate() == nil {
		t.Fatal("remote cleartext endpoint accepted")
	}
}

func TestSettingsLimitDatasetConcurrencyAndGeneratedData(t *testing.T) {
	settings := DefaultSettings()
	if settings.Validate() != nil {
		t.Fatal("defaults invalid")
	}
	settings.Rows = 100000
	if settings.Validate() == nil {
		t.Fatal("unapproved initial dataset accepted")
	}
	settings = DefaultSettings()
	settings.Concurrency = []int{16}
	if settings.Validate() == nil {
		t.Fatal("unapproved concurrency accepted")
	}
	settings = DefaultSettings()
	settings.MaxGeneratedBytes++
	if settings.Validate() == nil {
		t.Fatal("generated data exceeds 256MiB ceiling")
	}
}

func TestCredentialRedactionIncludesRawMetricsStringsAndFields(t *testing.T) {
	root := t.TempDir()
	artifacts, err := NewArtifacts(filepath.Join(root, "run"), 1<<20, []string{testKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := artifacts.Record(map[string]any{"api_key": testKey, "rows": []any{testKey}, "metrics": json.RawMessage(`{"authorization":"Bearer ` + testKey + `","control_token":"hidden","actual":7}`)}); err != nil {
		t.Fatal(err)
	}
	if err := artifacts.Summary(testConfig().Redacted()); err != nil {
		t.Fatal(err)
	}
	if err := artifacts.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"operations.jsonl", "run.json"} {
		body, err := os.ReadFile(filepath.Join(root, "run", name))
		if err != nil || strings.Contains(string(body), testKey) || strings.Contains(string(body), `"hidden"`) {
			t.Fatal("secret appeared in artifacts")
		}
		info, _ := os.Stat(filepath.Join(root, "run", name))
		if info.Mode().Perm() != 0600 {
			t.Fatal("raw artifacts not private")
		}
	}
}

func TestArtifactAdmissionAndSerialOutputLock(t *testing.T) {
	root := t.TempDir()
	global := filepath.Join(root, "global.lock")
	output := filepath.Join(root, "output")
	first, err := acquireLocks(output, global)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := acquireLocks(filepath.Join(root, "different_output"), global); err == nil {
		second.Close()
		t.Fatal("competing different-output runner admitted")
	}
	first.Close()
	second, err := acquireLocks(output, global)
	if err != nil {
		t.Fatal("serial lock was not released")
	}
	second.Close()
	artifacts, err := NewArtifacts(filepath.Join(root, "run"), 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	if artifacts.Record(map[string]string{"too_large": strings.Repeat("a", 32)}) == nil {
		t.Fatal("artifact budget ignored")
	}
}

func TestJointGateRequiresExplicitMainAndAllStages(t *testing.T) {
	gate := JointGate{Version: 1, JointPass: true, ConfirmedBy: "main", SourceGit: "matching", ObservedAt: "2026-10-06T00:00:00Z", Stages: []string{"P0", "P1", "P2", "P3", "P4", "P5", "P6"}}
	if gate.Validate("matching") != nil {
		t.Fatal("valid main gate rejected")
	}
	if gate.Validate("different") == nil {
		t.Fatal("wrong source admitted")
	}
	gate.Stages = gate.Stages[:6]
	if gate.Validate("matching") == nil {
		t.Fatal("P0-P6 joint gate incomplete")
	}
	gate.Stages = append(gate.Stages, "P6")
	gate.ConfirmedBy = "exploratory"
	if gate.Validate("matching") == nil {
		t.Fatal("exploratory gate admitted pressure")
	}
}
