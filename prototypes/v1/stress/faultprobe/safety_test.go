package faultprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeHost struct {
	mu           sync.Mutex
	identities   map[int]ProcessIdentity
	kills        []int
	changed      bool
	inspectCount int
}

func (host *fakeHost) Inspect(ctx context.Context, pid int) (ProcessIdentity, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.inspectCount++
	identity, found := host.identities[pid]
	if !found {
		return ProcessIdentity{}, ErrProcessGone
	}
	if host.changed {
		identity.Start = "replacement"
	}
	return identity, nil
}

func (host *fakeHost) SignalKill(pid int) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.kills = append(host.kills, pid)
	delete(host.identities, pid)
	return nil
}

func (host *fakeHost) killed() bool {
	host.mu.Lock()
	defer host.mu.Unlock()
	return len(host.kills) > 0
}

func privateJSON(t *testing.T, path string, value any) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil || os.WriteFile(path, body, 0600) != nil {
		t.Fatal("private test JSON unavailable")
	}
}

func unitFixture(t *testing.T) (Config, Connection, Topology, *fakeHost, *Log) {
	t.Helper()
	root, err := os.MkdirTemp(os.Getenv("TMPDIR"), "peri-stress-unitfixture-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	config := DefaultConfig()
	config.RunDir = root
	config.AllowWorkerKill = true
	config.RustBinary = filepath.Join(root, "peri-loom-v1")
	config.SourceRoot = filepath.Join(root, "not_a_repository")
	config.GateFile = filepath.Join(root, "joint.json")
	config.ConnectionFile = filepath.Join(root, "connection.json")
	config.TopologyFile = filepath.Join(root, "topology.json")
	connection := Connection{Endpoint: "http://127.0.0.1:9443", ProjectAPIKey: strings.Repeat("p", 32), GlobalAPIKey: strings.Repeat("g", 32)}
	connection.Database.Namespace.Tenant = "private_tenant"
	connection.Database.Namespace.Database = filepath.Base(root) + "-a"
	connection.Database.Branch = "main"
	topology := Topology{LauncherPID: 42000, Gateway: connection.Endpoint, Authority: "http://127.0.0.1:9100", Broker: "http://127.0.0.1:9101", Workers: map[string]string{"worker-a": "http://127.0.0.1:9200", "worker-b": "http://127.0.0.1:9201"}, Processes: map[string]struct {
		PID int `json:"pid"`
	}{"authority": {42001}, "platform": {42002}, "worker-a": {42003}, "worker-b": {42004}}}
	privateJSON(t, config.ConnectionFile, connection)
	privateJSON(t, config.TopologyFile, topology)
	if os.WriteFile(config.RustBinary, []byte("unit fixture not an executed binary"), 0700) != nil {
		t.Fatal("binary fixture failed")
	}
	host := &fakeHost{identities: map[int]ProcessIdentity{}}
	for index, worker := range []string{"worker-a", "worker-b"} {
		cache := workerCache(config, worker)
		if os.Mkdir(cache, 0700) != nil || os.WriteFile(filepath.Join(cache, "segment"), []byte("synthetic cache"), 0600) != nil {
			t.Fatal("cache fixture failed")
		}
		address := []string{"127.0.0.1:9200", "127.0.0.1:9201"}[index]
		host.identities[topology.Processes[worker].PID] = ProcessIdentity{PID: topology.Processes[worker].PID, UID: os.Getuid(), Start: "original-start", Args: []string{config.RustBinary, "owned-worker", topology.Authority, cache, worker, address}}
	}
	file, err := os.OpenFile(filepath.Join(root, "unit-events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return config, connection, topology, host, &Log{file: file, secrets: []string{connection.ProjectAPIKey, connection.GlobalAPIKey}}
}

func TestPrivateLatestLauncherTopologyAndBoundedConfig(t *testing.T) {
	config, connection, topology, _, _ := unitFixture(t)
	if _, actual, err := config.Validate(); err != nil || actual.LauncherPID != topology.LauncherPID {
		t.Fatalf("latest launcher topology rejected: %v", err)
	}
	invalid := config
	invalid.AllowWorkerKill = false
	if _, _, err := invalid.Validate(); err == nil {
		t.Fatal("kill authorization bypass")
	}
	invalid = config
	invalid.ACKs = 33
	if _, _, err := invalid.Validate(); err == nil {
		t.Fatal("ACK bound bypass")
	}
	invalid = config
	invalid.Case = "metadata_starvation"
	if _, _, err := invalid.Validate(); err == nil {
		t.Fatal("starvation accepted fewer than three DBs")
	}
	connection.Database.Namespace.Database = "demo"
	privateJSON(t, config.ConnectionFile, connection)
	if _, _, err := config.Validate(); err == nil {
		t.Fatal("nonfixture database accepted")
	}
	connection.Database.Namespace.Database = filepath.Base(config.RunDir) + "-a"
	privateJSON(t, config.ConnectionFile, connection)
	if os.Chmod(config.TopologyFile, 0644) != nil {
		t.Fatal("chmod")
	}
	if _, _, err := config.Validate(); err == nil {
		t.Fatal("public topology accepted")
	}
}

func TestExactOwnedWorkerCacheCommandAndPIDGuards(t *testing.T) {
	config, _, topology, host, log := unitFixture(t)
	original := host.identities[topology.Processes["worker-a"].PID]
	if err := validateWorker(config, topology, "worker-a", original); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{0, 1, 2, 3, 4, 5} {
		modified := original
		modified.Args = append([]string{}, original.Args...)
		modified.Args[index] += "-other"
		if validateWorker(config, topology, "worker-a", modified) == nil {
			t.Fatalf("argv %d mismatch accepted", index)
		}
	}
	if validateWorker(config, topology, "authority", original) == nil {
		t.Fatal("authority eligible for kill")
	}
	host.changed = true
	if killOwned(context.Background(), config, topology, "worker-a", original, host, log) == nil || len(host.kills) != 0 {
		t.Fatal("reused PID signaled")
	}
	host.changed = false
	if err := killOwned(context.Background(), config, topology, "worker-a", original, host, log); err != nil {
		t.Fatal(err)
	}
	if len(host.kills) != 1 || host.kills[0] != original.PID {
		t.Fatal("wrong PID signaled")
	}
	if _, err := os.Lstat(workerCache(config, "worker-a")); !os.IsNotExist(err) {
		t.Fatal("owner cache remained")
	}
	if _, err := os.Stat(workerCache(config, "worker-b")); err != nil {
		t.Fatal("remaining worker cache deleted")
	}
}

func TestSymlinkCacheAndFreshFixtureClaim(t *testing.T) {
	config, _, topology, host, log := unitFixture(t)
	outside := filepath.Join(config.RunDir, "authority-data")
	if os.Mkdir(outside, 0700) != nil || os.WriteFile(filepath.Join(outside, "keep"), []byte("untouched"), 0600) != nil {
		t.Fatal("fixture sentinel")
	}
	link := filepath.Join(workerCache(config, "worker-a"), "danger")
	if os.Symlink(outside, link) != nil {
		t.Fatal("fixture symlink")
	}
	original := host.identities[topology.Processes["worker-a"].PID]
	if killOwned(context.Background(), config, topology, "worker-a", original, host, log) == nil || len(host.kills) != 0 {
		t.Fatal("symlink cache allowed fault")
	}
	if _, err := os.Stat(filepath.Join(outside, "keep")); err != nil {
		t.Fatal("authority sentinel modified")
	}
	claimed, err := claimFixture(config.RunDir)
	if err != nil {
		t.Fatal(err)
	}
	defer claimed.file.Close()
	if _, err := claimFixture(config.RunDir); err == nil {
		t.Fatal("fixture reused")
	}
}

func TestGateMissingPreventsAnyProcessOrAPIWork(t *testing.T) {
	config, _, _, host, _ := unitFixture(t)
	result, err := Run(context.Background(), config, host)
	if err == nil || result.Status != "BLOCKED" || host.inspectCount != 0 || len(host.kills) != 0 {
		t.Fatal("gate bypass")
	}
	gate := Gate{Version: 1, JointPass: true, ConfirmedBy: "main", SourceGit: strings.Repeat("a", 40), Stages: []string{"P0", "P1", "P2", "P3", "P4", "P5", "P6"}, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if gate.Validate(gate.SourceGit) != nil {
		t.Fatal("valid gate rejected")
	}
	if gate.Validate(strings.Repeat("b", 40)) == nil {
		t.Fatal("wrong HEAD accepted")
	}
	gate.Stages = gate.Stages[:6]
	if gate.Validate(gate.SourceGit) == nil {
		t.Fatal("missing P6 accepted")
	}
}

func TestRedactionAndExactInt64NativeEvidence(t *testing.T) {
	secret := strings.Repeat("private", 8)
	actual := map[string]any{"native": json.RawMessage(`{"authority":{"put_total":3},"workers":{"cache_bytes":9223372036854775807}}`), "grant": map[string]any{"lease_id": "capability", "grant_id": "capability", "epoch": json.Number("18446744073709551000")}, "credential": secret, "session": "opaque", "message": secret}
	body, err := sanitize(actual, []string{secret})
	if err != nil || strings.Contains(string(body), secret) || strings.Contains(string(body), "capability") || strings.Contains(string(body), "opaque") || !strings.Contains(string(body), "9223372036854775807") || !strings.Contains(string(body), "18446744073709551000") {
		t.Fatalf("redaction/precision failed: %s %v", body, err)
	}
}

func TestProcessIdentityParserRefusesZombieAndMismatch(t *testing.T) {
	identity, err := parseIdentity(fmt.Sprintf("42003 %d S Tue Oct 6 01:02:03 2026 /x/peri-loom-v1 owned-worker http://127.0.0.1:1 /x/cache-a worker-a 127.0.0.1:2", os.Getuid()), 42003)
	if err != nil || len(identity.Args) != 6 {
		t.Fatal("command parse")
	}
	if identity, err := parseIdentity("42003 501 Z Tue Oct 6 01:02:03 2026 dead", 42003); !errors.Is(err, ErrProcessZombie) || errors.Is(err, ErrProcessGone) || identity.PID != 42003 || identity.UID != 501 || identity.Start == "" {
		t.Fatal("zombie must retain metadata without proving process disappearance")
	}
	if _, err := parseIdentity("42004 501 S Tue Oct 6 01:02:03 2026 alive", 42003); err == nil {
		t.Fatal("wrong pid accepted")
	}
}
