package platform

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestOwnedStopProofDefaultCLINativeResponseLoss(test *testing.T) {
	for _, command := range []string{"open_session", "close_session"} {
		test.Run(command, func(test *testing.T) { ownedStopProofDefaultCLINativeResponseLoss(test, command) })
	}
}

func ownedStopProofDefaultCLINativeResponseLoss(test *testing.T, command string) {
	python, err := exec.LookPath("python3")
	if err != nil {
		test.Fatal("Python launcher required")
	}
	directory, err := os.MkdirTemp("/tmp", "peri-owned-proof-")
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { os.RemoveAll(directory) })
	working, err := os.Getwd()
	if err != nil {
		test.Fatal(err)
	}
	launcher := filepath.Join(working, "dev.py")
	fixture := &p6VerifyFixture{test: test, directory: directory, rust: p6VerifyRustBinary(test), binary: filepath.Join(directory, "peri-platform"), client: &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 35 * time.Second}}
	buildArguments := []string{"build"}
	if os.Getenv("PERI_P6_VERIFY_RACE_CLI") == "1" {
		buildArguments = append(buildArguments, "-race")
	}
	build := exec.Command("go", append(buildArguments, "-o", fixture.binary, "./cmd/peri-platform")...)
	build.Env = p6VerifyEnvironment(nil)
	if output, err := build.CombinedOutput(); err != nil {
		test.Fatalf("actual CLI build: %v\n%s", err, output)
	}
	environment := map[string]string{"PERI_RUST_BINARY": fixture.rust, "PERI_PLATFORM_BINARY": fixture.binary}
	listen := p6VerifyAddress(test)
	addresses := []string{p6VerifyAddress(test), p6VerifyAddress(test), p6VerifyAddress(test), p6VerifyAddress(test)}
	proxy := &unknownResponseLossProxy{counts: map[string]int{}}
	proxyAddresses := []string{
		proxy.route(test, "worker-a", "http://"+addresses[2]),
		proxy.route(test, "worker-b", "http://"+addresses[3]),
	}
	launcherScript := `import importlib.util,json,pathlib,sys
script,directory,listen = sys.argv[1:4]
addresses = iter(sys.argv[4:8])
proxies = sys.argv[8:10]
spec = importlib.util.spec_from_file_location("owned_launcher", script)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.free_address = lambda: next(addresses)
prepare = module.Project.prepare
def prepare_with_proxy(project):
    prepare(project)
    for index, worker in enumerate(project.config["controller"]["workers"]):
        worker["worker"]["address"] = proxies[index]
    module.atomic_private_json(project.directory / "platform.json", project.config)
module.Project.prepare = prepare_with_proxy
sys.argv = [script, "--no-build", "--run-dir", directory, "--listen", listen]
sys.exit(module.main())`
	project := p6VerifyStart(test, python, append([]string{"-B", "-c", launcherScript, launcher, directory, listen}, append(addresses, proxyAddresses...)...), environment)
	fixture.configPath = filepath.Join(directory, "platform.json")
	fixture.config, err = ReadServiceConfig(fixture.configPath)
	if err != nil || fixture.config.OwnedStopProof == nil || len(fixture.config.Controller.Workers) != len(proxyAddresses) {
		test.Fatal("launcher did not configure default production adapter")
	}
	for index, worker := range fixture.config.Controller.Workers {
		if worker.Worker.Address != proxyAddresses[index] || worker.Worker.ID != []string{"worker-a", "worker-b"}[index] {
			test.Fatalf("initial launcher worker proxy configuration mismatch at %d", index)
		}
	}
	test.Cleanup(func() {
		select {
		case <-project.finished:
			return
		default:
		}
		_ = project.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-project.finished:
		case <-time.After(50 * time.Second):
			test.Error("owned launcher failed to stop and join stack")
		}
	})
	fixture.endpoint = project.ready["address"].(string)
	encoded, err := privateBytes(filepath.Join(directory, "connection.json"), 8192)
	var connection struct {
		Credential string          `json:"project_api_key"`
		Database   json.RawMessage `json:"database"`
	}
	if err != nil || json.Unmarshal(encoded, &connection) != nil || json.Unmarshal(connection.Database, &fixture.database) != nil {
		test.Fatal("fresh public project connection unavailable")
	}
	fixture.credential = connection.Credential
	var retainedSession string
	if command == "close_session" {
		status, opened := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/open", "acknowledged_open", map[string]any{"database": fixture.database})
		var result struct {
			Session string `json:"session"`
		}
		if status != 200 || json.Unmarshal(opened["result"], &result) != nil || result.Session == "" {
			test.Fatal("native Open ACK required before Close loss")
		}
		retainedSession = result.Session
	}
	proxy.mutex.Lock()
	proxy.counts, proxy.target = map[string]int{}, command
	proxy.mutex.Unlock()
	path, input := "/v1/sql/open", map[string]any{"database": fixture.database}
	if retainedSession != "" {
		path, input = "/v1/sql/close", map[string]any{"session": retainedSession}
	}
	status, unknown := unknownNativeCall(test, fixture, fixture.credential, path, "actual_launcher_loss", input)
	if status != 502 || !bytes.Contains(unknown["error"], []byte(`"outcome_unknown"`)) {
		test.Fatal("actual native Open loss not unknown")
	}
	proxy.mutex.Lock()
	worker, acknowledged, dropped := proxy.worker, proxy.nativeACKs, proxy.dropped
	before := map[string]int{}
	for command, count := range proxy.counts {
		before[command] = count
	}
	proxy.mutex.Unlock()
	if acknowledged != 1 || dropped != 1 {
		test.Fatal("native execution ACK was not actually consumed and dropped")
	}
	status, read := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations", "read_record", map[string]any{"database": fixture.database})
	var listing struct {
		Operations []OperationStatus `json:"operations"`
	}
	if status != 200 || json.Unmarshal(read["result"], &listing) != nil || len(listing.Operations) == 0 {
		test.Fatal("operation read unavailable")
	}
	for _, record := range listing.Operations {
		if record.State != "quarantined" {
			continue
		}
		status, _ = unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations/decision", "decision_"+record.OperationID, map[string]any{"database": fixture.database, "operation_id": record.OperationID, "decision": "retain_isolation_new_database_required"})
		if status != 200 {
			test.Fatal("retain decision failed")
		}
	}
	if command == "open_session" {
		// An open cannot commit, so there is nothing to quarantine or retire. The
		// owned stop proof still has to join the worker, and the restarted platform
		// must not reconstruct a live unknown from the terminal record.
		if len(listing.Operations) != 1 || listing.Operations[0].State != "terminal" || listing.Operations[0].BusinessOutcome != "unknown" ||
			listing.Operations[0].CleanupOutcome != "unknown" || listing.Operations[0].MayHaveCommitted || listing.Operations[0].ExecutionProven {
			test.Fatalf("non-committing open lost its durable form: %+v", listing.Operations)
		}
		if status, _ := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations/retire-local", "no_quarantine", map[string]any{"database": fixture.database}); status != 409 {
			test.Fatalf("retirement acted on a non-committing open: %d", status)
		}
		// Release is idempotent when nothing is quarantined: it returns the same
		// terminal, non-committing record instead of inventing an isolation.
		status, released := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/isolation/release", "no_quarantine", map[string]any{"database": fixture.database})
		if status != 200 || !bytes.Contains(released["result"], []byte(`"state":"terminal"`)) || !bytes.Contains(released["result"], []byte(`"may_have_committed":false`)) {
			test.Fatalf("release rewrote a non-committing open: %d %s", status, released["result"])
		}
		var opened struct {
			Session string `json:"session"`
		}
		status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/open", "after_open_unknown", map[string]any{"database": fixture.database})
		if status != 200 || json.Unmarshal(reply["result"], &opened) != nil || opened.Session == "" {
			test.Fatalf("open unknown isolated the namespace: %d %s", status, reply["result"])
		}
		if status, _ := unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/close", "after_open_unknown_close", map[string]any{"session": opened.Session}); status != 200 {
			test.Fatal("admitted session did not close")
		}
		stop := exec.Command(python, "-B", launcher, "--run-dir", directory, "--stop-worker", worker)
		stop.Env = p6VerifyEnvironment(nil)
		if output, err := stop.CombinedOutput(); err != nil {
			test.Fatalf("actual launcher stop+Wait failed: %v\n%s", err, output)
		}
		_ = project.command.Process.Signal(syscall.SIGTERM)
		select {
		case <-project.finished:
		case <-time.After(50 * time.Second):
			test.Fatal("launcher shutdown did not join owned stack")
		}
		fixture.startPlatform()
		status, read = unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations", "restart_read", map[string]any{"database": fixture.database})
		if status != 200 || !bytes.Contains(read["result"], []byte(`"state":"terminal"`)) || !bytes.Contains(read["result"], []byte(`"may_have_committed":false`)) {
			test.Fatal("CLI restart rewrote the non-committing open")
		}
		metrics := unknownNativeMetrics(test, fixture)
		if string(metrics["unknown_held_permits"]) != "0" {
			test.Fatalf("CLI restart reconstructed a non-committing open: %s", metrics["unknown_held_permits"])
		}
		fixture.platform.stop(test, false)
		fixture.config.OwnedStopProof.Generation = p6VerifyToken(test)
		p6VerifyPrivate(test, fixture.configPath, fixture.config)
		rebound := exec.Command(fixture.binary, "-config", fixture.configPath)
		rebound.Env = p6VerifyEnvironment(nil)
		if output, err := rebound.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("cannot be rebound")) {
			test.Fatal("actual CLI rebound old state to new generation")
		}
		test.Log("OWNED_STOP_CLI case=open_session native_ack=1 dropped_response=1 may_have_committed=0 owned_wait=1 namespace_isolated=0 retired_quota_not_rebuilt=1")
		return
	}
	retire := func(requestID string, accepted bool) {
		test.Helper()
		status, reply := unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations/retire-local", requestID, map[string]any{"database": fixture.database})
		if (status == 200) != accepted {
			test.Fatalf("unexpected CLI retirement: %s status=%d", requestID, status)
		}
		if accepted && (!bytes.Contains(reply["result"], []byte(`"local_resources_retired":true`)) || !bytes.Contains(reply["result"], []byte(`"state":"quarantined"`))) {
			test.Fatal("retirement claimed recovery or omitted durable retirement")
		}
		metrics := unknownNativeMetrics(test, fixture)
		var held int
		if json.Unmarshal(metrics["unknown_held_permits"], &held) != nil || (held == 0) != accepted {
			test.Fatal("retirement recycled unproven resources")
		}
	}
	retire("live_missing_proof", false)
	status, _ = unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations/retire-local", "no_caller_proof", map[string]any{"database": fixture.database, "proof": "/tmp/arbitrary", "pid": os.Getpid()})
	if status != 400 {
		test.Fatal("HTTP requester supplied proof path or PID")
	}
	config := *fixture.config.OwnedStopProof
	identityEncoded, err := privateBytes(filepath.Join(config.Directory, worker+".identity.json"), 8192)
	var signed ownedSignedProof
	var identity ownedProcessIdentity
	if err != nil || json.Unmarshal(identityEncoded, &signed) != nil || json.Unmarshal([]byte(signed.Payload), &identity) != nil {
		test.Fatal("launcher owned incarnation unavailable")
	}
	proof := identity
	proof.Waited = true
	writeProof := func(forged bool) {
		test.Helper()
		payload, err := json.Marshal(proof)
		if err != nil {
			test.Fatal(err)
		}
		key, err := os.ReadFile(config.KeyFile)
		if err != nil {
			test.Fatal(err)
		}
		signer := hmac.New(sha256.New, key)
		signer.Write(payload)
		mac := hex.EncodeToString(signer.Sum(nil))
		if forged {
			mac = p6VerifyToken(test)
		}
		p6VerifyPrivate(test, filepath.Join(config.Directory, worker+".stop.json"), ownedSignedProof{Payload: string(payload), MAC: mac})
	}
	writeProof(true)
	retire("forged_proof", false)
	proof.Generation = p6VerifyToken(test)
	writeProof(false)
	retire("wrong_generation", false)
	if err := os.Remove(filepath.Join(config.Directory, worker+".stop.json")); err != nil {
		test.Fatal(err)
	}
	stop := exec.Command(python, "-B", launcher, "--run-dir", directory, "--stop-worker", worker)
	stop.Env = p6VerifyEnvironment(nil)
	if output, err := stop.CombinedOutput(); err != nil {
		test.Fatalf("actual launcher stop+Wait failed: %v\n%s", err, output)
	}
	retire("after_owned_wait", true)
	proxy.mutex.Lock()
	if len(proxy.counts) != len(before) {
		proxy.mutex.Unlock()
		test.Fatal("retirement dispatched new native commands")
	}
	for command, count := range before {
		if proxy.counts[command] != count {
			proxy.mutex.Unlock()
			test.Fatal("retirement opened, closed or replayed native work")
		}
	}
	proxy.mutex.Unlock()
	_ = project.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-project.finished:
	case <-time.After(50 * time.Second):
		test.Fatal("launcher shutdown did not join owned stack")
	}
	fixture.startPlatform()
	status, read = unknownNativeCall(test, fixture, fixture.credential, "/v1/databases/operations", "restart_read", map[string]any{"database": fixture.database})
	if status != 200 || !bytes.Contains(read["result"], []byte(`"local_resources_retired":true`)) || !bytes.Contains(read["result"], []byte(`"state":"quarantined"`)) {
		test.Fatal("CLI restart forgot durable local retirement or namespace isolation")
	}
	metrics := unknownNativeMetrics(test, fixture)
	if string(metrics["unknown_held_permits"]) != "0" {
		test.Fatal("CLI restart reconstructed already retired quota")
	}
	status, _ = unknownNativeCall(test, fixture, fixture.credential, "/v1/sql/open", "restart_no_open", map[string]any{"database": fixture.database})
	if status == 200 {
		test.Fatal("retired namespace reopened")
	}
	fixture.platform.stop(test, false)
	fixture.config.OwnedStopProof.Generation = p6VerifyToken(test)
	p6VerifyPrivate(test, fixture.configPath, fixture.config)
	rebound := exec.Command(fixture.binary, "-config", fixture.configPath)
	rebound.Env = p6VerifyEnvironment(nil)
	if output, err := rebound.CombinedOutput(); err == nil || !bytes.Contains(output, []byte("cannot be rebound")) {
		test.Fatal("actual CLI rebound old state to new generation")
	}
	test.Log("OWNED_STOP_CLI native_ack=1 dropped_response=1 live_rejected=1 forged_rejected=1 wrong_generation_rejected=1 owned_wait=1 local_retired=1 restart_sealed=1 retired_quota_not_rebuilt=1 native_replays=0 blind_close=0 new_sessions=0")
}
