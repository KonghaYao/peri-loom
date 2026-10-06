package controller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestControllerCLIActualServicePlacementAndMigration(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	directory := test.TempDir()
	binary := filepath.Join(directory, "peri-controller")
	build := exec.Command("go", "build", "-o", binary, "./cmd/peri-controller")
	if output, err := build.CombinedOutput(); err != nil {
		test.Fatalf("build controller CLI: %v: %s", err, output)
	}
	configuration := ServiceConfig{AuthorityURL: cluster.authority.address, AuthorityToken: cluster.authorityToken, APIToken: p5VerifyToken(test)}
	for _, identity := range []string{"verify-a", "verify-b"} {
		worker := cluster.workers[identity]
		configuration.Workers = append(configuration.Workers, ConfiguredWorker{Worker: worker.worker, ControlToken: worker.controlToken})
	}
	encoded, err := json.Marshal(configuration)
	if err != nil {
		test.Fatal(err)
	}
	configPath := filepath.Join(directory, "controller.json")
	if err := os.WriteFile(configPath, encoded, 0600); err != nil {
		test.Fatal(err)
	}
	command := exec.Command(binary, "-config", configPath, "-state", filepath.Join(directory, "metadata"), "-listen", "127.0.0.1:0")
	command.Env = []string{}
	var diagnostics p5VerifyDiagnostics
	command.Stderr = &diagnostics
	stdout, err := command.StdoutPipe()
	if err != nil {
		test.Fatal(err)
	}
	if err := command.Start(); err != nil {
		test.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	stopped := false
	test.Cleanup(func() {
		if !stopped {
			_ = command.Process.Kill()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
			}
		}
	})
	ready := make(chan []byte, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- append([]byte{}, scanner.Bytes()...)
		}
		for scanner.Scan() {
		}
		close(ready)
	}()
	var announcement struct {
		Ready   bool   `json:"ready"`
		Address string `json:"address"`
		Service string `json:"service"`
	}
	select {
	case line := <-ready:
		if json.Unmarshal(line, &announcement) != nil || !announcement.Ready || announcement.Service != "controller" {
			test.Fatal("controller readiness invalid")
		}
	case <-time.After(8 * time.Second):
		test.Fatal("controller did not start")
	}
	database := DatabaseID{Namespace: Namespace{Tenant: "cli", Database: "service"}, Branch: "main"}
	var placement Placement
	cliManagementCall(test, announcement.Address, configuration.APIToken, "/v1/databases/place", map[string]any{"database": database, "region": "local", "role": "writer"}, &placement)
	if placement.Phase != "active" || placement.Epoch == 0 {
		test.Fatalf("CLI placement did not converge: %+v", placement)
	}
	initial := placement
	var opened struct {
		Session string `json:"session"`
	}
	worker := cluster.workers[placement.DesiredWorker]
	reply, status, err := p5VerifyRPC(worker.worker.Address, "/sql", worker.sqlToken, map[string]any{"command": "open_session", "database": database})
	if err != nil || status != 200 || !reply.OK || json.Unmarshal(reply.Result, &opened) != nil {
		test.Fatal("actual CLI owner SQL session failed")
	}
	reply, _, err = p5VerifyRPC(worker.worker.Address, "/sql", worker.sqlToken, map[string]any{"command": "execute", "session": opened.Session, "sql": "CREATE TABLE sample(value INTEGER PRIMARY KEY); INSERT INTO sample VALUES(7);"})
	if err != nil || !reply.OK {
		test.Fatal("actual CLI SQL acknowledgement failed")
	}
	reply, _, err = p5VerifyRPC(worker.worker.Address, "/sql", worker.sqlToken, map[string]any{"command": "close_session", "session": opened.Session})
	if err != nil || !reply.OK {
		test.Fatal("actual session cleanup failed")
	}
	target := "verify-a"
	if placement.DesiredWorker == target {
		target = "verify-b"
	}
	cliManagementCall(test, announcement.Address, configuration.APIToken, "/v1/databases/migrate", map[string]any{"database": database, "worker_id": target}, &placement)
	if placement.Phase != "active" || placement.DesiredWorker != target || placement.Epoch <= initial.Epoch {
		test.Fatal("CLI migration did not fence and converge")
	}
	worker = cluster.workers[target]
	reply, _, err = p5VerifyRPC(worker.worker.Address, "/sql", worker.sqlToken, map[string]any{"command": "open_session", "database": database})
	if err != nil || !reply.OK || json.Unmarshal(reply.Result, &opened) != nil {
		test.Fatal("migration replacement session failed")
	}
	reply, _, err = p5VerifyRPC(worker.worker.Address, "/sql", worker.sqlToken, map[string]any{"command": "query", "session": opened.Session, "sql": "SELECT value FROM sample;"})
	if err != nil || !reply.OK || string(reply.Result) != "[[7]]" {
		test.Fatalf("acknowledged rows missing after CLI migration: %s", reply.Result)
	}
	_, _, _ = p5VerifyRPC(worker.worker.Address, "/sql", worker.sqlToken, map[string]any{"command": "close_session", "session": opened.Session})
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		test.Fatal(err)
	}
	select {
	case err := <-finished:
		stopped = true
		if err != nil {
			test.Fatal("controller failed clean shutdown")
		}
	case <-time.After(8 * time.Second):
		test.Fatal("controller clean shutdown timeout")
	}
	store, err := OpenStore(filepath.Join(directory, "metadata"))
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	metadata, err := store.Snapshot()
	if err != nil || metadata.Epoch < placement.Epoch || metadata.Placements[database.Key()].DesiredWorker != target {
		test.Fatal("CLI metadata did not persist")
	}
	test.Logf("actual Go CLI+authority+two workers acknowledged row7, graceful epoch %d->%d migration, clean exit and metadata reopen", initial.Epoch, placement.Epoch)
}

func cliManagementCall(test *testing.T, address, token, path string, input, output any) {
	test.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+path, bytes.NewReader(encoded))
	if err != nil {
		test.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Transport: &http.Transport{Proxy: nil}}
	response, err := client.Do(request)
	if err != nil {
		test.Fatal("controller management transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		test.Fatalf("controller management %s rejected with %d", path, response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1024*1024))
	if err := decoder.Decode(output); err != nil {
		test.Fatal(err)
	}
}
