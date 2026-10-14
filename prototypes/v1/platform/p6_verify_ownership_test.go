package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func (fixture *p6VerifyFixture) placement() controller.Placement {
	fixture.test.Helper()
	placement := fixture.placementStatus()
	if placement.Grant == nil {
		fixture.test.Fatal("actual placement grant missing")
	}
	return placement
}

func (fixture *p6VerifyFixture) placementStatus() controller.Placement {
	fixture.test.Helper()
	reply := fixture.must(fixture.credential, "/v1/databases/status", map[string]any{"database": fixture.database})
	placement, err := p6VerifyDecodePlacementStatus(reply.Result)
	if err != nil {
		fixture.test.Fatalf("actual placement status invalid: %v", err)
	}
	return placement
}

func p6VerifyDecodePlacementStatus(payload json.RawMessage) (controller.Placement, error) {
	var status struct {
		Placement   *controller.Placement `json:"placement"`
		Worker      json.RawMessage       `json:"worker"`
		Refs        json.RawMessage       `json:"refs"`
		Diagnostics json.RawMessage       `json:"diagnostics"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&status) != nil || decoder.Decode(new(any)) != io.EOF || status.Placement == nil {
		return controller.Placement{}, errors.New("malformed placement status")
	}
	placement := *status.Placement
	switch placement.Phase {
	case "active":
		if placement.Grant == nil {
			return controller.Placement{}, errors.New("active placement grant missing")
		}
	case "grant_intent", "grant_pending", "acquire_pending", "renew_intent", "renew_pending", "renew_worker_pending", "migrate_drain", "suspend_drain", "revoke_pending", "suspended":
	default:
		return controller.Placement{}, errors.New("unknown placement phase")
	}
	return placement, nil
}

func TestP6VerifyPlacementStatusDecode(test *testing.T) {
	cases := []struct {
		name    string
		payload string
		valid   bool
	}{
		{"grant intent without grant", `{"placement":{"phase":"grant_intent"},"worker":{},"refs":{},"diagnostics":{}}`, true},
		{"grant pending without grant", `{"placement":{"phase":"grant_pending","grant":null}}`, true},
		{"active with grant", `{"placement":{"phase":"active","grant":{"binding":{"epoch":2,"worker_id":"worker-b"}}}}`, true},
		{"active without grant", `{"placement":{"phase":"active"}}`, false},
		{"active null grant", `{"placement":{"phase":"active","grant":null}}`, false},
		{"missing placement", `{}`, false},
		{"null placement", `{"placement":null}`, false},
		{"unknown phase", `{"placement":{"phase":"unexpected"}}`, false},
		{"unknown result field", `{"placement":{"phase":"grant_pending"},"unexpected":true}`, false},
		{"unknown placement field", `{"placement":{"phase":"grant_pending","unexpected":true}}`, false},
		{"invalid grant field type", `{"placement":{"phase":"active","grant":{"binding":{"epoch":"2"}}}}`, false},
		{"malformed response", `{"placement":`, false},
		{"trailing response", `{"placement":{"phase":"grant_pending"}} {}`, false},
	}
	for _, selected := range cases {
		test.Run(selected.name, func(test *testing.T) {
			_, err := p6VerifyDecodePlacementStatus(json.RawMessage(selected.payload))
			if (err == nil) != selected.valid {
				test.Fatalf("placement decode valid=%t error=%v", selected.valid, err)
			}
		})
	}
}

func (fixture *p6VerifyFixture) broker(token string, binding controller.Binding) (int, p6VerifyReply) {
	fixture.test.Helper()
	encoded, err := json.Marshal(map[string]any{"version": 1, "request_id": "verify-broker", "database": binding.Database, "binding": binding, "access_role": binding.Role})
	if err != nil {
		fixture.test.Fatal(err)
	}
	request, err := http.NewRequest("POST", "http://"+fixture.config.BrokerListen+"/internal/key", bytes.NewReader(encoded))
	if err != nil {
		fixture.test.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := fixture.client.Do(request)
	if err != nil {
		fixture.test.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 65536))
	if err != nil {
		fixture.test.Fatal(err)
	}
	var reply p6VerifyReply
	if json.Unmarshal(body, &reply) != nil {
		fixture.test.Fatal("invalid broker response")
	}
	clear(body)
	return response.StatusCode, reply
}

func TestP6VerifyEncryptedMigrationActualKillFailover(test *testing.T) {
	fixture := p6VerifyNew(test)
	initial := fixture.placement()
	target := "worker-b"
	if initial.DesiredWorker == target {
		target = "worker-a"
	}
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE proof(id INTEGER PRIMARY KEY); INSERT INTO proof VALUES(7)")
	fixture.sql(session, "execute", "BEGIN IMMEDIATE; INSERT INTO proof VALUES(999)")
	fixture.deny(fixture.credential, "/v1/databases/migrate", map[string]any{"database": fixture.database, "worker_id": target})
	draining := fixture.placement()
	if draining.Epoch != initial.Epoch || draining.Grant.Binding.WorkerID != initial.DesiredWorker {
		test.Fatal("failed transaction drain advanced epoch prematurely")
	}
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	fixture.must(fixture.credential, "/v1/databases/migrate", map[string]any{"database": fixture.database, "worker_id": target})
	migrated := fixture.placement()
	if migrated.DesiredWorker != target || migrated.Epoch <= initial.Epoch {
		test.Fatal("graceful migration did not advance ownership")
	}
	status, denied := fixture.broker(fixture.config.WorkerKeyTokens[initial.DesiredWorker], initial.Grant.Binding)
	if status != 409 || denied.OK || denied.Error.Code != "LEASE_INVALID" {
		test.Fatalf("old broker binding status=%d code=%s", status, denied.Error.Code)
	}
	status, denied = fixture.broker(fixture.credential, migrated.Grant.Binding)
	if status != 401 || denied.OK {
		test.Fatal("project API key accessed privileged key broker")
	}
	recovered := fixture.open(fixture.credential, fixture.database)
	rows := fixture.sql(recovered, "query", "SELECT id FROM proof ORDER BY id")
	if string(rows.Result) != "[[7]]" {
		test.Fatalf("graceful recovery rows=%s", rows.Result)
	}
	fixture.sql(recovered, "execute", "INSERT INTO proof VALUES(8)")
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": recovered})
	worker := fixture.workers[target]
	worker.stop(test, false)
	var exit *exec.ExitError
	if !errors.As(worker.err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		test.Fatal("worker did not actually terminate by SIGKILL")
	}
	cache := "cache-a"
	if target == "worker-b" {
		cache = "cache-b"
	}
	if err := os.RemoveAll(filepath.Join(fixture.directory, cache)); err != nil {
		test.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var replacement controller.Placement
	for time.Now().Before(deadline) {
		replacement = fixture.placementStatus()
		if replacement.Phase == "active" && replacement.DesiredWorker != target && replacement.Epoch > migrated.Epoch {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if replacement.Phase != "active" || replacement.DesiredWorker == target || replacement.Epoch <= migrated.Epoch {
		test.Fatalf("actual automatic failover phase=%s worker=%s epoch=%d", replacement.Phase, replacement.DesiredWorker, replacement.Epoch)
	}
	if replacement.Database != fixture.database || replacement.Grant == nil || replacement.Grant.Binding.Database != fixture.database || replacement.Grant.Binding.Epoch != replacement.Epoch || replacement.Grant.Binding.WorkerID != replacement.DesiredWorker {
		test.Fatal("actual automatic failover grant binding mismatch")
	}
	fresh := fixture.open(fixture.credential, fixture.database)
	rows = fixture.sql(fresh, "query", "SELECT id FROM proof ORDER BY id")
	if string(rows.Result) != "[[7],[8]]" {
		test.Fatalf("post-kill acknowledged rows=%s", rows.Result)
	}
	if integrity := fixture.sql(fresh, "query", "PRAGMA integrity_check"); string(integrity.Result) != `[["ok"]]` {
		test.Fatal("post-kill native integrity failure")
	}
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": fresh})
	test.Logf("actual encrypted graceful epoch %d->%d; SIGKILL+whole-cache-delete automatic epoch%d ACK7/8 integrity", initial.Epoch, migrated.Epoch, replacement.Epoch)
}
