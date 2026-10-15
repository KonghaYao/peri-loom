package platform

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublishImageInvalidatesAlreadyLoadedWorker(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "peri-loom-import-mini-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	t.Setenv("TMPDIR", root)
	fixture := p6VerifyNew(t)
	if err := os.Chmod(fixture.directory, 0700); err != nil {
		t.Fatal(err)
	}
	session := fixture.open(fixture.credential, fixture.database)
	defer fixture.call(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	if got := fixture.sql(session, "query", "SELECT name FROM sqlite_schema WHERE name = 'imported'").Result; string(got) != "[]" {
		t.Fatalf("expected loaded empty worker before publish, got %s", got)
	}

	stageAndPublishImportedImage(t, fixture)

	oldStatus, old := fixture.call(fixture.credential, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT id FROM imported"})
	fresh := fixture.open(fixture.credential, fixture.database)
	defer fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": fresh})
	freshStatus, current := fixture.call(fixture.credential, "/v1/sql/query", map[string]any{"session": fresh, "sql": "SELECT id FROM imported"})
	schemaStatus, schema := fixture.call(fixture.credential, "/v1/databases/schema", map[string]any{"database": fixture.database})
	t.Logf("after acknowledged HEAD update: old status=%d result=%s code=%s; new status=%d result=%s code=%s; schema status=%d result=%s",
		oldStatus, old.Result, old.Error.Code, freshStatus, current.Result, current.Error.Code, schemaStatus, schema.Result)
	oldSessionSafe := (oldStatus == 200 && bytes.Equal(old.Result, []byte("[[7]]"))) ||
		(oldStatus == 403 && old.Error.Code == "invalid_session") ||
		(oldStatus == 409 && old.Error.Code == "session_moved")
	if !oldSessionSafe ||
		freshStatus != 200 || !bytes.Equal(current.Result, []byte("[[7]]")) ||
		schemaStatus != 200 || !strings.Contains(string(schema.Result), "imported") {
		t.Fatal("published table must be visible to existing worker sessions, new sessions, and schema inspection")
	}
}

func TestPublishedImageVisibleAfterFreshWorkerMigration(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "peri-loom-import-migrate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	t.Setenv("TMPDIR", root)
	fixture := p6VerifyNew(t)
	if err := os.Chmod(fixture.directory, 0700); err != nil {
		t.Fatal(err)
	}
	session := fixture.open(fixture.credential, fixture.database)
	if got := fixture.sql(session, "query", "SELECT name FROM sqlite_schema WHERE name = 'imported'").Result; string(got) != "[]" {
		t.Fatalf("expected loaded empty worker before publish, got %s", got)
	}
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	stageAndPublishImportedImage(t, fixture)
	currentWorker := fixture.placement().DesiredWorker
	nextWorker := "worker-a"
	if currentWorker == nextWorker {
		nextWorker = "worker-b"
	}
	fixture.must(fixture.credential, "/v1/databases/migrate", map[string]any{"database": fixture.database, "worker_id": nextWorker})
	migrated := fixture.placement()
	if migrated.Phase != "active" || migrated.DesiredWorker != nextWorker {
		t.Fatalf("published image was not migrated to %s: phase=%s worker=%s", nextWorker, migrated.Phase, migrated.DesiredWorker)
	}
	fresh := fixture.open(fixture.credential, fixture.database)
	defer fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": fresh})
	if got := fixture.sql(fresh, "query", "SELECT id FROM imported").Result; !bytes.Equal(got, []byte("[[7]]")) {
		t.Fatalf("fresh worker did not load published image: %s", got)
	}
}

func stageAndPublishImportedImage(t *testing.T, fixture *p6VerifyFixture) {
	t.Helper()
	image := filepath.Join(fixture.directory, "image.db")
	source := filepath.Join(fixture.directory, "source.db")
	command := exec.Command("sqlite3", "-cmd", ".filectrl reserve_bytes 48", source,
		"CREATE TABLE imported(id INTEGER PRIMARY KEY); INSERT INTO imported VALUES(7); VACUUM INTO '"+image+"';")
	command.Env = p6VerifyEnvironment(nil)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create SQLite image: %v: %s", err, output)
	}
	if err := os.Chmod(image, 0600); err != nil {
		t.Fatal(err)
	}

	placement := fixture.placement()
	binding := placement.Grant.Binding
	stage := filepath.Join(fixture.directory, "staged")
	privateFixture := filepath.Join(fixture.directory, "import.json")
	p6VerifyPrivate(t, privateFixture, map[string]any{
		"authority_endpoint": fixture.config.Controller.AuthorityURL,
		"authority_token":    fixture.config.WorkerAuthorityTokens[binding.WorkerID],
		"broker_endpoint":    "http://" + fixture.config.BrokerListen,
		"broker_token":       fixture.config.WorkerKeyTokens[binding.WorkerID],
		"binding":            binding,
		"epoch":              binding.Epoch,
		"source":             image,
		"destination":        stage,
	})
	for _, action := range []string{"stage-image", "publish-image"} {
		command := exec.Command(fixture.rust, action, privateFixture)
		command.Env = p6VerifyEnvironment(nil)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v: %s", action, err, output)
		}
		if action == "publish-image" {
			var acknowledgement struct {
				OK         bool   `json:"ok"`
				ManifestID string `json:"manifest_id"`
			}
			lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
			if err := json.Unmarshal(lines[len(lines)-1], &acknowledgement); err != nil || !acknowledgement.OK || acknowledgement.ManifestID == "" {
				t.Fatalf("publish HEAD acknowledgement invalid: %v: %s", err, output)
			}
		}
	}

}
