package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigurationRequiresPrivateFileAndDistinctSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	config := ServiceConfig{AuthorityURL: "http://127.0.0.1:9002", AuthorityToken: strings.Repeat("a", 32), APIToken: strings.Repeat("b", 32), Workers: []ConfiguredWorker{{Worker: Worker{ID: "worker-a", Address: "http://127.0.0.1:9003", Region: "local", Capacity: 32}, ControlToken: strings.Repeat("c", 32)}}}
	bytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadServiceConfig(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadServiceConfig(path); err == nil {
		t.Fatal("public credential file accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadServiceConfig(link); err == nil {
		t.Fatal("symlink credential file accepted")
	}
	config.Workers[0].ControlToken = config.AuthorityToken
	bytes, _ = json.Marshal(config)
	if err := os.WriteFile(path, bytes, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadServiceConfig(path); err == nil {
		t.Fatal("reused service credential accepted")
	}
}

func TestDatabaseIdentifierCannotAliasOrEscape(t *testing.T) {
	database := DatabaseID{Namespace: Namespace{Tenant: "tenant", Database: "db"}, Branch: "main"}
	if database.Validate() != nil || database.Key() != "tenant/db/main" {
		t.Fatal("valid identifier rejected")
	}
	for _, bad := range []string{"", "../main", "a/b", strings.Repeat("a", 129), "é"} {
		database.Branch = bad
		if database.Validate() == nil {
			t.Fatalf("invalid identifier accepted %q", bad)
		}
	}
}
