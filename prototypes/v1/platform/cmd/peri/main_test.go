package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const commandCredential = "synthetic-command-credential_____________"

func commandReply(writer http.ResponseWriter, requestID string, result any) {
	json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": requestID, "ok": true, "result": result, "error": nil})
}

func TestCommandPrivateCredentialFilesAndNoCredentialArgument(test *testing.T) {
	path := filepath.Join(test.TempDir(), "credential")
	if err := os.WriteFile(path, []byte(commandCredential), 0600); err != nil {
		test.Fatal(err)
	}
	if value, err := readCredential(path); err != nil || value != commandCredential {
		test.Fatal("private credential rejected")
	}
	if err := os.Chmod(path, 0644); err != nil {
		test.Fatal(err)
	}
	if _, err := readCredential(path); err == nil {
		test.Fatal("public credential accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		test.Fatal(err)
	}
	link := path + "-link"
	if err := os.Symlink(path, link); err != nil {
		test.Fatal(err)
	}
	if _, err := readCredential(link); err == nil {
		test.Fatal("credential symlink accepted")
	}
	var output bytes.Buffer
	if err := run([]string{"status", "--token", commandCredential}, strings.NewReader(""), &output); err == nil {
		test.Fatal("credential command-line argument accepted")
	}
	if strings.Contains(output.String(), commandCredential) {
		test.Fatal("credential echoed")
	}
}

func TestSQLFlagsAreExplicitAndGCDefaultsToInspection(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		var body map[string]any
		json.NewDecoder(request.Body).Decode(&body)
		if request.Header.Get("Authorization") != "Bearer "+commandCredential {
			test.Error("credential absent")
		}
		switch request.URL.Path {
		case "/v1/sql/open":
			commandReply(writer, body["request_id"].(string), map[string]any{"session": "0123456789abcdef0123456789abcdef", "readonly": false})
		case "/v1/sql/query":
			commandReply(writer, body["request_id"].(string), [][]any{{1}})
		case "/v1/sql/close":
			commandReply(writer, body["request_id"].(string), map[string]bool{"closed": true})
		default:
			commandReply(writer, body["request_id"].(string), map[string]bool{"inspected": true})
		}
	}))
	defer server.Close()
	arguments := []string{"sql", "--endpoint", server.URL, "--database", "tenant/database/main", "--query", "INSERT INTO items VALUES(1) RETURNING id"}
	var output bytes.Buffer
	if err := run(arguments, strings.NewReader(""), &output); err != nil {
		test.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "[[1]]" || len(paths) != 3 {
		test.Fatalf("query wire failed: %s %v", output.String(), paths)
	}
	paths = nil
	if err := run([]string{"sql", "--endpoint", server.URL, "--database", "tenant/database/main"}, strings.NewReader(""), &output); err == nil || len(paths) != 0 {
		test.Fatal("SQL mode guessed")
	}
	if err := run([]string{"gc", "--endpoint", server.URL, "--database", "tenant/database/main"}, strings.NewReader(""), &output); err != nil {
		test.Fatal(err)
	}
	if paths[0] != "/v1/databases/gc-inspect" {
		test.Fatal("GC collected without explicit flag")
	}
}

func TestShellMigrationDoesNotReplayOrReconnectUntilExplicitCommand(test *testing.T) {
	test.Setenv("PERI_API_KEY", commandCredential)
	opens, queries, executes, closes := 0, 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		json.NewDecoder(request.Body).Decode(&body)
		requestID := body["request_id"].(string)
		switch request.URL.Path {
		case "/v1/sql/open":
			opens++
			commandReply(writer, requestID, map[string]any{"session": "0123456789abcdef0123456789abcdef", "readonly": false})
		case "/v1/sql/execute":
			executes++
			commandReply(writer, requestID, map[string]bool{"executed": true})
		case "/v1/sql/query":
			queries++
			if queries == 1 {
				writer.WriteHeader(409)
				json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": requestID, "ok": false, "result": nil, "error": map[string]string{"code": "session_moved"}})
			} else {
				commandReply(writer, requestID, [][]any{{7}})
			}
		case "/v1/sql/close":
			closes++
			commandReply(writer, requestID, map[string]bool{"closed": true})
		}
	}))
	defer server.Close()
	input := strings.NewReader(":execute BEGIN\n:query INSERT INTO items VALUES(1) RETURNING id\n:execute COMMIT\n:reconnect\n:query SELECT 7\n:quit\n")
	var output bytes.Buffer
	if err := run([]string{"shell", "--endpoint", server.URL, "--database", "tenant/database/main"}, input, &output); err != nil {
		test.Fatal(err)
	}
	if opens != 2 || queries != 2 || executes != 1 || !strings.Contains(output.String(), "session_moved") || !strings.Contains(output.String(), "no SQL or transaction replay") {
		test.Fatalf("shell replayed/reconnected: opens=%d queries=%d executes=%d output=%s", opens, queries, executes, output.String())
	}
	if closes != 1 {
		test.Fatalf("moved session closed or healthy close repeated: closes=%d", closes)
	}
}
