package sdk

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const testCredential = "synthetic-api-credential__________________"
const testSession = "0123456789abcdef0123456789abcdef"

func testDatabase() DatabaseID {
	return DatabaseID{Namespace: Namespace{Tenant: "tenant", Database: "database"}, Branch: "main"}
}

func writeReply(writer http.ResponseWriter, identifier string, result any) {
	writer.Header().Set("Content-Type", "application/json")
	json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": identifier, "ok": true, "result": result, "error": nil})
}

func TestClientNativeWireShapesAndLosslessIntegerRows(test *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Header.Get("Authorization") != "Bearer "+testCredential {
			test.Error("credential header mismatch")
		}
		var body map[string]json.RawMessage
		json.NewDecoder(request.Body).Decode(&body)
		var identifier string
		json.Unmarshal(body["request_id"], &identifier)
		switch request.URL.Path {
		case "/v1/sql/open":
			writeReply(writer, identifier, map[string]any{"session": testSession, "readonly": false})
		case "/v1/sql/query":
			writeReply(writer, identifier, [][]any{{json.Number("9223372036854775807"), "row", json.Number("-9007199254740993")}})
		case "/v1/sql/execute":
			writeReply(writer, identifier, map[string]bool{"executed": true})
		case "/v1/sql/close":
			writeReply(writer, identifier, map[string]bool{"closed": true})
		case "/v1/databases/snapshot":
			if _, found := body["name"]; !found {
				test.Error("snapshot name missing")
			}
			writeReply(writer, identifier, map[string]bool{"created": true})
		default:
			test.Error("unexpected endpoint")
			writer.WriteHeader(404)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.URL, testCredential, Options{})
	if err != nil {
		test.Fatal(err)
	}
	session, err := client.Open(context.Background(), testDatabase())
	if err != nil {
		test.Fatal(err)
	}
	if err := session.Execute(context.Background(), "BEGIN; INSERT INTO items VALUES(1); COMMIT", "execute-1"); err != nil {
		test.Fatal(err)
	}
	rows, err := session.Query(context.Background(), "SELECT value FROM items", "query-1")
	if err != nil || rows[0][0].(json.Number).String() != "9223372036854775807" {
		test.Fatalf("integer precision lost: %v %v", rows, err)
	}
	negative, exact := rows[0][2].(json.Number)
	if !exact || negative.String() != "-9007199254740993" {
		test.Fatalf("negative integer precision lost: %v", rows)
	}
	maximum, err := rows[0][0].(json.Number).Int64()
	if err != nil || maximum != int64(9223372036854775807) {
		test.Fatalf("explicit int64 conversion failed: %v %v", maximum, err)
	}
	negativeInteger, err := negative.Int64()
	if err != nil || negativeInteger != int64(-9007199254740993) {
		test.Fatalf("explicit negative int64 conversion failed: %v %v", negativeInteger, err)
	}
	encodedRows, err := json.Marshal(rows)
	if err != nil || string(encodedRows) != `[[9223372036854775807,"row",-9007199254740993]]` {
		test.Fatalf("integer wire literals changed: %s %v", encodedRows, err)
	}
	database := testDatabase()
	if _, err := client.Management(context.Background(), "snapshot", ManagementRequest{Database: &database, Name: "snap", RequestID: "snapshot-1"}); err != nil {
		test.Fatal(err)
	}
	if err := session.Close(context.Background()); err != nil {
		test.Fatal(err)
	}
	if requests.Load() != 5 {
		test.Fatalf("unexpected replay count %d", requests.Load())
	}
	for _, value := range []any{client, *client, session, *session} {
		if strings.Contains(fmt.Sprintf("%+v %#v", value, value), testCredential) {
			test.Fatal("SDK formatting exposed credential")
		}
	}
}

func TestExecuteAndWriteQueryLostACKNeverReplayOrRestoreSession(test *testing.T) {
	for _, action := range []string{"execute", "query"} {
		test.Run(action, func(test *testing.T) {
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var body map[string]any
				json.NewDecoder(request.Body).Decode(&body)
				if request.URL.Path == "/v1/sql/open" {
					writeReply(writer, body["request_id"].(string), map[string]any{"session": testSession, "readonly": false})
					return
				}
				writes.Add(1)
				connection, _, err := writer.(http.Hijacker).Hijack()
				if err != nil {
					test.Error(err)
					return
				}
				connection.Close()
			}))
			defer server.Close()
			client, _ := NewClient(server.URL, testCredential, Options{})
			session, err := client.Open(context.Background(), testDatabase())
			if err != nil {
				test.Fatal(err)
			}
			if action == "execute" {
				err = session.Execute(context.Background(), "INSERT INTO items VALUES(1)", "lost-ack")
			} else {
				_, err = session.Query(context.Background(), "INSERT INTO items VALUES(1) RETURNING id", "lost-ack")
			}
			var failure *Error
			if !errors.As(err, &failure) || !failure.OutcomeUnknown || failure.RequestID != "lost-ack" {
				test.Fatalf("uncertainty not typed: %v", err)
			}
			if err := session.Execute(context.Background(), "INSERT INTO items VALUES(2)", "no-replay"); err == nil {
				test.Fatal("poisoned session reused")
			}
			if writes.Load() != 1 {
				test.Fatalf("SQL replayed %d times", writes.Load())
			}
		})
	}
}

func TestSessionMovedRequiresExplicitNewSession(test *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var body map[string]any
		json.NewDecoder(request.Body).Decode(&body)
		if request.URL.Path == "/v1/sql/open" {
			writeReply(writer, body["request_id"].(string), map[string]any{"session": testSession, "readonly": false})
			return
		}
		writer.WriteHeader(409)
		json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": body["request_id"], "ok": false, "result": nil, "error": map[string]string{"code": "session_moved"}})
	}))
	defer server.Close()
	client, _ := NewClient(server.URL, testCredential, Options{})
	session, _ := client.Open(context.Background(), testDatabase())
	_, err := session.Query(context.Background(), "SELECT 1", "moved")
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "session_moved" || failure.OutcomeUnknown {
		test.Fatalf("migration error changed: %v", err)
	}
	if _, err := session.Query(context.Background(), "SELECT 1", "again"); err == nil || calls.Load() != 2 {
		test.Fatal("session was transparently restored")
	}
}

func TestTLSEndpointAndLimitsFailClosed(test *testing.T) {
	for _, endpoint := range []string{"http://localhost:8080", "http://example.com", "https://user:password@example.com", "https://example.com/?token=secret"} {
		if _, err := NewClient(endpoint, testCredential, Options{}); err == nil {
			test.Fatalf("insecure endpoint accepted: %s", endpoint)
		}
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		json.NewDecoder(request.Body).Decode(&body)
		writeReply(writer, body["request_id"].(string), map[string]any{"session": testSession, "readonly": true})
	}))
	defer server.Close()
	client, _ := NewClient(server.URL, testCredential, Options{})
	if _, err := client.Open(context.Background(), testDatabase()); err == nil {
		test.Fatal("untrusted TLS certificate accepted")
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client, _ = NewClient(server.URL, testCredential, Options{RootCAs: roots})
	if _, err := client.Open(context.Background(), testDatabase()); err != nil {
		test.Fatal(err)
	}
	client, _ = NewClient(server.URL, testCredential, Options{RootCAs: roots, MaxRequestBytes: 1})
	if _, err := client.Open(context.Background(), testDatabase()); err == nil {
		test.Fatal("oversized request dispatched")
	}
	client, _ = NewClient(server.URL, testCredential, Options{RootCAs: roots, MaxResponseBytes: 4})
	if _, err := client.Open(context.Background(), testDatabase()); err == nil {
		test.Fatal("oversized response accepted")
	}
}
