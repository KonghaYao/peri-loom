package faultprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestStarvationUnknownManagementStopsWithoutRollbackOrRetry(t *testing.T) {
	config, connection, _, _, log := unitFixture(t)
	config.Case = "metadata_starvation"
	for index := 1; index < 3; index++ {
		database := connection.Database
		database.Namespace.Database = fmt.Sprintf("%s-%d", database.Namespace.Database, index)
		connection.Databases = append(connection.Databases, database)
	}
	var state sync.Mutex
	var sessions []DatabaseID
	writes := map[string]int{}
	prefixes := map[string]string{}
	requests := map[string]bool{}
	migrations, rollbacks, closes := 0, 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		state.Lock()
		defer state.Unlock()
		var input testRequest
		if json.NewDecoder(request.Body).Decode(&input) != nil {
			t.Error("invalid request")
		}
		if requests[input.RequestID] {
			t.Error("request replay")
		}
		requests[input.RequestID] = true
		switch request.URL.Path {
		case "/v1/databases/status":
			wireReply(writer, input, grantSnapshot(input.Database, 1, "worker-a"), "", 200)
		case "/v1/databases/migrate":
			migrations++
			wireReply(writer, input, nil, "operation_incomplete", 409)
		case "/v1/sql/open":
			sessions = append(sessions, input.Database)
			wireReply(writer, input, map[string]any{"session": fmt.Sprintf("%032x", len(sessions)), "readonly": false}, "", 200)
		case "/v1/sql/close":
			closes++
			wireReply(writer, input, map[string]any{"closed": true}, "", 200)
		case "/v1/sql/execute":
			if input.SQL == "ROLLBACK" {
				rollbacks++
			}
			if strings.HasPrefix(input.SQL, "CREATE TABLE") {
				prefixes[input.Session] = strings.TrimSuffix(strings.Fields(input.SQL)[2], "_account(id")
			}
			if strings.HasPrefix(input.SQL, "BEGIN IMMEDIATE") {
				writes[input.Session]++
			}
			wireReply(writer, input, map[string]any{"executed": true}, "", 200)
		case "/v1/sql/query":
			var rows [][]any
			if strings.Contains(input.SQL, "_ledger") {
				for sequence := 1; sequence <= writes[input.Session]; sequence++ {
					rows = append(rows, []any{sequence, fmt.Sprintf("%s_business_%04d", prefixes[input.Session], sequence), int64(sequence%7) - 3})
				}
			} else if strings.Contains(input.SQL, "_account") {
				balance := int64(100000)
				for sequence := 1; sequence <= writes[input.Session]; sequence++ {
					balance += int64(sequence%7) - 3
				}
				rows = [][]any{{1, balance}}
			} else {
				rows = [][]any{{"ok"}}
			}
			wireReply(writer, input, rows, "", 200)
		default:
			t.Error("unexpected API")
		}
	}))
	defer server.Close()
	connection.Endpoint = server.URL
	gateway, err := newClient(connection, connection.ProjectAPIKey, "sql", config, log)
	if err != nil {
		t.Fatal(err)
	}
	management, err := newClient(connection, connection.GlobalAPIKey, "management", config, log)
	if err != nil {
		t.Fatal(err)
	}
	failure := metadataStarvation(context.Background(), config, connection, gateway, management, "peri_stress_starvation", log)
	state.Lock()
	defer state.Unlock()
	if failure == nil || !failure.Unknown || migrations < 1 || migrations > 2 || rollbacks != 0 || closes != 0 {
		t.Fatalf("uncertain migration did not stop: %+v migrations=%d rollback=%d close=%d", failure, migrations, rollbacks, closes)
	}
}

func TestPlacementEpochRegressionAndDiagnosticsUnavailable(t *testing.T) {
	config, connection, _, _, log := unitFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var input testRequest
		_ = json.NewDecoder(request.Body).Decode(&input)
		wireReply(writer, input, grantSnapshot(input.Database, 8, "worker-b"), "", 200)
	}))
	defer server.Close()
	connection.Endpoint = server.URL
	management, err := newClient(connection, connection.GlobalAPIKey, "management", config, log)
	if err != nil {
		t.Fatal(err)
	}
	actual, failure := management.Status(context.Background(), connection.Database)
	if failure != nil || !writerPlacement(actual) {
		t.Fatalf("optional diagnostics blocked actual placement: %v", failure)
	}
	previous := actual
	previous.Epoch = 9
	if _, failure = awaitEpoch(context.Background(), management, connection.Database, previous, config.RecoveryTimeout); failure == nil || failure.Code != "epoch_regression" {
		t.Fatal("epoch regression accepted")
	}
}
