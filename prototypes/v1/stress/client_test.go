package stress

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "private_project_key_0123456789abcdef"

func unitClient(t *testing.T, endpoint string, settings Settings) *Client {
	t.Helper()
	client, err := NewClient(endpoint, testKey, "", settings, &IDs{Prefix: "unit"}, &DataBudget{Maximum: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func respond(writer http.ResponseWriter, identifier string, result any, code string, status int) {
	var failure any
	if code != "" {
		failure = map[string]any{"code": code}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"version": 1, "request_id": identifier, "ok": code == "", "result": result, "error": failure})
}

func TestPersistentOpaqueSessionAndFreshRequestIDs(t *testing.T) {
	var identifiers []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+testKey {
			t.Error("credential not confined to authorization header")
		}
		var input wireRequest
		_ = json.NewDecoder(request.Body).Decode(&input)
		identifiers = append(identifiers, input.RequestID)
		if request.URL.Path == "/v1/sql/open" {
			respond(writer, input.RequestID, map[string]any{"session": strings.Repeat("a", 32), "readonly": false}, "", 200)
			return
		}
		if input.Session != strings.Repeat("a", 32) {
			t.Error("opaque session was not reused")
		}
		if request.URL.Path == "/v1/sql/query" {
			respond(writer, input.RequestID, [][]int{{42}}, "", 200)
		} else {
			respond(writer, input.RequestID, map[string]bool{"closed": true}, "", 200)
		}
	}))
	defer server.Close()
	client := unitClient(t, server.URL, DefaultSettings())
	session, failure := client.Open(context.Background(), DatabaseID{})
	if failure != nil {
		t.Fatal(failure)
	}
	for index := 0; index < 2; index++ {
		rows, _, failure := session.SQL(context.Background(), true, "SELECT 42")
		if failure != nil || len(rows) != 1 {
			t.Fatal("query failed")
		}
	}
	if failure := session.Close(context.Background()); failure != nil {
		t.Fatal(failure)
	}
	seen := map[string]bool{}
	for _, identity := range identifiers {
		if seen[identity] || !Label(identity) {
			t.Fatal("request ID reused or invalid")
		}
		seen[identity] = true
	}
	if len(identifiers) != 4 {
		t.Fatal("unexpected additional HTTP attempt")
	}
}

func TestQueryTransportFailureIsUnknownAndNeverRetried(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		connection, _, _ := writer.(http.Hijacker).Hijack()
		_ = connection.Close()
	}))
	defer server.Close()
	session := &Session{client: unitClient(t, server.URL, DefaultSettings()), ID: strings.Repeat("a", 32)}
	_, _, failure := session.SQL(context.Background(), true, "SELECT 1")
	if failure == nil || !failure.Unknown || requests.Load() != 1 {
		t.Fatal("query failure was retried or declared known")
	}
	_, _, _ = session.SQL(context.Background(), true, "SELECT 1")
	if requests.Load() != 1 {
		t.Fatal("poisoned session sent more SQL")
	}
	if closeFailure := session.Close(context.Background()); closeFailure == nil || !closeFailure.Unknown || requests.Load() != 1 {
		t.Fatal("unknown session was blindly closed")
	}
}

func TestRedirectNeverReplaysSQL(t *testing.T) {
	var targetRequests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetRequests.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Location", target.URL)
		writer.WriteHeader(307)
	}))
	defer server.Close()
	session := &Session{client: unitClient(t, server.URL, DefaultSettings()), ID: strings.Repeat("a", 32)}
	_, _, failure := session.SQL(context.Background(), false, "INSERT INTO items VALUES(1)")
	if failure == nil || !failure.Unknown || targetRequests.Load() != 0 {
		t.Fatal("redirect replayed SQL or hid unresolved outcome")
	}
}

func TestBoundedResponseAndSQLIngress(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		writer.Header().Set("Content-Length", "67108864")
		writer.WriteHeader(200)
	}))
	defer server.Close()
	settings := DefaultSettings()
	settings.MaxResponseBytes = 1024
	session := &Session{client: unitClient(t, server.URL, settings), ID: strings.Repeat("a", 32)}
	_, _, failure := session.SQL(context.Background(), true, "SELECT 1")
	if failure == nil || !failure.Unknown || failure.Code != "response_budget" {
		t.Fatal("unbounded response accepted")
	}
	session = &Session{client: unitClient(t, server.URL, settings), ID: strings.Repeat("a", 32)}
	_, _, failure = session.SQL(context.Background(), true, strings.Repeat(" ", 65537))
	if failure == nil || failure.Class != "client_limit" || requests.Load() != 1 {
		t.Fatal("SQL body bound did not reject before HTTP")
	}
}

func TestActualAdmissionAndTimeoutClassificationWithoutRetries(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		var input wireRequest
		_ = json.NewDecoder(request.Body).Decode(&input)
		respond(writer, input.RequestID, nil, "quota_exceeded", 429)
	}))
	defer server.Close()
	session := &Session{client: unitClient(t, server.URL, DefaultSettings()), ID: strings.Repeat("a", 32)}
	_, _, failure := session.SQL(context.Background(), true, "SELECT 1")
	if failure == nil || failure.Class != "admission" || failure.Unknown || session.poisoned || requests.Load() != 1 {
		t.Fatal("admission rejection misclassified")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { time.Sleep(30 * time.Millisecond) }))
	defer slow.Close()
	settings := DefaultSettings()
	settings.RequestTimeout = 5 * time.Millisecond
	session = &Session{client: unitClient(t, slow.URL, settings), ID: strings.Repeat("b", 32)}
	_, _, failure = session.SQL(context.Background(), true, "SELECT 2")
	if failure == nil || !failure.TimedOut || !failure.Unknown || failure.Class != "timeout" {
		t.Fatal("timeout did not preserve unknown outcome")
	}
}

func TestTLSVerifiesPeerAndAllowsConfiguredCAOnly(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		respond(writer, "", map[string]bool{"actual": true}, "", 200)
	}))
	defer server.Close()
	client := unitClient(t, server.URL, DefaultSettings())
	if _, failure := client.Metrics(context.Background()); failure == nil || requests.Load() != 0 {
		t.Fatal("untrusted TLS peer accepted")
	}
	certificate, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	client, err = NewClient(server.URL, testKey, path, DefaultSettings(), &IDs{Prefix: "ca"}, &DataBudget{Maximum: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, failure := client.Metrics(context.Background()); failure != nil || requests.Load() != 1 {
		t.Fatal("explicit CA verification failed")
	}
}

func TestConcurrentRequestIDsAndBudgetAdmissionBound(t *testing.T) {
	ids := &IDs{Prefix: "unit"}
	budget := &DataBudget{Maximum: 64}
	identities := make(chan string, 128)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for count := 0; count < 16; count++ {
				identities <- ids.Next()
				budget.Charge(1)
			}
		}()
	}
	workers.Wait()
	close(identities)
	seen := map[string]bool{}
	for identity := range identities {
		if seen[identity] {
			t.Fatal("request ID collision")
		}
		seen[identity] = true
	}
	if len(seen) != 128 || budget.used.Load() != 64 || budget.Charge(1) {
		t.Fatal("concurrent budget admission escaped ceiling")
	}
}

func TestMalformedResponseCannotFabricateSuccessOrLeakCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		fmt.Fprintf(writer, `{"version":1,"request_id":"wrong","ok":true,"result":[["%s"]],"error":null}`, testKey)
	}))
	defer server.Close()
	session := &Session{client: unitClient(t, server.URL, DefaultSettings()), ID: strings.Repeat("a", 32)}
	_, _, failure := session.SQL(context.Background(), true, "SELECT 1")
	if failure == nil || !failure.Unknown || strings.Contains(failure.Error(), testKey) {
		t.Fatal("unmatched ID accepted or credential leaked")
	}
	classified := classify(500, testKey)
	if strings.Contains(classified.Error(), testKey) || !classified.Unknown {
		t.Fatal("untrusted error code leaked credential")
	}
}
