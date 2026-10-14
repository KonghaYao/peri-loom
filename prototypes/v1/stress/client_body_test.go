package stress

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSlowResponseBodyIsTimeoutAndUnknownWithoutRetry(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		_, _ = writer.Write([]byte("{"))
		writer.(http.Flusher).Flush()
		time.Sleep(30 * time.Millisecond)
	}))
	defer server.Close()
	settings := DefaultSettings()
	settings.RequestTimeout = 5 * time.Millisecond
	session := &Session{client: unitClient(t, server.URL, settings), ID: strings.Repeat("a", 32)}
	_, _, failure := session.SQL(context.Background(), true, "SELECT 1")
	if failure == nil || !failure.Unknown || !failure.TimedOut || failure.Class != "timeout" || calls.Load() != 1 {
		t.Fatal("body timeout mislabeled or retried")
	}
}

func TestUnknownLengthBodyIsStillBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.(http.Flusher).Flush()
		_, _ = writer.Write([]byte(strings.Repeat("x", 4096)))
	}))
	defer server.Close()
	settings := DefaultSettings()
	settings.MaxResponseBytes = 1024
	session := &Session{client: unitClient(t, server.URL, settings), ID: strings.Repeat("a", 32)}
	_, _, failure := session.SQL(context.Background(), true, "SELECT 1")
	if failure == nil || !failure.Unknown || failure.Code != "response_unavailable" {
		t.Fatal("chunked/unknown length escaped response cap")
	}
}

func TestRPCRecorderFailureBlocksAnyFollowingNativeDispatch(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var input wireRequest
		_ = json.NewDecoder(request.Body).Decode(&input)
		respond(writer, input.RequestID, [][]int{{1}}, "", 200)
	}))
	defer server.Close()
	artifacts, err := NewArtifacts(t.TempDir()+"/run", 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	client := unitClient(t, server.URL, DefaultSettings())
	client.artifacts = artifacts
	session := &Session{client: client, ID: strings.Repeat("a", 32)}
	_, _, first := session.SQL(context.Background(), true, "SELECT 1")
	if first != nil {
		t.Fatal("known first response was falsely discarded")
	}
	_, _, second := session.SQL(context.Background(), true, "SELECT 1")
	if second == nil || second.Class != "artifact_error" || calls.Load() != 1 {
		t.Fatal("evidence failure did not block following dispatch")
	}
}
