package platform

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type delayedHranaWriter struct {
	*httptest.ResponseRecorder
	delay time.Duration
}

func (writer *delayedHranaWriter) Write(bytes []byte) (int, error) {
	time.Sleep(writer.delay)
	return writer.ResponseRecorder.Write(bytes)
}

func TestHranaHTTPBoundaryIncludesOuterWrite(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	observer.httpBoundary = true
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(hranaTraceWrites))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-ID", "outer-boundary")
	writer := &delayedHranaWriter{httptest.NewRecorder(), 10 * time.Millisecond}
	newHranaHTTP(fixture.gateway).ServeHTTP(writer, request)
	if writer.Code != http.StatusOK || strings.Contains(writer.Body.String(), `"type":"error"`) {
		test.Fatalf("pipeline failed: %d", writer.Code)
	}
	records := requestPhaseRecords(test, observer)
	if len(records) != 5 {
		test.Fatalf("got %d records, want four inner and one envelope", len(records))
	}
	outer := records[len(records)-1]
	if outer.Scope != "hrana_http" || outer.RequestID != "outer-boundary" || outer.OuterRequestID != outer.RequestID || outer.Action != "hrana_http" || outer.Status != 0 || outer.Outcome != "handler_returned" || outer.HranaStep != 0 {
		test.Fatalf("invalid envelope: %+v", outer)
	}
	if outer.StartedUnixNS <= 0 || outer.FinishedUnixNS < outer.StartedUnixNS || outer.TotalNS < uint64(writer.delay) {
		test.Fatalf("missing outer timing: %+v", outer)
	}
	var innerNS uint64
	for _, inner := range records[:len(records)-1] {
		if inner.Scope != "" || inner.OuterRequestID != outer.RequestID || inner.StartedUnixNS < outer.StartedUnixNS || inner.FinishedUnixNS > outer.FinishedUnixNS {
			test.Fatalf("inner request escaped the HTTP envelope: %+v", inner)
		}
		innerNS += inner.TotalNS
	}
	if outer.TotalNS < innerNS+uint64(writer.delay) {
		test.Fatalf("outer writer delay disappeared: outer=%d inner=%d delay=%d", outer.TotalNS, innerNS, writer.delay)
	}
	if outer.AccountedNS != 0 || outer.UnaccountedNS != outer.TotalNS || len(outer.Spans) != 0 {
		test.Fatal("HTTP envelope pretended its time was an inner execution phase")
	}
}

func TestHranaHTTPBoundaryDefaultOffAndBounded(test *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", nil)
	for _, observer := range []*requestPhaseObserver{nil, newRequestPhaseObserver()} {
		if allocations := testing.AllocsPerRun(20, func() { observeHranaHTTPBoundary(request, observer)() }); allocations != 0 {
			test.Fatalf("disabled boundary allocated %f objects", allocations)
		}
	}
	observer := newRequestPhaseObserverWithConfig(requestPhaseConfig{HTTPBoundary: true, Capacity: 2})
	for range 3 {
		observeHranaHTTPBoundary(beginHranaCorrelation(request, observer), observer)()
	}
	if records := requestPhaseRecords(test, observer); len(records) != 2 || observer.stats().Overwritten != 1 {
		test.Fatal("HTTP envelopes bypassed the bounded observer")
	}
}

func TestHranaHTTPBoundaryPersistsWithoutRequestSecrets(test *testing.T) {
	fixture, observer := requestPhaseObserverFixtureAll(test)
	observer.httpBoundary = true
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	sink, err := phaseSink(context.Background(), observer, directory)
	if err != nil {
		test.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/secret-path?token=secret-query", strings.NewReader("secret-body"))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("X-Request-ID", "rejected header")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || !sink.stop() {
		test.Fatal("invalid request did not return or sink did not stop")
	}
	encoded, err := os.ReadFile(filepath.Join(directory, requestPhaseFileName))
	if err != nil {
		test.Fatal(err)
	}
	for _, secret := range []string{fixture.tokenA, "secret-path", "secret-query", "secret-body", "rejected header"} {
		if strings.Contains(string(encoded), secret) {
			test.Fatal("private boundary record contained unvalidated request content")
		}
	}
	lines := strings.Split(strings.TrimSpace(string(encoded)), "\n")
	if len(lines) != 2 {
		test.Fatalf("got %d lines, want envelope and footer", len(lines))
	}
	var record requestPhaseRecord
	if json.Unmarshal([]byte(lines[0]), &record) != nil || record.Scope != "hrana_http" || record.StartedUnixNS == 0 || record.FinishedUnixNS < record.StartedUnixNS || record.TotalNS == 0 {
		test.Fatal("sink lost boundary timing")
	}
	var wire map[string]any
	if json.Unmarshal([]byte(lines[0]), &wire) != nil || wire["status"] != nil {
		test.Fatal("unwrapped writer acquired an invented HTTP status")
	}
}

func TestHranaHTTPBoundaryEnvironment(test *testing.T) {
	for _, value := range []string{"0", "1", "true", "", "2"} {
		test.Run(value, func(test *testing.T) {
			test.Setenv(requestPhaseHTTPEnvironment, value)
			config, err := requestPhaseConfigFromEnvironment()
			if value == "0" || value == "1" {
				if err != nil || config.HTTPBoundary != (value == "1") {
					test.Fatalf("invalid config: %+v %v", config, err)
				}
			} else if err == nil {
				test.Fatal("invalid observer setting silently accepted")
			}
		})
	}
}
