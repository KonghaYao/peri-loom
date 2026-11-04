package platform

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// prefetchProbeWriter reports whether the next page's fetch had already been
// dispatched when a page reached the client. The adapter writes one page per
// Write, so the sentinel page marks the point where the worker would otherwise
// idle through the transcode and flush of the current page.
type prefetchProbeWriter struct {
	*httptest.ResponseRecorder
	sentinel     []byte
	fetchEntered chan struct{}
	arrived      chan struct{}
	once         sync.Once
	missingFetch bool
}

func newPrefetchProbe(sentinel string) *prefetchProbeWriter {
	return &prefetchProbeWriter{
		ResponseRecorder: httptest.NewRecorder(),
		sentinel:         []byte(sentinel),
		fetchEntered:     make(chan struct{}, 4),
		arrived:          make(chan struct{}),
	}
}

func (writer *prefetchProbeWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, writer.sentinel) {
		writer.once.Do(func() {
			select {
			case <-writer.fetchEntered:
			case <-time.After(time.Second):
				writer.missingFetch = true
			}
			close(writer.arrived)
		})
	}
	return writer.ResponseRecorder.Write(p)
}

func pagedCursorHook(firstPage, lastPage string, fetches *int, entered chan<- struct{}) func(context.Context, controller.Worker, map[string]any) (any, error) {
	return func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": firstCursorColumns(), "is_readonly": true, "is_explain": false}, nil
		case "open_read_cursor_first":
			return map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{firstPage}}, "done": false}, nil
		case "fetch_read_cursor":
			*fetches++
			entered <- struct{}{}
			return map[string]any{"rows": [][]any{{lastPage}}, "done": true}, nil
		case "close_read_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	}
}

func servePagedCursor(handler http.Handler, fixture *gatewayFixture, sql string, writer http.ResponseWriter) {
	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"` + sql + `","args":[],"named_args":[],"want_rows":true}}]}}`
	request := httptest.NewRequest(http.MethodPost, hranaV3JSONPath+"/cursor", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(writer, request)
}

// TestHranaV3JSONReadCursorFetchesTheNextPageWhileWritingTheCurrent pins the
// order that makes the page pipeline real: the fetch for page two must already
// be inside the worker dispatch when page one reaches the client. A synchronous
// fetch (the previous shape) is issued only after that page was written, so the
// probe records a missing fetch instead.
func TestHranaV3JSONReadCursorFetchesTheNextPageWhileWritingTheCurrent(t *testing.T) {
	fixture := newGatewayFixture(t)
	fetches := 0
	probe := newPrefetchProbe("page-one")
	fixture.upstream.setHook(pagedCursorHook("page-one", "page-two", &fetches, probe.fetchEntered))
	handler := newHranaHTTP(fixture.gateway)
	done := make(chan struct{})
	go func() {
		defer close(done)
		servePagedCursor(handler, fixture, "SELECT txt FROM messages", probe)
	}()
	select {
	case <-probe.arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("first page never reached the client")
	}
	if probe.missingFetch {
		t.Fatal("next page fetch was not in flight while the current page was written")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cursor stream did not finish")
	}
	stream := probe.ResponseRecorder.Body.String()
	first, second := strings.Index(stream, "page-one"), strings.Index(stream, "page-two")
	if probe.ResponseRecorder.Code != http.StatusOK || first < 0 || second < first {
		t.Fatalf("cursor stream incomplete: %d %s", probe.ResponseRecorder.Code, stream)
	}
	if fetches != 1 {
		t.Fatalf("fetch count = %d, want one fetch for the second page", fetches)
	}
}

// TestHranaV3JSONWriteCursorKeepsPageFetchesSequential proves the pipeline stays
// off the write path, where a page carries the settled write accounting.
func TestHranaV3JSONWriteCursorKeepsPageFetchesSequential(t *testing.T) {
	fixture := newGatewayFixture(t)
	fetches := 0
	probe := newPrefetchProbe("write-one")
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": firstCursorColumns(), "is_readonly": false, "is_explain": false}, nil
		case "open_write_cursor_first":
			return map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{"write-one"}}, "done": false, "affected_row_count": nil, "last_insert_rowid": nil}, nil
		case "fetch_write_cursor":
			fetches++
			probe.fetchEntered <- struct{}{}
			return map[string]any{"rows": [][]any{{"write-two"}}, "done": true, "affected_row_count": 1, "last_insert_rowid": 1}, nil
		case "close_write_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	handler := newHranaHTTP(fixture.gateway)
	done := make(chan struct{})
	go func() {
		defer close(done)
		servePagedCursor(handler, fixture, "UPDATE messages SET txt = 'x' RETURNING txt", probe)
	}()
	select {
	case <-probe.arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("write cursor first page never reached the client")
	}
	if !probe.missingFetch {
		t.Fatal("write cursor prefetched the next page")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("write cursor stream did not finish")
	}
	if fetches != 1 || probe.ResponseRecorder.Code != http.StatusOK {
		t.Fatalf("write cursor fetches = %d status = %d", fetches, probe.ResponseRecorder.Code)
	}
}

// TestHranaV3JSONReadCursorJoinsThePrefetchBeforeClosing covers the abandoned
// stream: the client disconnects while a prefetch is in flight, so the adapter
// must join it before the cleanup close, which the worker session serializes
// against the in-flight RPC.
func TestHranaV3JSONReadCursorJoinsThePrefetchBeforeClosing(t *testing.T) {
	fixture := newGatewayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": firstCursorColumns(), "is_readonly": true, "is_explain": false}, nil
		case "open_read_cursor_first":
			return map[string]any{"cols": firstCursorColumns(), "rows": [][]any{{"page-one"}}, "done": false}, nil
		case "fetch_read_cursor":
			// The client goes away while this prefetched fetch is in flight; the
			// fetch itself keeps running to completion.
			cancel()
			time.Sleep(20 * time.Millisecond)
			return map[string]any{"rows": [][]any{{"page-two"}}, "done": true}, nil
		case "close_read_cursor":
			return map[string]bool{"closed": true}, nil
		}
		return nil, nil
	})
	listen := newHranaHTTP(fixture.gateway)
	handler := listen.(*hranaHTTP)
	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"SELECT txt FROM messages","args":[],"named_args":[],"want_rows":true}}]}}`
	request := httptest.NewRequest(http.MethodPost, hranaV3JSONPath+"/cursor", strings.NewReader(body)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, request)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("abandoned cursor stream hung while joining the prefetch")
	}
	if len(handler.streams) != 0 {
		t.Fatal("abandoned stream stayed registered")
	}
}
