package platform

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHranaSessionSeatsOutlivePermitsAndIdleExpiryReturnsThem(t *testing.T) {
	fixture := newGatewayFixture(t)
	handler := newHranaHTTP(fixture.gateway).(*hranaHTTP)
	pipeline := func() (string, *httptest.ResponseRecorder) {
		request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`))
		request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var result struct {
			Baton string `json:"baton"`
		}
		_ = json.Unmarshal(response.Body.Bytes(), &result)
		return result.Baton, response
	}
	seats := fixture.state.state.Projects["project-a"].Quotas.MaxSessions
	batons := make([]string, 0, seats)
	for index := 0; index < seats; index++ {
		baton, response := pipeline()
		if response.Code != http.StatusOK || baton == "" {
			t.Fatalf("open %d: %d %s", index, response.Code, response.Body.String())
		}
		batons = append(batons, baton)
	}
	_, refused := pipeline()
	if refused.Code != http.StatusTooManyRequests ||
		!strings.Contains(refused.Body.String(), `"code":"quota_exceeded"`) ||
		!strings.Contains(refused.Body.String(), `"partial_execution_possible":false`) ||
		!strings.Contains(refused.Body.String(), `"retry_allowed":false`) {
		t.Fatalf("full seats did not refuse before dispatch: %d %s", refused.Code, refused.Body.String())
	}
	metrics := fixture.gateway.Metrics()
	if metrics["active_permits"] != 0 {
		t.Fatalf("idle streams hold concurrency permits: %v", metrics["active_permits"])
	}
	if metrics["session_seats"] != seats {
		t.Fatalf("session_seats = %v, want %d", metrics["session_seats"], seats)
	}
	projects, _ := metrics["projects"].(map[string]any)
	project, _ := projects["project-a"].(map[string]any)
	if project["sessions"] != seats {
		t.Fatalf("project sessions = %v, want %d", project["sessions"], seats)
	}
	reasons, _ := metrics["rejection_reasons_total"].(map[string]uint64)
	if reasons[string(rejectionSessionLimit)] != 1 {
		t.Fatalf("session refusal not named at its site: %v", reasons)
	}
	for index, baton := range batons {
		handler.mu.Lock()
		stream := handler.streams[baton]
		handler.mu.Unlock()
		if stream == nil {
			t.Fatalf("stream %s vanished before its idle timer", baton)
		}
		stream.mu.Lock()
		if stream.timer == nil {
			stream.mu.Unlock()
			t.Fatalf("stream %s has no idle timer", baton)
		}
		stream.expiresAt = time.Now().Add(-time.Second)
		stream.timer.Reset(time.Millisecond)
		stream.mu.Unlock()
		deadline := time.Now().Add(5 * time.Second)
		for fixture.upstream.count("close_session") < index+1 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if closed := fixture.upstream.count("close_session"); closed != index+1 {
			t.Fatalf("idle expiry closed %d sessions, want %d", closed, index+1)
		}
	}
	if seats := fixture.gateway.admission.sessionSeats(); seats != 0 {
		t.Fatalf("idle expiry kept %d session seats", seats)
	}
	if baton, response := pipeline(); response.Code != http.StatusOK || baton == "" {
		t.Fatalf("released seat did not admit a new stream: %d %s", response.Code, response.Body.String())
	}
}

func TestHranaCleanupUsesSQLRequestBudget(t *testing.T) {
	for _, testCase := range []struct {
		requestTimeout time.Duration
		want           time.Duration
	}{
		{requestTimeout: 0, want: 20 * time.Second},
		{requestTimeout: 10 * time.Second, want: 20 * time.Second},
		{requestTimeout: 30 * time.Second, want: 30 * time.Second},
		{requestTimeout: 180 * time.Second, want: 180 * time.Second},
	} {
		if got := hranaCleanupTimeout(testCase.requestTimeout); got != testCase.want {
			t.Errorf("request timeout %v: cleanup timeout = %v, want %v", testCase.requestTimeout, got, testCase.want)
		}
	}
}
