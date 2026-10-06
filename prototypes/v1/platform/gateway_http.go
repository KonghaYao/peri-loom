package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	controller "peri-loom/controller"
)

type gatewayRequest struct {
	Database  *controller.DatabaseID `json:"database,omitempty"`
	Session   string                 `json:"session,omitempty"`
	SQL       string                 `json:"sql,omitempty"`
	RequestID string                 `json:"request_id,omitempty"`
}

func gatewayAction(path string) string {
	switch path {
	case "/v1/sql/open", "/v1/sql/open/database":
		return "open"
	case "/v1/sql/execute":
		return "execute"
	case "/v1/sql/query":
		return "query"
	case "/v1/sql/close":
		return "close"
	default:
		return "invalid"
	}
}

func (gateway *Gateway) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	action := gatewayAction(request.URL.Path)
	identifier, err := controller.RandomID()
	if err != nil {
		http.Error(writer, "request unavailable", 503)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	_ = http.NewResponseController(writer).SetReadDeadline(time.Now().Add(10 * time.Second))
	_ = http.NewResponseController(writer).SetWriteDeadline(time.Now().Add(30 * time.Second))
	var principal Principal
	var permit *admissionPermit
	result := gatewayFailure(400, "invalid_request")
	defer func() {
		if result.finish != nil {
			defer result.finish()
		}
		if permit != nil {
			defer permit.finish()
		}
		outcome := result.code
		if outcome == "" {
			outcome = "ok"
		}
		event := AuditEvent{RequestID: identifier, Subject: principal.Subject, Project: principal.Project, Action: action, Outcome: outcome, TimestampMS: time.Now().UnixMilli()}
		if event.Subject == "" || event.Project == "" {
			event.Subject, event.Project = "unauthenticated", "unauthenticated"
		}
		if result.session != nil {
			event.Database = result.session.database.Key()
		}
		if auditErr := gateway.config.Audit.Record(event); auditErr != nil {
			if result.dispatched && action != "close" {
				result.code = "outcome_unknown"
				result.status = 502
				result.value = nil
				result.stream = nil
				if result.session != nil {
					result.session.poisoned = true
				}
				permit.retain()
			} else {
				result.code = "audit_unavailable"
				result.status = 503
				result.value = nil
				result.stream = nil
			}
		}
		gateway.counters.observe(action, result.code, time.Since(started))
		gateway.writeResponse(writer, identifier, result)
	}()
	if request.Method != "POST" || action == "invalid" || request.URL.RawQuery != "" {
		return
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		result = gatewayFailure(401, "unauthenticated")
		return
	}
	credential := strings.TrimPrefix(values[0], "Bearer ")
	principal, err = gateway.config.Identity.Authenticate(credential)
	if err != nil || principal.Subject == "" || principal.Project == "" || (principal.ExpiresMS > 0 && principal.ExpiresMS <= time.Now().UnixMilli()) {
		principal = Principal{}
		result = gatewayFailure(401, "unauthenticated")
		return
	}
	state, err := gateway.config.State.Snapshot()
	if err != nil {
		result = gatewayFailure(503, "metadata_unavailable")
		return
	}
	project, found := state.Projects[principal.Project]
	if !found || !quotaValid(project.Quotas) {
		result = gatewayFailure(403, "forbidden")
		return
	}
	gateway.mu.Lock()
	if gateway.closing {
		gateway.mu.Unlock()
		result = gatewayFailure(503, "gateway_closing")
		return
	}
	permit, found = gateway.admission.reserve(principal.Project, project.Quotas, action == "open", action == "close")
	gateway.mu.Unlock()
	if !found {
		result = gatewayFailure(429, "quota_exceeded")
		return
	}
	limit := sqlWireLimit
	if project.Quotas.MaxSQLBytes <= (sqlWireLimit-8192)/6 {
		limit = project.Quotas.MaxSQLBytes*6 + 8192
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, int64(limit)))
	if err != nil {
		result = gatewayFailure(413, "request_limit")
		return
	}
	parsed, valid := decodeGatewayRequest(ctx, body, action)
	if !valid {
		return
	}
	if parsed.RequestID != "" {
		if !sqlLabel(parsed.RequestID) {
			return
		}
		identifier = parsed.RequestID
	}
	if (action == "execute" || action == "query") && (len(parsed.SQL) == 0 || len(parsed.SQL) > project.Quotas.MaxSQLBytes) {
		result = gatewayFailure(413, "sql_limit")
		return
	}
	switch action {
	case "open":
		result = gateway.open(ctx, principal, *parsed.Database, identifier, permit)
	case "execute", "query", "close":
		result = gateway.dispatch(ctx, principal, parsed, identifier, action, project.Quotas, permit, strings.Contains(request.Header.Get("Accept"), "application/x-ndjson"))
	}
}

func decodeGatewayRequest(ctx context.Context, body []byte, action string) (gatewayRequest, bool) {
	var request gatewayRequest
	if !utf8.Valid(body) {
		return request, false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if sqlJSONValue(ctx, decoder, 0) != nil {
		return request, false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return request, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return request, false
	}
	allowed := map[string]bool{"request_id": true}
	switch action {
	case "open":
		allowed["database"] = true
	case "execute", "query":
		allowed["session"] = true
		allowed["sql"] = true
	case "close":
		allowed["session"] = true
	}
	for field := range fields {
		if !allowed[field] {
			return request, false
		}
	}
	decoder = json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		return request, false
	}
	if action == "open" {
		return request, request.Database != nil && request.Database.Validate() == nil
	}
	return request, publicSessionID(request.Session)
}

func (gateway *Gateway) writeResponse(writer http.ResponseWriter, requestID string, result gatewayResult) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	if result.code == "" && result.stream != nil {
		writer.Header().Set("Content-Type", "application/x-ndjson")
		writer.WriteHeader(200)
		_, _ = writer.Write(result.stream)
		return
	}
	response := map[string]any{"version": 1, "request_id": requestID, "ok": result.code == "", "result": result.value, "error": nil}
	if result.code != "" {
		response["result"] = nil
		failure := map[string]any{"code": result.code}
		if result.code == "result_limit" && result.dispatched {
			failure["execution_completed"] = true
		}
		if result.code == "sql_error" && result.dispatched {
			failure["execution_completed"] = true
			failure["may_have_committed"] = true
		}
		response["error"] = failure
	}
	writer.Header().Set("Content-Type", "application/json")
	status := result.status
	if status == 0 {
		status = 200
	}
	writer.WriteHeader(status)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(response)
}

func publicSessionID(identifier string) bool {
	if len(identifier) != 32 {
		return false
	}
	for _, character := range identifier {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
