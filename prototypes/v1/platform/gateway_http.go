package platform

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	controller "peri-loom/controller"
)

type gatewayRequest struct {
	Database    *controller.DatabaseID `json:"database,omitempty"`
	Session     string                 `json:"session,omitempty"`
	SQL         string                 `json:"sql,omitempty"`
	MaxRows     int                    `json:"max_rows,omitempty"`
	WriteCursor bool                   `json:"write_cursor,omitempty"`
	Args        []sqlArgument          `json:"args,omitempty"`
	NamedArgs   []namedSQLArgument     `json:"named_args,omitempty"`
	RequestID   string                 `json:"request_id,omitempty"`
}

type sqlArgument struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value,omitempty"`
	Base64 string          `json:"base64,omitempty"`
}

func (argument sqlArgument) MarshalJSON() ([]byte, error) {
	if argument.Type == "blob" {
		return json.Marshal(struct {
			Type   string `json:"type"`
			Base64 string `json:"base64"`
		}{Type: argument.Type, Base64: argument.Base64})
	}
	return json.Marshal(struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value,omitempty"`
	}{Type: argument.Type, Value: argument.Value})
}

type namedSQLArgument struct {
	Name  string      `json:"name"`
	Value sqlArgument `json:"value"`
}

func gatewayAction(path string) string {
	switch path {
	case "/v1/sql/open", "/v1/sql/open/database":
		return "open"
	case "/v1/sql/execute":
		return "execute"
	case "/v1/sql/query":
		return "query"
	case "/v1/sql/statement":
		return "statement"
	case "/v1/sql/describe":
		return "describe"
	case "/v1/sql/cursor/open":
		return "cursor_open"
	case "/v1/sql/cursor/fetch":
		return "cursor_fetch"
	case "/v1/sql/cursor/close":
		return "cursor_close"
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
	timeout := gateway.config.RequestTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(request.Context(), timeout)
	defer cancel()
	_ = http.NewResponseController(writer).SetReadDeadline(time.Now().Add(10 * time.Second))
	_ = http.NewResponseController(writer).SetWriteDeadline(time.Now().Add(timeout))
	var principal Principal
	var permit *admissionPermit
	result := gatewayFailure(400, "invalid_request")
	correlationValues := request.Header.Values("X-Request-ID")
	correlationValid := len(correlationValues) == 0 || len(correlationValues) == 1 && sqlLabel(correlationValues[0])
	if correlationValid && len(correlationValues) == 1 {
		identifier = correlationValues[0]
	}
	defer func() {
		if result.finish != nil {
			defer result.finish()
		}
		if permit != nil {
			defer permit.finish()
		}
		if result.operationPending {
			gateway.settleOperation(result.operationDatabase, result.operationID, result.operationStage, &result, permit)
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
			if result.code != "outcome_unknown" {
				result.auditExecutionCompleted = result.dispatched && (result.code == "" || result.code == "sql_error" || result.code == "result_limit")
				result.code = "audit_unavailable"
				result.status = 503
				result.value = nil
				result.stream = nil
			}
		}
		gateway.counters.observe(action, result.code, time.Since(started))
		gateway.writeResponse(writer, identifier, result)
	}()
	if !correlationValid || request.Method != "POST" || action == "invalid" || request.URL.RawQuery != "" {
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
	var parsed gatewayRequest
	var earlyBody []byte
	var database *controller.DatabaseID
	if principal.Project == "*" && principal.Role == "admin" && principal.Database == nil {
		earlyBody, err = io.ReadAll(http.MaxBytesReader(writer, request.Body, sqlWireLimit))
		if err != nil {
			result = gatewayFailure(413, "request_limit")
			return
		}
		var valid bool
		parsed, valid = decodeGatewayRequest(ctx, earlyBody, action)
		if !valid {
			return
		}
		if action == "open" {
			if parsed.Database == nil {
				result = gatewayFailure(403, "forbidden")
				return
			}
			database = parsed.Database
		} else {
			gateway.mu.Lock()
			session := gateway.sessions[parsed.Session]
			gateway.mu.Unlock()
			if session == nil || session.subject != principal.Subject || session.keyID != principal.KeyID {
				result = gatewayFailure(403, "invalid_session")
				return
			}
			principal.Project = session.project
		}
	}
	state, err := readGatewayRequestState(gateway.config.State, principal.Project, database)
	if err != nil {
		result = gatewayFailure(503, "metadata_unavailable")
		return
	}
	if database != nil {
		descriptor, exists := state.Databases[database.Key()]
		if !exists || descriptor.ID != *database {
			result = gatewayFailure(403, "forbidden")
			return
		}
		principal.Project = descriptor.Project
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
	deferredAdmission := action != "open" && gateway.admission.eligible(project.Quotas)
	if !deferredAdmission {
		permit, found = gateway.admission.reserve(principal.Project, project.Quotas, action == "open", action == "close")
	}
	gateway.mu.Unlock()
	if !deferredAdmission && !found {
		result = gatewayFailure(429, "quota_exceeded")
		return
	}
	var releaseClassification func()
	if deferredAdmission {
		var ok bool
		releaseClassification, ok = gateway.admission.classify(principal.Project, project.Quotas)
		if !ok {
			result = gatewayFailure(429, "quota_exceeded")
			return
		}
		defer releaseClassification()
	}
	limit := sqlWireLimit
	if project.Quotas.MaxSQLBytes <= (sqlWireLimit-8192)/6 {
		limit = project.Quotas.MaxSQLBytes*6 + 8192
	}
	if earlyBody != nil {
		if len(earlyBody) > limit {
			result = gatewayFailure(413, "request_limit")
			return
		}
	} else {
		body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, int64(limit)))
		if err != nil {
			result = gatewayFailure(413, "request_limit")
			return
		}
		var valid bool
		parsed, valid = decodeGatewayRequest(ctx, body, action)
		if !valid {
			return
		}
	}
	if parsed.RequestID != "" {
		if !sqlLabel(parsed.RequestID) || len(correlationValues) == 1 && parsed.RequestID != identifier {
			return
		}
		identifier = parsed.RequestID
	}
	if (action == "execute" || action == "query" || action == "statement" || action == "describe" || action == "cursor_open") && (len(parsed.SQL) == 0 || len(parsed.SQL) > project.Quotas.MaxSQLBytes) {
		result = gatewayFailure(413, "sql_limit")
		return
	}
	if releaseClassification != nil {
		releaseClassification()
	}
	switch action {
	case "open":
		result = gateway.open(ctx, principal, *parsed.Database, identifier, permit, project.Quotas)
	case "execute", "query", "statement", "describe", "close", "cursor_open", "cursor_fetch", "cursor_close":
		result = gateway.dispatch(ctx, principal, parsed, identifier, action, project.Quotas, permit, strings.Contains(request.Header.Get("Accept"), "application/x-ndjson"))
		if result.permit != nil {
			permit = result.permit
		}
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
	case "execute", "query", "statement", "describe", "cursor_open":
		allowed["session"] = true
		allowed["sql"] = true
		if action == "statement" || action == "cursor_open" {
			allowed["args"] = true
			allowed["named_args"] = true
		}
		if action == "cursor_open" {
			allowed["write_cursor"] = true
		}
	case "cursor_fetch":
		allowed["session"] = true
		allowed["max_rows"] = true
		allowed["write_cursor"] = true
	case "cursor_close":
		allowed["session"] = true
		allowed["write_cursor"] = true
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
	if action == "cursor_fetch" && (request.MaxRows < 1 || request.MaxRows > 256) {
		return request, false
	}
	if (action == "statement" || action == "cursor_open") && (!validSQLArguments(request.Args) || len(request.NamedArgs) > 999 || len(request.Args)+len(request.NamedArgs) > 999) {
		return request, false
	}
	for _, argument := range request.NamedArgs {
		if argument.Name == "" || len(argument.Name) > 256 || !validSQLArguments([]sqlArgument{argument.Value}) {
			return request, false
		}
	}
	return request, publicSessionID(request.Session)
}

func validSQLArguments(args []sqlArgument) bool {
	if len(args) > 999 {
		return false
	}
	for _, arg := range args {
		switch arg.Type {
		case "null":
			if len(arg.Value) != 0 || arg.Base64 != "" {
				return false
			}
		case "integer":
			var value string
			if json.Unmarshal(arg.Value, &value) != nil || arg.Base64 != "" {
				return false
			}
			if _, err := strconv.ParseInt(value, 10, 64); err != nil {
				return false
			}
		case "float":
			var value float64
			if json.Unmarshal(arg.Value, &value) != nil || arg.Base64 != "" {
				return false
			}
		case "text":
			var value string
			if json.Unmarshal(arg.Value, &value) != nil || arg.Base64 != "" {
				return false
			}
		case "blob":
			if len(arg.Value) != 0 {
				return false
			}
			if _, err := base64.StdEncoding.Strict().DecodeString(arg.Base64); err != nil {
				return false
			}
		default:
			return false
		}
	}
	return true
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
		if result.code == "outcome_unknown" && result.operationID != "" {
			failure["operation_id"], failure["stage"], failure["retry_allowed"] = result.operationID, result.operationStage, false
			failure["business_outcome"], failure["cleanup_outcome"] = "unknown", "not_applicable"
			if result.operationStage == "close_session" {
				failure["business_outcome"], failure["cleanup_outcome"] = "not_applicable", "unknown"
			}
			failure["possible_commit"], failure["execution_proven"] = true, false
			if result.rpc != nil {
				failure["rpc_observation"] = map[string]bool{"ambiguous": result.rpc.Ambiguous, "rejected": result.rpc.Rejected, "execution_completed": result.rpc.ExecutionCompleted}
			}
			if record := result.operationObservation; record != nil {
				failure["business_outcome"], failure["cleanup_outcome"] = record.BusinessOutcome, record.CleanupOutcome
				failure["execution_proven"], failure["possible_commit"] = record.ExecutionProven, record.MayHaveCommitted
			}
		}
		if result.code == "result_limit" && result.dispatched {
			failure["execution_completed"] = true
		}
		if result.code == "audit_unavailable" && result.auditExecutionCompleted {
			failure["execution_completed"] = true
			failure["operation_id"] = result.operationID
			failure["retry_allowed"] = false
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
