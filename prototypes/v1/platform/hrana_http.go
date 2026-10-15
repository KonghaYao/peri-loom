package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	controller "peri-loom/controller"
)

type hranaHTTP struct {
	gateway  *Gateway
	timeout  time.Duration
	mu       sync.Mutex
	streams  map[string]*hranaStream
	reserved int
	wsOpen   int
	wsConns  map[*websocket.Conn]struct{}
}

const maxHranaStreams = 256
const maxHranaPipelineSteps = 64
const maxHranaPipelineResultBytes = 8 * 1024 * 1024
const hranaPipelineTimeout = 30 * time.Second
const hranaIdleTimeout = 5 * time.Minute

type hranaStream struct {
	mu         sync.Mutex
	session    string
	database   controller.DatabaseID
	credential [32]byte
	storedSQL  map[int]string
	expiresAt  time.Time
	timer      *time.Timer
}

type hranaRequest struct {
	Baton    string `json:"baton"`
	Requests []struct {
		Type  string         `json:"type"`
		SQL   string         `json:"sql"`
		SQLID int            `json:"sql_id"`
		Stmt  hranaStatement `json:"stmt"`
		Batch struct {
			Steps []struct {
				Stmt      hranaStatement  `json:"stmt"`
				Condition json.RawMessage `json:"condition"`
			} `json:"steps"`
		} `json:"batch"`
	} `json:"requests"`
}

type hranaStatement struct {
	SQL       string       `json:"sql"`
	SQLID     int          `json:"sql_id"`
	Args      []hranaValue `json:"args"`
	NamedArgs []struct {
		Name  string     `json:"name"`
		Value hranaValue `json:"value"`
	} `json:"named_args"`
	WantRows bool `json:"want_rows"`
}

type hranaValue struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value"`
	Base64 string          `json:"base64"`
}

type hranaResponse struct {
	Baton   *string `json:"baton"`
	Results []any   `json:"results"`
}

func newHranaHTTP(gateway *Gateway) http.Handler {
	return &hranaHTTP{gateway: gateway, timeout: hranaPipelineTimeout, streams: make(map[string]*hranaStream), wsConns: make(map[*websocket.Conn]struct{})}
}

func (handler *hranaHTTP) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	if serveHranaWS(writer, request, handler) {
		return
	}
	if serveHranaV3(writer, request, handler) {
		return
	}
	if serveHranaV3JSONCursor(writer, request, handler) {
		return
	}
	parts := strings.Split(request.URL.Path, "/")
	if len(parts) != 7 || parts[0] != "" || parts[1] != "db" || (parts[5] != "v2" && parts[5] != "v3") || parts[6] != "pipeline" || request.URL.RawQuery != "" {
		http.NotFound(writer, request)
		return
	}
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: parts[2], Database: parts[3]}, Branch: parts[4]}
	if database.Validate() != nil || request.Method != http.MethodPost {
		http.Error(writer, "invalid database or method", 400)
		return
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		http.Error(writer, "unauthenticated", 401)
		return
	}
	credential := strings.TrimPrefix(values[0], "Bearer ")
	principal, err := handler.gateway.config.Identity.Authenticate(credential)
	if err != nil || principal.Subject == "" || principal.Project == "" {
		http.Error(writer, "forbidden", 403)
		return
	}
	quota, err := handler.gateway.authorizeQuota(principal, database)
	if err != nil {
		http.Error(writer, "forbidden", 403)
		return
	}
	resultLimit := min(maxHranaPipelineResultBytes, quota.MaxResultBytes)
	resultReserve := min(maxHranaPipelineSteps*1024, max(128, resultLimit/8))
	if resultLimit <= resultReserve {
		http.Error(writer, "invalid result budget", 503)
		return
	}
	if request.Header.Get("Content-Type") != "application/json" {
		http.Error(writer, "JSON required", 415)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, sqlWireLimit))
	if err != nil {
		http.Error(writer, "request limit", 413)
		return
	}
	var pipeline hranaRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	if decoder.Decode(&pipeline) != nil || decoder.Decode(new(any)) != io.EOF || len(pipeline.Requests) == 0 || len(pipeline.Requests) > 64 {
		http.Error(writer, "invalid pipeline", 400)
		return
	}
	digest := sha256.Sum256([]byte(credential))
	stream := &hranaStream{database: database, credential: digest}
	newStream := pipeline.Baton == ""
	if pipeline.Baton != "" {
		var found bool
		handler.mu.Lock()
		stream, found = handler.streams[pipeline.Baton]
		handler.mu.Unlock()
		if !found || stream.credential != digest || stream.database != database {
			http.Error(writer, "invalid baton", 403)
			return
		}
		if !stream.mu.TryLock() {
			http.Error(writer, "baton busy; do not automatically retry SQL", 409)
			return
		}
		defer stream.mu.Unlock()
		handler.mu.Lock()
		current := handler.streams[pipeline.Baton] == stream
		handler.mu.Unlock()
		if !current {
			http.Error(writer, "closed baton", 403)
			return
		}
		if stream.timer != nil && !time.Now().Before(stream.expiresAt) {
			handler.expireStreamLocked(pipeline.Baton, stream)
			http.Error(writer, "expired baton", 403)
			return
		}
	} else {
		handler.mu.Lock()
		if len(handler.streams)+handler.reserved >= maxHranaStreams {
			handler.mu.Unlock()
			http.Error(writer, "Hrana stream capacity reached; do not retry an unknown SQL outcome", http.StatusTooManyRequests)
			return
		}
		handler.reserved++
		handler.mu.Unlock()
		defer func() {
			handler.mu.Lock()
			handler.reserved--
			handler.mu.Unlock()
		}()
	}
	response := hranaResponse{Results: make([]any, 0, len(pipeline.Requests))}
	if stream.storedSQL == nil {
		stream.storedSQL = make(map[int]string)
	}
	totalSteps := 0
	for _, entry := range pipeline.Requests {
		if entry.Type != "close" && entry.Type != "execute" && entry.Type != "batch" && entry.Type != "sequence" && entry.Type != "store_sql" && entry.Type != "close_sql" && entry.Type != "get_autocommit" && entry.Type != "describe" {
			http.Error(writer, "unsupported Hrana request", 400)
			return
		}
		if len(entry.Batch.Steps) > 64 || entry.Type == "store_sql" && (entry.SQLID < 0 || len(entry.SQL) == 0 || len(entry.SQL) > 65536) {
			http.Error(writer, "Hrana pipeline limit", 413)
			return
		}
		if entry.Type == "batch" {
			totalSteps += len(entry.Batch.Steps)
		} else if entry.Type == "execute" || entry.Type == "sequence" || entry.Type == "describe" {
			totalSteps++
		}
		if totalSteps > maxHranaPipelineSteps {
			http.Error(writer, "Hrana pipeline step limit", http.StatusRequestEntityTooLarge)
			return
		}
		for index, step := range entry.Batch.Steps {
			if !hranaValidCondition(step.Condition, index) {
				http.Error(writer, "unsupported batch condition", 400)
				return
			}
		}
	}
	ctx, cancel := context.WithTimeout(request.Context(), handler.timeout)
	defer cancel()
	request = request.WithContext(ctx)
	resultBytes := 0
	dispatched := false
	limitFailure := func(status int, code string) {
		if newStream && stream.session != "" {
			if pipeline.Baton != "" {
				handler.mu.Lock()
				delete(handler.streams, pipeline.Baton)
				handler.mu.Unlock()
				if stream.timer != nil {
					stream.timer.Stop()
				}
			}
			go handler.cleanupRetiredSession(stream.session, 0)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"error": map[string]any{
				"code": code, "partial_execution_possible": dispatched,
				"outcome_unknown": code == "pipeline_timeout", "retryable": false,
			},
		})
	}
	for _, entry := range pipeline.Requests {
		if ctx.Err() != nil {
			limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
			return
		}
		previousResults := len(response.Results)
		if (entry.Type == "execute" || entry.Type == "batch" || entry.Type == "sequence" || entry.Type == "store_sql" || entry.Type == "get_autocommit" || entry.Type == "describe") && stream.session == "" {
			code, result, failure := handler.call(request, credential, "/v1/sql/open", map[string]any{"database": database})
			if code != 200 || failure != "" {
				http.Error(writer, "open failed: "+failure, code)
				return
			}
			var opened struct {
				Session string `json:"session"`
			}
			if json.Unmarshal(result, &opened) != nil || !publicSessionID(opened.Session) {
				http.Error(writer, "open outcome unknown", 502)
				return
			}
			stream.session = opened.Session
		}
		switch entry.Type {
		case "describe":
			if entry.SQL == "" {
				entry.SQL = stream.storedSQL[entry.SQLID]
			}
			if entry.SQL == "" {
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": "missing SQL", "code": "invalid_arguments"}})
				continue
			}
			code, value, failure := handler.call(request, credential, "/v1/sql/describe", map[string]any{"session": stream.session, "sql": entry.SQL})
			if code >= 500 || failure == "outcome_unknown" {
				http.Error(writer, "describe outcome unknown: "+failure, 502)
				return
			}
			if failure != "" {
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": failure, "code": failure}})
			} else {
				var result map[string]any
				if json.Unmarshal(value, &result) != nil || !validDescribeResult(value) {
					http.Error(writer, "invalid native describe result", 502)
					return
				}
				response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "describe", "result": result}})
			}
		case "get_autocommit":
			isAutocommit, failure := handler.autocommit(request, credential, stream.session)
			if failure != "" {
				http.Error(writer, "native autocommit outcome unknown: "+failure, 502)
				return
			}
			response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "get_autocommit", "is_autocommit": isAutocommit}})
		case "sequence":
			if entry.SQL == "" {
				entry.SQL = stream.storedSQL[entry.SQLID]
			}
			if entry.SQL == "" {
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": "missing SQL", "code": "invalid_arguments"}})
				continue
			}
			dispatched = true
			code, _, failure := handler.call(request, credential, "/v1/sql/execute", map[string]any{"session": stream.session, "sql": entry.SQL})
			if ctx.Err() != nil {
				limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
				return
			}
			if code >= 500 || failure == "outcome_unknown" {
				http.Error(writer, "SQL outcome unknown: "+failure, 502)
				return
			}
			if failure != "" {
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": failure, "code": failure}})
			} else {
				response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "sequence"}})
			}
		case "store_sql":
			if len(stream.storedSQL) >= 64 && stream.storedSQL[entry.SQLID] == "" {
				http.Error(writer, "stored SQL limit", 413)
				return
			}
			stream.storedSQL[entry.SQLID] = entry.SQL
			response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "store_sql"}})
		case "close_sql":
			delete(stream.storedSQL, entry.SQLID)
			response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "close_sql"}})
		case "close":
			if stream.session != "" {
				code, _, failure := handler.call(request, credential, "/v1/sql/close", map[string]any{"session": stream.session})
				if code != 200 || failure != "" {
					http.Error(writer, "close outcome unknown: "+failure, 502)
					return
				}
			}
			stream.session = ""
			response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "close"}})
		case "execute":
			if entry.Stmt.SQL == "" {
				entry.Stmt.SQL = stream.storedSQL[entry.Stmt.SQLID]
			}
			dispatched = true
			value, failure, unknown := handler.execute(request, credential, stream.session, entry.Stmt)
			if ctx.Err() != nil {
				limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
				return
			}
			if unknown {
				http.Error(writer, "SQL outcome unknown: "+failure, 502)
				return
			}
			if failure != "" {
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": failure, "code": failure}})
			} else {
				response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": value}})
			}
		case "batch":
			results := make([]any, len(entry.Batch.Steps))
			errors := make([]any, len(entry.Batch.Steps))
			batchResultBytes := 0
			for index, step := range entry.Batch.Steps {
				if ctx.Err() != nil {
					limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
					return
				}
				isAutocommit := false
				if hranaConditionNeedsAutocommit(step.Condition) {
					var failure string
					isAutocommit, failure = handler.autocommit(request, credential, stream.session)
					if failure != "" {
						http.Error(writer, "native autocommit outcome unknown: "+failure, 502)
						return
					}
				}
				if !hranaCondition(step.Condition, results, errors, isAutocommit) {
					continue
				}
				if step.Stmt.SQL == "" {
					step.Stmt.SQL = stream.storedSQL[step.Stmt.SQLID]
				}
				dispatched = true
				value, failure, unknown := handler.execute(request, credential, stream.session, step.Stmt)
				if ctx.Err() != nil {
					limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
					return
				}
				if unknown {
					http.Error(writer, "SQL outcome unknown: "+failure, 502)
					return
				}
				if failure != "" {
					errors[index] = map[string]any{"message": failure, "code": failure}
					continue
				}
				encoded, err := json.Marshal(value)
				if err != nil || len(encoded) > resultLimit-resultBytes-batchResultBytes-resultReserve {
					limitFailure(http.StatusRequestEntityTooLarge, "pipeline_result_limit")
					return
				}
				batchResultBytes += len(encoded)
				results[index] = value
			}
			response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "batch", "result": map[string]any{"step_results": results, "step_errors": errors}}})
		}
		if len(response.Results) > previousResults {
			encoded, err := json.Marshal(response.Results[len(response.Results)-1])
			if err != nil || len(encoded) > resultLimit-resultBytes-resultReserve {
				limitFailure(http.StatusRequestEntityTooLarge, "pipeline_result_limit")
				return
			}
			resultBytes += len(encoded)
		}
	}
	if ctx.Err() != nil {
		limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
		return
	}
	if stream.session != "" {
		if pipeline.Baton == "" {
			identifier, err := controller.RandomID()
			if err != nil {
				http.Error(writer, "baton unavailable", 503)
				return
			}
			pipeline.Baton = identifier
		}
		handler.mu.Lock()
		handler.streams[pipeline.Baton] = stream
		handler.mu.Unlock()
		handler.refreshIdleStream(pipeline.Baton, stream)
		response.Baton = &pipeline.Baton
	} else if pipeline.Baton != "" {
		handler.mu.Lock()
		delete(handler.streams, pipeline.Baton)
		handler.mu.Unlock()
		if stream.timer != nil {
			stream.timer.Stop()
		}
	}
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > resultLimit {
		limitFailure(http.StatusRequestEntityTooLarge, "pipeline_result_limit")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(encoded)
	_, _ = writer.Write([]byte("\n"))
}

func hranaValidCondition(raw json.RawMessage, step int) bool {
	return hranaValidConditionDepth(raw, step, 0)
}

func hranaValidConditionDepth(raw json.RawMessage, step, depth int) bool {
	if len(raw) == 0 {
		return true
	}
	if depth > 8 {
		return false
	}
	var condition struct {
		Type  string            `json:"type"`
		Step  int               `json:"step"`
		Cond  json.RawMessage   `json:"cond"`
		Conds []json.RawMessage `json:"conds"`
	}
	if json.Unmarshal(raw, &condition) != nil {
		return false
	}
	switch condition.Type {
	case "ok", "error":
		return condition.Step >= 0 && condition.Step < step
	case "not":
		return len(condition.Cond) != 0 && hranaValidConditionDepth(condition.Cond, step, depth+1)
	case "and", "or":
		if condition.Conds == nil || len(condition.Conds) > 64 {
			return false
		}
		for _, part := range condition.Conds {
			if len(part) == 0 || !hranaValidConditionDepth(part, step, depth+1) {
				return false
			}
		}
		return true
	case "is_autocommit":
		return true
	default:
		return false
	}
}

func hranaConditionNeedsAutocommit(raw json.RawMessage) bool {
	var condition struct {
		Type  string            `json:"type"`
		Cond  json.RawMessage   `json:"cond"`
		Conds []json.RawMessage `json:"conds"`
	}
	if json.Unmarshal(raw, &condition) != nil {
		return false
	}
	if condition.Type == "is_autocommit" || len(condition.Cond) > 0 && hranaConditionNeedsAutocommit(condition.Cond) {
		return true
	}
	for _, part := range condition.Conds {
		if hranaConditionNeedsAutocommit(part) {
			return true
		}
	}
	return false
}

func hranaCondition(raw json.RawMessage, results, errors []any, isAutocommit bool) bool {
	if len(raw) == 0 {
		return true
	}
	var condition struct {
		Type  string            `json:"type"`
		Step  int               `json:"step"`
		Cond  json.RawMessage   `json:"cond"`
		Conds []json.RawMessage `json:"conds"`
	}
	if json.Unmarshal(raw, &condition) != nil {
		return false
	}
	switch condition.Type {
	case "ok":
		return condition.Step >= 0 && condition.Step < len(results) && results[condition.Step] != nil
	case "error":
		return condition.Step >= 0 && condition.Step < len(errors) && errors[condition.Step] != nil
	case "not":
		return !hranaCondition(condition.Cond, results, errors, isAutocommit)
	case "and":
		for _, part := range condition.Conds {
			if !hranaCondition(part, results, errors, isAutocommit) {
				return false
			}
		}
		return true
	case "or":
		for _, part := range condition.Conds {
			if hranaCondition(part, results, errors, isAutocommit) {
				return true
			}
		}
		return false
	case "is_autocommit":
		return isAutocommit
	default:
		return false
	}
}

func (handler *hranaHTTP) autocommit(request *http.Request, credential, session string) (bool, string) {
	code, result, failure := handler.call(request, credential, "/v1/sql/statement", map[string]any{"session": session, "sql": "SELECT 1 AS state_probe"})
	if code != http.StatusOK || failure != "" {
		return false, "native_autocommit_unavailable"
	}
	var native struct {
		Cols []struct {
			Name string `json:"name"`
		} `json:"cols"`
		Rows             [][]int64 `json:"rows"`
		AffectedRowCount int64     `json:"affected_row_count"`
		LastInsertRowid  *int64    `json:"last_insert_rowid"`
		Autocommit       *bool     `json:"autocommit"`
	}
	if json.Unmarshal(result, &native) != nil || len(native.Cols) != 1 || native.Cols[0].Name != "state_probe" || len(native.Rows) != 1 || len(native.Rows[0]) != 1 || native.Rows[0][0] != 1 || native.AffectedRowCount != 0 || native.LastInsertRowid != nil || native.Autocommit == nil {
		return false, "invalid_native_autocommit"
	}
	return *native.Autocommit, ""
}

func (handler *hranaHTTP) call(request *http.Request, credential, path string, input any) (int, json.RawMessage, string) {
	encoded, _ := json.Marshal(input)
	internal := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded)).WithContext(request.Context())
	internal.Header.Set("Authorization", "Bearer "+credential)
	reply := httptest.NewRecorder()
	handler.gateway.ServeHTTP(reply, internal)
	var envelope struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
		Error  struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(reply.Body.Bytes(), &envelope) != nil {
		log.Printf("Hrana internal gateway reply invalid: path=%s status=%d", path, reply.Code)
		return 502, nil, "invalid_gateway_reply"
	}
	if !envelope.OK {
		if reply.Code >= http.StatusInternalServerError {
			log.Printf("Hrana internal gateway failure: path=%s status=%d code=%s", path, reply.Code, envelope.Error.Code)
		}
		return reply.Code, nil, envelope.Error.Code
	}
	return reply.Code, envelope.Result, ""
}

func (handler *hranaHTTP) execute(request *http.Request, credential, session string, statement hranaStatement) (any, string, bool) {
	if statement.SQL == "" {
		return nil, "invalid_arguments", false
	}
	args := make([]sqlArgument, len(statement.Args))
	for index, argument := range statement.Args {
		args[index] = sqlArgument{Type: argument.Type, Value: argument.Value, Base64: argument.Base64}
	}
	namedArgs := make([]namedSQLArgument, len(statement.NamedArgs))
	for index, argument := range statement.NamedArgs {
		namedArgs[index] = namedSQLArgument{Name: argument.Name, Value: sqlArgument{Type: argument.Value.Type, Value: argument.Value.Value, Base64: argument.Value.Base64}}
	}
	if !validSQLArguments(args) || len(args)+len(namedArgs) > 999 {
		return nil, "invalid_arguments", false
	}
	for _, argument := range namedArgs {
		if argument.Name == "" || len(argument.Name) > 256 || !validSQLArguments([]sqlArgument{argument.Value}) {
			return nil, "invalid_arguments", false
		}
	}
	if statement.WantRows && strings.Contains(strings.ToUpper(statement.SQL), "RETURNING") {
		code, description, failure := handler.call(request, credential, "/v1/sql/describe", map[string]any{"session": session, "sql": statement.SQL})
		if failure != "" {
			return nil, failure, code >= 500 || failure == "outcome_unknown"
		}
		var prepared struct {
			Readonly bool  `json:"is_readonly"`
			Cols     []any `json:"cols"`
		}
		if code != http.StatusOK || !validDescribeResult(description) || json.Unmarshal(description, &prepared) != nil {
			return nil, "invalid_native_describe", true
		}
		if !prepared.Readonly && len(prepared.Cols) > 0 {
			return handler.executeCursor(request, credential, session, statement.SQL, args, namedArgs, true, false)
		}
	}
	code, result, failure := handler.call(request, credential, "/v1/sql/statement", map[string]any{"session": session, "sql": statement.SQL, "args": args, "named_args": namedArgs})
	if failure != "" {
		if failure == "sql_error" || failure == "result_limit" {
			readonly, describeFailure, fatal := handler.cursorStatementReadonly(request, credential, session, statement.SQL)
			if fatal {
				return nil, describeFailure, true
			}
			if describeFailure == "" && readonly {
				return handler.executeCursor(request, credential, session, statement.SQL, args, namedArgs, false, !statement.WantRows)
			}
		}
		return nil, failure, code >= 500 || failure == "outcome_unknown" || failure == "result_limit"
	}
	var native struct {
		Cols []struct {
			Name     string  `json:"name"`
			Decltype *string `json:"decltype"`
		} `json:"cols"`
		Rows             [][]any `json:"rows"`
		AffectedRowCount int64   `json:"affected_row_count"`
		LastInsertRowid  *int64  `json:"last_insert_rowid"`
	}
	decoder := json.NewDecoder(bytes.NewReader(result))
	decoder.UseNumber()
	if decoder.Decode(&native) != nil || native.Cols == nil || native.Rows == nil {
		return nil, "invalid_native_statement", true
	}
	cols := make([]any, len(native.Cols))
	for index, column := range native.Cols {
		cols[index] = map[string]any{"name": column.Name, "decltype": column.Decltype}
	}
	converted := make([][]any, 0, len(native.Rows))
	for _, row := range native.Rows {
		values := make([]any, 0, len(row))
		for _, value := range row {
			convertedValue, ok := hranaOutputValue(value)
			if !ok {
				return nil, "invalid_native_value", true
			}
			values = append(values, convertedValue)
		}
		converted = append(converted, values)
	}
	var rowid *string
	if native.LastInsertRowid != nil {
		encoded := strconv.FormatInt(*native.LastInsertRowid, 10)
		rowid = &encoded
	}
	return map[string]any{"cols": cols, "rows": converted, "affected_row_count": native.AffectedRowCount, "last_insert_rowid": rowid}, "", false
}

func hranaOutputValue(value any) (any, bool) {
	switch typed := value.(type) {
	case nil:
		return map[string]any{"type": "null"}, true
	case json.Number:
		if strings.ContainsAny(string(typed), ".eE") {
			numeric, err := typed.Float64()
			if err != nil {
				return nil, false
			}
			return map[string]any{"type": "float", "value": numeric}, true
		}
		return map[string]any{"type": "integer", "value": string(typed)}, true
	case string:
		return map[string]any{"type": "text", "value": typed}, true
	case []any:
		blob := make([]byte, len(typed))
		for index, element := range typed {
			number, ok := element.(json.Number)
			if !ok {
				return nil, false
			}
			integer, err := number.Int64()
			if err != nil || integer < 0 || integer > 255 {
				return nil, false
			}
			blob[index] = byte(integer)
		}
		return map[string]any{"type": "blob", "base64": base64.StdEncoding.EncodeToString(blob)}, true
	default:
		return nil, false
	}
}
