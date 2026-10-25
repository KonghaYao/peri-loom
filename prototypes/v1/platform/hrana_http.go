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
	"peri-loom/platform/sqlcontract"
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
	// A new stream that fails before its baton reaches the client owns a native
	// session nobody can address again: the stream is not registered, no idle
	// timer runs for it, and the native sweeper skips Hrana sessions. One
	// deferred account covers every early return after the session exists; only
	// a response carrying the baton disarms it.
	batonDelivered := false
	defer func() {
		if !newStream || stream.session == "" || batonDelivered {
			return
		}
		handler.detachStream(pipeline.Baton, stream)
		go handler.cleanupRetiredSession(stream.session, 0)
	}()
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
	var firstFailure *hranaReply
	firstFailureLocal := false
	rememberFailure := func(reply hranaReply) {
		if firstFailure == nil {
			firstFailure = &reply
		}
	}
	rememberLocalFailure := func(reply hranaReply) {
		if firstFailure == nil {
			firstFailure, firstFailureLocal = &reply, true
		}
	}
	limitFailure := func(status int, code string) {
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
			reply := handler.call(request, credential, "/v1/sql/open", map[string]any{"database": database})
			if reply.status != 200 || reply.code != "" {
				hranaPipelineFailure(writer, pipeline.Baton, dispatched, reply)
				return
			}
			var opened struct {
				Session string `json:"session"`
			}
			if json.Unmarshal(reply.result, &opened) != nil {
				hranaPipelineFailure(writer, pipeline.Baton, dispatched, hranaReply{status: http.StatusBadGateway, code: "invalid_native_open", unknown: true})
				return
			}
			if !publicSessionID(opened.Session) {
				// The reply named a session the public id cannot address, so the
				// deferred account of a valid id cannot retire it: schedule that
				// cleanup from the parsed value while it is still known.
				if opened.Session != "" {
					go handler.cleanupRetiredSession(opened.Session, 0)
				}
				hranaPipelineFailure(writer, pipeline.Baton, dispatched, hranaReply{status: http.StatusBadGateway, code: "invalid_native_open", unknown: true})
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
				missing := hranaReply{status: http.StatusBadRequest, code: "invalid_arguments"}
				rememberLocalFailure(missing)
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": "missing SQL", "code": missing.code}})
				continue
			}
			reply := handler.call(request, credential, "/v1/sql/describe", map[string]any{"session": stream.session, "sql": entry.SQL})
			if reply.unknown {
				hranaPipelineFailure(writer, pipeline.Baton, dispatched, reply)
				return
			}
			if reply.code != "" {
				rememberFailure(reply)
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": hranaErrorMessage(reply), "code": reply.code}})
			} else {
				var result map[string]any
				if json.Unmarshal(reply.result, &result) != nil || !validDescribeResult(reply.result) {
					hranaPipelineFailure(writer, pipeline.Baton, dispatched, hranaReply{status: http.StatusBadGateway, code: "invalid_native_describe", unknown: true})
					return
				}
				response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "describe", "result": result}})
			}
		case "get_autocommit":
			isAutocommit, reply := handler.autocommit(request, credential, stream.session)
			if reply.code != "" {
				if reply.unknown {
					hranaPipelineFailure(writer, pipeline.Baton, dispatched, reply)
					return
				}
				rememberFailure(reply)
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": hranaErrorMessage(reply), "code": reply.code}})
				continue
			}
			response.Results = append(response.Results, map[string]any{"type": "ok", "response": map[string]any{"type": "get_autocommit", "is_autocommit": isAutocommit}})
		case "sequence":
			if entry.SQL == "" {
				entry.SQL = stream.storedSQL[entry.SQLID]
			}
			if entry.SQL == "" {
				missing := hranaReply{status: http.StatusBadRequest, code: "invalid_arguments"}
				rememberLocalFailure(missing)
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": "missing SQL", "code": missing.code}})
				continue
			}
			dispatched = true
			reply := handler.call(request, credential, "/v1/sql/execute", map[string]any{"session": stream.session, "sql": entry.SQL})
			if ctx.Err() != nil {
				limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
				return
			}
			if reply.unknown {
				hranaPipelineFailure(writer, pipeline.Baton, dispatched, reply)
				return
			}
			if reply.code != "" {
				rememberFailure(reply)
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": hranaErrorMessage(reply), "code": reply.code}})
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
				reply := handler.call(request, credential, "/v1/sql/close", map[string]any{"session": stream.session})
				if reply.status != 200 || reply.code != "" {
					if firstFailure != nil {
						if firstFailureLocal {
							hranaPipelineFailureWithResults(writer, pipeline.Baton, dispatched, *firstFailure, response.Results, reply)
						} else {
							hranaPipelineFailure(writer, pipeline.Baton, dispatched, *firstFailure, reply)
						}
					} else {
						hranaPipelineFailure(writer, pipeline.Baton, dispatched, reply)
					}
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
			value, reply := handler.execute(request, credential, stream.session, entry.Stmt)
			if ctx.Err() != nil {
				limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
				return
			}
			if reply.unknown {
				hranaPipelineFailure(writer, pipeline.Baton, dispatched, reply)
				return
			}
			if reply.code != "" {
				rememberFailure(reply)
				response.Results = append(response.Results, map[string]any{"type": "error", "error": map[string]any{"message": hranaErrorMessage(reply), "code": reply.code}})
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
					var conditionReply hranaReply
					isAutocommit, conditionReply = handler.autocommit(request, credential, stream.session)
					if conditionReply.code != "" {
						if conditionReply.unknown {
							hranaPipelineFailure(writer, pipeline.Baton, dispatched, conditionReply)
							return
						}
						rememberFailure(conditionReply)
						errors[index] = map[string]any{"message": hranaErrorMessage(conditionReply), "code": conditionReply.code}
						continue
					}
				}
				if !hranaCondition(step.Condition, results, errors, isAutocommit) {
					continue
				}
				if step.Stmt.SQL == "" {
					step.Stmt.SQL = stream.storedSQL[step.Stmt.SQLID]
				}
				dispatched = true
				value, reply := handler.execute(request, credential, stream.session, step.Stmt)
				if ctx.Err() != nil {
					limitFailure(http.StatusGatewayTimeout, "pipeline_timeout")
					return
				}
				if reply.unknown {
					hranaPipelineFailure(writer, pipeline.Baton, dispatched, reply)
					return
				}
				if reply.code != "" {
					rememberFailure(reply)
					errors[index] = map[string]any{"message": hranaErrorMessage(reply), "code": reply.code}
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
	if _, err := writer.Write(encoded); err != nil {
		// A write that failed before the baton could reach the client leaves the
		// stream unaddressable; the deferred account retires its session.
		return
	}
	batonDelivered = true
	_, _ = writer.Write([]byte("\n"))
}

// detachStream removes a registered stream whose baton never reached the client
// and stops its idle timer. The caller retires the stream's session itself, so
// the timer must not keep a second claim on it.
func (handler *hranaHTTP) detachStream(baton string, stream *hranaStream) {
	if baton == "" {
		return
	}
	handler.mu.Lock()
	registered := handler.streams[baton] == stream
	if registered {
		delete(handler.streams, baton)
	}
	handler.mu.Unlock()
	if registered && stream.timer != nil {
		stream.timer.Stop()
	}
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

// autocommit answers the protocol's is_autocommit question. A failed probe
// returns the platform's own reply instead of a synthetic code: the same reply
// that tells a REST caller the session is isolated or the statement's outcome is
// unknown has to reach the Hrana caller unchanged, or the client reads a
// database-level isolation as an autocommit feature being unavailable.
func (handler *hranaHTTP) autocommit(request *http.Request, credential, session string) (bool, hranaReply) {
	reply := handler.probe(request, credential, "/v1/sql/session_state", map[string]any{"session": session})
	if reply.status != http.StatusOK || reply.code != "" {
		return false, reply
	}
	var native struct {
		Autocommit *bool `json:"autocommit"`
	}
	if json.Unmarshal(reply.result, &native) != nil || native.Autocommit == nil {
		return false, hranaReply{status: http.StatusBadGateway, code: "invalid_native_autocommit", unknown: true}
	}
	return *native.Autocommit, hranaReply{}
}

// hranaPipelineFailure answers a pipeline that can no longer continue. A
// dispatched statement has no place for a partial result, so the error carries
// the platform's code together with its correlation and commit risk, and repeats
// the baton when one already exists so the caller can tie the failure to the
// stream it used. A caller must not retry: the reply may leave an outcome
// unresolved.
func hranaPipelineFailure(writer http.ResponseWriter, baton string, dispatched bool, reply hranaReply, cleanup ...hranaReply) {
	hranaPipelineFailureReply(writer, baton, dispatched, reply, nil, cleanup...)
}

// hranaPipelineFailureWithResults is the local-validation exception: the first
// failure is already a step result, so a later close failure must not erase it.
func hranaPipelineFailureWithResults(writer http.ResponseWriter, baton string, dispatched bool, reply hranaReply, results []any, cleanup ...hranaReply) {
	hranaPipelineFailureReply(writer, baton, dispatched, reply, results, cleanup...)
}

func hranaPipelineFailureReply(writer http.ResponseWriter, baton string, dispatched bool, reply hranaReply, results []any, cleanup ...hranaReply) {
	body := map[string]any{"error": hranaFailureFields(reply, dispatched)}
	if results != nil {
		body["results"] = results
	}
	if len(cleanup) > 0 {
		// 清理失败单独关联，不能覆盖语句本身的状态、关联标识或执行证据。
		body["cleanup_error"] = map[string]any{"status": cleanup[0].status, "error": hranaFailureFields(cleanup[0], false)}
	}
	if baton != "" {
		body["baton"] = baton
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(reply.status)
	_ = json.NewEncoder(writer).Encode(body)
}

// Only the Hrana adapter can attach this context value. A probe answers a
// protocol question about a statement (its shape, its writability) or about the
// session's own connection state with a dispatch that cannot commit, so a
// successful probe is served without an operation record and without an audit
// row; the client action that needed the answer records its own. A failed probe
// becomes the client's error and keeps its row. A public header cannot set the
// marker.
type hranaProbeContextKey struct{}

func hranaProbeRequest(ctx context.Context, action string) bool {
	if ctx.Value(hranaProbeContextKey{}) == nil {
		return false
	}
	return action == "describe" || action == "session_state"
}

// hranaReply is the platform's answer to one internal SQL request. It repeats
// the reply's own execution proof so that a consumer decides whether a failure
// leaves the statement's fate unknown from that proof rather than from the HTTP
// status the platform happened to map the failure to. The status code only says
// how the platform answered, never what happened to the statement.
type hranaReply struct {
	status int
	result json.RawMessage
	code   string
	// message repeats the platform's already-published classification when the
	// reply was decoded from a pipeline result instead of a gateway envelope.
	// hranaErrorMessage falls back to the code when no platform text exists.
	message string
	// unknown and readOnlyCandidate are the contract's own answers, decoded from
	// the reply the gateway published by sqlcontract and never recomputed here.
	// unknown is the only condition that may abort a pipeline, drop a connection
	// or end a cursor stream: a failure the platform proved — settled or rejected
	// — stays a step error and the pipeline continues.
	unknown           bool
	readOnlyCandidate bool
	// failure keeps the platform's own failure object. An adapter that answers
	// the client itself forwards the platform's correlation, stage and commit
	// risk instead of inventing a transport-specific code.
	failure *sqlcontract.Failure
}

// hranaErrorMessage answers a step error with the platform's own text when the
// platform published one. The text is the platform's classification, never the
// worker's raw failure, so the adapter may forward it. A locally built failure
// has no platform object and keeps its code as the message.
func hranaErrorMessage(reply hranaReply) string {
	if reply.failure != nil {
		if message := reply.failure.Message(); message != "" {
			return message
		}
	}
	if reply.message != "" {
		return reply.message
	}
	return reply.code
}

// hranaFatalDescribeReply keeps the reply the platform published behind a fatal
// describe probe. Only the adapter's status and unknown marker change: the
// failure object carries the platform's own message and must survive, or the
// pipeline failure falls back to repeating the code.
func hranaFatalDescribeReply(reply hranaReply) hranaReply {
	reply.status = http.StatusBadGateway
	reply.unknown = true
	return reply
}

// hranaGatewayReply decodes the gateway's answer into the decision the adapter
// acts on. It is the only place the Hrana adapter reads a gateway reply, and it
// reads no evidence and no code name itself: sqlcontract decodes both, so an
// adapter cannot disagree with the Go, TypeScript, browser and console clients
// about what a reply proved.
func hranaGatewayReply(status int, body []byte, path string) hranaReply {
	var envelope struct {
		OK     bool                 `json:"ok"`
		Result json.RawMessage      `json:"result"`
		Error  *sqlcontract.Failure `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		log.Printf("Hrana internal gateway reply invalid: path=%s status=%d", path, status)
		return hranaReply{status: 502, code: "invalid_gateway_reply", unknown: true}
	}
	if status == http.StatusAccepted {
		// The gateway accepted an open for controller activation. Hrana has no
		// field for the platform's loading object, but it must not mistake the
		// body for an opened session either; the client retries the open.
		return hranaReply{status: http.StatusServiceUnavailable, code: "activation_pending"}
	}
	if !envelope.OK {
		if envelope.Error == nil {
			log.Printf("Hrana internal gateway failure without a failure object: path=%s status=%d", path, status)
			return hranaReply{status: 502, code: "invalid_gateway_reply", unknown: true}
		}
		if status >= http.StatusInternalServerError {
			log.Printf("Hrana internal gateway failure: path=%s status=%d code=%s", path, status, envelope.Error.Code())
		}
		return hranaReply{status: status, code: envelope.Error.Code(),
			unknown: envelope.Error.Unknown(status), readOnlyCandidate: envelope.Error.ReadOnlyCandidate(),
			failure: envelope.Error}
	}
	return hranaReply{status: status, result: envelope.Result}
}

func (handler *hranaHTTP) call(request *http.Request, credential, path string, input any) hranaReply {
	return handler.dispatch(request, credential, path, input, false)
}

func (handler *hranaHTTP) probe(request *http.Request, credential, path string, input any) hranaReply {
	return handler.dispatch(request, credential, path, input, true)
}

func (handler *hranaHTTP) dispatch(request *http.Request, credential, path string, input any, probe bool) hranaReply {
	encoded, _ := json.Marshal(input)
	ctx := context.WithValue(request.Context(), hranaSessionContextKey{}, true)
	if probe {
		ctx = context.WithValue(ctx, hranaProbeContextKey{}, true)
	}
	internal := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded)).WithContext(ctx)
	internal.Header.Set("Authorization", "Bearer "+credential)
	recorder := httptest.NewRecorder()
	handler.gateway.ServeHTTP(recorder, internal)
	return hranaGatewayReply(recorder.Code, recorder.Body.Bytes(), path)
}

func (handler *hranaHTTP) execute(request *http.Request, credential, session string, statement hranaStatement) (any, hranaReply) {
	invalid := hranaReply{status: http.StatusBadRequest, code: "invalid_arguments"}
	if statement.SQL == "" {
		return nil, invalid
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
		return nil, invalid
	}
	for _, argument := range namedArgs {
		if argument.Name == "" || len(argument.Name) > 256 || !validSQLArguments([]sqlArgument{argument.Value}) {
			return nil, invalid
		}
	}
	if statement.WantRows && strings.Contains(strings.ToUpper(statement.SQL), "RETURNING") {
		prepared := handler.probe(request, credential, "/v1/sql/describe", map[string]any{"session": session, "sql": statement.SQL})
		if prepared.code != "" {
			return nil, prepared
		}
		var description struct {
			Readonly bool  `json:"is_readonly"`
			Cols     []any `json:"cols"`
		}
		if prepared.status != http.StatusOK || !validDescribeResult(prepared.result) || json.Unmarshal(prepared.result, &description) != nil {
			return nil, hranaReply{status: http.StatusBadGateway, code: "invalid_native_describe", unknown: true}
		}
		if !description.Readonly && len(description.Cols) > 0 {
			return handler.executeCursor(request, credential, session, statement.SQL, args, namedArgs, true, false)
		}
	}
	reply := handler.call(request, credential, "/v1/sql/statement", map[string]any{"session": session, "sql": statement.SQL, "args": args, "named_args": namedArgs, "want_rows": statement.WantRows})
	if reply.code != "" {
		if reply.readOnlyCandidate {
			readonly, describeReply, fatal := handler.cursorStatementReadonly(request, credential, session, statement.SQL)
			if fatal {
				return nil, hranaFatalDescribeReply(describeReply)
			}
			if describeReply.code == "" && readonly {
				return handler.executeCursor(request, credential, session, statement.SQL, args, namedArgs, false, !statement.WantRows)
			}
		}
		return nil, reply
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
	decoder := json.NewDecoder(bytes.NewReader(reply.result))
	decoder.UseNumber()
	if decoder.Decode(&native) != nil || native.Cols == nil || native.Rows == nil {
		return nil, hranaReply{status: http.StatusBadGateway, code: "invalid_native_statement", unknown: true}
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
				return nil, hranaReply{status: http.StatusBadGateway, code: "invalid_native_value", unknown: true}
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
	return map[string]any{"cols": cols, "rows": converted, "affected_row_count": native.AffectedRowCount, "last_insert_rowid": rowid}, hranaReply{}
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
