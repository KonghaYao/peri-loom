package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	controller "peri-loom/controller"
)

type hranaWSRequest struct {
	Type     string          `json:"type"`
	StreamID int             `json:"stream_id"`
	CursorID int             `json:"cursor_id"`
	MaxCount int             `json:"max_count"`
	SQLID    int             `json:"sql_id"`
	SQL      string          `json:"sql"`
	Stmt     hranaStatement  `json:"stmt"`
	Batch    json.RawMessage `json:"batch"`
}

type hranaWSMessage struct {
	Type      string         `json:"type"`
	JWT       string         `json:"jwt"`
	RequestID int            `json:"request_id"`
	Request   hranaWSRequest `json:"request"`
}

func serveHranaWS(writer http.ResponseWriter, request *http.Request, handler *hranaHTTP) bool {
	parts := strings.Split(strings.TrimSuffix(request.URL.Path, "/"), "/")
	if len(parts) != 5 || parts[0] != "" || parts[1] != "db" {
		return false
	}
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: parts[2], Database: parts[3]}, Branch: parts[4]}
	if database.Validate() != nil || request.Method != http.MethodGet || request.URL.RawQuery != "" || !websocket.IsWebSocketUpgrade(request) {
		http.Error(writer, "invalid WebSocket request", http.StatusBadRequest)
		return true
	}
	handler.mu.Lock()
	handler.gateway.mu.Lock()
	closing := handler.gateway.closing
	handler.gateway.mu.Unlock()
	if closing {
		handler.mu.Unlock()
		http.Error(writer, "gateway closing", http.StatusServiceUnavailable)
		return true
	}
	if handler.wsOpen >= 128 {
		handler.mu.Unlock()
		http.Error(writer, "WebSocket capacity reached", http.StatusTooManyRequests)
		return true
	}
	handler.wsOpen++
	handler.mu.Unlock()
	defer func() {
		handler.mu.Lock()
		handler.wsOpen--
		handler.mu.Unlock()
	}()
	upgrader := websocket.Upgrader{Subprotocols: []string{"hrana3-protobuf", "hrana3", "hrana2", "hrana1"}, HandshakeTimeout: 5 * time.Second}
	connection, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return true
	}
	defer connection.Close()
	handler.mu.Lock()
	handler.wsConns[connection] = struct{}{}
	handler.mu.Unlock()
	defer func() {
		handler.mu.Lock()
		delete(handler.wsConns, connection)
		handler.mu.Unlock()
	}()
	handler.gateway.mu.Lock()
	closing = handler.gateway.closing
	handler.gateway.mu.Unlock()
	if closing {
		return true
	}
	connection.SetReadLimit(sqlWireLimit)
	connection.SetReadDeadline(time.Now().Add(10 * time.Second))
	connection.SetPongHandler(func(string) error { return connection.SetReadDeadline(time.Now().Add(5 * time.Minute)) })
	if connection.Subprotocol() != "hrana1" && connection.Subprotocol() != "hrana2" && connection.Subprotocol() != "hrana3" && connection.Subprotocol() != "hrana3-protobuf" {
		return true
	}
	protobuf := connection.Subprotocol() == "hrana3-protobuf"
	var hello hranaWSMessage
	if hranaWSRead(connection, protobuf, &hello) != nil || hello.Type != "hello" {
		return true
	}
	if hello.JWT == "" {
		_ = hranaWSWrite(connection, protobuf, map[string]any{"type": "hello_error", "error": map[string]any{"message": "unauthenticated", "code": "unauthenticated"}})
		return true
	}
	principal, err := handler.gateway.config.Identity.Authenticate(hello.JWT)
	if err != nil || principal.Subject == "" || principal.Project == "" || principal.ExpiresMS > 0 && principal.ExpiresMS <= time.Now().UnixMilli() || handler.gateway.authorize(principal, database) != nil {
		_ = hranaWSWrite(connection, protobuf, map[string]any{"type": "hello_error", "error": map[string]any{"message": "forbidden", "code": "forbidden"}})
		return true
	}
	if hranaWSWrite(connection, protobuf, map[string]any{"type": "hello_ok"}) != nil {
		return true
	}
	streams := make(map[int]string)
	storedSQL := make(map[int]string)
	cursors := make(map[int]*hranaWSCursor)
	defer func() {
		for _, baton := range streams {
			handler.mu.Lock()
			stream := handler.streams[baton]
			handler.mu.Unlock()
			if stream != nil {
				stream.mu.Lock()
				handler.expireStreamLocked(baton, stream)
				stream.mu.Unlock()
			}
		}
	}()
	for {
		connection.SetReadDeadline(time.Now().Add(5 * time.Minute))
		var message hranaWSMessage
		if err := hranaWSRead(connection, protobuf, &message); err != nil {
			return true
		}
		if message.Type != "request" || message.RequestID < 0 {
			return true
		}
		response, failure, fatal := handler.wsRequest(request, hello.JWT, database, connection.Subprotocol(), streams, storedSQL, cursors, message.Request)
		if fatal {
			log.Printf("hrana WebSocket outcome unknown: %s", message.Request.Type)
			return true
		}
		var output any
		if failure != "" {
			output = map[string]any{"type": "response_error", "request_id": message.RequestID, "error": map[string]any{"message": failure, "code": failure}}
		} else {
			output = map[string]any{"type": "response_ok", "request_id": message.RequestID, "response": response}
		}
		connection.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err := hranaWSWrite(connection, protobuf, output); err != nil {
			log.Printf("hrana WebSocket reply invalid: %s: %v", message.Request.Type, err)
			return true
		}
	}
}

func (handler *hranaHTTP) wsPipeline(ctx context.Context, credential string, database controller.DatabaseID, baton string, entry any) (string, map[string]any, error) {
	encoded, err := json.Marshal(map[string]any{"baton": baton, "requests": []any{entry}})
	if err != nil {
		return "", nil, err
	}
	path := "/db/" + database.Namespace.Tenant + "/" + database.Namespace.Database + "/" + database.Branch + "/v2/pipeline"
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Content-Type", "application/json")
	reply := httptest.NewRecorder()
	handler.ServeHTTP(reply, request)
	if reply.Code != 200 {
		return "", nil, errors.New("Hrana pipeline outcome unknown")
	}
	var result struct {
		Baton   *string          `json:"baton"`
		Results []map[string]any `json:"results"`
	}
	decoder := json.NewDecoder(bytes.NewReader(reply.Body.Bytes()))
	decoder.UseNumber()
	if decoder.Decode(&result) != nil || len(result.Results) != 1 {
		return "", nil, errors.New("invalid Hrana pipeline reply")
	}
	if result.Baton != nil {
		return *result.Baton, result.Results[0], nil
	}
	return "", result.Results[0], nil
}

func (handler *hranaHTTP) wsRequest(request *http.Request, credential string, database controller.DatabaseID, subprotocol string, streams map[int]string, storedSQL map[int]string, cursors map[int]*hranaWSCursor, message hranaWSRequest) (map[string]any, string, bool) {
	if message.StreamID < 0 || message.SQLID < 0 || message.CursorID < 0 {
		return nil, "invalid_arguments", false
	}
	if message.Type == "fetch_cursor" {
		cursor := cursors[message.CursorID]
		if cursor == nil || message.MaxCount < 1 {
			return nil, "invalid_cursor", false
		}
		return handler.wsFetchCursor(request, credential, cursor, message.MaxCount)
	}
	if message.Type == "close_cursor" {
		if cursors[message.CursorID] == nil {
			return nil, "invalid_cursor", false
		}
		cursor := cursors[message.CursorID]
		if cursor.native && cursor.active {
			code, _, failure := handler.call(request, credential, "/v1/sql/cursor/close", map[string]any{"session": cursor.session, "write_cursor": cursor.write})
			if failure != "" {
				return nil, failure, code >= 500 || failure == "outcome_unknown"
			}
		}
		delete(cursors, message.CursorID)
		return map[string]any{"type": "close_cursor"}, "", false
	}
	switch message.Type {
	case "open_stream":
		if _, exists := streams[message.StreamID]; exists || len(streams) >= 32 {
			return nil, "stream_limit", false
		}
		handler.mu.Lock()
		if len(handler.streams)+handler.reserved >= maxHranaStreams {
			handler.mu.Unlock()
			return nil, "stream_limit", false
		}
		handler.reserved++
		handler.mu.Unlock()
		defer func() {
			handler.mu.Lock()
			handler.reserved--
			handler.mu.Unlock()
		}()
		code, result, failure := handler.call(request, credential, "/v1/sql/open", map[string]any{"database": database})
		if code >= 500 || failure == "outcome_unknown" {
			return nil, "", true
		}
		if failure != "" {
			return nil, failure, false
		}
		var opened struct {
			Session string `json:"session"`
		}
		if json.Unmarshal(result, &opened) != nil || !publicSessionID(opened.Session) {
			return nil, "", true
		}
		baton, err := controller.RandomID()
		if err != nil {
			_, _, _ = handler.call(request, credential, "/v1/sql/close", map[string]any{"session": opened.Session})
			return nil, "", true
		}
		handler.mu.Lock()
		handler.streams[baton] = &hranaStream{session: opened.Session, credential: sha256.Sum256([]byte(credential)), database: database, storedSQL: make(map[int]string)}
		handler.mu.Unlock()
		streams[message.StreamID] = baton
		return map[string]any{"type": "open_stream"}, "", false
	case "store_sql":
		if message.SQL == "" || len(message.SQL) > 65536 || len(storedSQL) >= 64 && storedSQL[message.SQLID] == "" {
			return nil, "invalid_arguments", false
		}
		storedSQL[message.SQLID] = message.SQL
		return map[string]any{"type": "store_sql"}, "", false
	case "close_sql":
		delete(storedSQL, message.SQLID)
		return map[string]any{"type": "close_sql"}, "", false
	}
	baton, found := streams[message.StreamID]
	if !found {
		return nil, "stream_not_found", false
	}
	if message.Type == "close_stream" {
		for _, cursor := range cursors {
			if cursor.streamID == message.StreamID {
				return nil, "cursor_open", false
			}
		}
		_, result, err := handler.wsPipeline(request.Context(), credential, database, baton, map[string]any{"type": "close"})
		if err != nil {
			return nil, "", true
		}
		if result["type"] != "ok" {
			return nil, "", true
		}
		delete(streams, message.StreamID)
		return map[string]any{"type": "close_stream"}, "", false
	}
	for _, cursor := range cursors {
		if cursor.streamID == message.StreamID && cursor.native {
			return nil, "cursor_open", false
		}
	}
	var entry map[string]any
	switch message.Type {
	case "execute":
		statement := message.Stmt
		if statement.SQL == "" {
			statement.SQL = storedSQL[statement.SQLID]
		}
		entry = map[string]any{"type": "execute", "stmt": statement}
	case "batch":
		var batch struct {
			Steps []hranaReadCursorStep `json:"steps"`
		}
		if json.Unmarshal(message.Batch, &batch) != nil || len(batch.Steps) > 64 {
			return nil, "invalid_arguments", false
		}
		for index := range batch.Steps {
			if batch.Steps[index].Stmt.SQL == "" {
				batch.Steps[index].Stmt.SQL = storedSQL[batch.Steps[index].Stmt.SQLID]
			}
		}
		entry = map[string]any{"type": "batch", "batch": batch}
	case "open_cursor":
		if _, exists := cursors[message.CursorID]; exists || len(cursors) >= 16 {
			return nil, "cursor_limit", false
		}
		for _, cursor := range cursors {
			if cursor.streamID == message.StreamID {
				return nil, "cursor_open", false
			}
		}
		var batch struct {
			Steps []hranaReadCursorStep `json:"steps"`
		}
		if json.Unmarshal(message.Batch, &batch) != nil || len(batch.Steps) > 64 {
			return nil, "invalid_arguments", false
		}
		for index := range batch.Steps {
			if batch.Steps[index].Stmt.SQL == "" {
				batch.Steps[index].Stmt.SQL = storedSQL[batch.Steps[index].Stmt.SQLID]
			}
		}
		readOnly := len(batch.Steps) > 0
		for index, step := range batch.Steps {
			if step.Stmt.SQL == "" || !hranaValidCondition(step.Condition, index) {
				readOnly = false
				break
			}
		}
		if readOnly {
			handler.mu.Lock()
			stream := handler.streams[baton]
			handler.mu.Unlock()
			if stream == nil || stream.session == "" {
				return nil, "", true
			}
			cursor := &hranaWSCursor{streamID: message.StreamID, session: stream.session, native: true, steps: batch.Steps, results: make([]any, len(batch.Steps)), failures: make([]any, len(batch.Steps))}
			failure, fatal := handler.wsStartReadStep(request, credential, cursor)
			if failure != "" {
				return nil, failure, fatal
			}
			cursors[message.CursorID] = cursor
			return map[string]any{"type": "open_cursor"}, "", false
		}
		entry = map[string]any{"type": "batch", "batch": batch}
	case "sequence", "describe":
		if message.SQL == "" {
			message.SQL = storedSQL[message.SQLID]
		}
		entry = map[string]any{"type": message.Type, "sql": message.SQL}
	case "get_autocommit":
		if subprotocol != "hrana3" && subprotocol != "hrana3-protobuf" {
			return nil, "unsupported_protocol", false
		}
		entry = map[string]any{"type": "get_autocommit"}
	default:
		return nil, "unsupported_request", false
	}
	next, result, err := handler.wsPipeline(request.Context(), credential, database, baton, entry)
	if err != nil || next != baton {
		log.Printf("hrana WebSocket pipeline not confirmed: %s: %v", message.Type, err)
		return nil, "", true
	}
	if result["type"] == "error" {
		failure, ok := result["error"].(map[string]any)
		if !ok {
			return nil, "", true
		}
		code, ok := failure["code"].(string)
		return nil, code, !ok
	}
	response, ok := result["response"].(map[string]any)
	if message.Type == "open_cursor" && ok && result["type"] == "ok" {
		entries, err := hranaWSCursorEntries(response)
		if err != nil {
			log.Printf("hrana WebSocket cursor metadata invalid: %v", err)
			return nil, "", true
		}
		cursors[message.CursorID] = &hranaWSCursor{streamID: message.StreamID, entries: entries}
		return map[string]any{"type": "open_cursor"}, "", false
	}
	return response, "", !ok || result["type"] != "ok"
}
