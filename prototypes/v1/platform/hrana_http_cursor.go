package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	controller "peri-loom/controller"
)

func (handler *hranaHTTP) serveNativeHTTPReadCursor(writer http.ResponseWriter, request *http.Request, parts []string, input map[string]any) bool {
	requests, valid := input["requests"].([]any)
	if !valid || len(requests) != 1 {
		return false
	}
	streamRequest, valid := requests[0].(map[string]any)
	if !valid {
		return false
	}
	batch, valid := streamRequest["batch"].(map[string]any)
	if !valid {
		return false
	}
	steps, valid := batch["steps"].([]any)
	if !valid || len(steps) == 0 || len(steps) > 64 {
		return false
	}
	readSteps := make([]hranaReadCursorStep, 0, len(steps))
	for index, rawStep := range steps {
		step, valid := rawStep.(map[string]any)
		if !valid {
			return false
		}
		_, valid = step["stmt"].(map[string]any)
		if !valid {
			return false
		}
		encoded, err := json.Marshal(step)
		if err != nil {
			http.Error(writer, "invalid cursor statement", 400)
			return true
		}
		var readStep hranaReadCursorStep
		if json.Unmarshal(encoded, &readStep) != nil || !hranaValidCondition(readStep.Condition, index) {
			http.Error(writer, "invalid cursor statement", 400)
			return true
		}
		readSteps = append(readSteps, readStep)
	}
	baton, _ := input["baton"].(string)
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: parts[2], Database: parts[3]}, Branch: parts[4]}
	if database.Validate() != nil {
		http.Error(writer, "invalid database", 400)
		return true
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		http.Error(writer, "unauthenticated", 401)
		return true
	}
	credential := strings.TrimPrefix(values[0], "Bearer ")
	principal, err := handler.gateway.config.Identity.Authenticate(credential)
	if err != nil || principal.Subject == "" || principal.Project == "" || principal.ExpiresMS > 0 && principal.ExpiresMS <= time.Now().UnixMilli() || handler.gateway.authorize(principal, database) != nil {
		http.Error(writer, "forbidden", 403)
		return true
	}
	digest := sha256.Sum256([]byte(credential))
	var stream *hranaStream
	if baton == "" {
		handler.mu.Lock()
		if len(handler.streams)+handler.reserved >= maxHranaStreams {
			handler.mu.Unlock()
			http.Error(writer, "Hrana stream capacity reached", http.StatusTooManyRequests)
			return true
		}
		handler.reserved++
		handler.mu.Unlock()
		defer func() { handler.mu.Lock(); handler.reserved--; handler.mu.Unlock() }()
		code, result, failure := handler.call(request, credential, "/v1/sql/open", map[string]any{"database": database})
		if failure != "" || code != http.StatusOK {
			http.Error(writer, "cursor session unavailable", code)
			return true
		}
		var opened struct {
			Session string `json:"session"`
		}
		if json.Unmarshal(result, &opened) != nil || !publicSessionID(opened.Session) {
			http.Error(writer, "cursor session outcome unknown", 502)
			return true
		}
		baton, err = controller.RandomID()
		if err != nil {
			handler.cleanupRetiredSession(opened.Session, 0)
			http.Error(writer, "cursor baton unavailable", 503)
			return true
		}
		stream = &hranaStream{session: opened.Session, database: database, credential: digest, storedSQL: make(map[int]string)}
		stream.mu.Lock()
		handler.mu.Lock()
		handler.streams[baton] = stream
		handler.mu.Unlock()
	} else {
		handler.mu.Lock()
		stream = handler.streams[baton]
		handler.mu.Unlock()
		if stream == nil || stream.database != database || stream.credential != digest {
			http.Error(writer, "invalid baton", 403)
			return true
		}
		if !stream.mu.TryLock() {
			http.Error(writer, "baton busy; do not retry SQL", 409)
			return true
		}
		handler.mu.Lock()
		current := handler.streams[baton] == stream
		handler.mu.Unlock()
		if !current || stream.timer != nil && !time.Now().Before(stream.expiresAt) {
			handler.expireStreamLocked(baton, stream)
			stream.mu.Unlock()
			http.Error(writer, "expired baton", 403)
			return true
		}
	}
	active := false
	activeWrite := false
	failed := false
	openedStream := input["baton"] == nil || input["baton"] == ""
	sentHeader := false
	defer func() {
		if active {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			code, _, failure := handler.call(request.WithContext(cleanup), credential, "/v1/sql/cursor/close", map[string]any{"session": stream.session, "write_cursor": activeWrite})
			if failure != "" || code != http.StatusOK {
				log.Printf("Hrana native cursor cleanup not proven: status=%d code=%s", code, failure)
				failed = true
			}
		}
		if failed || request.Context().Err() != nil || openedStream && !sentHeader {
			handler.expireStreamLocked(baton, stream)
		} else {
			handler.refreshIdleStream(baton, stream)
		}
		stream.mu.Unlock()
	}()
	writeFrame := func(payload []byte) error {
		frame := append(hranaProtoVar(uint64(len(payload))), payload...)
		_, err := writer.Write(frame)
		return err
	}
	results := make([]any, len(readSteps))
	failures := make([]any, len(readSteps))
	startResponse := func() bool {
		if sentHeader {
			return true
		}
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
		sentHeader = true
		if writeFrame(hranaProtoString(1, baton)) != nil {
			failed = true
			return false
		}
		return true
	}
	for index, step := range readSteps {
		allowed, failure := handler.readCursorCondition(request, credential, stream.session, step.Condition, results, failures)
		if failure != "" {
			failed = true
			if !sentHeader {
				http.Error(writer, "cursor condition unavailable", 502)
			}
			return true
		}
		if !allowed {
			continue
		}
		statement := step.Stmt
		if statement.SQL == "" {
			statement.SQL = stream.storedSQL[statement.SQLID]
		}
		readonly := false
		if statement.WantRows {
			var unknown bool
			readonly, failure, unknown = handler.cursorStatementReadonly(request, credential, stream.session, statement.SQL)
			if failure != "" {
				if unknown {
					failed = true
					if !sentHeader {
						http.Error(writer, "cursor describe outcome unknown", http.StatusBadGateway)
					}
					return true
				}
				if !startResponse() {
					return true
				}
				frame, err := hranaProtoCursorStreamEntry(map[string]any{"type": "step_error", "step": index, "error": map[string]any{"message": failure, "code": failure}})
				if err != nil || writeFrame(frame) != nil {
					failed = true
					return true
				}
				failures[index] = true
				continue
			}
		}
		if !readonly && !statement.WantRows {
			value, failure, unknown := handler.execute(request, credential, stream.session, statement)
			if unknown {
				failed = true
				if !sentHeader {
					http.Error(writer, "cursor step outcome unknown", http.StatusBadGateway)
				}
				return true
			}
			if failure != "" {
				if !startResponse() {
					return true
				}
				frame, err := hranaProtoCursorStreamEntry(map[string]any{"type": "step_error", "step": index, "error": map[string]any{"message": failure, "code": failure}})
				if err != nil || writeFrame(frame) != nil {
					failed = true
					return true
				}
				failures[index] = true
				continue
			}
			entries, err := hranaNativeStatementCursorEntries(value, index)
			if err != nil {
				failed = true
				if !sentHeader {
					http.Error(writer, "invalid native cursor step", http.StatusBadGateway)
				}
				return true
			}
			if !startResponse() {
				return true
			}
			for _, entry := range entries {
				frame, err := hranaProtoCursorStreamEntry(entry)
				if err != nil || writeFrame(frame) != nil {
					failed = true
					return true
				}
			}
			results[index] = true
			continue
		}
		args := make([]sqlArgument, len(statement.Args))
		for index, argument := range statement.Args {
			args[index] = sqlArgument{Type: argument.Type, Value: argument.Value, Base64: argument.Base64}
		}
		namedArgs := make([]namedSQLArgument, len(statement.NamedArgs))
		for index, argument := range statement.NamedArgs {
			namedArgs[index] = namedSQLArgument{Name: argument.Name, Value: sqlArgument{Type: argument.Value.Type, Value: argument.Value.Value, Base64: argument.Value.Base64}}
		}
		writeCursor := !readonly
		code, result, failure := handler.call(request, credential, "/v1/sql/cursor/open", map[string]any{"session": stream.session, "sql": statement.SQL, "args": args, "named_args": namedArgs, "write_cursor": writeCursor})
		if failure != "" || code != http.StatusOK {
			if failure != "" && code < 500 && failure != "outcome_unknown" {
				encoded, err := hranaProtoError(map[string]any{"message": failure, "code": failure})
				if err != nil || !startResponse() {
					failed = true
					return true
				}
				if writeFrame(hranaProtoBytes(3, append(hranaProtoNumber(1, uint64(index)), hranaProtoBytes(2, encoded)...))) != nil {
					failed = true
					return true
				}
				failures[index] = true
				continue
			}
			failed = sentHeader
			if !sentHeader {
				http.Error(writer, "cursor open failed: "+failure, http.StatusBadGateway)
			}
			return true
		}
		active = true
		activeWrite = writeCursor
		var metadata struct {
			Cols []any `json:"cols"`
		}
		if json.Unmarshal(result, &metadata) != nil || metadata.Cols == nil {
			failed = true
			if !sentHeader {
				http.Error(writer, "invalid cursor metadata", 502)
			}
			return true
		}
		columns, err := hranaProtoColumns(metadata.Cols, 2)
		if err != nil {
			failed = true
			if !sentHeader {
				http.Error(writer, "invalid cursor columns", 502)
			}
			return true
		}
		if !startResponse() {
			return true
		}
		if writeFrame(hranaProtoBytes(1, append(hranaProtoNumber(1, uint64(index)), columns...))) != nil {
			failed = true
			return true
		}
		for {
			if request.Context().Err() != nil {
				failed = true
				return true
			}
			code, result, failure = handler.call(request, credential, "/v1/sql/cursor/fetch", map[string]any{"session": stream.session, "max_rows": 256, "write_cursor": writeCursor})
			if failure != "" || code != http.StatusOK {
				log.Printf("Hrana native cursor fetch failed: step=%d status=%d code=%s", index, code, failure)
				failed = true
				return true
			}
			var page struct {
				Rows             [][]any `json:"rows"`
				Done             bool    `json:"done"`
				AffectedRowCount *uint32 `json:"affected_row_count"`
				LastInsertRowid  *int64  `json:"last_insert_rowid"`
			}
			decoder := json.NewDecoder(bytes.NewReader(result))
			decoder.UseNumber()
			if decoder.Decode(&page) != nil || page.Rows == nil {
				log.Printf("Hrana native cursor fetch returned invalid page: step=%d", index)
				failed = true
				return true
			}
			for _, row := range page.Rows {
				var encoded []byte
				for _, cell := range row {
					value, valid := hranaOutputValue(cell)
					if !valid {
						failed = true
						return true
					}
					binary, err := hranaProtoResultValue(value)
					if err != nil {
						failed = true
						return true
					}
					encoded = hranaProtoAppend(encoded, 1, binary)
				}
				if writeFrame(hranaProtoBytes(4, encoded)) != nil {
					failed = true
					return true
				}
			}
			if page.Done {
				active = false
				affected := uint64(0)
				var lastRowid any
				if writeCursor {
					if page.AffectedRowCount == nil {
						failed = true
						return true
					}
					affected = uint64(*page.AffectedRowCount)
					if page.LastInsertRowid != nil {
						lastRowid = strconv.FormatInt(*page.LastInsertRowid, 10)
					}
				}
				frame, err := hranaProtoCursorStreamEntry(map[string]any{"type": "step_end", "affected_row_count": json.Number(strconv.FormatUint(affected, 10)), "last_insert_rowid": lastRowid})
				if err != nil || writeFrame(frame) != nil {
					failed = true
					return true
				}
				results[index] = true
				break
			}
		}
	}
	if !sentHeader {
		startResponse()
	}
	return true
}
