package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	controller "peri-loom/controller"
)

type hranaJSONCursorReply struct {
	header http.Header
	status int
	body   bytes.Buffer
	tooBig bool
}

func (reply *hranaJSONCursorReply) Header() http.Header { return reply.header }

func (reply *hranaJSONCursorReply) WriteHeader(status int) {
	if reply.status == 0 {
		reply.status = status
	}
}

func (reply *hranaJSONCursorReply) Write(data []byte) (int, error) {
	if reply.status == 0 {
		reply.status = http.StatusOK
	}
	if reply.tooBig {
		return len(data), nil
	}
	if len(data) > sqlWireLimit-reply.body.Len() {
		_, _ = reply.body.Write(data[:sqlWireLimit-reply.body.Len()])
		reply.tooBig = true
		return len(data), nil
	}
	_, _ = reply.body.Write(data)
	return len(data), nil
}

func hranaJSONCursorObject(raw json.RawMessage, keys ...string) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, false
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		key, ok := token.(string)
		if !ok {
			return nil, false
		}
		allowed := false
		for _, candidate := range keys {
			if key == candidate {
				allowed = true
				break
			}
		}
		if !allowed || object[key] != nil {
			return nil, false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, false
		}
		object[key] = value
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') || decoder.Decode(new(any)) != io.EOF {
		return nil, false
	}
	return object, true
}

func hranaJSONCursorArgument(raw json.RawMessage) bool {
	fields, ok := hranaJSONCursorObject(raw, "type", "value", "base64")
	if !ok {
		return false
	}
	var argument hranaValue
	if json.Unmarshal(raw, &argument) != nil {
		return false
	}
	if fields["type"] == nil || string(fields["type"]) == "null" {
		return false
	}
	if argument.Type == "float" || argument.Type == "text" || argument.Type == "integer" {
		if len(fields["value"]) == 0 || string(fields["value"]) == "null" {
			return false
		}
	}
	if value, present := fields["base64"]; present && (string(value) == "null" || json.Unmarshal(value, new(string)) != nil) {
		return false
	}
	return validSQLArguments([]sqlArgument{{Type: argument.Type, Value: argument.Value, Base64: argument.Base64}})
}

func hranaJSONCursorStatement(raw json.RawMessage) bool {
	fields, ok := hranaJSONCursorObject(raw, "sql", "sql_id", "args", "named_args", "want_rows")
	if !ok {
		return false
	}
	var statement hranaStatement
	if json.Unmarshal(raw, &statement) != nil {
		return false
	}
	if sql, present := fields["sql"]; present && (string(sql) == "null" || json.Unmarshal(sql, new(string)) != nil || len(statement.SQL) > 65536) {
		return false
	}
	if id, present := fields["sql_id"]; present && (string(id) == "null" || json.Unmarshal(id, new(int)) != nil || statement.SQLID < 0) {
		return false
	}
	if statement.SQL == "" && len(fields["sql_id"]) == 0 {
		return false
	}
	if rows, present := fields["want_rows"]; present && (string(rows) == "null" || json.Unmarshal(rows, new(bool)) != nil) {
		return false
	}
	if args, present := fields["args"]; present && (len(args) == 0 || args[0] != '[') {
		return false
	}
	if args, present := fields["named_args"]; present && (len(args) == 0 || args[0] != '[') {
		return false
	}
	if len(statement.Args)+len(statement.NamedArgs) > 999 {
		return false
	}
	var args []json.RawMessage
	if len(fields["args"]) != 0 {
		if json.Unmarshal(fields["args"], &args) != nil {
			return false
		}
		for _, argument := range args {
			if !hranaJSONCursorArgument(argument) {
				return false
			}
		}
	}
	if len(fields["named_args"]) != 0 {
		if json.Unmarshal(fields["named_args"], &args) != nil {
			return false
		}
		for _, argument := range args {
			entry, ok := hranaJSONCursorObject(argument, "name", "value")
			if !ok || !hranaJSONCursorArgument(entry["value"]) {
				return false
			}
			var name string
			if json.Unmarshal(entry["name"], &name) != nil || name == "" || len(name) > 256 {
				return false
			}
		}
	}
	return true
}

func hranaJSONCursorEntries(data []byte, count int) (any, []any, bool) {
	var response struct {
		Baton   *string `json:"baton"`
		Results []struct {
			Type     string `json:"type"`
			Response struct {
				Type   string `json:"type"`
				Result struct {
					StepResults []json.RawMessage `json:"step_results"`
					StepErrors  []json.RawMessage `json:"step_errors"`
				} `json:"result"`
			} `json:"response"`
		} `json:"results"`
	}
	if json.Unmarshal(data, &response) != nil || len(response.Results) != 1 || response.Results[0].Type != "ok" || response.Results[0].Response.Type != "batch" {
		return nil, nil, false
	}
	result := response.Results[0].Response.Result
	if len(result.StepResults) != count || len(result.StepErrors) != count {
		return nil, nil, false
	}
	entries := make([]any, 0, count*2)
	for index := range result.StepResults {
		step, failure := result.StepResults[index], result.StepErrors[index]
		if len(step) == 0 || len(failure) == 0 {
			return nil, nil, false
		}
		if string(failure) != "null" {
			if string(step) != "null" {
				return nil, nil, false
			}
			var issue struct {
				Message string  `json:"message"`
				Code    *string `json:"code"`
			}
			if json.Unmarshal(failure, &issue) != nil || issue.Message == "" {
				return nil, nil, false
			}
			entries = append(entries, map[string]any{"type": "step_error", "step": index, "error": json.RawMessage(failure)})
			continue
		}
		if string(step) == "null" {
			continue
		}
		var value struct {
			Cols []struct {
				Name     *string `json:"name"`
				Decltype *string `json:"decltype"`
			} `json:"cols"`
			Rows             [][]json.RawMessage `json:"rows"`
			AffectedRowCount *uint64             `json:"affected_row_count"`
			LastInsertRowid  *string             `json:"last_insert_rowid"`
		}
		if json.Unmarshal(step, &value) != nil || value.Cols == nil || value.Rows == nil || value.AffectedRowCount == nil {
			return nil, nil, false
		}
		for _, row := range value.Rows {
			if len(row) != len(value.Cols) {
				return nil, nil, false
			}
			for _, cell := range row {
				var item any
				decoder := json.NewDecoder(bytes.NewReader(cell))
				decoder.UseNumber()
				if decoder.Decode(&item) != nil {
					return nil, nil, false
				}
				if _, err := hranaProtoResultValue(item); err != nil {
					return nil, nil, false
				}
			}
		}
		var rowid any
		if value.LastInsertRowid != nil {
			if _, err := strconv.ParseInt(*value.LastInsertRowid, 10, 64); err != nil {
				return nil, nil, false
			}
			rowid = json.Number(*value.LastInsertRowid)
		}
		entries = append(entries, map[string]any{"type": "step_begin", "step": index, "cols": value.Cols})
		for _, row := range value.Rows {
			entries = append(entries, map[string]any{"type": "row", "row": row})
		}
		entries = append(entries, map[string]any{"type": "step_end", "affected_row_count": value.AffectedRowCount, "last_insert_rowid": rowid})
	}
	return response.Baton, entries, true
}

func (handler *hranaHTTP) retireJSONCursorBaton(reply *hranaJSONCursorReply, previous *string) {
	identifier := ""
	if previous != nil {
		identifier = *previous
	} else {
		decoder := json.NewDecoder(bytes.NewReader(reply.body.Bytes()))
		if token, err := decoder.Token(); err == nil && token == json.Delim('{') {
			if key, err := decoder.Token(); err == nil && key == "baton" {
				_ = decoder.Decode(&identifier)
			}
		}
	}
	if identifier == "" {
		return
	}
	handler.mu.Lock()
	stream := handler.streams[identifier]
	handler.mu.Unlock()
	if stream == nil {
		return
	}
	stream.mu.Lock()
	handler.expireStreamLocked(identifier, stream)
	stream.mu.Unlock()
}

func (handler *hranaHTTP) servePagedJSONCursor(writer http.ResponseWriter, request *http.Request, database controller.DatabaseID, baton *string, steps []hranaReadCursorStep) {
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		http.Error(writer, "unauthenticated", 401)
		return
	}
	credential := strings.TrimPrefix(values[0], "Bearer ")
	principal, err := handler.gateway.config.Identity.Authenticate(credential)
	if err != nil || principal.Subject == "" || principal.Project == "" || principal.ExpiresMS > 0 && principal.ExpiresMS <= time.Now().UnixMilli() || handler.gateway.authorize(principal, database) != nil {
		http.Error(writer, "forbidden", 403)
		return
	}
	digest := sha256.Sum256([]byte(credential))
	identifier := ""
	if baton != nil {
		identifier = *baton
	}
	var stream *hranaStream
	if identifier == "" {
		handler.mu.Lock()
		if len(handler.streams)+handler.reserved >= maxHranaStreams {
			handler.mu.Unlock()
			http.Error(writer, "Hrana stream capacity reached", 429)
			return
		}
		handler.reserved++
		handler.mu.Unlock()
		defer func() {
			handler.mu.Lock()
			handler.reserved--
			handler.mu.Unlock()
		}()
		reply := handler.call(request, credential, "/v1/sql/open", map[string]any{"database": database})
		if reply.code != "" || reply.status != http.StatusOK {
			http.Error(writer, "cursor session unavailable: "+reply.code, reply.status)
			return
		}
		var opened struct {
			Session string `json:"session"`
		}
		if json.Unmarshal(reply.result, &opened) != nil {
			http.Error(writer, "cursor session outcome unknown", 502)
			return
		}
		if !publicSessionID(opened.Session) {
			// A parsed session the public id cannot address is still the only
			// handle for retiring it; keep the reply unchanged but retire it.
			if opened.Session != "" {
				handler.cleanupRetiredSession(opened.Session, 0)
			}
			http.Error(writer, "cursor session outcome unknown", 502)
			return
		}
		identifier, err = controller.RandomID()
		if err != nil {
			handler.cleanupRetiredSession(opened.Session, 0)
			http.Error(writer, "cursor baton unavailable", 503)
			return
		}
		stream = &hranaStream{session: opened.Session, database: database, credential: digest, storedSQL: make(map[int]string)}
		stream.mu.Lock()
		handler.mu.Lock()
		handler.streams[identifier] = stream
		handler.mu.Unlock()
	} else {
		handler.mu.Lock()
		stream = handler.streams[identifier]
		handler.mu.Unlock()
		if stream == nil || stream.database != database || stream.credential != digest {
			http.Error(writer, "invalid baton", 403)
			return
		}
		if !stream.mu.TryLock() {
			http.Error(writer, "baton busy; do not retry SQL", 409)
			return
		}
		handler.mu.Lock()
		current := handler.streams[identifier] == stream
		handler.mu.Unlock()
		if !current || stream.timer != nil && !time.Now().Before(stream.expiresAt) {
			handler.expireStreamLocked(identifier, stream)
			stream.mu.Unlock()
			http.Error(writer, "expired baton", 403)
			return
		}
	}
	started, failed, active, activeWrite := false, false, false, false
	writeCommitted, clientStopped := false, false
	defer func() {
		if request.Context().Err() != nil {
			clientStopped = true
		}
		closedNative := false
		if active {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cleanupInput := map[string]any{"session": stream.session, "write_cursor": activeWrite}
			if activeWrite {
				cleanupInput["cursor_dispatch_stopped"] = true
				if clientStopped {
					cleanupInput["cursor_client_stopped"] = true
				}
			}
			reply := handler.call(request.WithContext(cleanup), credential, "/v1/sql/cursor/close", cleanupInput)
			closedNative = reply.code == "" && reply.status == http.StatusOK
			if !closedNative {
				log.Printf("Hrana JSON cursor cleanup not proven: status=%d code=%s client_stopped=%t dispatch_stopped=true committed=%t", reply.status, reply.code, clientStopped, writeCommitted)
				failed = true
			}
		}
		if activeWrite && clientStopped {
			outcome := "unknown"
			if writeCommitted {
				outcome = "committed"
			} else if closedNative {
				// The close was acknowledged; only the gateway's operation record
				// knows whether the worker proved an abort.
				outcome = "close_acknowledged"
			}
			log.Printf("Hrana JSON write cursor abort: native_outcome=%s committed=%t", outcome, writeCommitted)
		}
		if failed || request.Context().Err() != nil || baton == nil && !started {
			handler.expireStreamLocked(identifier, stream)
		} else {
			handler.refreshIdleStream(identifier, stream)
		}
		stream.mu.Unlock()
	}()
	encoder := json.NewEncoder(writer)
	var rowEncoder hranaJSONCursorRow
	flusher, _ := writer.(http.Flusher)
	start := func() bool {
		if started {
			return true
		}
		writer.Header().Set("Content-Type", "application/x-ndjson")
		writer.WriteHeader(http.StatusOK)
		started = true
		if encoder.Encode(map[string]any{"baton": identifier, "base_url": nil}) != nil {
			clientStopped = true
			failed = true
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	write := func(entry any) bool {
		if !start() {
			return false
		}
		if encoder.Encode(entry) != nil {
			clientStopped = true
			failed = true
			return false
		}
		return true
	}
	fatal := func(message string) {
		failed = true
		if !started {
			http.Error(writer, "cursor outcome unknown: "+message, 502)
		} else {
			_ = encoder.Encode(map[string]any{"type": "error", "error": map[string]any{"message": message, "code": "outcome_unknown"}})
		}
	}
	writeRow := func(cells []any) bool {
		if !start() {
			return false
		}
		rowEncoder.begin()
		if err := rowEncoder.appendCells(cells); err != nil {
			if errors.Is(err, errHranaJSONCursorValue) {
				fatal("invalid cursor value")
			} else {
				clientStopped = true
				failed = true
			}
			return false
		}
		if _, err := writer.Write(rowEncoder.bytes()); err != nil {
			clientStopped = true
			failed = true
			return false
		}
		return true
	}
	nativeFailure := func(reply hranaReply) bool {
		if reply.failure == nil || reply.failure.OperationID() == "" {
			return false
		}
		failed = true
		if !started {
			// A fused first page can already have committed before the first
			// Hrana frame. Keep its durable query handle in this response too.
			hranaPipelineFailure(writer, identifier, true, reply)
		} else {
			_ = encoder.Encode(map[string]any{"type": "error", "error": hranaFailureFields(reply, true)})
		}
		return true
	}
	results, failures := make([]any, len(steps)), make([]any, len(steps))
	// This stays on the stack under stream.mu: consume it at the next step,
	// including a skipped or failed step, and never carry it to another HTTP batch.
	var nextAutocommit *bool
	for index, step := range steps {
		if request.Context().Err() != nil {
			failed = true
			return
		}
		snapshot := nextAutocommit
		nextAutocommit = nil
		allowed, conditionReply := handler.readCursorConditionSnapshot(request, credential, stream.session, step.Condition, results, failures, snapshot)
		if conditionReply.code != "" {
			fatal("cursor condition unavailable: " + conditionReply.code)
			return
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
			var describeReply hranaReply
			var unknown bool
			readonly, describeReply, unknown = handler.cursorStatementReadonly(request, credential, stream.session, statement.SQL)
			if describeReply.code != "" {
				if unknown {
					fatal("cursor describe failed: " + describeReply.code)
					return
				}
				failures[index] = true
				if !write(map[string]any{"type": "step_error", "step": index, "error": map[string]any{"message": hranaErrorMessage(describeReply), "code": describeReply.code}}) {
					return
				}
				continue
			}
		}
		if !readonly && !statement.WantRows {
			value, executeReply := handler.execute(request, credential, stream.session, statement)
			if executeReply.unknown {
				fatal("cursor step failed: " + executeReply.code)
				return
			}
			if executeReply.code != "" {
				failures[index] = true
				if !write(map[string]any{"type": "step_error", "step": index, "error": map[string]any{"message": hranaErrorMessage(executeReply), "code": executeReply.code}}) {
					return
				}
				continue
			}
			entries, err := hranaNativeStatementCursorEntries(value, index)
			if err != nil {
				fatal("invalid cursor statement result")
				return
			}
			for _, entry := range entries {
				if !write(entry) {
					return
				}
			}
			results[index] = true
			continue
		}
		args := make([]sqlArgument, len(statement.Args))
		for position, argument := range statement.Args {
			args[position] = sqlArgument{Type: argument.Type, Value: argument.Value, Base64: argument.Base64}
		}
		namedArgs := make([]namedSQLArgument, len(statement.NamedArgs))
		for position, argument := range statement.NamedArgs {
			namedArgs[position] = namedSQLArgument{Name: argument.Name, Value: sqlArgument{Type: argument.Value.Type, Value: argument.Value.Value, Base64: argument.Value.Base64}}
		}
		writing := !readonly
		reply := handler.startCursor(request, credential, map[string]any{"session": stream.session, "sql": statement.SQL, "args": args, "named_args": namedArgs, "write_cursor": writing, "max_rows": 256})
		if reply.code != "" || reply.status != http.StatusOK {
			if reply.unknown {
				if !nativeFailure(reply) {
					fatal("cursor first page failed: " + reply.code)
				}
				return
			}
			if reply.failure != nil && reply.failure.Executed() {
				// The first fetch still ends the batch on a stepped SQL error.
				// No successful metadata was returned, so send the protocol's
				// global cursor error after its header, retaining the native proof.
				failed = true
				if start() {
					_ = encoder.Encode(map[string]any{"type": "error", "error": hranaSettledFailureFields(reply, true)})
				}
				return
			}
			if reply.code != "" && !reply.unknown {
				failures[index] = true
				if !write(map[string]any{"type": "step_error", "step": index, "error": map[string]any{"message": hranaErrorMessage(reply), "code": reply.code}}) {
					return
				}
				continue
			}
			fatal("cursor open failed: " + reply.code)
			return
		}
		var metadata struct {
			Cols       []json.RawMessage `json:"cols"`
			Done       bool              `json:"done"`
			Autocommit *bool             `json:"autocommit"`
		}
		if json.Unmarshal(reply.result, &metadata) != nil || metadata.Cols == nil {
			fatal("invalid cursor metadata")
			return
		}
		active, activeWrite = !metadata.Done, writing
		if writing {
			writeCommitted = metadata.Done
		}
		if !write(map[string]any{"type": "step_begin", "step": index, "cols": metadata.Cols}) {
			return
		}
		firstPage := true
		for {
			if request.Context().Err() != nil {
				clientStopped = true
				failed = true
				return
			}
			if !firstPage {
				reply = handler.call(request, credential, "/v1/sql/cursor/fetch", map[string]any{"session": stream.session, "max_rows": 256, "write_cursor": writing})
			}
			firstPage = false
			if reply.code != "" || reply.status != http.StatusOK {
				if request.Context().Err() != nil {
					clientStopped = true
				}
				if nativeFailure(reply) {
					// The stream can no longer promise a settled write. Publish the
					// platform's operation so the client can query what is owed
					// instead of retrying.
					return
				}
				fatal("cursor fetch failed: " + reply.code)
				return
			}
			var page struct {
				Rows             [][]any `json:"rows"`
				Done             bool    `json:"done"`
				AffectedRowCount *uint32 `json:"affected_row_count"`
				LastInsertRowid  *int64  `json:"last_insert_rowid"`
			}
			decoder := json.NewDecoder(bytes.NewReader(reply.result))
			decoder.UseNumber()
			if decoder.Decode(&page) != nil || page.Rows == nil || len(page.Rows) > 256 || len(page.Rows) == 0 && !page.Done {
				fatal("invalid cursor page")
				return
			}
			if writing && page.Done {
				writeCommitted = true
			}
			for _, row := range page.Rows {
				if len(row) != len(metadata.Cols) {
					fatal("invalid cursor row")
					return
				}
				if !writeRow(row) {
					return
				}
			}
			if flusher != nil {
				flusher.Flush()
			}
			if page.Done {
				active = false
				affected := uint32(0)
				var rowid any
				if writing {
					if page.AffectedRowCount == nil {
						fatal("write cursor result unknown")
						return
					}
					affected = *page.AffectedRowCount
					if page.LastInsertRowid != nil {
						rowid = json.Number(strconv.FormatInt(*page.LastInsertRowid, 10))
					}
				}
				if !write(map[string]any{"type": "step_end", "affected_row_count": affected, "last_insert_rowid": rowid}) {
					return
				}
				results[index] = true
				if metadata.Done {
					nextAutocommit = metadata.Autocommit
				}
				break
			}
		}
	}
	if !start() {
		return
	}
	if flusher != nil {
		flusher.Flush()
	}
}

func serveHranaV3JSONCursor(writer http.ResponseWriter, request *http.Request, handler *hranaHTTP) bool {
	parts := strings.Split(request.URL.Path, "/")
	if len(parts) != 7 || parts[0] != "" || parts[1] != "db" || parts[5] != "v3" || parts[6] != "cursor" {
		return false
	}
	if request.Method != http.MethodPost || request.URL.RawQuery != "" {
		http.NotFound(writer, request)
		return true
	}
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: parts[2], Database: parts[3]}, Branch: parts[4]}
	if database.Validate() != nil {
		http.Error(writer, "invalid database", 400)
		return true
	}
	if request.Header.Get("Content-Type") != "application/json" {
		http.Error(writer, "JSON required", 415)
		return true
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, sqlWireLimit))
	if err != nil {
		http.Error(writer, "request limit", 413)
		return true
	}
	fields, ok := hranaJSONCursorObject(body, "baton", "batch")
	if !ok {
		http.Error(writer, "invalid cursor", 400)
		return true
	}
	var baton *string
	if raw, present := fields["baton"]; present && json.Unmarshal(raw, &baton) != nil {
		http.Error(writer, "invalid baton", 400)
		return true
	}
	if baton != nil && *baton == "" {
		http.Error(writer, "invalid baton", 400)
		return true
	}
	batch, ok := hranaJSONCursorObject(fields["batch"], "steps")
	var steps []json.RawMessage
	if !ok || json.Unmarshal(batch["steps"], &steps) != nil || len(steps) == 0 || len(steps) > 64 {
		http.Error(writer, "invalid cursor batch", 400)
		return true
	}
	readSteps := make([]hranaReadCursorStep, 0, len(steps))
	paged := false
	for index, raw := range steps {
		step, valid := hranaJSONCursorObject(raw, "stmt", "condition")
		if !valid || !hranaJSONCursorStatement(step["stmt"]) || len(step["condition"]) != 0 && !hranaValidCondition(step["condition"], index) {
			http.Error(writer, "invalid cursor step", 400)
			return true
		}
		var readStep hranaReadCursorStep
		if json.Unmarshal(raw, &readStep) != nil {
			http.Error(writer, "invalid cursor step", 400)
			return true
		}
		paged = paged || readStep.Stmt.WantRows
		readSteps = append(readSteps, readStep)
	}
	if paged {
		handler.servePagedJSONCursor(writer, request, database, baton, readSteps)
		return true
	}
	encoded, err := json.Marshal(map[string]any{"baton": baton, "requests": []any{map[string]any{"type": "batch", "batch": json.RawMessage(fields["batch"])}}})
	if err != nil || len(encoded) > sqlWireLimit {
		http.Error(writer, "request limit", 413)
		return true
	}
	forwarded := request.Clone(request.Context())
	forwarded.URL.Path = strings.Join(parts[:5], "/") + "/v2/pipeline"
	forwarded.Body = io.NopCloser(bytes.NewReader(encoded))
	forwarded.ContentLength = int64(len(encoded))
	forwarded.Header = request.Header.Clone()
	forwarded.Header.Set("Content-Type", "application/json")
	reply := &hranaJSONCursorReply{header: make(http.Header)}
	handler.ServeHTTP(reply, forwarded)
	if reply.tooBig {
		handler.retireJSONCursorBaton(reply, baton)
		http.Error(writer, "cursor response limit; SQL outcome unknown", 502)
		return true
	}
	if reply.status != http.StatusOK {
		if reply.status >= 500 {
			handler.retireJSONCursorBaton(reply, baton)
		}
		writer.WriteHeader(reply.status)
		_, _ = writer.Write(reply.body.Bytes())
		return true
	}
	newBaton, entries, valid := hranaJSONCursorEntries(reply.body.Bytes(), len(steps))
	if !valid {
		handler.retireJSONCursorBaton(reply, baton)
		http.Error(writer, "invalid cursor backend response; SQL outcome unknown", 502)
		return true
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	if encoder.Encode(map[string]any{"baton": newBaton, "base_url": nil}) != nil {
		http.Error(writer, "cursor encoding failed", 502)
		return true
	}
	for _, entry := range entries {
		if encoder.Encode(entry) != nil {
			http.Error(writer, "cursor encoding failed", 502)
			return true
		}
	}
	writer.Header().Set("Content-Type", "application/x-ndjson")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(output.Bytes())
	return true
}
