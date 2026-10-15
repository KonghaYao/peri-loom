package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
)

type hranaWSCursor struct {
	streamID int
	entries  []map[string]any
	native   bool
	session  string
	ended    bool
	active   bool
	write    bool
	steps    []hranaReadCursorStep
	step     int
	results  []any
	failures []any
}

type hranaReadCursorStep struct {
	Stmt      hranaStatement  `json:"stmt"`
	Condition json.RawMessage `json:"condition,omitempty"`
}

func (handler *hranaHTTP) readCursorCondition(request *http.Request, credential, session string, condition json.RawMessage, results, failures []any) (bool, string) {
	if len(condition) == 0 {
		return true, ""
	}
	isAutocommit := false
	if hranaConditionNeedsAutocommit(condition) {
		var failure string
		isAutocommit, failure = handler.autocommit(request, credential, session)
		if failure != "" {
			return false, failure
		}
	}
	return hranaCondition(condition, results, failures, isAutocommit), ""
}

func (handler *hranaHTTP) cursorStatementReadonly(request *http.Request, credential, session, sql string) (bool, string, bool) {
	code, result, failure := handler.probe(request, credential, "/v1/sql/describe", map[string]any{"session": session, "sql": sql})
	if failure != "" {
		return false, failure, code >= 500 || failure == "outcome_unknown"
	}
	if code != http.StatusOK || !validDescribeResult(result) {
		return false, "invalid_native_describe", true
	}
	var description struct {
		Readonly bool `json:"is_readonly"`
	}
	if json.Unmarshal(result, &description) != nil {
		return false, "invalid_native_describe", true
	}
	return description.Readonly, "", false
}

func (handler *hranaHTTP) wsStartReadStep(request *http.Request, credential string, cursor *hranaWSCursor) (string, bool) {
	for cursor.step < len(cursor.steps) {
		step := cursor.steps[cursor.step]
		allowed, failure := handler.readCursorCondition(request, credential, cursor.session, step.Condition, cursor.results, cursor.failures)
		if failure != "" {
			return failure, true
		}
		if allowed {
			readonly := false
			if step.Stmt.WantRows {
				var fatal bool
				readonly, failure, fatal = handler.cursorStatementReadonly(request, credential, cursor.session, step.Stmt.SQL)
				if failure != "" {
					if fatal {
						return failure, true
					}
					cursor.failures[cursor.step] = true
					cursor.entries = append(cursor.entries, map[string]any{"type": "step_error", "step": cursor.step, "error": map[string]any{"message": failure, "code": failure}})
					cursor.step++
					continue
				}
			}
			if !readonly && !step.Stmt.WantRows {
				value, failure, unknown := handler.execute(request, credential, cursor.session, step.Stmt)
				if failure != "" {
					if unknown {
						return failure, true
					}
					cursor.failures[cursor.step] = true
					cursor.entries = append(cursor.entries, map[string]any{"type": "step_error", "step": cursor.step, "error": map[string]any{"message": failure, "code": failure}})
				} else {
					entries, err := hranaNativeStatementCursorEntries(value, cursor.step)
					if err != nil {
						return "invalid_native_statement", true
					}
					cursor.results[cursor.step] = true
					cursor.entries = append(cursor.entries, entries...)
				}
				cursor.step++
				if len(cursor.entries) >= 1000 {
					return "", false
				}
				continue
			}
			cols, failure, fatal := handler.wsOpenReadStep(request, credential, cursor.session, step.Stmt, !readonly)
			if failure != "" {
				if fatal {
					return failure, true
				}
				cursor.failures[cursor.step] = true
				cursor.entries = append(cursor.entries, map[string]any{"type": "step_error", "step": cursor.step, "error": map[string]any{"message": failure, "code": failure}})
				cursor.step++
				continue
			}
			cursor.entries = append(cursor.entries, map[string]any{"type": "step_begin", "step": cursor.step, "cols": cols})
			cursor.active = true
			cursor.write = !readonly
			return "", false
		}
		cursor.step++
	}
	cursor.ended = true
	return "", false
}

func (handler *hranaHTTP) wsOpenReadStep(request *http.Request, credential, session string, statement hranaStatement, write bool) ([]any, string, bool) {
	args := make([]sqlArgument, len(statement.Args))
	for index, argument := range statement.Args {
		args[index] = sqlArgument{Type: argument.Type, Value: argument.Value, Base64: argument.Base64}
	}
	namedArgs := make([]namedSQLArgument, len(statement.NamedArgs))
	for index, argument := range statement.NamedArgs {
		namedArgs[index] = namedSQLArgument{Name: argument.Name, Value: sqlArgument{Type: argument.Value.Type, Value: argument.Value.Value, Base64: argument.Value.Base64}}
	}
	code, result, failure := handler.call(request, credential, "/v1/sql/cursor/open", map[string]any{"session": session, "sql": statement.SQL, "args": args, "named_args": namedArgs, "write_cursor": write})
	if failure != "" {
		return nil, failure, code >= 500 || failure == "outcome_unknown"
	}
	var metadata struct {
		Cols []any `json:"cols"`
	}
	if json.Unmarshal(result, &metadata) != nil || metadata.Cols == nil {
		return nil, "invalid_native_cursor", true
	}
	return metadata.Cols, "", false
}

func (handler *hranaHTTP) wsFetchCursor(request *http.Request, credential string, cursor *hranaWSCursor, maxCount int) (map[string]any, string, bool) {
	count := min(maxCount, 1000)
	if cursor.native && !cursor.active && !cursor.ended && len(cursor.entries) < count {
		failure, fatal := handler.wsStartReadStep(request, credential, cursor)
		if failure != "" {
			return nil, failure, fatal
		}
	}
	if cursor.native && cursor.active && !cursor.ended && len(cursor.entries) < count {
		pageSize := min(256, count-len(cursor.entries))
		code, result, failure := handler.call(request, credential, "/v1/sql/cursor/fetch", map[string]any{"session": cursor.session, "max_rows": pageSize, "write_cursor": cursor.write})
		if failure != "" {
			fatal := code >= 500 || failure == "outcome_unknown"
			if !fatal {
				cursor.ended = true
				cursor.entries = nil
			}
			return nil, failure, fatal
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
			return nil, "invalid_native_cursor_page", true
		}
		for _, row := range page.Rows {
			values := make([]any, 0, len(row))
			for _, value := range row {
				converted, valid := hranaOutputValue(value)
				if !valid {
					return nil, "invalid_native_cursor_value", true
				}
				values = append(values, converted)
			}
			cursor.entries = append(cursor.entries, map[string]any{"type": "row", "row": values})
		}
		if page.Done {
			cursor.active = false
			affected := json.Number("0")
			var lastRowid any
			if cursor.write {
				if page.AffectedRowCount == nil {
					return nil, "invalid_native_write_cursor_page", true
				}
				affected = json.Number(strconv.FormatUint(uint64(*page.AffectedRowCount), 10))
				if page.LastInsertRowid != nil {
					lastRowid = strconv.FormatInt(*page.LastInsertRowid, 10)
				}
			}
			cursor.entries = append(cursor.entries, map[string]any{"type": "step_end", "affected_row_count": affected, "last_insert_rowid": lastRowid})
			cursor.results[cursor.step] = true
			cursor.step++
			failure, fatal := handler.wsStartReadStep(request, credential, cursor)
			if failure != "" {
				if !fatal {
					cursor.ended = true
				}
				return nil, failure, fatal
			}
		}
	}
	entries := cursor.entries[:min(count, len(cursor.entries))]
	cursor.entries = cursor.entries[len(entries):]
	return map[string]any{"type": "fetch_cursor", "entries": entries, "done": len(cursor.entries) == 0 && (!cursor.native || cursor.ended)}, "", false
}

func hranaNativeStatementCursorEntries(value any, index int) ([]map[string]any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var statement map[string]any
	if decoder.Decode(&statement) != nil {
		return nil, errors.New("invalid native cursor statement")
	}
	entries, err := hranaWSCursorEntries(map[string]any{"type": "batch", "result": map[string]any{"step_results": []any{statement}, "step_errors": []any{nil}}})
	if err != nil {
		return nil, err
	}
	entries[0]["step"] = index
	return entries, nil
}

func hranaWSCursorEntries(response map[string]any) ([]map[string]any, error) {
	if response["type"] != "batch" {
		return nil, errors.New("invalid cursor batch")
	}
	result, ok := response["result"].(map[string]any)
	if !ok {
		return nil, errors.New("invalid cursor result")
	}
	steps, ok := result["step_results"].([]any)
	if !ok || len(steps) > 64 {
		return nil, errors.New("invalid cursor steps")
	}
	failures, ok := result["step_errors"].([]any)
	if !ok || len(failures) != len(steps) {
		return nil, errors.New("invalid cursor step failures")
	}
	entries := make([]map[string]any, 0, len(steps)*2)
	for index := range steps {
		if failures[index] != nil {
			failure, valid := failures[index].(map[string]any)
			if !valid {
				return nil, errors.New("invalid cursor failure")
			}
			entries = append(entries, map[string]any{"type": "step_error", "step": index, "error": failure})
			continue
		}
		if steps[index] == nil {
			continue
		}
		statement, valid := steps[index].(map[string]any)
		if !valid {
			return nil, errors.New("invalid cursor statement")
		}
		cols, valid := statement["cols"].([]any)
		if !valid {
			return nil, errors.New("invalid cursor columns")
		}
		entries = append(entries, map[string]any{"type": "step_begin", "step": index, "cols": cols})
		rows, valid := statement["rows"].([]any)
		if !valid || len(rows)+len(entries) > 20000 {
			return nil, errors.New("cursor result exceeds limit")
		}
		for _, row := range rows {
			if _, valid := row.([]any); !valid {
				return nil, errors.New("invalid cursor row")
			}
			entries = append(entries, map[string]any{"type": "row", "row": row})
		}
		affected, valid := statement["affected_row_count"].(json.Number)
		if !valid {
			return nil, errors.New("invalid cursor affected rows")
		}
		if _, err := strconv.ParseUint(string(affected), 10, 32); err != nil {
			return nil, errors.New("invalid cursor affected count")
		}
		entries = append(entries, map[string]any{"type": "step_end", "affected_row_count": affected, "last_insert_rowid": statement["last_insert_rowid"]})
	}
	return entries, nil
}

func hranaWSProtoFetch(response map[string]any) ([]byte, error) {
	entries, valid := response["entries"].([]map[string]any)
	if !valid {
		return nil, errors.New("invalid cursor fetch entries")
	}
	encoded := []byte{}
	for _, entry := range entries {
		frame, err := hranaProtoCursorStreamEntry(entry)
		if err != nil {
			return nil, err
		}
		encoded = hranaProtoAppend(encoded, 1, frame)
	}
	done, valid := response["done"].(bool)
	if !valid {
		return nil, errors.New("invalid cursor done")
	}
	if done {
		encoded = append(encoded, hranaProtoNumber(2, 1)...)
	} else {
		encoded = append(encoded, hranaProtoNumber(2, 0)...)
	}
	return encoded, nil
}

func hranaProtoCursorStreamEntry(entry map[string]any) ([]byte, error) {
	var field int
	var value []byte
	switch entry["type"] {
	case "step_begin":
		field = 1
		step, valid := entry["step"].(int)
		if !valid || step < 0 || step > 64 {
			return nil, errors.New("invalid cursor step")
		}
		value = hranaProtoNumber(1, uint64(step))
		cols, err := hranaProtoColumns(entry["cols"], 2)
		if err != nil {
			return nil, err
		}
		value = append(value, cols...)
	case "step_end":
		field = 2
		affected, err := hranaProtoUInt(entry["affected_row_count"])
		if err != nil || affected > math.MaxUint32 {
			return nil, errors.New("invalid cursor affected count")
		}
		value = hranaProtoNumber(1, affected)
		if text, valid := entry["last_insert_rowid"].(string); valid {
			rowid, err := strconv.ParseUint(text, 10, 64)
			if err != nil {
				return nil, err
			}
			value = append(value, hranaProtoNumber(2, rowid)...)
		}
	case "step_error":
		field = 3
		step, valid := entry["step"].(int)
		if !valid || step < 0 {
			return nil, errors.New("invalid cursor step")
		}
		failure, err := hranaProtoError(entry["error"])
		if err != nil {
			return nil, err
		}
		value = append(hranaProtoNumber(1, uint64(step)), hranaProtoBytes(2, failure)...)
	case "row":
		field = 4
		row, valid := entry["row"].([]any)
		if !valid {
			return nil, errors.New("invalid cursor row")
		}
		for _, cell := range row {
			encodedCell, err := hranaProtoResultValue(cell)
			if err != nil {
				return nil, err
			}
			value = hranaProtoAppend(value, 1, encodedCell)
		}
	default:
		return nil, errors.New("unsupported cursor entry")
	}
	return hranaProtoBytes(field, value), nil
}
