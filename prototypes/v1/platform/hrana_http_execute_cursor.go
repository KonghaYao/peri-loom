package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

func (handler *hranaHTTP) executeCursor(request *http.Request, credential, session, sql string, args []sqlArgument, namedArgs []namedSQLArgument, writing, discardRows bool) (value any, failure string, unknown bool) {
	reply := handler.call(request, credential, "/v1/sql/cursor/open", map[string]any{"session": session, "sql": sql, "args": args, "named_args": namedArgs, "write_cursor": writing})
	if reply.code != "" {
		return nil, reply.code, reply.unknown
	}
	var opened struct {
		Cols []struct {
			Name     string  `json:"name"`
			Decltype *string `json:"decltype"`
		} `json:"cols"`
	}
	active := true
	defer func() {
		if !active {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		close := handler.call(request.WithContext(cleanup), credential, "/v1/sql/cursor/close", map[string]any{"session": session, "write_cursor": writing})
		if close.code != "" || close.status != http.StatusOK {
			value, failure, unknown = nil, "cursor_cleanup_unknown", true
		}
	}()
	if reply.status != http.StatusOK || json.Unmarshal(reply.result, &opened) != nil || opened.Cols == nil {
		return nil, "invalid_native_cursor", true
	}
	cols := make([]any, len(opened.Cols))
	for index, column := range opened.Cols {
		cols[index] = map[string]any{"name": column.Name, "decltype": column.Decltype}
	}
	rows := make([][]any, 0)
	responseBytes := 0
	for {
		reply = handler.call(request, credential, "/v1/sql/cursor/fetch", map[string]any{"session": session, "max_rows": 256, "write_cursor": writing})
		if reply.code != "" {
			return nil, reply.code, writing || reply.unknown
		}
		if !discardRows {
			responseBytes += len(reply.result)
		}
		var page struct {
			Rows             [][]any `json:"rows"`
			Done             bool    `json:"done"`
			AffectedRowCount *uint32 `json:"affected_row_count"`
			LastInsertRowid  *int64  `json:"last_insert_rowid"`
		}
		decoder := json.NewDecoder(bytes.NewReader(reply.result))
		decoder.UseNumber()
		if reply.status != http.StatusOK || decoder.Decode(&page) != nil || page.Rows == nil || len(page.Rows) > 256 {
			return nil, "invalid_native_cursor", true
		}
		if !discardRows && (responseBytes > 8*1024*1024 || len(rows)+len(page.Rows) > 100_000) {
			if writing && page.Done {
				active = false
				return nil, "outcome_unknown", true
			}
			return nil, "result_limit", false
		}
		for _, row := range page.Rows {
			converted := make([]any, 0, len(row))
			for _, cell := range row {
				output, valid := hranaOutputValue(cell)
				if !valid {
					return nil, "invalid_native_value", true
				}
				converted = append(converted, output)
			}
			if !discardRows {
				rows = append(rows, converted)
			}
		}
		if page.Done {
			active = false
			var rowid *string
			affected := uint32(0)
			if writing {
				if page.AffectedRowCount == nil {
					return nil, "outcome_unknown", true
				}
				affected = *page.AffectedRowCount
				if page.LastInsertRowid != nil {
					text := strconv.FormatInt(*page.LastInsertRowid, 10)
					rowid = &text
				}
			}
			return map[string]any{"cols": cols, "rows": rows, "affected_row_count": affected, "last_insert_rowid": rowid}, "", false
		}
		if !discardRows && len(rows) == 100_000 {
			return nil, "result_limit", false
		}
	}
}
