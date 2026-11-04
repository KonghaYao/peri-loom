package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// Only a Go caller can set this marker. The public cursor/open contract still
// only prepares: an HTTP field or header cannot turn it into an execution.
type hranaCursorStartContextKey struct{}

func (handler *hranaHTTP) startCursor(request *http.Request, credential string, input any) hranaReply {
	ctx := context.WithValue(request.Context(), hranaCursorStartContextKey{}, true)
	return handler.call(request.WithContext(ctx), credential, "/v1/sql/cursor/open", input)
}

// validCursorFirstPage applies the first-page rules to the spans the strict
// reply scan already indexed, so the fused page is not parsed again: a completed
// cursor may carry a boolean autocommit snapshot, an active one only null, and
// every row has exactly one cell per column.
func validCursorFirstPage(page hranaCursorPage) bool {
	if !page.hasCols {
		return false
	}
	metadata := make([]byte, 0, len(page.cols)+10)
	metadata = append(metadata, `{"cols":`...)
	metadata = append(metadata, page.cols...)
	metadata = append(metadata, '}')
	if !validCursorOpenResult(metadata) {
		return false
	}
	if page.hasAutocommit {
		if page.done {
			if snapshot := string(page.autocommit); snapshot != "true" && snapshot != "false" {
				return false
			}
		} else if string(page.autocommit) != "null" {
			return false
		}
	}
	if page.rows == 0 && !page.done {
		return false
	}
	var columns []json.RawMessage
	_ = json.Unmarshal(page.cols, &columns)
	for index := 0; index < page.rows; index++ {
		if len(page.row(index)) != len(columns) {
			return false
		}
	}
	return true
}

// A first page has the existing metadata and page contracts, together. Decode
// both before the gateway acknowledges the write; partial or old-worker replies
// remain unknown and are never retried as an open followed by a fetch.
func validCursorFirstResponse(encoded json.RawMessage, maxRows int, writing bool) bool {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if sqlJSONValue(context.Background(), decoder, 0) != nil {
		return false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil {
		return false
	}
	// Older workers omit this optional observation. They still provide the
	// full cursor result; the adapter asks session_state as before. An explicit
	// snapshot must be boolean on done, and null while a cursor is active.
	if raw, present := fields["autocommit"]; present {
		if string(fields["done"]) == "true" {
			if string(raw) != "true" && string(raw) != "false" {
				return false
			}
		} else if string(raw) != "null" {
			return false
		}
		delete(fields, "autocommit")
	}
	cols, present := fields["cols"]
	if !present {
		return false
	}
	metadata, _ := json.Marshal(map[string]json.RawMessage{"cols": cols})
	if !validCursorOpenResult(metadata) {
		return false
	}
	delete(fields, "cols")
	page, _ := json.Marshal(fields)
	if !validCursorFetchResponse(page, maxRows, writing) {
		return false
	}
	var columns []json.RawMessage
	var rows [][]json.RawMessage
	_ = json.Unmarshal(cols, &columns)
	_ = json.Unmarshal(fields["rows"], &rows)
	if len(rows) == 0 && string(fields["done"]) != "true" {
		return false
	}
	for _, row := range rows {
		if len(row) != len(columns) {
			return false
		}
	}
	return true
}
