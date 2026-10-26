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
