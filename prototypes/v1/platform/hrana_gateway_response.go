package platform

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
)

// hranaGatewayResponse is private to in-process Hrana calls. The gateway may
// hand it a successful native JSON result only when writeResponse is reached,
// after operation settlement and the audit barrier. Every other reply retains
// the HTTP envelope and the shared failure decoder.
type hranaGatewayResponse struct {
	*httptest.ResponseRecorder
	result json.RawMessage
}

func newHranaGatewayResponse() *hranaGatewayResponse {
	return &hranaGatewayResponse{ResponseRecorder: httptest.NewRecorder()}
}

func (response *hranaGatewayResponse) capture(value any, validated bool) bool {
	raw, ok := value.(json.RawMessage)
	if !ok || raw == nil {
		return false
	}
	// sqlDecode has already accepted a validated result as exactly one JSON
	// value and given the output its own copy of that span. A compact span is
	// therefore safe to retain as-is; only a spaced one still needs json.Compact
	// to keep the historical result bytes and budget length. Without the marker
	// the old compact-validate pass remains the only proof the bytes are JSON.
	if validated && len(raw) > 0 && hranaJSONCompact(raw) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		response.result = raw
		return true
	}
	// Encoder.SetEscapeHTML(false) compacts RawMessage without changing its
	// string or number spelling. Keep those exact bytes, including the length
	// used by executeCursor's result budget, and own them beyond this request.
	// Invalid JSON falls back to the old encoding/invalid-reply behavior.
	var compact bytes.Buffer
	if json.Compact(&compact, raw) != nil {
		return false
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	response.result = compact.Bytes()
	return true
}

// hranaJSONCompact reports whether raw contains no JSON whitespace outside a
// string, so json.Compact would return the same bytes. It is only called on
// values sqlDecode validated; it still treats an unterminated string as not
// compact so a malformed input falls back to the compacting validator.
func hranaJSONCompact(raw []byte) bool {
	for position := 0; position < len(raw); {
		switch raw[position] {
		case ' ', '\t', '\n', '\r':
			return false
		case '"':
			position++
			for position < len(raw) {
				quote := bytes.IndexByte(raw[position:], '"')
				if quote < 0 {
					return false
				}
				end := position + quote
				backslashes := 0
				for end-backslashes-1 >= position && raw[end-backslashes-1] == '\\' {
					backslashes++
				}
				position = end + 1
				if backslashes%2 == 0 {
					break
				}
			}
		default:
			position++
		}
	}
	return true
}

func (response *hranaGatewayResponse) reply(path string) hranaReply {
	if response.result != nil {
		return hranaReply{status: http.StatusOK, result: response.result}
	}
	return hranaGatewayReply(response.Code, response.Body.Bytes(), path)
}
