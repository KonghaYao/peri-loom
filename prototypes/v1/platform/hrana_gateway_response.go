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
	result     json.RawMessage
	open       *hranaOpenReply
	cursorPage *hranaCursorPage
}

func newHranaGatewayResponse() *hranaGatewayResponse {
	return &hranaGatewayResponse{ResponseRecorder: httptest.NewRecorder()}
}

// captureOpen keeps the fused open answer in the gateway's already-parsed form.
// Only the in-process adapter that asked for the fusion ever sees this
// recorder, so the public open envelope is never affected.
func (response *hranaGatewayResponse) captureOpen(reply *hranaOpenReply) bool {
	if reply == nil {
		return false
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	response.open = reply
	return true
}

func (response *hranaGatewayResponse) capture(value any, validated bool, page *hranaCursorPage) bool {
	raw, ok := value.(json.RawMessage)
	if !ok || raw == nil {
		return false
	}
	// The page indexes this exact span; a different value would leave its
	// offsets describing bytes the adapter never sees.
	if page != nil && !hranaCursorPageSameBytes(page.source, raw) {
		page = nil
	}
	// sqlDecode has already accepted a validated result as exactly one JSON
	// value and, for a read cursor page, indexed that same span in place. A
	// compact span is therefore safe to retain as-is; only a spaced one still
	// needs json.Compact to keep the historical result bytes and budget length.
	// Without the marker the old compact-validate pass remains the only proof
	// the bytes are JSON.
	if validated && len(raw) > 0 && (page != nil && page.compact || hranaJSONCompact(raw)) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		response.result = hranaEscapeJSONLineSeparators(raw)
		if page != nil && hranaCursorPageSameBytes(page.source, response.result) {
			response.cursorPage = page
		}
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
	// Compacting or escaping copies the bytes, so the page's offsets no longer
	// describe the result and the adapter must scan the new buffer itself.
	response.result = hranaEscapeJSONLineSeparators(compact.Bytes())
	return true
}

// hranaEscapeJSONLineSeparators rewrites the two runes every caller of the
// legacy envelope received as escapes. encoding/json escapes U+2028 and U+2029
// inside strings even when SetEscapeHTML is false, because JavaScript treats
// them as line terminators. The retained span is otherwise copied verbatim, so
// without this pass a result carrying them would leave the gateway as different
// bytes than writeResponse produces for the same value. Every other byte,
// including <, > and &, stays exactly as written.
func hranaEscapeJSONLineSeparators(raw []byte) []byte {
	if !bytes.Contains(raw, []byte("\u2028")) && !bytes.Contains(raw, []byte("\u2029")) {
		return raw
	}
	escaped := make([]byte, 0, len(raw)+12)
	for position := 0; position < len(raw); {
		if raw[position] == 0xE2 && position+2 < len(raw) && raw[position+1] == 0x80 {
			switch raw[position+2] {
			case 0xA8:
				escaped = append(escaped, `\u2028`...)
				position += 3
				continue
			case 0xA9:
				escaped = append(escaped, `\u2029`...)
				position += 3
				continue
			}
		}
		escaped = append(escaped, raw[position])
		position++
	}
	return escaped
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
	if response.open != nil {
		return hranaReply{status: http.StatusOK, open: response.open}
	}
	if response.result != nil {
		return hranaReply{status: http.StatusOK, result: response.result, cursorPage: response.cursorPage}
	}
	return hranaGatewayReply(response.Code, response.Body.Bytes(), path)
}
