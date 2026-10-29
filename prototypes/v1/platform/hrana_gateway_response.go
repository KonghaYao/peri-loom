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

func (response *hranaGatewayResponse) capture(value any) bool {
	raw, ok := value.(json.RawMessage)
	if !ok || raw == nil {
		return false
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

func (response *hranaGatewayResponse) reply(path string) hranaReply {
	if response.result != nil {
		return hranaReply{status: http.StatusOK, result: response.result}
	}
	return hranaGatewayReply(response.Code, response.Body.Bytes(), path)
}
