package platform

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	controller "peri-loom/controller"
)

type keyRequest struct {
	Version    uint32                `json:"version"`
	RequestID  string                `json:"request_id"`
	Database   controller.DatabaseID `json:"database"`
	Binding    controller.Binding    `json:"binding"`
	AccessRole string                `json:"access_role"`
}
type keyResult struct {
	DescriptorID       string             `json:"descriptor_id"`
	DescriptorRevision uint64             `json:"descriptor_revision"`
	DomainID           string             `json:"domain_id"`
	DEKID              string             `json:"dek_id"`
	Cipher             string             `json:"cipher"`
	DEKHex             string             `json:"dek_hex"`
	ValidatedBinding   controller.Binding `json:"validated_binding"`
	AccessRole         string             `json:"access_role"`
	ValidForMS         uint64             `json:"valid_for_ms"`
}
type keyError struct {
	Code string `json:"code"`
}
type keyReply struct {
	Version   uint32     `json:"version"`
	RequestID string     `json:"request_id"`
	OK        bool       `json:"ok"`
	Result    *keyResult `json:"result"`
	Error     *keyError  `json:"error"`
}

func (broker *keyBroker) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Pragma", "no-cache")
	writer.Header().Set("Vary", "Authorization")
	writer.Header().Set("Content-Type", "application/json")
	reject := func(status int, id, code string) {
		writer.WriteHeader(status)
		json.NewEncoder(writer).Encode(keyReply{Version: 1, RequestID: id, Error: &keyError{Code: code}})
	}
	if request.Method != "POST" || request.URL.Path != "/internal/key" || request.URL.RawQuery != "" {
		reject(400, "", "INVALID_REQUEST")
		return
	}
	authorizations := request.Header.Values("Authorization")
	if len(authorizations) != 1 || !strings.HasPrefix(authorizations[0], "Bearer ") {
		reject(401, "", "UNAUTHENTICATED")
		return
	}
	credential := strings.TrimPrefix(authorizations[0], "Bearer ")
	if !validCredential(credential) {
		reject(401, "", "UNAUTHENTICATED")
		return
	}
	worker := broker.worker(credential)
	if worker == "" {
		reject(401, "", "UNAUTHENTICATED")
		return
	}
	encoded, err := io.ReadAll(io.LimitReader(request.Body, 16385))
	if err != nil || len(encoded) > 16384 {
		reject(400, "", "INVALID_REQUEST")
		return
	}
	var input keyRequest
	if strictDecode(encoded, &input) != nil || input.Version != 1 || !identifierPattern.MatchString(input.RequestID) || input.Database != input.Binding.Database || !validBinding(input.Binding, input.AccessRole) {
		reject(400, "", "INVALID_REQUEST")
		return
	}
	if worker != input.Binding.WorkerID {
		reject(403, input.RequestID, "FORBIDDEN")
		return
	}
	select {
	case broker.inflight[worker] <- struct{}{}:
		defer func() { <-broker.inflight[worker] }()
	default:
		reject(429, input.RequestID, "RATE_LIMITED")
		return
	}
	descriptor, dek, grant, start, err := broker.resolve(request.Context(), input.Binding, input.AccessRole)
	if err != nil {
		status := 503
		code := "UNAVAILABLE"
		if errors.Is(err, errLeaseInvalid) {
			status = 409
			code = "LEASE_INVALID"
		} else if errors.Is(err, errKeyNotReady) {
			status = 409
			code = "KEY_NOT_READY"
		} else if errors.Is(err, errKeyForbidden) {
			status = 403
			code = "FORBIDDEN"
		}
		reject(status, input.RequestID, code)
		return
	}
	defer clear(dek)
	now, err := broker.clock.now()
	if err != nil || now < start {
		reject(503, input.RequestID, "UNAVAILABLE")
		return
	}
	elapsed := (now - start) / 1000000
	if grant.RemainingTTLMS <= elapsed+5 || request.Context().Err() != nil {
		reject(409, input.RequestID, "LEASE_INVALID")
		return
	}
	remaining := grant.RemainingTTLMS - elapsed - 5
	if remaining > 1000 {
		remaining = 1000
	}
	result := keyResult{DescriptorID: descriptor.ID, DescriptorRevision: descriptor.Revision, DomainID: descriptor.DomainID, DEKID: descriptor.DEKID, Cipher: descriptor.Cipher, DEKHex: keyHex(dek), ValidatedBinding: grant.Binding, AccessRole: input.AccessRole, ValidForMS: remaining}
	json.NewEncoder(writer).Encode(keyReply{Version: 1, RequestID: input.RequestID, OK: true, Result: &result})
}
