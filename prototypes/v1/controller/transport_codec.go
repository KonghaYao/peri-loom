package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"unicode/utf8"
)

func validOutput(output any) bool {
	if output == nil {
		return true
	}
	value := reflect.ValueOf(output)
	return value.Kind() == reflect.Pointer && !value.IsNil()
}

func validAuthorityID(command []byte, requestID string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(command, &fields) != nil || fields == nil {
		return false
	}
	var operation string
	if json.Unmarshal(fields["command"], &operation) != nil || operation == "" {
		return false
	}
	if operation == "grant" || operation == "renew" || operation == "revoke" {
		request := fields["request"]
		fields = nil
		if json.Unmarshal(request, &fields) != nil || fields == nil {
			return false
		}
	} else if operation != "maintenance" {
		return true
	}
	var innerID string
	return json.Unmarshal(fields["request_id"], &innerID) == nil && innerID == requestID
}

func decodeRPCResponse(body []byte, requestID string, output any, authority bool) error {
	return decodeRPCResponseContext(context.Background(), body, requestID, output, authority)
}

func decodeRPCResponseContext(ctx context.Context, body []byte, requestID string, output any, authority bool) error {
	invalid := func() error { return &RPCError{Message: "invalid RPC response", Ambiguous: true, cause: ctx.Err()} }
	if !utf8.Valid(body) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if validateJSONValue(decoder, 0, ctx) != nil {
		return invalid()
	}
	if _, err := decoder.Token(); err != io.EOF {
		return invalid()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return invalid()
	}
	allowed := map[string]bool{"version": true, "request_id": true, "ok": true, "result": true, "error": true}
	if authority {
		allowed["precondition_failed"] = true
	}
	if len(fields) != len(allowed) {
		return invalid()
	}
	for name := range fields {
		if !allowed[name] {
			return invalid()
		}
	}
	var version uint32
	var identity string
	var success bool
	var precondition bool
	var message *string
	if json.Unmarshal(fields["version"], &version) != nil || version != 1 || json.Unmarshal(fields["request_id"], &identity) != nil || identity != requestID || json.Unmarshal(fields["ok"], &success) != nil || string(fields["ok"]) == "null" || json.Unmarshal(fields["error"], &message) != nil {
		return invalid()
	}
	if authority && (json.Unmarshal(fields["precondition_failed"], &precondition) != nil || string(fields["precondition_failed"]) == "null") {
		return invalid()
	}
	result := bytes.TrimSpace(fields["result"])
	if ctx.Err() != nil {
		return invalid()
	}
	if success {
		if message != nil || precondition {
			return invalid()
		}
		if output != nil {
			value := reflect.ValueOf(output)
			decoded := reflect.New(value.Elem().Type())
			decoder := json.NewDecoder(bytes.NewReader(result))
			decoder.UseNumber()
			decoder.DisallowUnknownFields()
			if decoder.Decode(decoded.Interface()) != nil {
				return invalid()
			}
			if ctx.Err() != nil {
				return invalid()
			}
			value.Elem().Set(decoded.Elem())
		}
		return nil
	}
	if message == nil || !bytes.Equal(result, []byte("null")) {
		return invalid()
	}
	return &RPCError{Message: "RPC operation rejected", Ambiguous: !precondition, Rejected: precondition, PreconditionFailed: precondition}
}

func validateJSONValue(decoder *json.Decoder, depth int, ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > 128 {
		return errors.New("JSON depth exceeded")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	opening, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	if opening == '{' {
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, valid := key.(string)
			if !valid || seen[name] {
				return errors.New("duplicate JSON field")
			}
			seen[name] = true
			if err := validateJSONValue(decoder, depth+1, ctx); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("invalid JSON object")
		}
		return nil
	}
	if opening != '[' {
		return errors.New("invalid JSON value")
	}
	for decoder.More() {
		if err := validateJSONValue(decoder, depth+1, ctx); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return errors.New("invalid JSON array")
	}
	return nil
}
