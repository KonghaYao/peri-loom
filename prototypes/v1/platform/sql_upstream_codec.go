package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"unicode/utf8"
)

func sqlOutput(output any) bool {
	if output == nil {
		return true
	}
	value := reflect.ValueOf(output)
	return value.Kind() == reflect.Pointer && !value.IsNil()
}

func sqlDecode(ctx context.Context, body []byte, requestID string, output any) error {
	invalid := func() error { return &SQLRPCError{Ambiguous: true, cause: ctx.Err()} }
	if !utf8.Valid(body) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if sqlJSONValue(ctx, decoder, 0) != nil {
		return invalid()
	}
	if _, err := decoder.Token(); err != io.EOF {
		return invalid()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || (len(fields) != 5 && len(fields) != 6) {
		return invalid()
	}
	for _, field := range []string{"version", "request_id", "ok", "result", "error"} {
		if _, found := fields[field]; !found {
			return invalid()
		}
	}
	completedRaw, hasCompleted := fields["execution_completed"]
	if len(fields) == 6 && !hasCompleted {
		return invalid()
	}
	var version uint32
	var identity string
	var success bool
	var message *string
	if json.Unmarshal(fields["version"], &version) != nil || version != 1 || json.Unmarshal(fields["request_id"], &identity) != nil || identity != requestID || json.Unmarshal(fields["ok"], &success) != nil || bytes.Equal(fields["ok"], []byte("null")) || json.Unmarshal(fields["error"], &message) != nil || ctx.Err() != nil {
		return invalid()
	}
	if hasCompleted {
		var completed bool
		if success || json.Unmarshal(completedRaw, &completed) != nil || !completed {
			return invalid()
		}
	}
	if !success {
		if message == nil || !bytes.Equal(bytes.TrimSpace(fields["result"]), []byte("null")) {
			return invalid()
		}
		return &SQLRPCError{Ambiguous: !hasCompleted, ExecutionCompleted: hasCompleted}
	}
	if message != nil {
		return invalid()
	}
	if output != nil {
		value := reflect.ValueOf(output)
		decoded := reflect.New(value.Elem().Type())
		decoder := json.NewDecoder(bytes.NewReader(fields["result"]))
		decoder.UseNumber()
		decoder.DisallowUnknownFields()
		if decoder.Decode(decoded.Interface()) != nil || ctx.Err() != nil {
			return invalid()
		}
		value.Elem().Set(decoded.Elem())
	}
	return nil
}

func sqlJSONValue(ctx context.Context, decoder *json.Decoder, depth int) error {
	if ctx.Err() != nil {
		return ctx.Err()
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
			if err := sqlJSONValue(ctx, decoder, depth+1); err != nil {
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
		if err := sqlJSONValue(ctx, decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return errors.New("invalid JSON array")
	}
	return nil
}
