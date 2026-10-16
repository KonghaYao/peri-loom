package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"unicode/utf8"

	controller "peri-loom/controller"
)

func sqlDecodeWriter(ctx context.Context, body []byte, requestID string, output any) (*SessionWriterState, error) {
	reason := sqlOpenStrictDecode
	invalid := func() (*SessionWriterState, error) {
		return nil, observeSQLOpenError(ctx, requestID, sqlOpenCodecV2, reason, &SQLRPCError{Ambiguous: true})
	}
	if !utf8.Valid(body) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if sqlJSONValue(ctx, decoder, 0) != nil {
		return invalid()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return invalid()
	}
	var version uint32
	if json.Unmarshal(fields["version"], &version) != nil || version != 2 {
		return invalid()
	}
	stateRaw, found := fields["session_state"]
	var state *SessionWriterState
	if found {
		if !writerStateFields(stateRaw) {
			return invalid()
		}
		decoder := json.NewDecoder(bytes.NewReader(stateRaw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&state) != nil || state == nil || !upstreamSessionID(state.Session) {
			return invalid()
		}
		delete(fields, "session_state")
	}
	fields["version"] = json.RawMessage("1")
	normalized, err := json.Marshal(fields)
	if err != nil {
		return invalid()
	}
	var decoded reflect.Value
	var temporary any
	if output != nil {
		decoded = reflect.New(reflect.ValueOf(output).Elem().Type())
		temporary = decoded.Interface()
	}
	err = sqlDecode(ctx, normalized, requestID, temporary)
	var failure *SQLRPCError
	settled := err == nil || errors.As(err, &failure) && failure.evidence.Executed() && !failure.Ambiguous
	if settled != found {
		reason = sqlOpenWriterPresence
		return invalid()
	}
	if err == nil && output != nil {
		reflect.ValueOf(output).Elem().Set(decoded.Elem())
	}
	return state, err
}

func writerCommandValid(operation string, encoded []byte, fields map[string]json.RawMessage) bool {
	if operation == "open_session" {
		if len(fields) != 2 {
			return false
		}
		var command struct {
			Command  string                `json:"command"`
			Database controller.DatabaseID `json:"database"`
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		return decoder.Decode(&command) == nil && command.Database.Validate() == nil
	}
	expected := 3
	if operation == "close_session" || operation == "close_read_cursor" || operation == "close_write_cursor" {
		expected = 2
	} else if operation == "statement" || operation == "open_read_cursor" || operation == "open_write_cursor" {
		expected = 5
	}
	// The row projection is optional: an absent field and an explicit true mean
	// the same thing to the worker, so a caller that omits it keeps the full
	// result this contract always allowed.
	if operation == "statement" {
		if raw, exists := fields["want_rows"]; exists {
			var wantRows bool
			if json.Unmarshal(raw, &wantRows) != nil {
				return false
			}
			expected++
		}
	}
	var session string
	if len(fields) != expected || json.Unmarshal(fields["session"], &session) != nil || !upstreamSessionID(session) {
		return false
	}
	if expected == 3 && operation != "fetch_read_cursor" && operation != "fetch_write_cursor" {
		var statement string
		if json.Unmarshal(fields["sql"], &statement) != nil || bytes.Equal(bytes.TrimSpace(fields["sql"]), []byte("null")) {
			return false
		}
	}
	if operation == "fetch_read_cursor" || operation == "fetch_write_cursor" {
		var maxRows int
		return json.Unmarshal(fields["max_rows"], &maxRows) == nil && maxRows >= 1 && maxRows <= 256
	}
	if operation == "statement" || operation == "open_read_cursor" || operation == "open_write_cursor" {
		var args []sqlArgument
		var namedArgs []namedSQLArgument
		if json.Unmarshal(fields["args"], &args) != nil || args == nil || !validSQLArguments(args) || json.Unmarshal(fields["named_args"], &namedArgs) != nil || namedArgs == nil || len(args)+len(namedArgs) > 999 {
			return false
		}
		for _, argument := range namedArgs {
			if argument.Name == "" || len(argument.Name) > 256 || !validSQLArguments([]sqlArgument{argument.Value}) {
				return false
			}
		}
		var sql string
		if json.Unmarshal(fields["sql"], &sql) != nil || sql == "" {
			return false
		}
	}
	return true
}

func writerStateFields(raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 4 {
		return false
	}
	for _, name := range []string{"session", "binding", "owner", "sequence"} {
		if len(fields[name]) == 0 || bytes.Equal(bytes.TrimSpace(fields[name]), []byte("null")) {
			return false
		}
	}
	var binding map[string]json.RawMessage
	if json.Unmarshal(fields["binding"], &binding) != nil || len(binding) != 10 {
		return false
	}
	for _, name := range []string{"database", "root_incarnation", "authority_term", "branch_incarnation", "epoch", "worker_id", "lease_id", "grant_id", "renewal_sequence", "role"} {
		if len(binding[name]) == 0 || bytes.Equal(bytes.TrimSpace(binding[name]), []byte("null")) {
			return false
		}
	}
	var database map[string]json.RawMessage
	var namespace map[string]json.RawMessage
	if json.Unmarshal(binding["database"], &database) != nil || len(database) != 2 || json.Unmarshal(database["namespace"], &namespace) != nil || len(namespace) != 2 {
		return false
	}
	for _, raw := range []json.RawMessage{database["branch"], namespace["tenant"], namespace["database"]} {
		var value string
		if json.Unmarshal(raw, &value) != nil || value == "" {
			return false
		}
	}
	return true
}
