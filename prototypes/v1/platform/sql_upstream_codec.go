package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// sqlJSONSpan locates one top-level JSON member value in the reply body. The
// span is the exact token, without the separating colon or whitespace, which is
// what json.Unmarshal would have stored in a map[string]json.RawMessage.
type sqlJSONSpan struct {
	start int
	end   int
}

func (span sqlJSONSpan) bytes(body []byte) []byte {
	return body[span.start:span.end]
}

// sqlJSONFields validates body and returns the span of each top-level member.
// The body is first accepted by json.Valid, the standard library's own grammar
// check (the same scanner Decoder.Token walks), so the structural pass below
// only has to find boundaries in JSON it can trust. It then tracks what the
// token walk used to enforce: object keys are compared after unescaping, so a
// duplicate is rejected at any depth, and a value nested deeper than 128
// containers is rejected.
//
// Skipping values structurally, instead of asking Decoder.Token for every
// scalar, avoids unquoting and allocating every string in a 20 MiB result. A
// boundary bug in the structural pass can only reject a body json.Valid
// accepted or produce a span that the later field decodes refuse; it cannot
// make json.Valid accept invalid bytes. Escaped keys still unquote through
// encoding/json itself, so duplicate detection keeps the decoder's notion of
// key identity. A span is otherwise exactly what json.Unmarshal would have
// stored in a map[string]json.RawMessage: the value token without its colon or
// surrounding whitespace.
func sqlJSONFields(ctx context.Context, body []byte) (map[string]sqlJSONSpan, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !json.Valid(body) {
		return nil, errors.New("invalid JSON")
	}
	position, err := sqlJSONWhitespace(body, 0)
	if err != nil || position >= len(body) || body[position] != '{' {
		return nil, errors.New("JSON top level is not an object")
	}
	position++
	fields := make(map[string]sqlJSONSpan, 6)
	for {
		if position, err = sqlJSONWhitespace(body, position); err != nil {
			return nil, err
		}
		if position >= len(body) {
			return nil, errors.New("invalid JSON object")
		}
		if body[position] == '}' {
			position++
			break
		}
		keyStart := position
		if position, err = sqlJSONStringEnd(body, position); err != nil {
			return nil, err
		}
		key, err := sqlJSONKey(body[keyStart:position])
		if err != nil {
			return nil, err
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, errors.New("duplicate JSON field")
		}
		// Reserve the name before its value is skipped so a second spelling of
		// the same key is still rejected after the first value.
		fields[key] = sqlJSONSpan{}
		if position, err = sqlJSONWhitespace(body, position); err != nil {
			return nil, err
		}
		if position >= len(body) || body[position] != ':' {
			return nil, errors.New("invalid JSON object separator")
		}
		if position, err = sqlJSONWhitespace(body, position+1); err != nil {
			return nil, err
		}
		valueStart := position
		if position, err = sqlJSONSkipValue(ctx, body, position, 1); err != nil {
			return nil, err
		}
		fields[key] = sqlJSONSpan{start: valueStart, end: position}
		if position, err = sqlJSONWhitespace(body, position); err != nil {
			return nil, err
		}
		if position >= len(body) {
			return nil, errors.New("invalid JSON object")
		}
		if body[position] == ',' {
			position++
			continue
		}
		if body[position] != '}' {
			return nil, errors.New("invalid JSON object")
		}
		position++
		break
	}
	if rest, err := sqlJSONWhitespace(body, position); err != nil || rest != len(body) {
		return nil, errors.New("trailing JSON data")
	}
	return fields, nil
}

// sqlJSONSkipValue returns the offset just past the JSON value at position.
// body is already valid JSON, so it only needs to find the end and to apply
// the duplicate-key and depth rules to nested objects.
func sqlJSONSkipValue(ctx context.Context, body []byte, position, depth int) (int, error) {
	if depth > 128 {
		return 0, errors.New("JSON depth exceeded")
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if position >= len(body) {
		return 0, errors.New("invalid JSON value")
	}
	var err error
	switch body[position] {
	case '{':
		position++
		var seen map[string]struct{}
		for {
			if position, err = sqlJSONWhitespace(body, position); err != nil {
				return 0, err
			}
			if position >= len(body) {
				return 0, errors.New("invalid JSON object")
			}
			if body[position] == '}' {
				return position + 1, nil
			}
			keyStart := position
			if position, err = sqlJSONStringEnd(body, position); err != nil {
				return 0, err
			}
			key, err := sqlJSONKey(body[keyStart:position])
			if err != nil {
				return 0, err
			}
			if _, duplicate := seen[key]; duplicate {
				return 0, errors.New("duplicate JSON field")
			}
			if seen == nil {
				seen = make(map[string]struct{}, 4)
			}
			seen[key] = struct{}{}
			if position, err = sqlJSONWhitespace(body, position); err != nil {
				return 0, err
			}
			if position >= len(body) || body[position] != ':' {
				return 0, errors.New("invalid JSON object separator")
			}
			if position, err = sqlJSONWhitespace(body, position+1); err != nil {
				return 0, err
			}
			if position, err = sqlJSONSkipValue(ctx, body, position, depth+1); err != nil {
				return 0, err
			}
			if position, err = sqlJSONWhitespace(body, position); err != nil {
				return 0, err
			}
			if position >= len(body) {
				return 0, errors.New("invalid JSON object")
			}
			if body[position] == ',' {
				position++
				continue
			}
			if body[position] != '}' {
				return 0, errors.New("invalid JSON object")
			}
			return position + 1, nil
		}
	case '[':
		position++
		for {
			if position, err = sqlJSONWhitespace(body, position); err != nil {
				return 0, err
			}
			if position >= len(body) {
				return 0, errors.New("invalid JSON array")
			}
			if body[position] == ']' {
				return position + 1, nil
			}
			if position, err = sqlJSONSkipValue(ctx, body, position, depth+1); err != nil {
				return 0, err
			}
			if position, err = sqlJSONWhitespace(body, position); err != nil {
				return 0, err
			}
			if position >= len(body) {
				return 0, errors.New("invalid JSON array")
			}
			if body[position] == ',' {
				position++
				continue
			}
			if body[position] != ']' {
				return 0, errors.New("invalid JSON array")
			}
			return position + 1, nil
		}
	case '"':
		return sqlJSONStringEnd(body, position)
	case 't':
		return position + 4, nil
	case 'f':
		return position + 5, nil
	case 'n':
		return position + 4, nil
	default:
		// A number. json.Valid already accepted its grammar, so the next
		// structural delimiter ends it.
		position++
		for position < len(body) && !sqlJSONDelimiter(body[position]) {
			position++
		}
		return position, nil
	}
}

// sqlJSONStringEnd returns the offset just past the closing quote of the string
// starting at position. body is valid JSON, so the first quote found is the end
// unless an escape precedes it; the bounded backslash probe keeps text-heavy
// results at one indexed scan per string.
func sqlJSONStringEnd(body []byte, position int) (int, error) {
	for position++; position < len(body); {
		quote := bytes.IndexByte(body[position:], '"')
		if quote < 0 {
			return 0, errors.New("unterminated JSON string")
		}
		end := position + quote
		if bytes.IndexByte(body[position:end], '\\') < 0 {
			return end + 1, nil
		}
		index := position
		for index < end {
			if body[index] == '\\' {
				index += 2
			} else {
				index++
			}
		}
		if index == end {
			return end + 1, nil
		}
		position = end + 1
	}
	return 0, errors.New("unterminated JSON string")
}

// sqlJSONKey unescapes one object key. Keys are compared the way Decoder.Token
// returns them, so "a" and "\u0061" remain duplicates.
func sqlJSONKey(raw []byte) (string, error) {
	if bytes.IndexByte(raw, '\\') < 0 {
		return string(raw[1 : len(raw)-1]), nil
	}
	var key string
	if json.Unmarshal(raw, &key) != nil {
		return "", errors.New("invalid JSON object key")
	}
	return key, nil
}

func sqlJSONWhitespace(body []byte, position int) (int, error) {
	if position < 0 || position > len(body) {
		return 0, errors.New("invalid JSON offset")
	}
	for position < len(body) && sqlJSONSpace(body[position]) {
		position++
	}
	return position, nil
}

func sqlJSONSpace(character byte) bool {
	return character == ' ' || character == '\t' || character == '\n' || character == '\r'
}

func sqlJSONDelimiter(character byte) bool {
	return sqlJSONSpace(character) || character == ',' || character == '}' || character == ']'
}

func sqlDecode(ctx context.Context, body []byte, requestID string, output any) error {
	invalid := func() error {
		return observeSQLOpenError(ctx, requestID, sqlOpenCodecV1, sqlOpenStrictDecode, &SQLRPCError{Ambiguous: true, cause: ctx.Err()})
	}
	if !utf8.Valid(body) {
		return invalid()
	}
	fields, err := sqlJSONFields(ctx, body)
	if err != nil || (len(fields) != 5 && len(fields) != 6) {
		return invalid()
	}
	for _, field := range []string{"version", "request_id", "ok", "result", "error"} {
		if _, found := fields[field]; !found {
			return invalid()
		}
	}
	completedSpan, hasCompleted := fields["execution_completed"]
	if len(fields) == 6 && !hasCompleted {
		return invalid()
	}
	var version uint32
	var identity string
	var success bool
	var message *string
	if json.Unmarshal(fields["version"].bytes(body), &version) != nil || version != 1 || json.Unmarshal(fields["request_id"].bytes(body), &identity) != nil || identity != requestID || json.Unmarshal(fields["ok"].bytes(body), &success) != nil || bytes.Equal(fields["ok"].bytes(body), []byte("null")) || json.Unmarshal(fields["error"].bytes(body), &message) != nil || ctx.Err() != nil {
		return invalid()
	}
	var completed bool
	if hasCompleted {
		if success || bytes.Equal(bytes.TrimSpace(completedSpan.bytes(body)), []byte("null")) || json.Unmarshal(completedSpan.bytes(body), &completed) != nil {
			return invalid()
		}
	}
	if !success {
		if message == nil || !bytes.Equal(bytes.TrimSpace(fields["result"].bytes(body)), []byte("null")) {
			return invalid()
		}
		return observeSQLOpenError(ctx, requestID, sqlOpenCodecV1, sqlOpenWorkerNegative, sqlWorkerFailureFromReply(hasCompleted, completed, *message))
	}
	if message != nil {
		return invalid()
	}
	if output != nil {
		// sqlJSONFields above already accepted result as exactly one JSON
		// value, so a RawMessage output needs only an owned copy of its span;
		// decoding it again would repeat the same scan and allocation. The copy
		// keeps the reply independent of the read buffer that sqlDecodeWriter
		// and its callers may still reuse.
		if raw, ok := output.(*json.RawMessage); ok {
			copied := append([]byte(nil), fields["result"].bytes(body)...)
			if ctx.Err() != nil {
				return invalid()
			}
			*raw = copied
			return nil
		}
		value := reflect.ValueOf(output)
		decoded := reflect.New(value.Elem().Type())
		decoder := json.NewDecoder(bytes.NewReader(fields["result"].bytes(body)))
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
