package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// sqlDecodeLegacy is the pre-span decoder, kept verbatim as the oracle for the
// strict envelope contract. sqlDecode now reads top-level members from spans
// found during the validating token walk; this copy still expands the envelope
// into map[string]json.RawMessage, so every acceptance decision and every
// decoded output can be compared against exactly what shipped before.
func sqlDecodeLegacy(ctx context.Context, body []byte, requestID string, output any) error {
	invalid := func() error {
		return observeSQLOpenError(ctx, requestID, sqlOpenCodecV1, sqlOpenStrictDecode, &SQLRPCError{Ambiguous: true, cause: ctx.Err()})
	}
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
	var completed bool
	if hasCompleted {
		if success || bytes.Equal(bytes.TrimSpace(completedRaw), []byte("null")) || json.Unmarshal(completedRaw, &completed) != nil {
			return invalid()
		}
	}
	if !success {
		if message == nil || !bytes.Equal(bytes.TrimSpace(fields["result"]), []byte("null")) {
			return invalid()
		}
		return observeSQLOpenError(ctx, requestID, sqlOpenCodecV1, sqlOpenWorkerNegative, sqlWorkerFailureFromReply(hasCompleted, completed, *message))
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

type sqlTestPage struct {
	Cols []json.RawMessage   `json:"cols"`
	Rows [][]json.RawMessage `json:"rows"`
	Done bool                `json:"done"`
}

func sqlTestCompactEnvelope(requestID string, result string) string {
	return `{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + result + `,"error":null}`
}

func sqlTestErrorEnvelope(requestID, message, extra string) string {
	envelope := `{"version":1,"request_id":"` + requestID + `","ok":false,"result":null,"error":"` + message + `"`
	if extra != "" {
		envelope += `,` + extra
	}
	return envelope + `}`
}

func sqlTestDowngradeError(test *testing.T, err error) *SQLRPCError {
	test.Helper()
	var failure *SQLRPCError
	if !errors.As(err, &failure) {
		test.Fatalf("expected SQLRPCError, got %T", err)
	}
	return failure
}

func sqlTestDecodeErrorsEqual(test *testing.T, legacy, current error) {
	test.Helper()
	if legacy == nil || current == nil {
		if legacy != current {
			test.Fatalf("acceptance differs: legacy=%v current=%v", legacy, current)
		}
		return
	}
	left, right := sqlTestDowngradeError(test, legacy), sqlTestDowngradeError(test, current)
	if left.Ambiguous != right.Ambiguous || left.StatusCode != right.StatusCode ||
		left.workerFailure != right.workerFailure ||
		left.evidence.Executed() != right.evidence.Executed() ||
		left.evidence.Rejected() != right.evidence.Rejected() ||
		left.evidence.CommitRisk() != right.evidence.CommitRisk() ||
		(left.cause == nil) != (right.cause == nil) ||
		left.Error() != right.Error() {
		test.Fatalf("error classification differs: legacy=%+v current=%+v", left, right)
	}
}

// sqlDecodeDifferentialCorpus covers every strict-decoding rule sqlDecode
// enforces: UTF-8, duplicate members at any depth, depth, trailing data,
// top-level shape, member count, required members, ok/error/result agreement,
// execution_completed, request_id echo and unknown typed fields.
func sqlDecodeDifferentialCorpus() []struct {
	name string
	body string
} {
	deep := func(levels int) string {
		return strings.Repeat("[", levels) + `0` + strings.Repeat("]", levels)
	}
	deepEmpty := func(levels int) string {
		return strings.Repeat("[", levels) + strings.Repeat("]", levels)
	}
	return []struct {
		name string
		body string
	}{
		{"success_null", sqlTestCompactEnvelope("rid", `null`)},
		{"success_object", sqlTestCompactEnvelope("rid", `{"cols":[{"name":"a","decltype":"TEXT"}],"rows":[[1,"x\"y"]],"done":false}`)},
		{"success_array", sqlTestCompactEnvelope("rid", `[[1,"x"],[2,null],[3,true]]`)},
		{"success_number", sqlTestCompactEnvelope("rid", `-0.5e+09`)},
		{"success_bignumber", sqlTestCompactEnvelope("rid", `1e999`)},
		{"success_string_escapes", sqlTestCompactEnvelope("rid", `"a\\b\"c\u2028d"`)},
		{"success_spaced", " \n{\t\"version\" : 1 , \"request_id\" : \"rid\" , \"ok\" : true , \"result\" : { \"rows\" : [ [ 1 , \"x\" ] ] } , \"error\" : null } \n"},
		{"success_escaped_key", `{"version":1,"request_id":"rid","ok":true,"res\u0075lt":{"rows":[]},"error":null}`},
		{"success_duplicate_free_nested", sqlTestCompactEnvelope("rid", `{"rows":[[1,{"a":1}]],"done":true}`)},
		{"negative_plain", sqlTestErrorEnvelope("rid", "boom", "")},
		{"negative_starved_ambiguous", `{"version":1,"request_id":"rid","ok":false,"result":null,"error":"boom"}`},
		{"negative_completed", sqlTestErrorEnvelope("rid", "boom", `"execution_completed":true`)},
		{"negative_not_completed", sqlTestErrorEnvelope("rid", "boom", `"execution_completed":false`)},
		{"depth_127", sqlTestCompactEnvelope("rid", deep(127))},
		{"depth_128", sqlTestCompactEnvelope("rid", deep(128))},
		{"depth_129", sqlTestCompactEnvelope("rid", deep(129))},
		{"depth_empty_128", sqlTestCompactEnvelope("rid", deepEmpty(128))},
		{"depth_empty_129", sqlTestCompactEnvelope("rid", deepEmpty(129))},
		{"depth_object_128", sqlTestCompactEnvelope("rid", strings.Repeat(`{"a":`, 128)+`0`+strings.Repeat(`}`, 128))},
		{"depth_object_129", sqlTestCompactEnvelope("rid", strings.Repeat(`{"a":`, 129)+`0`+strings.Repeat(`}`, 129))},
		{"empty", ""},
		{"whitespace", "   \n\t "},
		{"top_null", `null`},
		{"top_array", `[]`},
		{"top_string", `"x"`},
		{"top_number", `1`},
		{"truncated", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null`},
		{"trailing_token", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null} null`},
		{"trailing_garbage", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null}x`},
		{"duplicate_top", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null,"version":1}`},
		{"duplicate_escaped_top", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null,"vers\u0069on":1}`},
		{"duplicate_nested", sqlTestCompactEnvelope("rid", `{"rows":[],"rows":[]}`)},
		{"duplicate_nested_escape", sqlTestCompactEnvelope("rid", `{"a":1,"\u0061":2}`)},
		{"duplicate_nested_unicode", sqlTestCompactEnvelope("rid", `{"é":1,"\u00e9":2}`)},
		{"non_duplicate_nested_unicode", sqlTestCompactEnvelope("rid", `{"é":1,"e":2}`)},
		{"duplicate_deep", sqlTestCompactEnvelope("rid", `[[{"a":1,"a":2}]]`)},
		{"invalid_utf8_result", sqlTestCompactEnvelope("rid", "\"\xff\"")},
		{"invalid_utf8_key", "{\"version\":1,\"request_id\":\"rid\",\"ok\":true,\"result\":null,\"error\":null,\"\xff\":1}"},
		{"missing_error", `{"version":1,"request_id":"rid","ok":true,"result":null}`},
		{"missing_result", `{"version":1,"request_id":"rid","ok":true,"error":null}`},
		{"missing_ok", `{"version":1,"request_id":"rid","result":null,"error":null}`},
		{"missing_version", `{"request_id":"rid","ok":true,"result":null,"error":null}`},
		{"missing_request_id", `{"version":1,"ok":true,"result":null,"error":null}`},
		{"six_unknown", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null,"extra":1}`},
		{"seven_fields", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null,"extra":1,"more":2}`},
		{"four_fields", `{"version":1,"request_id":"rid","ok":true,"result":null}`},
		{"version_two", `{"version":2,"request_id":"rid","ok":true,"result":null,"error":null}`},
		{"version_string", `{"version":"1","request_id":"rid","ok":true,"result":null,"error":null}`},
		{"version_fraction", `{"version":1.0,"request_id":"rid","ok":true,"result":null,"error":null}`},
		{"request_mismatch", sqlTestCompactEnvelope("other", `null`)},
		{"request_number", `{"version":1,"request_id":1,"ok":true,"result":null,"error":null}`},
		{"ok_null", `{"version":1,"request_id":"rid","ok":null,"result":null,"error":null}`},
		{"ok_number", `{"version":1,"request_id":"rid","ok":1,"result":null,"error":null}`},
		{"ok_string", `{"version":1,"request_id":"rid","ok":"true","result":null,"error":null}`},
		{"error_number", `{"version":1,"request_id":"rid","ok":false,"result":null,"error":5}`},
		{"error_object", `{"version":1,"request_id":"rid","ok":false,"result":null,"error":{}}`},
		{"error_true", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":"unexpected"}`},
		{"negative_null_message", `{"version":1,"request_id":"rid","ok":false,"result":null,"error":null}`},
		{"negative_nonnull_result", `{"version":1,"request_id":"rid","ok":false,"result":{},"error":"boom"}`},
		{"completed_with_success", `{"version":1,"request_id":"rid","ok":true,"result":null,"error":null,"execution_completed":true}`},
		{"completed_null", `{"version":1,"request_id":"rid","ok":false,"result":null,"error":"boom","execution_completed":null}`},
		{"completed_number", `{"version":1,"request_id":"rid","ok":false,"result":null,"error":"boom","execution_completed":1}`},
		{"control_character", `{"version":1,"request_id":"rid","ok":true,"result":"a` + "\x01" + `b","error":null}`},
		{"bad_escape", `{"version":1,"request_id":"rid","ok":true,"result":"\q","error":null}`},
		{"bom", "\xef\xbb\xbf" + sqlTestCompactEnvelope("rid", `null`)},
		{"unterminated", sqlTestCompactEnvelope("rid", `{"rows":[1,2`)},
	}
}

func TestSQLDecodeMatchesLegacyOracle(test *testing.T) {
	for _, scenario := range sqlDecodeDifferentialCorpus() {
		scenario := scenario
		test.Run(scenario.name, func(test *testing.T) {
			body := []byte(scenario.body)
			contexts := []struct {
				name string
				ctx  context.Context
			}{
				{"active", context.Background()},
				{"cancelled", func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				}()},
			}
			for _, execution := range contexts {
				test.Run(execution.name, func(test *testing.T) {
					var legacyRaw, currentRaw json.RawMessage
					legacyErr := sqlDecodeLegacy(execution.ctx, body, "rid", &legacyRaw)
					currentErr := sqlDecode(execution.ctx, body, "rid", &currentRaw)
					sqlTestDecodeErrorsEqual(test, legacyErr, currentErr)
					if legacyErr == nil && !bytes.Equal(legacyRaw, currentRaw) {
						test.Fatalf("RawMessage output differs: legacy=%q current=%q", legacyRaw, currentRaw)
					}

					var legacyPage, currentPage sqlTestPage
					legacyErr = sqlDecodeLegacy(execution.ctx, body, "rid", &legacyPage)
					currentErr = sqlDecode(execution.ctx, body, "rid", &currentPage)
					sqlTestDecodeErrorsEqual(test, legacyErr, currentErr)
					if legacyErr == nil && !reflect.DeepEqual(legacyPage, currentPage) {
						test.Fatalf("typed output differs: legacy=%+v current=%+v", legacyPage, currentPage)
					}

					legacyErr = sqlDecodeLegacy(execution.ctx, body, "rid", nil)
					currentErr = sqlDecode(execution.ctx, body, "rid", nil)
					sqlTestDecodeErrorsEqual(test, legacyErr, currentErr)
				})
			}
		})
	}
}

func TestSQLDecodeBoundaryExpectations(test *testing.T) {
	deep := func(levels int) string {
		return strings.Repeat("[", levels) + `0` + strings.Repeat("]", levels)
	}
	deepEmpty := func(levels int) string {
		return strings.Repeat("[", levels) + strings.Repeat("]", levels)
	}
	for _, scenario := range []struct {
		name string
		body string
		ok   bool
	}{
		{"depth_127", sqlTestCompactEnvelope("rid", deep(127)), true},
		{"depth_128", sqlTestCompactEnvelope("rid", deep(128)), false},
		{"depth_empty_128", sqlTestCompactEnvelope("rid", deepEmpty(128)), true},
		{"depth_empty_129", sqlTestCompactEnvelope("rid", deepEmpty(129)), false},
		{"duplicate_escaped", sqlTestCompactEnvelope("rid", `{"a":1,"\u0061":2}`), false},
		{"duplicate_unicode", sqlTestCompactEnvelope("rid", `{"é":1,"\u00e9":2}`), false},
		{"distinct_unicode", sqlTestCompactEnvelope("rid", `{"é":1,"e":2}`), true},
	} {
		scenario := scenario
		test.Run(scenario.name, func(test *testing.T) {
			err := sqlDecode(context.Background(), []byte(scenario.body), "rid", nil)
			if (err == nil) != scenario.ok {
				test.Fatalf("err=%v; want accepted=%t", err, scenario.ok)
			}
			legacy := sqlDecodeLegacy(context.Background(), []byte(scenario.body), "rid", nil)
			if (legacy == nil) != scenario.ok {
				test.Fatalf("oracle acceptance changed: err=%v; want accepted=%t", legacy, scenario.ok)
			}
		})
	}
}

func TestSQLDecodeOwnsRawResult(test *testing.T) {
	body := []byte(sqlTestCompactEnvelope("rid", `{"rows":[["owned"]]}`))
	var result json.RawMessage
	if err := sqlDecode(context.Background(), body, "rid", &result); err != nil {
		test.Fatalf("decode failed: %v", err)
	}
	want := append([]byte(nil), result...)
	for index := range body {
		body[index] = 'x'
	}
	if !bytes.Equal(result, want) {
		test.Fatal("RawMessage aliased the reply buffer after decode")
	}
}

func sqlTestWorkerPage(rows, textRepeat int) []byte {
	text := strings.Repeat("content ", textRepeat)
	var body bytes.Buffer
	body.WriteString(`{"cols":[{"name":"id","decltype":"INTEGER"},{"name":"body","decltype":"TEXT"}],"rows":[`)
	for row := 0; row < rows; row++ {
		if row > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `[%d,"%s%d"]`, row, text, row)
	}
	body.WriteString(`],"done":false}`)
	return body.Bytes()
}

// TestSQLDecodePageSampleMatchesLegacy compares one 740 KiB cursor page - the
// 256-row unit the Hrana cursor paths fetch - through both decoders.
func TestSQLDecodePageSampleMatchesLegacy(t *testing.T) {
	page := sqlTestWorkerPage(256, 350)
	if len(page) < 700*1024 {
		t.Fatalf("page sample too small: %d", len(page))
	}
	envelope := []byte(sqlTestCompactEnvelope("page", string(page)))
	var legacy, current json.RawMessage
	if err := sqlDecodeLegacy(context.Background(), envelope, "page", &legacy); err != nil {
		t.Fatalf("legacy decode failed: %v", err)
	}
	if err := sqlDecode(context.Background(), envelope, "page", &current); err != nil {
		t.Fatalf("current decode failed: %v", err)
	}
	if !bytes.Equal(legacy, current) || !bytes.Equal(current, page) {
		t.Fatalf("page sample differs: legacy=%d current=%d page=%d", len(legacy), len(current), len(page))
	}
}

// sqlTestRandomJSON builds arbitrary valid JSON. Object keys come from a small
// set whose spellings include \u escapes, so duplicate rejection is exercised
// at random depth with keys that only compare equal after unescaping.
func sqlTestRandomJSON(rng *rand.Rand, depth int) string {
	if depth >= 5 || rng.Intn(3) == 0 {
		switch rng.Intn(6) {
		case 0:
			return "null"
		case 1:
			return "true"
		case 2:
			return "false"
		case 3:
			return strconv.FormatInt(int64(rng.Intn(2001)-1000), 10)
		case 4:
			return strconv.FormatFloat(float64(rng.Intn(10000)-5000)/128, 'g', -1, 64)
		default:
			encoded, _ := json.Marshal([]string{"x", "a \"quoted\" \\ value", "line\nbreak", "\u2028", ""}[rng.Intn(5)])
			return string(encoded)
		}
	}
	keys := []string{`a`, `b`, `\u0061`, `\u0062`, `c`, `key with space`}
	var builder strings.Builder
	if rng.Intn(2) == 0 {
		builder.WriteByte('{')
		for index := 0; index < rng.Intn(4); index++ {
			if index > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(`"` + keys[rng.Intn(len(keys))] + `":`)
			builder.WriteString(sqlTestRandomJSON(rng, depth+1))
		}
		builder.WriteByte('}')
		return builder.String()
	}
	builder.WriteByte('[')
	for index := 0; index < rng.Intn(4); index++ {
		if index > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(sqlTestRandomJSON(rng, depth+1))
	}
	builder.WriteByte(']')
	return builder.String()
}

func TestSQLDecodeRandomizedDifferential(test *testing.T) {
	rng := rand.New(rand.NewSource(28))
	mutate := func(body []byte) []byte {
		if len(body) == 0 {
			return body
		}
		noise := []byte(`{}[],:"\ 	` + "\n0123456789truefalsenull\xff")
		position := rng.Intn(len(body))
		switch rng.Intn(4) {
		case 0:
			return append(body[:position:position], body[position+1:]...)
		case 1:
			return append(body[:position:position], append([]byte{noise[rng.Intn(len(noise))]}, body[position:]...)...)
		case 2:
			copied := append([]byte(nil), body...)
			copied[position] = noise[rng.Intn(len(noise))]
			return copied
		default:
			end := position + rng.Intn(8)
			if end > len(body) {
				end = len(body)
			}
			copied := append([]byte(nil), body...)
			return append(copied[:position:position], append(append([]byte(nil), body[position:end]...), copied[position:]...)...)
		}
	}
	for iteration := 0; iteration < 4000; iteration++ {
		if iteration%8 == 7 {
			body := make([]byte, rng.Intn(96))
			for index := range body {
				body[index] = byte(rng.Intn(128))
			}
			legacyErr := sqlDecodeLegacy(context.Background(), body, "rid", nil)
			currentErr := sqlDecode(context.Background(), body, "rid", nil)
			sqlTestDecodeErrorsEqual(test, legacyErr, currentErr)
			continue
		}
		result := sqlTestRandomJSON(rng, 0)
		body := []byte(sqlTestCompactEnvelope("rid", result))
		switch iteration % 4 {
		case 1:
			body = mutate(body)
		case 2:
			body = mutate(mutate(body))
		case 3:
			body = []byte(sqlTestErrorEnvelope("rid", "boom", `"execution_completed":false`))
		}
		var legacyRaw, currentRaw json.RawMessage
		legacyErr := sqlDecodeLegacy(context.Background(), body, "rid", &legacyRaw)
		currentErr := sqlDecode(context.Background(), body, "rid", &currentRaw)
		if legacyErr != nil || currentErr != nil {
			if legacyErr == nil || currentErr == nil {
				test.Fatalf("iteration %d acceptance differs for %q: legacy=%v current=%v", iteration, body, legacyErr, currentErr)
			}
			sqlTestDecodeErrorsEqual(test, legacyErr, currentErr)
			continue
		}
		if !bytes.Equal(legacyRaw, currentRaw) {
			test.Fatalf("iteration %d result differs for %q", iteration, body)
		}
	}
}

// TestHranaJSONCompactMatchesCompaction pins the capture fast path to the
// bytes json.Compact produces: it may claim an already-compact result only when
// compaction would be a no-op.
func TestHranaJSONCompactMatchesCompaction(test *testing.T) {
	rng := rand.New(rand.NewSource(82))
	samples := []string{
		`{"rows":[],"done":true}`,
		`{"text":"a b\tc"}`,
		`"spaced string"`,
		`null`,
		`[1,2,{"a":"\\\"quote\\\""}]`,
		" { \"a\" : 1 } ",
		"{\n\"a\":[1, 2]\n}",
	}
	for iteration := 0; iteration < 2000; iteration++ {
		samples = append(samples, sqlTestRandomJSON(rng, 0))
	}
	for _, sample := range samples {
		raw := []byte(sample)
		var compact bytes.Buffer
		if json.Compact(&compact, raw) != nil {
			continue
		}
		if hranaJSONCompact(raw) && !bytes.Equal(compact.Bytes(), raw) {
			test.Fatalf("capture would keep spaced bytes for %q: compact=%q", raw, compact.Bytes())
		}
	}
}

// TestHranaCaptureValidatedMatchesLegacy proves the capture fast path keeps the
// bytes the compacting path produced: for valid JSON, a skipped validation may
// only change ownership, never the response payload.
func TestHranaCaptureValidatedMatchesLegacy(test *testing.T) {
	rng := rand.New(rand.NewSource(82))
	for iteration := 0; iteration < 2000; iteration++ {
		raw := json.RawMessage(sqlTestRandomJSON(rng, 0))
		switch iteration % 3 {
		case 1:
			raw = json.RawMessage(" \n\t" + string(raw) + " \t\r\n")
		case 2:
			raw = json.RawMessage(strings.ReplaceAll(string(raw), ",", " , "))
		}
		legacy := newHranaGatewayResponse()
		if !legacy.capture(raw, false) {
			test.Fatalf("legacy capture refused valid result %q", raw)
		}
		validated := newHranaGatewayResponse()
		if !validated.capture(raw, true) {
			test.Fatalf("validated capture refused valid result %q", raw)
		}
		if !bytes.Equal(legacy.result, validated.result) {
			test.Fatalf("capture bytes differ: legacy=%q validated=%q", legacy.result, validated.result)
		}
	}
}

func TestHranaGatewayResponseValidatedResult(t *testing.T) {
	body := []byte(sqlTestCompactEnvelope("rid", `{"cols":[],"rows":[],"done":true,"autocommit":true}`))
	var reply json.RawMessage
	if err := sqlDecode(context.Background(), body, "rid", &reply); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	want := append([]byte(nil), reply...)

	response := newHranaGatewayResponse()
	(&Gateway{}).writeResponse(response, "rid", gatewayResult{value: reply, validatedResult: true})
	if !bytes.Equal(response.result, want) {
		t.Fatalf("validated result changed: %q", response.result)
	}
	for index := range body {
		body[index] = 'x'
	}
	if !bytes.Equal(response.result, want) {
		t.Fatal("validated capture aliased the reply buffer")
	}

	spaced := newHranaGatewayResponse()
	(&Gateway{}).writeResponse(spaced, "rid", gatewayResult{value: json.RawMessage(" \n {\"rows\":[]} \t"), validatedResult: true})
	if string(spaced.result) != `{"rows":[]}` {
		t.Fatalf("spaced validated result not compacted: %q", spaced.result)
	}
}

func BenchmarkSQLDecodeLegacyPage(b *testing.B) {
	envelope := []byte(sqlTestCompactEnvelope("page", string(sqlTestWorkerPage(256, 350))))
	b.SetBytes(int64(len(envelope)))
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var output json.RawMessage
		if err := sqlDecodeLegacy(context.Background(), envelope, "page", &output); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLDecodePage(b *testing.B) {
	envelope := []byte(sqlTestCompactEnvelope("page", string(sqlTestWorkerPage(256, 350))))
	b.SetBytes(int64(len(envelope)))
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var output json.RawMessage
		if err := sqlDecode(context.Background(), envelope, "page", &output); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHranaCaptureLegacy(b *testing.B) {
	page := sqlTestWorkerPage(256, 350)
	b.SetBytes(int64(len(page)))
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		response := newHranaGatewayResponse()
		if !response.capture(json.RawMessage(page), false) {
			b.Fatal("capture refused the page")
		}
	}
}

func BenchmarkHranaCaptureValidated(b *testing.B) {
	page := sqlTestWorkerPage(256, 350)
	b.SetBytes(int64(len(page)))
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		response := newHranaGatewayResponse()
		if !response.capture(json.RawMessage(page), true) {
			b.Fatal("capture refused the page")
		}
	}
}
