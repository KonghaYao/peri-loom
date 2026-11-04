package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	controller "peri-loom/controller"
)

// cursorPageLegacyFirstPage is validCursorFirstResponse without its token walk.
// sqlDecodeLegacy already rejects the duplicate keys and over-deep nesting that
// walk enforced; the one thing it also did was decode every number as a float64
// (without UseNumber), so it refused values such as 1e999 that every other read
// path accepts as valid JSON. That accident is not part of the first-page
// contract and is pinned separately by
// TestCursorPageFirstPageAcceptsOutOfFloat64Numbers.
func cursorPageLegacyFirstPage(result json.RawMessage, maxRows int) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(result, &fields) != nil {
		return false
	}
	if raw, present := fields["autocommit"]; present {
		if string(fields["done"]) == "true" {
			if string(raw) != "true" && string(raw) != "false" {
				return false
			}
		} else if string(raw) != "null" {
			return false
		}
		delete(fields, "autocommit")
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
	if !validCursorFetchResponse(page, maxRows, false) {
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

// cursorPageLegacyAcceptance is the pre-R52 read cursor decision, with the
// first-page token walk split out as described above.
func cursorPageLegacyAcceptance(body []byte, requestID string, start bool, maxRows int) bool {
	if !utf8.Valid(body) || sqlDecodeLegacy(context.Background(), body, requestID, nil) != nil {
		return false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false
	}
	result := fields["result"]
	if start {
		return cursorPageLegacyFirstPage(result, maxRows)
	}
	return validCursorFetchResponse(result, maxRows, false)
}

// cursorPageCurrentAcceptance is the R52 read cursor decision: one structural
// locate, one envelope skeleton check and one page scan, followed by the same
// row budget and first-page rules the gateway applies.
func cursorPageCurrentAcceptance(body []byte, requestID string, start bool, maxRows int) bool {
	var reply hranaCursorPageReply
	reply.allowMetadata = start
	if sqlDecode(context.Background(), body, requestID, &reply) != nil {
		return false
	}
	if reply.page.rows > maxRows {
		return false
	}
	return !start || validCursorFirstPage(reply.page)
}

func cursorPageDifferential(t *testing.T, body []byte, requestID string, label string) {
	t.Helper()
	for _, start := range []bool{false, true} {
		for _, maxRows := range []int{-1, 0, 1, 2, 8, 1000} {
			legacy := cursorPageLegacyAcceptance(body, requestID, start, maxRows)
			current := cursorPageCurrentAcceptance(body, requestID, start, maxRows)
			if legacy != current {
				t.Fatalf("%s: start=%t maxRows=%d legacy=%t current=%t body=%q", label, start, maxRows, legacy, current, body)
			}
		}
	}
}

func cursorPageColumns(count int) string {
	if count == 0 {
		return `[]`
	}
	columns := make([]string, count)
	for index := range columns {
		decltype := []string{`"TEXT"`, `null`, `"INTEGER"`}[index%3]
		columns[index] = `{"name":"c` + fmt.Sprint(index) + `","decltype":` + decltype + `}`
	}
	return "[" + strings.Join(columns, ",") + "]"
}

func cursorPageRandomColumns(rng *rand.Rand, count int) string {
	switch rng.Intn(9) {
	case 0:
		return `null`
	case 1:
		return `[{}]`
	case 2:
		return `[{"name":"","decltype":"TEXT"}]`
	case 3:
		return `[{"name":"a","decltype":1}]`
	case 4:
		return `[{"name":"a"}]`
	default:
		return cursorPageColumns(count)
	}
}

func cursorPageRandomMembers(rng *rand.Rand) []string {
	columns := rng.Intn(3)
	done := rng.Intn(2) == 0
	withCols := rng.Intn(2) == 0
	rows := make([]string, 0, 4)
	for row := 0; row < rng.Intn(4); row++ {
		if rng.Intn(8) == 0 {
			rows = append(rows, "null")
			continue
		}
		width := columns
		if rng.Intn(8) == 0 {
			width++
		}
		cells := make([]string, width)
		for cell := range cells {
			cells[cell] = hranaCursorPageRandomValue(rng)
		}
		rows = append(rows, "["+strings.Join(cells, ",")+"]")
	}
	// The old first-page rule refuses an active page with no rows; keep such a
	// page from being valid by accident so the accepted path stays exercised.
	if !done && len(rows) == 0 {
		cells := make([]string, columns)
		for cell := range cells {
			cells[cell] = hranaCursorPageRandomValue(rng)
		}
		rows = append(rows, "["+strings.Join(cells, ",")+"]")
	}
	doneToken := "true"
	if !done {
		doneToken = "false"
	}
	members := []string{
		`"done":` + doneToken,
		`"rows":[` + strings.Join(rows, ",") + `]`,
	}
	if withCols {
		if rng.Intn(8) == 0 {
			members = append(members, `"cols":`+cursorPageRandomColumns(rng, columns))
		} else {
			members = append(members, `"cols":`+cursorPageColumns(columns))
		}
	}
	if rng.Intn(3) == 0 {
		valid := `"null"`
		if done {
			valid = []string{"true", "false"}[rng.Intn(2)]
		}
		invalid := []string{"null", "true", "false", `"false"`, "0", "{}"}[rng.Intn(6)]
		if rng.Intn(6) == 0 {
			members = append(members, `"autocommit":`+invalid)
		} else {
			members = append(members, `"autocommit":`+valid)
		}
	}
	if rng.Intn(12) == 0 {
		members = append(members, `"extra":`+hranaCursorPageRandomValue(rng))
	}
	if rng.Intn(20) == 0 {
		// A duplicate member is valid JSON; the envelope walk must reject it.
		members = append(members, members[rng.Intn(len(members))])
	}
	rng.Shuffle(len(members), func(left, right int) { members[left], members[right] = members[right], members[left] })
	return members
}

func cursorPageRandomBody(rng *rand.Rand, requestID string) string {
	page := "{" + strings.Join(cursorPageRandomMembers(rng), ",") + "}"
	switch rng.Intn(6) {
	case 0:
		page = " \n\t" + strings.ReplaceAll(page, ",", " , ") + " \t\r\n "
	case 1:
		page = "{" + strings.ReplaceAll(strings.TrimPrefix(strings.TrimSuffix(page, "}"), "{"), ":", " : ") + "}"
	}
	fields := []string{
		`"version":1`,
		`"request_id":"` + requestID + `"`,
		`"ok":true`,
		`"result":` + page,
		`"error":null`,
	}
	if rng.Intn(3) == 0 {
		fields = append(fields, `"execution_completed":false`)
	}
	return "{" + strings.Join(fields, ",") + "}"
}

// TestCursorPageDecodeDifferential compares the new single-pass read cursor
// decode with the old combination on random pages: every cols/autocommit
// combination, null rows, nested cells, line separators and spacing must reach
// the same decision.
func TestCursorPageDecodeDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(52))
	for iteration := 0; iteration < 6000; iteration++ {
		body := []byte(cursorPageRandomBody(rng, "rid"))
		cursorPageDifferential(t, body, "rid", fmt.Sprintf("sample %d", iteration))
	}
}

func cursorPageRandomMutation(rng *rand.Rand, body []byte) []byte {
	noise := []byte(`{}[],:"\ 	` + "\n0123456789truefalsenull\xff")
	position := rng.Intn(len(body))
	switch rng.Intn(5) {
	case 0:
		return body[:position]
	case 1:
		return append(body[:position:position], body[position+1:]...)
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

// TestCursorPageDecodeRejectsMalformedBodies mutates whole envelopes, not just
// pages: a broken envelope, truncation, invalid escapes, duplicate keys, an
// illegal number or invalid UTF-8 must be refused without a panic, and the
// decision must match the old combination.
func TestCursorPageDecodeRejectsMalformedBodies(t *testing.T) {
	rng := rand.New(rand.NewSource(152))
	valid := []byte(cursorPageRandomBody(rng, "rid"))
	for iteration := 0; iteration < 8000; iteration++ {
		mutated := cursorPageRandomMutation(rng, valid)
		cursorPageDifferential(t, mutated, "rid", fmt.Sprintf("mutation %d", iteration))
	}
}

func cursorPageHandcraftedBodies(requestID string) []string {
	page := `{"rows":[[1]],"done":true}`
	return []string{
		`{}`,
		`null`,
		`[]`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + page + `,"error":null,"execution_completed":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":false,"result":` + page + `,"error":"x"}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + page + `,"error":"x"}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + page + `,"error":null,"extra":1}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + page + `,"error":null,"response":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":null,"result":` + page + `,"error":null}`,
		`{"version":2,"request_id":"` + requestID + `","ok":true,"result":` + page + `,"error":null}`,
		`{"version":1,"request_id":"other","ok":true,"result":` + page + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + page + `,"result":` + page + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + `{"rows":[],"done":true,"rows":[]}` + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + `{"rows":[],"done":true,"extra":1}` + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + `{"rows":[[1,]],"done":true}` + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + `{"rows":[[1.]],"done":true}` + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + `{"rows":[[01]],"done":true}` + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + `{"rows":[["\x"]],"done":true}` + `,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + `{"rows":[["\u12"]],"done":true}` + `,"error":null}`,
		"{\"version\":1,\"request_id\":\"" + requestID + "\",\"ok\":true,\"result\":{\"rows\":[[\"a\nb\"]],\"done\":true},\"error\":null}",
		"{\"version\":1,\"request_id\":\"" + requestID + "\",\"ok\":true,\"result\":{\"rows\":[[\"\xff\"]],\"done\":true},\"error\":null}",
		"{\"version\":1,\"request_id\":\"" + requestID + "\",\"ok\":true,\"result\":{\"rows\":[[\"\xe2\x80\xa8\"]],\"done\":true},\"error\":null}",
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":{"done":true},"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":{"rows":[]},"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":{"rows":null,"done":true},"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + page + `,"error":null,"execution_completed":true}`,
		`{"version":1,"request_id":"` + requestID + `","ok":false,"result":null,"error":"x","execution_completed":true}`,
		`{"version":1,"request_id":"` + requestID + `","ok":false,"result":null,"error":"x","execution_completed":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":false,"result":null,"error":null}`,
		`{"version":1,"request_id":"` + requestID + `","ok":true,"result":` + strings.Repeat("[", 129) + strings.Repeat("]", 129) + `,"error":null}`,
	}
}

func TestCursorPageDecodeHandcraftedBodies(t *testing.T) {
	for _, body := range cursorPageHandcraftedBodies("rid") {
		cursorPageDifferential(t, []byte(body), "rid", body)
	}
}

// TestCursorPageDecodeArbitraryBytes feeds the structural locator and the page
// scan bytes that never formed a valid envelope, so a boundary bug cannot rely
// on the mutator only producing near-JSON.
func TestCursorPageDecodeArbitraryBytes(t *testing.T) {
	rng := rand.New(rand.NewSource(252))
	alphabet := []byte(`{}[],:"\ 	` + "\n0123456789truefalsenull\x00\xff\xe2\x80")
	for iteration := 0; iteration < 5000; iteration++ {
		body := make([]byte, rng.Intn(64))
		for index := range body {
			body[index] = alphabet[rng.Intn(len(alphabet))]
		}
		var reply hranaCursorPageReply
		reply.allowMetadata = rng.Intn(2) == 0
		if err := sqlDecode(context.Background(), body, "rid", &reply); err == nil && !json.Valid(body) {
			t.Fatalf("decoder accepted invalid JSON: %q", body)
		}
	}
}

// TestCursorPageDecodeDepthMatchesLegacy pins the depth rule. The old decoder
// rejected any value below 128 containers, so both the container and the scalar
// inside the innermost container matter.
func TestCursorPageDecodeDepthMatchesLegacy(t *testing.T) {
	for _, depth := range []int{1, 100, 122, 123, 124, 125, 126, 127, 128, 129} {
		container := strings.Repeat("[", depth) + strings.Repeat("]", depth)
		scalar := strings.Repeat("[", depth) + "1" + strings.Repeat("]", depth)
		for _, result := range []string{
			`{"rows":[[` + container + `]],"done":true}`,
			`{"rows":[[` + scalar + `]],"done":true}`,
		} {
			body := []byte(`{"version":1,"request_id":"rid","ok":true,"result":` + result + `,"error":null}`)
			cursorPageDifferential(t, body, "rid", fmt.Sprintf("depth %d", depth))
		}
	}
}

// TestCursorPageFirstPageRules covers the fused first page one rule at a time.
func TestCursorPageFirstPageRules(t *testing.T) {
	columns := `[{"name":"a","decltype":"TEXT"}]`
	for _, scenario := range []struct {
		name  string
		page  string
		valid bool
	}{
		{"valid_done", `{"cols":` + columns + `,"rows":[["x"]],"done":true}`, true},
		{"empty_active_page", `{"cols":` + columns + `,"rows":[],"done":false}`, false},
		{"missing_cols", `{"rows":[],"done":true}`, false},
		{"null_cols", `{"cols":null,"rows":[],"done":true}`, false},
		{"empty_cols_empty_row", `{"cols":[],"rows":[[]],"done":true}`, true},
		{"autocommit_true_done", `{"cols":` + columns + `,"rows":[["x"]],"done":true,"autocommit":true}`, true},
		{"autocommit_false_done", `{"cols":` + columns + `,"rows":[["x"]],"done":true,"autocommit":false}`, true},
		{"autocommit_null_done", `{"cols":` + columns + `,"rows":[["x"]],"done":true,"autocommit":null}`, false},
		{"autocommit_text_done", `{"cols":` + columns + `,"rows":[["x"]],"done":true,"autocommit":"false"}`, false},
		{"autocommit_null_active", `{"cols":` + columns + `,"rows":[],"done":false,"autocommit":null}`, false},
		{"autocommit_true_active", `{"cols":` + columns + `,"rows":[],"done":false,"autocommit":true}`, false},
		{"missing_done", `{"cols":` + columns + `,"rows":[]}`, false},
		{"missing_rows", `{"cols":` + columns + `,"done":false}`, false},
		{"null_row", `{"cols":` + columns + `,"rows":[null],"done":true}`, false},
		{"row_width_short", `{"cols":` + columns + `,"rows":[[]],"done":true}`, false},
		{"row_width_long", `{"cols":` + columns + `,"rows":[["x","y"]],"done":true}`, false},
		{"two_columns", `{"cols":[{"name":"a","decltype":"TEXT"},{"name":"b","decltype":null}],"rows":[[1,2]],"done":true}`, true},
		{"unknown_member", `{"cols":` + columns + `,"rows":[["x"]],"done":true,"extra":1}`, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			body := []byte(`{"version":1,"request_id":"rid","ok":true,"result":` + scenario.page + `,"error":null}`)
			cursorPageDifferential(t, body, "rid", scenario.name)
			var reply hranaCursorPageReply
			reply.allowMetadata = true
			if err := sqlDecode(context.Background(), body, "rid", &reply); err != nil {
				if scenario.valid {
					t.Fatalf("decode rejected an expected-valid page %s: %v", scenario.page, err)
				}
				return
			}
			if got := validCursorFirstPage(reply.page); got != scenario.valid {
				t.Fatalf("%s: validCursorFirstPage=%t, want %t", scenario.page, got, scenario.valid)
			}
		})
	}
}

// TestCursorPageScannerDepthBound pins the configurable scanner bound: with the
// decode limit it rejects exactly the pages sqlJSONSkipValue rejects, including
// a scalar below the limit, while the adapter fallback keeps encoding/json's
// 10000-container rule.
func TestCursorPageScannerDepthBound(t *testing.T) {
	for _, depth := range []int{1, 123, 124, 125, 126, 127, 128, 129} {
		for _, suffix := range []string{"", "1"} {
			page := []byte(`{"rows":[[` + strings.Repeat("[", depth) + suffix + strings.Repeat("]", depth) + `]],"done":true}`)
			_, legacy := sqlJSONSkipValue(context.Background(), page, 0, 1)
			var decoder hranaCursorPageScanner
			decoder.maxDepth = sqlJSONDecodeMaxDepth
			_, decoded := decoder.scan(page, true, false)
			if (legacy == nil) != (decoded == nil) {
				t.Fatalf("depth %d suffix %q: skip=%v scan=%v", depth, suffix, legacy, decoded)
			}
			var fallback hranaCursorPageScanner
			if _, err := fallback.scan(page, true, false); err != nil {
				t.Fatalf("fallback scanner rejected depth %d suffix %q: %v", depth, suffix, err)
			}
		}
	}
}

// TestCursorPageFirstPageAcceptsOutOfFloat64Numbers pins the one deliberate
// difference from validCursorFirstResponse. Its token walk decoded every number
// as a float64, so a valid JSON number such as 1e999 failed the fused first
// page while the same number passed the fetch validator and the envelope
// decoder. SQLite values (int64 and finite f64) cannot produce one, and the
// first page now follows the same rule as the rest of the read path.
func TestCursorPageFirstPageAcceptsOutOfFloat64Numbers(t *testing.T) {
	page := `{"cols":[{"name":"a","decltype":"REAL"}],"rows":[[1e999]],"done":true}`
	if validCursorFirstResponse(json.RawMessage(page), 256, false) {
		t.Fatal("legacy first-page validator accepted an out-of-float64 number")
	}
	body := []byte(`{"version":1,"request_id":"rid","ok":true,"result":` + page + `,"error":null}`)
	var reply hranaCursorPageReply
	reply.allowMetadata = true
	if err := sqlDecode(context.Background(), body, "rid", &reply); err != nil {
		t.Fatalf("valid JSON number rejected: %v", err)
	}
	if !validCursorFirstPage(reply.page) {
		t.Fatal("first page with a valid JSON number was refused")
	}
}

// TestCursorPageDecodeKeepsTheSpan proves the strict decoder indexes the page
// in place: no result copy and no ownership change for the reply body.
func TestCursorPageDecodeKeepsTheSpan(t *testing.T) {
	body := []byte(sqlTestCompactEnvelope("rid", `{"cols":[{"name":"a","decltype":"TEXT"}],"rows":[[1,"x"]],"done":true,"autocommit":true}`))
	fields, err := sqlJSONFieldSpans(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	span := fields["result"]
	var reply hranaCursorPageReply
	reply.allowMetadata = true
	if err := sqlDecode(context.Background(), body, "rid", &reply); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(reply.page.source) != span.end-span.start || &reply.page.source[0] != &body[span.start] {
		t.Fatal("page span was copied out of the reply body")
	}
	if !reply.page.hasCols || !reply.page.hasAutocommit || string(reply.page.autocommit) != "true" {
		t.Fatalf("fused metadata spans missing: cols=%t autocommit=%t", reply.page.hasCols, reply.page.hasAutocommit)
	}
	if !reply.page.compact {
		t.Fatal("compact page reported as spaced")
	}
	spaced := []byte(sqlTestCompactEnvelope("rid", " \n{\"rows\" : [ [ 1 ] ], \"done\" : true}"))
	reply = hranaCursorPageReply{}
	if err := sqlDecode(context.Background(), spaced, "rid", &reply); err != nil {
		t.Fatalf("spaced decode failed: %v", err)
	}
	if reply.page.compact {
		t.Fatal("spaced page reported as compact")
	}
}

// TestCursorPageReplyJSONFallback covers the fixture path: an upstream that
// answers through encoding/json still produces a scanned page.
func TestCursorPageReplyJSONFallback(t *testing.T) {
	var reply hranaCursorPageReply
	reply.allowMetadata = true
	if err := json.Unmarshal([]byte(`{"cols":[{"name":"a","decltype":"TEXT"}],"rows":[[1]],"done":true}`), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.page.rows != 1 || !reply.page.hasCols || !validCursorFirstPage(reply.page) {
		t.Fatalf("fallback page incomplete: %+v", reply.page)
	}
	plain := hranaCursorPageReply{}
	if err := json.Unmarshal([]byte(`{"cols":[],"rows":[],"done":true}`), &plain); err == nil {
		t.Fatal("fallback accepted metadata without permission")
	}
}

// TestCursorPageHandoverKeepsPageOnlyForTheSameSpan covers the capture rule: the
// page survives while the published bytes stay the same span, and a compacted or
// line-separator-escaped copy drops it so the adapter scans again.
func TestCursorPageHandoverKeepsPageOnlyForTheSameSpan(t *testing.T) {
	compact := []byte(`{"rows":[[1]],"done":true}`)
	var scanner hranaCursorPageScanner
	page, err := scanner.scan(compact, true, false)
	if err != nil {
		t.Fatal(err)
	}
	response := newHranaGatewayResponse()
	if !response.capture(json.RawMessage(page.source), true, &page) {
		t.Fatal("capture refused the page")
	}
	if reply := response.reply("/test"); reply.cursorPage != &page {
		t.Fatal("compact page was not handed to the adapter")
	}

	spacedBody := []byte(" \n{\"rows\" : [[1]], \"done\":true} \t")
	spacedPage, err := scanner.scan(spacedBody, true, false)
	if err != nil {
		t.Fatal(err)
	}
	spaced := newHranaGatewayResponse()
	if !spaced.capture(json.RawMessage(spacedPage.source), true, &spacedPage) {
		t.Fatal("capture refused the spaced page")
	}
	if spaced.cursorPage != nil {
		t.Fatal("compacted page kept stale offsets")
	}
	if string(spaced.result) != `{"rows":[[1]],"done":true}` {
		t.Fatalf("spaced page bytes changed: %q", spaced.result)
	}

	separatorBody := []byte("{\"rows\":[[\"\xe2\x80\xa8\"]],\"done\":true}")
	separatorPage, err := scanner.scan(separatorBody, true, false)
	if err != nil {
		t.Fatal(err)
	}
	separator := newHranaGatewayResponse()
	if !separator.capture(json.RawMessage(separatorPage.source), true, &separatorPage) {
		t.Fatal("capture refused the separator page")
	}
	if separator.cursorPage != nil {
		t.Fatal("escaped page kept stale offsets")
	}
	if !strings.Contains(string(separator.result), `\u2028`) {
		t.Fatalf("line separator was not escaped: %q", separator.result)
	}

	foreign := newHranaGatewayResponse()
	if !foreign.capture(json.RawMessage(`{"rows":[],"done":true}`), true, &page) {
		t.Fatal("capture refused a foreign page result")
	}
	if foreign.cursorPage != nil {
		t.Fatal("page whose span is not the result was kept")
	}
}

// TestReadCursorPageSkipsAdapterRescan runs one fused read cursor through the
// in-process Hrana adapter. The fixture upstream scans the page through the
// JSON fallback; the gateway keeps that page, so the adapter must not scan the
// result a second time.
func TestReadCursorPageSkipsAdapterRescan(t *testing.T) {
	fixture := newGatewayFixture(t)
	columns := []any{map[string]any{"name": "answer", "decltype": "INTEGER"}}
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		switch command["command"] {
		case "describe":
			return map[string]any{"params": []any{}, "cols": columns, "is_readonly": true, "is_explain": false}, nil
		case "open_read_cursor_first":
			return map[string]any{"cols": columns, "rows": [][]any{{int64(42)}}, "done": true}, nil
		}
		return nil, nil
	})
	scans := 0
	hranaCursorPageScanHook = func() { scans++ }
	defer func() { hranaCursorPageScanHook = nil }()
	body := `{"baton":null,"batch":{"steps":[{"stmt":{"sql":"SELECT 42","args":[],"named_args":[],"want_rows":true}}]}}`
	response := hranaV3JSONCall(newHranaHTTP(fixture.gateway), fixture.tokenA, hranaV3JSONPath+"/cursor", "application/json", body)
	if response.Code != 200 || strings.Count(response.Body.String(), `"type":"row"`) != 1 {
		t.Fatalf("cursor failed: %d %s", response.Code, response.Body.String())
	}
	if scans != 1 {
		t.Fatalf("read cursor page was scanned %d times, want 1", scans)
	}
}

// TestSQLWriterPageKeepsMetadataPermission covers the v2 decode path: it
// re-decodes into a temporary value, which must carry the first-page permission.
func TestSQLWriterPageKeepsMetadataPermission(t *testing.T) {
	fields := map[string]any{
		"version":       2,
		"request_id":    "request",
		"ok":            true,
		"result":        json.RawMessage(`{"cols":[{"name":"a","decltype":"INTEGER"}],"rows":[[1]],"done":true}`),
		"error":         nil,
		"session_state": writerTestState(),
	}
	body, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	reply := hranaCursorPageReply{allowMetadata: true}
	state, err := sqlDecodeWriter(context.Background(), body, "request", &reply)
	if err != nil || state == nil {
		t.Fatalf("writer decode failed: state=%v err=%v", state, err)
	}
	if !reply.page.hasCols || reply.page.rows != 1 || !validCursorFirstPage(reply.page) {
		t.Fatalf("writer path lost the page: %+v", reply.page)
	}
}
