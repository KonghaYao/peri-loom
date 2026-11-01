package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

// validCursorFetchResultLegacy is the pre-span read cursor validator, kept
// verbatim as the oracle for the structural scanner.
func validCursorFetchResultLegacy(encoded json.RawMessage, maxRows int) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || len(fields) != 2 {
		return false
	}
	var done bool
	if json.Unmarshal(fields["done"], &done) != nil || string(fields["done"]) != "true" && string(fields["done"]) != "false" {
		return false
	}
	var rows [][]json.RawMessage
	return json.Unmarshal(fields["rows"], &rows) == nil && rows != nil && len(rows) <= maxRows
}

func hranaCursorPageValidatorCorpus() []struct {
	name  string
	body  string
	valid bool
} {
	return []struct {
		name  string
		body  string
		valid bool
	}{
		{"empty-done", `{"rows":[],"done":true}`, true},
		{"empty-active", `{"done":false,"rows":[]}`, true},
		{"one-empty-row", `{"done":true,"rows":[[]]}`, true},
		{"null-row", `{"done":false,"rows":[null]}`, true},
		{"mixed-rows", `{"done":false,"rows":[[1,"two"],[]]}`, true},
		{"nested-cell-values", `{"done":true,"rows":[[null,true,false,{"a":[]},[1,2]]]}`, true},
		{"spaced", " \n{\"done\" : true , \"rows\" : [ [ 1 ] ] } \t", true},
		{"spaced-done-value", `{"done": true,"rows":[]}`, true},
		{"escaped-key", `{"\u0064one":true,"rows":[]}`, true},
		{"duplicate-done-last-wins", `{"done":1,"done":true,"rows":[]}`, true},
		{"duplicate-rows-earlier-shape", `{"rows":true,"rows":[],"done":false}`, true},
		{"int64-min", `{"done":false,"rows":[[-9223372036854775808]]}`, true},
		{"int64-max", `{"done":false,"rows":[[9223372036854775807]]}`, true},
		{"integer-text-past-int64", `{"done":false,"rows":[[9223372036854775808]]}`, true},
		{"float-one", `{"done":false,"rows":[[1.0]]}`, true},
		{"float-exponent", `{"done":false,"rows":[[1e+20]]}`, true},
		{"float-negative-zero", `{"done":false,"rows":[[-0]]}`, true},
		{"float-negative-zero-fraction", `{"done":false,"rows":[[-0.0]]}`, true},
		{"float-out-of-range", `{"done":false,"rows":[[1e999]]}`, true},
		{"text-specials", `{"done":false,"rows":[["<html> & \"quoted\" \\ slash \n\t\r\b\f \u0000\u001f\u007f"]]}`, true},
		{"text-unicode", `{"done":false,"rows":[["中文 😀 \u2028\u2029 \ud83d\ude00 \ud800"]]}`, true},
		{"text-empty", `{"done":false,"rows":[[""]]}`, true},
		{"blob-array", `{"done":false,"rows":[[[0,1,255]]]}`, true},
		{"array-object-bool", `{"done":true,"rows":[[[],{},true,false]]}`, true},

		{"top-level-null", `null`, false},
		{"top-level-array", `[]`, false},
		{"empty-object", `{}`, false},
		{"missing-rows", `{"done":true}`, false},
		{"missing-done", `{"rows":[]}`, false},
		{"extra-field", `{"done":true,"rows":[],"extra":1}`, false},
		{"fused-cols", `{"done":true,"rows":[],"cols":[]}`, false},
		{"fused-autocommit", `{"done":true,"rows":[],"autocommit":true}`, false},
		{"rows-null", `{"rows":null,"done":true}`, false},
		{"rows-scalar", `{"rows":1,"done":true}`, false},
		{"rows-bool-element", `{"rows":[true],"done":true}`, false},
		{"rows-object-element", `{"rows":[{}],"done":true}`, false},
		{"rows-number-element", `{"rows":[1],"done":true}`, false},
		{"rows-trailing-comma", `{"rows":[[1],],"done":true}`, false},
		{"done-number", `{"done":1,"rows":[]}`, false},
		{"done-string", `{"done":"true","rows":[]}`, false},
		{"done-null", `{"done":null,"rows":[]}`, false},
		{"duplicate-done-last-invalid", `{"done":true,"done":1,"rows":[]}`, false},
		{"duplicate-rows-last-shape", `{"rows":[],"done":false,"rows":{}}`, false},
		{"truncated", `{"done":false,"rows":[[1]]`, false},
		{"unquoted-key", `{"done":false,rows:[]}`, false},
		{"trailing-data", `{"done":false,"rows":[]} extra`, false},
		{"object-trailing-comma", `{"done":false,"rows":[],}`, false},
		{"raw-control-in-string", "{\"done\":false,\"rows\":[[\"a\nb\"]]}", false},
		{"invalid-escape", `{"done":false,"rows":[["\x"]]}`, false},
		{"short-unicode-escape", `{"done":false,"rows":[["\u12"]]}`, false},
		{"leading-zero", `{"done":false,"rows":[[01]]}`, false},
		{"bare-fraction", `{"done":false,"rows":[[.5]]}`, false},
		{"trailing-fraction", `{"done":false,"rows":[[1.]]}`, false},
		{"bare-exponent", `{"done":false,"rows":[[1e]]}`, false},
		{"plus-exponent-missing-digits", `{"done":false,"rows":[[1e+]]}`, false},
		{"uppercase-literal", `{"done":TRUE,"rows":[]}`, false},
	}
}

func TestHranaCursorPageValidatorMatchesLegacy(t *testing.T) {
	for _, scenario := range hranaCursorPageValidatorCorpus() {
		t.Run(scenario.name, func(t *testing.T) {
			for _, maxRows := range []int{-1, 0, 1, 2, 1000} {
				body := json.RawMessage(scenario.body)
				legacy := validCursorFetchResultLegacy(body, maxRows)
				current := validCursorFetchResult(body, maxRows)
				if legacy != current {
					t.Fatalf("maxRows=%d legacy=%t current=%t for %s", maxRows, legacy, current, scenario.body)
				}
			}
		})
	}
}

// TestHranaCursorPageStructure proves the scanner reports the same done flag,
// row count and cell tokens that the old map-and-RawMessage decode produced.
func TestHranaCursorPageStructure(t *testing.T) {
	for _, scenario := range hranaCursorPageValidatorCorpus() {
		if !scenario.valid {
			continue
		}
		t.Run(scenario.name, func(t *testing.T) {
			body := []byte(scenario.body)
			var scanner hranaCursorPageScanner
			page, err := scanner.scan(body, true, false)
			if err != nil {
				t.Fatalf("scan rejected accepted page %s: %v", scenario.body, err)
			}
			// Mirror the legacy decode: a map keeps the last duplicate member.
			var fields map[string]json.RawMessage
			if json.Unmarshal(body, &fields) != nil {
				t.Fatalf("reference decode failed for %s", scenario.body)
			}
			var rows [][]json.RawMessage
			var done bool
			if json.Unmarshal(fields["rows"], &rows) != nil || json.Unmarshal(fields["done"], &done) != nil {
				t.Fatalf("reference member decode failed for %s", scenario.body)
			}
			if page.done != done || page.rows != len(rows) {
				t.Fatalf("page shape done=%t rows=%d, want done=%t rows=%d", page.done, page.rows, done, len(rows))
			}
			for index, row := range rows {
				cells := page.row(index)
				if len(cells) != len(row) {
					t.Fatalf("row %d has %d cells, want %d", index, len(cells), len(row))
				}
				for position, raw := range row {
					if token := page.cellBytes(cells[position]); !bytes.Equal(token, raw) {
						t.Fatalf("cell %d/%d token = %q, want %q", index, position, token, raw)
					}
				}
			}
		})
	}
}

func hranaCursorPageRandomText(rng *rand.Rand) string {
	switch rng.Intn(9) {
	case 0:
		return `""`
	case 1:
		return `"\u0000\u001f\u000a\u0008\u000c\u000d\u0009"`
	case 2:
		return `"<html> & </html>"`
	case 3:
		return `"\u003c\u003e\u0026\u2028\u2029"`
	case 4:
		return `"\ud83d\ude00 \ud800 \udc00"`
	case 5:
		return `"quote \" backslash \\ slash \/"`
	default:
		text := []string{"", "plain", "中文", "😀", "\u2028", "a b", "line\nbreak"}[rng.Intn(7)]
		encoded, _ := json.Marshal(text)
		return string(encoded)
	}
}

func hranaCursorPageRandomValue(rng *rand.Rand) string {
	switch rng.Intn(12) {
	case 0:
		return "null"
	case 1:
		return fmt.Sprintf("%d", rng.Int63()-1<<62)
	case 2:
		return fmt.Sprintf("%d", int64(rng.Intn(1000)-500))
	case 3:
		return []string{"9223372036854775807", "-9223372036854775808", "9223372036854775808"}[rng.Intn(3)]
	case 4:
		return []string{"0", "-0", "1.0", "-0.0", "1e+20", "1e-7", "5e-324", "1e999"}[rng.Intn(8)]
	case 5:
		return hranaCursorPageRandomText(rng)
	case 6:
		return "[]"
	case 7:
		var builder strings.Builder
		builder.WriteByte('[')
		for index := 0; index < rng.Intn(5); index++ {
			if index > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(strconv.Itoa(rng.Intn(256)))
		}
		builder.WriteByte(']')
		return builder.String()
	case 8:
		return []string{"true", "false"}[rng.Intn(2)]
	case 9:
		// Booleans and objects are valid page cells even though the cursor
		// encoders reject them later.
		return `{}`
	case 10:
		return `{"a":` + hranaCursorPageRandomValue(rng) + `}`
	default:
		return `[` + hranaCursorPageRandomValue(rng) + `,` + hranaCursorPageRandomValue(rng) + `]`
	}
}

func hranaCursorPageRandomPage(rng *rand.Rand) string {
	var builder strings.Builder
	builder.WriteString(`{"done":`)
	builder.WriteString([]string{"true", "false"}[rng.Intn(2)])
	builder.WriteString(`,"rows":[`)
	for row := 0; row < rng.Intn(5); row++ {
		if row > 0 {
			builder.WriteByte(',')
		}
		if rng.Intn(8) == 0 {
			builder.WriteString("null")
			continue
		}
		builder.WriteByte('[')
		for cell := 0; cell < rng.Intn(5); cell++ {
			if cell > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(hranaCursorPageRandomValue(rng))
		}
		builder.WriteByte(']')
	}
	builder.WriteString(`]}`)
	return builder.String()
}

func hranaCursorPageMutate(rng *rand.Rand, body []byte) []byte {
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

func TestHranaCursorPageRandomizedDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(86))
	samples := make([][]byte, 0, 8000)
	for iteration := 0; iteration < 4000; iteration++ {
		page := []byte(hranaCursorPageRandomPage(rng))
		samples = append(samples, page)
		if iteration%2 == 0 {
			samples = append(samples, hranaCursorPageMutate(rng, page))
		}
	}
	for index, sample := range samples {
		for _, maxRows := range []int{-1, 0, 2, 8, 1000} {
			legacy := validCursorFetchResultLegacy(sample, maxRows)
			current := validCursorFetchResult(sample, maxRows)
			if legacy != current {
				t.Fatalf("sample %d maxRows=%d legacy=%t current=%t page=%q", index, maxRows, legacy, current, sample)
			}
		}
	}
}

// TestHranaCursorPageFusedFirstPage covers the cursor_start page, which the
// Hrana JSON adapter reads directly and whose cols/autocommit members the old
// struct decode ignored.
func TestHranaCursorPageFusedFirstPage(t *testing.T) {
	for _, scenario := range []struct {
		body string
		rows int
	}{
		{`{"cols":[{"name":"a","decltype":"TEXT"}],"rows":[[1]],"done":true,"autocommit":false}`, 1},
		{`{"cols":[],"rows":[],"done":false,"autocommit":null}`, 0},
		{`{"cols":[],"rows":[[1]],"done":false}`, 1},
	} {
		var scanner hranaCursorPageScanner
		if _, err := scanner.scan([]byte(scenario.body), true, false); err == nil {
			t.Fatalf("plain scan accepted fused page %s", scenario.body)
		}
		page, err := scanner.scan([]byte(scenario.body), true, true)
		if err != nil {
			t.Fatalf("fused scan rejected %s: %v", scenario.body, err)
		}
		if page.rows != scenario.rows {
			t.Fatalf("fused page %s scanned rows=%d, want %d", scenario.body, page.rows, scenario.rows)
		}
	}
}

func TestHranaCursorPageDepthMatchesLegacy(t *testing.T) {
	for _, depth := range []int{1, 2, 100, 9995, 9997, 9998, 9999, 10001} {
		body := json.RawMessage(`{"done":false,"rows":[[` + strings.Repeat("[", depth) + strings.Repeat("]", depth) + `]]}`)
		legacy := validCursorFetchResultLegacy(body, 1)
		current := validCursorFetchResult(body, 1)
		if legacy != current {
			t.Fatalf("depth %d legacy=%t current=%t", depth, legacy, current)
		}
	}
}

// hranaCursorPageBenchmarkCorpus builds one 24.2 MiB page: the text-heavy
// 256-row shape the H1 cursor benchmark fetches.
func hranaCursorPageBenchmarkCorpus() []byte {
	chunk := `plain \"quoted\" \\ slash \n \t <html> & 中文 😀 \u2028 end `
	repeats := 242*1024*1024/10/256/4/len(chunk) + 1
	text := `"` + strings.Repeat(chunk, repeats) + `"`
	var builder bytes.Buffer
	builder.Grow(24400000)
	builder.WriteString(`{"done":false,"rows":[`)
	for row := 0; row < 256; row++ {
		if row > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(`[`)
		builder.WriteString(strconv.Itoa(row))
		for cell := 0; cell < 4; cell++ {
			builder.WriteByte(',')
			builder.WriteString(text)
		}
		builder.WriteString(`,1234.5,null]`)
	}
	builder.WriteString(`]}`)
	return builder.Bytes()
}

func BenchmarkCursorPageValidate(bench *testing.B) {
	page := hranaCursorPageBenchmarkCorpus()
	bench.SetBytes(int64(len(page)))
	bench.ResetTimer()
	bench.Run("legacy", func(bench *testing.B) {
		for index := 0; index < bench.N; index++ {
			if !validCursorFetchResultLegacy(page, 256) {
				bench.Fatal("legacy validator rejected the page")
			}
		}
	})
	bench.Run("span", func(bench *testing.B) {
		for index := 0; index < bench.N; index++ {
			if !validCursorFetchResult(page, 256) {
				bench.Fatal("span validator rejected the page")
			}
		}
	})
}
