package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func hranaCursorPageEncodeCorpus() []struct {
	name string
	body string
} {
	rawSeparators := "\u2028\u2029"
	return []struct {
		name string
		body string
	}{
		{"text", `{"done":false,"rows":[["hello","","<html> & \"quoted\" \\ slash \/ \n\t\r\b\f","\u0000\u001f\u007f","中文 😀 ` + rawSeparators + `","\ud83d\ude00 \ud800 \udc00"]]}`},
		{"text-escapes", `{"done":false,"rows":[["\u0000\u0001\u0008\u0009\u000a\u000b\u000c\u000d\u001f","\u0022\u005c\u002f\u003c\u003e\u0026\u2028\u2029"]]}`},
		{"integers", `{"done":false,"rows":[[null,0,-0,42,-42,9223372036854775807,-9223372036854775808]]}`},
		{"integers-past-int64", `{"done":false,"rows":[[9223372036854775808,123456789012345678901234567890]]}`},
		{"floats", `{"done":false,"rows":[[1.0,-0.0,1e+20,1e-7,5e-324,1.7976931348623157e308,1e-6,1e21,-1.5]]}`},
		{"float-out-of-range", `{"done":false,"rows":[[1e999]]}`},
		{"blobs", `{"done":false,"rows":[[[],[0],[255],[0,1,254,255]]]}`},
		{"blob-invalid-range", `{"done":false,"rows":[[[256],[-1],[1.0],["x"],[null]]]}`},
		{"booleans-and-objects", `{"done":false,"rows":[[true,false,{},{"a":1},"after"]]}`},
		{"empty-rows", `{"done":true,"rows":[[],["only"],["a","b","c"]]}`},
		{"mixed", `{"done":true,"rows":[[null,1,"two",2.5,[3]],[4,"five",null]]}`},
	}
}

func hranaCursorPageLegacyRows(t *testing.T, body []byte) (rows [][]any, done bool) {
	t.Helper()
	var page struct {
		Rows [][]any `json:"rows"`
		Done bool    `json:"done"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&page) != nil || page.Rows == nil {
		t.Fatalf("legacy page decode failed: %s", body)
	}
	return page.Rows, page.Done
}

func TestHranaCursorPageJSONTranscodeMatchesLegacy(t *testing.T) {
	scenarios := append(hranaCursorPageEncodeCorpus(), struct {
		name string
		body string
	}{"long-row", longRow(1000)})
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			body := []byte(scenario.body)
			legacy, done := hranaCursorPageLegacyRows(t, body)
			var scanner hranaCursorPageScanner
			page, err := scanner.scan(body, true, false)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if page.done != done || page.rows != len(legacy) {
				t.Fatalf("done=%t rows=%d, want done=%t rows=%d", page.done, page.rows, done, len(legacy))
			}
			for index, cells := range legacy {
				var want hranaJSONCursorRow
				want.begin()
				wantErr := want.appendCells(cells)
				var got hranaJSONCursorRow
				got.begin()
				gotErr := got.appendPageRow(&page, index)
				if (wantErr == nil) != (gotErr == nil) {
					t.Fatalf("row %d validity: legacy=%v direct=%v", index, wantErr, gotErr)
				}
				if wantErr != nil {
					if wantErr.Error() != gotErr.Error() {
						t.Fatalf("row %d error: legacy=%v direct=%v", index, wantErr, gotErr)
					}
					continue
				}
				if !bytes.Equal(want.bytes(), got.bytes()) {
					t.Fatalf("row %d bytes differ\nlegacy: %s\ndirect: %s", index, want.bytes(), got.bytes())
				}
			}
		})
	}
}

func TestHranaCursorPageProtoTranscodeMatchesLegacy(t *testing.T) {
	scenarios := append(hranaCursorPageEncodeCorpus(), struct {
		name string
		body string
	}{"long-row", longRow(1000)})
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			body := []byte(scenario.body)
			legacy, done := hranaCursorPageLegacyRows(t, body)
			var scanner hranaCursorPageScanner
			page, err := scanner.scan(body, true, false)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if page.done != done || page.rows != len(legacy) {
				t.Fatalf("done=%t rows=%d, want done=%t rows=%d", page.done, page.rows, done, len(legacy))
			}
			for index, cells := range legacy {
				var want hranaProtoCursorRowFrame
				want.begin()
				wantErr := error(nil)
				for _, cell := range cells {
					if wantErr = want.appendCell(cell); wantErr != nil {
						break
					}
				}
				var got hranaProtoCursorRowFrame
				got.begin()
				gotErr := got.appendPageRow(&page, index)
				if (wantErr == nil) != (gotErr == nil) {
					t.Fatalf("row %d validity: legacy=%v direct=%v", index, wantErr, gotErr)
				}
				if wantErr != nil {
					continue
				}
				if !bytes.Equal(want.frame(), got.frame()) {
					t.Fatalf("row %d bytes differ\nlegacy: %x\ndirect: %x", index, want.frame(), got.frame())
				}
			}
		})
	}
}

// TestHranaJSONAppendPageTextMatchesAppendString pins the direct text writer to
// the escaping the decoded-string writer produces.
func TestHranaJSONAppendPageTextMatchesAppendString(t *testing.T) {
	tokens := []string{
		`""`,
		`"plain"`,
		`"<html> & \"quoted\" \\ slash \/ \n\t\r\b\f"`,
		`"\u0000\u0001\u0008\u0009\u000a\u000b\u000c\u000d\u001f\u007f"`,
		`"\u0022\u005c\u002f\u003c\u003e\u0026\u2028\u2029"`,
		`"中文 😀 é ÿ"`,
		`"\ud83d\ude00 \ud800 \udc00 \ud800\udc00"`,
		`"\u00e9\u00ff\u0100\u07ff\ud7ff\ue000\uffff"`,
	}
	for index, token := range tokens {
		var decoded string
		if json.Unmarshal([]byte(token), &decoded) != nil {
			t.Fatalf("token %d is not valid JSON: %s", index, token)
		}
		want := hranaJSONAppendString(nil, decoded)
		got := hranaJSONAppendPageText(nil, []byte(token))
		if !bytes.Equal(want, got) {
			t.Fatalf("token %d escaping differs\nwant: %s\ngot:  %s", index, want, got)
		}
	}
}

func TestHranaCursorPageAppendTextMatchesDecode(t *testing.T) {
	tokens := []string{
		`""`,
		`"plain"`,
		`"<html> & \"quoted\" \\ slash \/ \n\t\r\b\f"`,
		`"\u0000\u001f\u007f"`,
		`"中文 😀 \u2028\u2029"`,
		`"\ud83d\ude00 \ud800 \udc00"`,
	}
	for index, token := range tokens {
		var decoded string
		if json.Unmarshal([]byte(token), &decoded) != nil {
			t.Fatalf("token %d is not valid JSON: %s", index, token)
		}
		got := hranaCursorPageAppendText(nil, []byte(token))
		if !bytes.Equal(got, []byte(decoded)) {
			t.Fatalf("token %d decode differs\nwant: %q\ngot:  %q", index, decoded, got)
		}
	}
}

func TestHranaCursorPageRecordReuse(t *testing.T) {
	var scanner hranaCursorPageScanner
	for round := 0; round < 3; round++ {
		for _, scenario := range hranaCursorPageEncodeCorpus() {
			body := []byte(scenario.body)
			legacy, _ := hranaCursorPageLegacyRows(t, body)
			page, err := scanner.scan(body, true, false)
			if err != nil {
				t.Fatalf("round %d scan failed: %v", round, err)
			}
			if page.rows != len(legacy) {
				t.Fatalf("round %d rows=%d, want %d", round, page.rows, len(legacy))
			}
			for index := range legacy {
				var row hranaJSONCursorRow
				row.begin()
				if row.appendPageRow(&page, index) != nil {
					continue
				}
				var want hranaJSONCursorRow
				want.begin()
				if want.appendCells(legacy[index]) == nil && !bytes.Equal(row.bytes(), want.bytes()) {
					t.Fatalf("round %d row %d diverged after reuse", round, index)
				}
			}
		}
	}
}

func BenchmarkCursorPageTranscode(bench *testing.B) {
	pageBytes := hranaCursorPageBenchmarkCorpus()
	var scanner hranaCursorPageScanner
	bench.SetBytes(int64(len(pageBytes)))
	bench.ResetTimer()
	bench.Run("json-legacy", func(bench *testing.B) {
		var row hranaJSONCursorRow
		for iteration := 0; iteration < bench.N; iteration++ {
			var decoded struct {
				Rows [][]any `json:"rows"`
			}
			decoder := json.NewDecoder(bytes.NewReader(pageBytes))
			decoder.UseNumber()
			if decoder.Decode(&decoded) != nil {
				bench.Fatal("legacy decode failed")
			}
			for index := range decoded.Rows {
				row.begin()
				if row.appendCells(decoded.Rows[index]) != nil {
					bench.Fatal("legacy encode failed")
				}
				_ = row.bytes()
			}
		}
	})
	bench.Run("json-span", func(bench *testing.B) {
		var row hranaJSONCursorRow
		for iteration := 0; iteration < bench.N; iteration++ {
			page, err := scanner.scan(pageBytes, true, false)
			if err != nil {
				bench.Fatal("scan failed")
			}
			for index := 0; index < page.rows; index++ {
				row.begin()
				if row.appendPageRow(&page, index) != nil {
					bench.Fatal("direct encode failed")
				}
				_ = row.bytes()
			}
		}
	})
	bench.Run("proto-legacy", func(bench *testing.B) {
		var frame hranaProtoCursorRowFrame
		for iteration := 0; iteration < bench.N; iteration++ {
			var decoded struct {
				Rows [][]any `json:"rows"`
			}
			decoder := json.NewDecoder(bytes.NewReader(pageBytes))
			decoder.UseNumber()
			if decoder.Decode(&decoded) != nil {
				bench.Fatal("legacy decode failed")
			}
			for index := range decoded.Rows {
				frame.begin()
				for _, cell := range decoded.Rows[index] {
					if frame.appendCell(cell) != nil {
						bench.Fatal("legacy encode failed")
					}
				}
				_ = frame.frame()
			}
		}
	})
	bench.Run("proto-span", func(bench *testing.B) {
		var frame hranaProtoCursorRowFrame
		for iteration := 0; iteration < bench.N; iteration++ {
			page, err := scanner.scan(pageBytes, true, false)
			if err != nil {
				bench.Fatal("scan failed")
			}
			for index := 0; index < page.rows; index++ {
				frame.begin()
				if frame.appendPageRow(&page, index) != nil {
					bench.Fatal("direct encode failed")
				}
				_ = frame.frame()
			}
		}
	})
}

// longRow keeps a table-shaped page from hiding per-row buffer growth.
func longRow(cells int) string {
	var builder strings.Builder
	builder.WriteString(`{"done":true,"rows":[[`)
	for index := 0; index < cells; index++ {
		if index > 0 {
			builder.WriteByte(',')
		}
		fmt.Fprintf(&builder, `"%d"`, index)
	}
	builder.WriteString(`]]}`)
	return builder.String()
}
