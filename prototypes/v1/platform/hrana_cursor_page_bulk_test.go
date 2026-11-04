package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// The bulk helpers below are the reference byte loops the fast paths must agree
// with; random inputs keep them honest about 8-byte alignment and length edges.
func hranaCursorPageReferenceIndexSpecial(body []byte) int {
	for index, character := range body {
		if character == '"' || character == '\\' || character < 0x20 {
			return index
		}
	}
	return len(body)
}

func hranaCursorPageReferenceIndexJSONSpecial(body []byte) int {
	for index, character := range body {
		switch character {
		case '\\', '<', '>', '&', 0xe2:
			return index
		}
	}
	return len(body)
}

func TestHranaCursorPageIndexSpecialMatchesReference(t *testing.T) {
	random := rand.New(rand.NewSource(51))
	alphabet := []byte{'"', '\\', 0x00, 0x1f, 0x20, 'a', 'Z', 0x7f, 0x80, 0xe2, 0xff}
	for iteration := 0; iteration < 2000; iteration++ {
		body := make([]byte, random.Intn(80))
		for index := range body {
			body[index] = alphabet[random.Intn(len(alphabet))]
		}
		got := hranaCursorPageIndexSpecial(body)
		want := hranaCursorPageReferenceIndexSpecial(body)
		if got != want {
			t.Fatalf("iteration %d body %x: got %d, want %d", iteration, body, got, want)
		}
	}
}

func TestHranaCursorPageIndexJSONSpecialMatchesReference(t *testing.T) {
	random := rand.New(rand.NewSource(52))
	alphabet := []byte{'\\', '<', '>', '&', 0xe2, 'a', 'Z', 0x7f, 0x80, 0xff}
	for iteration := 0; iteration < 2000; iteration++ {
		body := make([]byte, random.Intn(80))
		for index := range body {
			body[index] = alphabet[random.Intn(len(alphabet))]
		}
		got := hranaCursorPageIndexJSONSpecial(body)
		want := hranaCursorPageReferenceIndexJSONSpecial(body)
		if got != want {
			t.Fatalf("iteration %d body %x: got %d, want %d", iteration, body, got, want)
		}
	}
}

func TestHranaCursorPageIndexBackslashMatchesReference(t *testing.T) {
	random := rand.New(rand.NewSource(53))
	alphabet := []byte{'\\', 'a', '"', 0x00, 0xff}
	for iteration := 0; iteration < 2000; iteration++ {
		token := make([]byte, 2+random.Intn(60))
		for index := range token {
			token[index] = alphabet[random.Intn(len(alphabet))]
		}
		token[0], token[len(token)-1] = '"', '"'
		position := 1 + random.Intn(len(token)-1)
		got := hranaCursorPageIndexBackslash(token, position)
		want := -1
		if found := bytes.IndexByte(token[position:len(token)-1], '\\'); found >= 0 {
			want = found
		}
		if got != want {
			t.Fatalf("iteration %d token %x position %d: got %d, want %d", iteration, token, position, got, want)
		}
	}
}

// hranaCursorPageRandomTextToken builds one valid JSON text token whose pieces
// exercise raw runs, every escape form, and multi-byte runes the writer treats
// specially.
func hranaCursorPageRandomTextToken(random *rand.Rand) []byte {
	plain := "abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789<>:{}[]()!@#$%^&*_+-=.,;?/"
	literal := []string{"<", ">", "&", "\u2028", "\u2029", "中文", "😀", "\u007f", "é"}
	var builder strings.Builder
	builder.WriteByte('"')
	for piece := random.Intn(12); piece > 0; piece-- {
		switch random.Intn(8) {
		case 0:
			for length := random.Intn(24); length > 0; length-- {
				builder.WriteByte(plain[random.Intn(len(plain))])
			}
		case 1:
			builder.WriteString(`\"`)
		case 2:
			builder.WriteString(`\\`)
		case 3:
			builder.WriteString(`\/`)
		case 4:
			escapes := []string{`\b`, `\f`, `\n`, `\r`, `\t`}
			builder.WriteString(escapes[random.Intn(len(escapes))])
		case 5:
			fmt.Fprintf(&builder, `\u%04x`, random.Intn(0x10000))
		case 6:
			fmt.Fprintf(&builder, `\u%04x\u%04x`, 0xd800+random.Intn(0x400), 0xdc00+random.Intn(0x400))
		default:
			builder.WriteString(literal[random.Intn(len(literal))])
		}
	}
	builder.WriteByte('"')
	return []byte(builder.String())
}

func TestHranaCursorPageAppendTextRandomizedMatchesDecode(t *testing.T) {
	random := rand.New(rand.NewSource(54))
	for iteration := 0; iteration < 2000; iteration++ {
		token := hranaCursorPageRandomTextToken(random)
		var decoded string
		if err := json.Unmarshal(token, &decoded); err != nil {
			t.Fatalf("iteration %d token %s is not valid JSON: %v", iteration, token, err)
		}
		got := hranaCursorPageAppendText(nil, token)
		if !bytes.Equal(got, []byte(decoded)) {
			t.Fatalf("iteration %d token %s decode differs\nwant: %q\ngot:  %q", iteration, token, decoded, got)
		}
	}
}

func TestHranaJSONAppendPageTextRandomizedMatchesAppendString(t *testing.T) {
	random := rand.New(rand.NewSource(55))
	for iteration := 0; iteration < 2000; iteration++ {
		token := hranaCursorPageRandomTextToken(random)
		var decoded string
		if err := json.Unmarshal(token, &decoded); err != nil {
			t.Fatalf("iteration %d token %s is not valid JSON: %v", iteration, token, err)
		}
		want := hranaJSONAppendString(nil, decoded)
		got := hranaJSONAppendPageText(nil, token)
		if !bytes.Equal(want, got) {
			t.Fatalf("iteration %d token %s escaping differs\nwant: %s\ngot:  %s", iteration, token, want, got)
		}
	}
}

// TestHranaCursorPageRandomPageMatchesLegacyDiff walks whole pages so the span
// scanner and both row writers are compared against the decoding path on the
// same random text.
func TestHranaCursorPageRandomPageMatchesLegacyDiff(t *testing.T) {
	random := rand.New(rand.NewSource(56))
	for iteration := 0; iteration < 200; iteration++ {
		rows := 1 + random.Intn(6)
		var body strings.Builder
		body.WriteString(`{"done":true,"rows":[`)
		for row := 0; row < rows; row++ {
			if row > 0 {
				body.WriteByte(',')
			}
			body.WriteByte('[')
			for cell := 0; cell < 1+random.Intn(5); cell++ {
				if cell > 0 {
					body.WriteByte(',')
				}
				if random.Intn(5) == 0 {
					scalars := []string{"null", "true", "false", "0", "-17", "2.5", "[1,2]", `{"a":"b"}`}
					body.WriteString(scalars[random.Intn(len(scalars))])
					continue
				}
				body.Write(hranaCursorPageRandomTextToken(random))
			}
			body.WriteByte(']')
		}
		body.WriteString(`]}`)
		pageBytes := []byte(body.String())
		legacy, done := hranaCursorPageLegacyRows(t, pageBytes)
		var scanner hranaCursorPageScanner
		page, err := scanner.scan(pageBytes, true, false)
		if err != nil {
			t.Fatalf("iteration %d scan failed: %v\n%s", iteration, err, pageBytes)
		}
		if page.done != done || page.rows != len(legacy) {
			t.Fatalf("iteration %d done=%t rows=%d, want done=%t rows=%d", iteration, page.done, page.rows, done, len(legacy))
		}
		for index, cells := range legacy {
			var wantJSON hranaJSONCursorRow
			wantJSON.begin()
			wantErr := wantJSON.appendCells(cells)
			var gotJSON hranaJSONCursorRow
			gotJSON.begin()
			gotErr := gotJSON.appendPageRow(&page, index)
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("iteration %d row %d validity: legacy=%v direct=%v", iteration, index, wantErr, gotErr)
			}
			if wantErr == nil && !bytes.Equal(wantJSON.bytes(), gotJSON.bytes()) {
				t.Fatalf("iteration %d row %d json bytes differ\nlegacy: %s\ndirect: %s", iteration, index, wantJSON.bytes(), gotJSON.bytes())
			}

			var wantProto hranaProtoCursorRowFrame
			wantProto.begin()
			protoErr := error(nil)
			for _, cell := range cells {
				if protoErr = wantProto.appendCell(cell); protoErr != nil {
					break
				}
			}
			var gotProto hranaProtoCursorRowFrame
			gotProto.begin()
			directErr := gotProto.appendPageRow(&page, index)
			if (protoErr == nil) != (directErr == nil) {
				t.Fatalf("iteration %d row %d proto validity: legacy=%v direct=%v", iteration, index, protoErr, directErr)
			}
			if protoErr == nil && !bytes.Equal(wantProto.frame(), gotProto.frame()) {
				t.Fatalf("iteration %d row %d proto bytes differ\nlegacy: %x\ndirect: %x", iteration, index, wantProto.frame(), gotProto.frame())
			}
		}
	}
}
