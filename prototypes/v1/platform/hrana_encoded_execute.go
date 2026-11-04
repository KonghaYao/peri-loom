// Single-pass encoding of a Hrana execute entry for the JSON pipeline. The
// adapter used to decode the worker's statement result into generic values and
// then encode the same information twice, once for the result budget and once
// for the response. The published entry bytes are a pure function of the worker
// result bytes, so one scan can produce them. The scan is deliberately strict:
// any shape it cannot reproduce byte for byte returns false and the caller
// decodes the result the old way, so which path ran never changes the answer.
package platform

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// hranaEncodedEntry is a pipeline entry the JSON adapter already encoded.
// MarshalJSON returns the bytes unchanged: encoding/json compacts and
// HTML-escapes a Marshaler's output, and these bytes are already in exactly the
// form json.Marshal produces for the equivalent entry value. A failure path that
// re-encodes response.Results therefore publishes the same payload.
type hranaEncodedEntry []byte

func (entry hranaEncodedEntry) MarshalJSON() ([]byte, error) {
	if entry == nil {
		return []byte("null"), nil
	}
	return entry, nil
}

// hranaExecuteEntryContextKey marks the one caller that reads a whole pipeline
// entry back from executeWithin. The protobuf adapter consumes the same fused
// statement answer but publishes it through its own encoder, so it must keep
// receiving the decoded value.
type hranaExecuteEntryContextKey struct{}

func withHranaExecuteEntry(ctx context.Context) context.Context {
	return context.WithValue(ctx, hranaExecuteEntryContextKey{}, true)
}

func hranaExecuteEntryRequested(ctx context.Context) bool {
	requested, _ := ctx.Value(hranaExecuteEntryContextKey{}).(bool)
	return requested
}

const (
	hranaExecuteFieldCols = iota
	hranaExecuteFieldRows
	hranaExecuteFieldAffectedRowCount
	hranaExecuteFieldLastInsertRowID
	hranaExecuteFieldRowsRead
	hranaExecuteFieldRowsWritten
	hranaExecuteFieldQueryDurationMS
	hranaExecuteFieldCount
)

// hranaExecuteResultNames mirrors the json tags of hranaNativeStatement. The
// decoder matches tags exactly, then case-insensitively; a case-folded variant
// is refused here so the old path can apply that rule itself.
var hranaExecuteResultNames = [hranaExecuteFieldCount]string{
	"cols",
	"rows",
	"affected_row_count",
	"last_insert_rowid",
	"rows_read",
	"rows_written",
	"query_duration_ms",
}

const (
	hranaExecuteColumnName = iota
	hranaExecuteColumnDecltype
)

var hranaExecuteColumnNames = [2]string{"name", "decltype"}

// hranaExecuteFieldSpan is the byte range of one top-level value. Recording the
// span instead of the value keeps duplicate keys at their last occurrence, as
// the struct decoder sees them, without holding the payload twice.
type hranaExecuteFieldSpan struct {
	start, end int
	present    bool
}

func hranaExecuteNull(raw []byte) bool {
	return len(raw) == 4 && raw[0] == 'n' && raw[1] == 'u' && raw[2] == 'l' && raw[3] == 'l'
}

func hranaExecuteField(content, quoted []byte) (index int, known, folded bool) {
	if plainJSONContent(content) {
		for candidate, name := range hranaExecuteResultNames {
			if fusedKeyEqual(content, name) {
				return candidate, true, false
			}
		}
		for _, name := range hranaExecuteResultNames {
			if fusedKeyEqualFold(content, name) {
				return 0, false, true
			}
		}
		return 0, false, false
	}
	key, ok := fusedDecodeJSONString(quoted)
	if !ok {
		return 0, false, false
	}
	for candidate, name := range hranaExecuteResultNames {
		if key == name {
			return candidate, true, false
		}
	}
	for _, name := range hranaExecuteResultNames {
		if strings.EqualFold(key, name) {
			return 0, false, true
		}
	}
	return 0, false, false
}

func hranaExecuteColumnField(content, quoted []byte) (index int, known, folded bool) {
	if plainJSONContent(content) {
		for candidate, name := range hranaExecuteColumnNames {
			if fusedKeyEqual(content, name) {
				return candidate, true, false
			}
		}
		for _, name := range hranaExecuteColumnNames {
			if fusedKeyEqualFold(content, name) {
				return 0, false, true
			}
		}
		return 0, false, false
	}
	key, ok := fusedDecodeJSONString(quoted)
	if !ok {
		return 0, false, false
	}
	for candidate, name := range hranaExecuteColumnNames {
		if key == name {
			return candidate, true, false
		}
	}
	for _, name := range hranaExecuteColumnNames {
		if strings.EqualFold(key, name) {
			return 0, false, true
		}
	}
	return 0, false, false
}

// encodeHranaExecuteEntry encodes the execute entry for one worker statement
// result. The bytes are what json.Marshal produces for the map the legacy path
// builds: response.result keys sorted, every string HTML-escaped like the
// default encoder, numbers formatted by the same rules. A false return is never
// an error: the caller falls back to decoding and publishes the same answer,
// including the error classification for shapes this scan refuses.
func encodeHranaExecuteEntry(result json.RawMessage) (hranaEncodedEntry, bool) {
	if len(result) == 0 {
		return nil, false
	}
	scanner := fusedReplyScanner{data: result}
	scanner.skipSpace()
	if scanner.pos >= len(result) || result[scanner.pos] != '{' {
		return nil, false
	}
	scanner.pos++
	scanner.skipSpace()
	if scanner.pos < len(result) && result[scanner.pos] == '}' {
		return nil, false
	}
	var spans [hranaExecuteFieldCount]hranaExecuteFieldSpan
scanResult:
	for {
		scanner.skipSpace()
		if scanner.pos >= len(result) || result[scanner.pos] != '"' {
			return nil, false
		}
		keyStart, keyEnd, ok := scanner.scanString()
		if !ok {
			return nil, false
		}
		field, known, folded := hranaExecuteField(result[keyStart:keyEnd], result[keyStart-1:keyEnd+1])
		if folded {
			return nil, false
		}
		scanner.skipSpace()
		if scanner.pos >= len(result) || result[scanner.pos] != ':' {
			return nil, false
		}
		scanner.pos++
		scanner.skipSpace()
		valueStart := scanner.pos
		if !scanner.scanValue() {
			return nil, false
		}
		if known {
			spans[field] = hranaExecuteFieldSpan{start: valueStart, end: scanner.pos, present: true}
		}
		scanner.skipSpace()
		if scanner.pos >= len(result) {
			return nil, false
		}
		switch result[scanner.pos] {
		case ',':
			scanner.pos++
		case '}':
			scanner.pos++
			break scanResult
		default:
			return nil, false
		}
	}
	// The decoder stops after the first value, so trailing bytes never reach
	// the result either.
	columns := spans[hranaExecuteFieldCols]
	if !columns.present || hranaExecuteNull(result[columns.start:columns.end]) {
		return nil, false
	}
	rows := spans[hranaExecuteFieldRows]
	if !rows.present || hranaExecuteNull(result[rows.start:rows.end]) {
		return nil, false
	}
	var affectedRowCount int64
	if span := spans[hranaExecuteFieldAffectedRowCount]; span.present && !hranaExecuteNull(result[span.start:span.end]) {
		value, err := strconv.ParseInt(string(result[span.start:span.end]), 10, 64)
		if err != nil {
			return nil, false
		}
		affectedRowCount = value
	}
	var lastInsertRowid int64
	rowidPresent := false
	if span := spans[hranaExecuteFieldLastInsertRowID]; span.present && !hranaExecuteNull(result[span.start:span.end]) {
		value, err := strconv.ParseInt(string(result[span.start:span.end]), 10, 64)
		if err != nil {
			return nil, false
		}
		lastInsertRowid, rowidPresent = value, true
	}
	read := spans[hranaExecuteFieldRowsRead]
	written := spans[hranaExecuteFieldRowsWritten]
	duration := spans[hranaExecuteFieldQueryDurationMS]
	metricCount := 0
	for _, span := range []hranaExecuteFieldSpan{read, written, duration} {
		if span.present {
			metricCount++
		}
	}
	var rowsRead, rowsWritten uint64
	var queryDuration float64
	if metricCount != 0 {
		// The legacy path accepts the counters all together or not at all, and
		// refuses an explicit null in any of them.
		if metricCount != 3 {
			return nil, false
		}
		for _, span := range []hranaExecuteFieldSpan{read, written, duration} {
			if hranaExecuteNull(result[span.start:span.end]) {
				return nil, false
			}
		}
		var err error
		if rowsRead, err = strconv.ParseUint(string(result[read.start:read.end]), 10, 64); err != nil {
			return nil, false
		}
		if rowsWritten, err = strconv.ParseUint(string(result[written.start:written.end]), 10, 64); err != nil {
			return nil, false
		}
		queryDuration, err = strconv.ParseFloat(string(result[duration.start:duration.end]), 64)
		if err != nil || math.IsNaN(queryDuration) || math.IsInf(queryDuration, 0) || queryDuration < 0 {
			return nil, false
		}
	}
	// Each cell gains its wrapper object, so the entry is always a little larger
	// than the worker result; a small margin keeps the buffer from being grown and
	// copied once the rows have been written.
	entry := make([]byte, 0, len(result)+len(result)/16+256)
	entry = append(entry, `{"response":{"result":{"affected_row_count":`...)
	entry = strconv.AppendInt(entry, affectedRowCount, 10)
	entry = append(entry, `,"cols":`...)
	var ok bool
	if entry, ok = appendHranaExecuteColumns(entry, result[columns.start:columns.end]); !ok {
		return nil, false
	}
	entry = append(entry, `,"last_insert_rowid":`...)
	if rowidPresent {
		// The legacy map stores the rowid as its decimal *string*.
		entry = hranaJSONAppendString(entry, strconv.FormatInt(lastInsertRowid, 10))
	} else {
		entry = append(entry, "null"...)
	}
	if metricCount == 3 {
		entry = append(entry, `,"query_duration_ms":`...)
		entry = hranaJSONAppendFloat(entry, queryDuration)
	}
	entry = append(entry, `,"rows":`...)
	if entry, ok = appendHranaExecuteRows(entry, result[rows.start:rows.end]); !ok {
		return nil, false
	}
	if metricCount == 3 {
		entry = append(entry, `,"rows_read":`...)
		entry = strconv.AppendUint(entry, rowsRead, 10)
		entry = append(entry, `,"rows_written":`...)
		entry = strconv.AppendUint(entry, rowsWritten, 10)
	}
	entry = append(entry, `},"type":"execute"},"type":"ok"}`...)
	return hranaEncodedEntry(entry), true
}

func appendHranaExecuteColumns(dst, raw []byte) ([]byte, bool) {
	scanner := fusedReplyScanner{data: raw}
	scanner.skipSpace()
	if scanner.pos >= len(raw) || raw[scanner.pos] != '[' {
		return dst, false
	}
	scanner.pos++
	scanner.skipSpace()
	dst = append(dst, '[')
	if scanner.pos < len(raw) && raw[scanner.pos] == ']' {
		scanner.pos++
		return append(dst, ']'), true
	}
	first := true
	for {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		var ok bool
		if dst, ok = appendHranaExecuteColumn(dst, &scanner); !ok {
			return dst, false
		}
		scanner.skipSpace()
		if scanner.pos >= len(raw) {
			return dst, false
		}
		switch raw[scanner.pos] {
		case ',':
			scanner.pos++
		case ']':
			scanner.pos++
			return append(dst, ']'), true
		default:
			return dst, false
		}
	}
}

// appendHranaExecuteColumn emits one column object. A null column decodes into
// the zero struct, which the legacy path publishes as an empty name and a null
// declaration.
func appendHranaExecuteColumn(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	scanner.skipSpace()
	data := scanner.data
	if scanner.pos >= len(data) {
		return dst, false
	}
	switch data[scanner.pos] {
	case 'n':
		if !scanner.scanLiteral("null") {
			return dst, false
		}
		return append(dst, `{"decltype":null,"name":""}`...), true
	case '{':
		return appendHranaExecuteColumnObject(dst, scanner)
	default:
		return dst, false
	}
}

func appendHranaExecuteColumnObject(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	data := scanner.data
	scanner.pos++
	var name, decltype []byte
	scanner.skipSpace()
	if scanner.pos < len(data) && data[scanner.pos] == '}' {
		scanner.pos++
		return append(dst, `{"decltype":null,"name":""}`...), true
	}
scanColumn:
	for {
		scanner.skipSpace()
		if scanner.pos >= len(data) || data[scanner.pos] != '"' {
			return dst, false
		}
		keyStart, keyEnd, ok := scanner.scanString()
		if !ok {
			return dst, false
		}
		field, known, folded := hranaExecuteColumnField(data[keyStart:keyEnd], data[keyStart-1:keyEnd+1])
		if folded {
			return dst, false
		}
		scanner.skipSpace()
		if scanner.pos >= len(data) || data[scanner.pos] != ':' {
			return dst, false
		}
		scanner.pos++
		scanner.skipSpace()
		valueStart := scanner.pos
		if !scanner.consumeStringOrNull() {
			return dst, false
		}
		if known {
			if field == hranaExecuteColumnName {
				name = data[valueStart:scanner.pos]
			} else {
				decltype = data[valueStart:scanner.pos]
			}
		}
		scanner.skipSpace()
		if scanner.pos >= len(data) {
			return dst, false
		}
		switch data[scanner.pos] {
		case ',':
			scanner.pos++
		case '}':
			scanner.pos++
			break scanColumn
		default:
			return dst, false
		}
	}
	dst = append(dst, `{"decltype":`...)
	var ok bool
	// A null in either field decodes into the zero value: the empty string for
	// name, a nil pointer (encoded as null) for decltype.
	if dst, ok = appendHranaExecuteColumnString(dst, decltype, true); !ok {
		return dst, false
	}
	dst = append(dst, `,"name":`...)
	if dst, ok = appendHranaExecuteColumnString(dst, name, false); !ok {
		return dst, false
	}
	return append(dst, '}'), true
}

func appendHranaExecuteColumnString(dst, raw []byte, nullable bool) ([]byte, bool) {
	if raw == nil || hranaExecuteNull(raw) {
		if nullable {
			return append(dst, "null"...), true
		}
		return append(dst, `""`...), true
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return dst, false
	}
	return hranaJSONAppendString(dst, value), true
}

func appendHranaExecuteRows(dst, raw []byte) ([]byte, bool) {
	scanner := fusedReplyScanner{data: raw}
	scanner.skipSpace()
	if scanner.pos >= len(raw) || raw[scanner.pos] != '[' {
		return dst, false
	}
	scanner.pos++
	scanner.skipSpace()
	dst = append(dst, '[')
	if scanner.pos < len(raw) && raw[scanner.pos] == ']' {
		scanner.pos++
		return append(dst, ']'), true
	}
	for {
		var ok bool
		if dst, ok = appendHranaExecuteRow(dst, &scanner); !ok {
			return dst, false
		}
		scanner.skipSpace()
		if scanner.pos >= len(raw) {
			return dst, false
		}
		switch raw[scanner.pos] {
		case ',':
			scanner.pos++
			dst = append(dst, ',')
		case ']':
			scanner.pos++
			return append(dst, ']'), true
		default:
			return dst, false
		}
	}
}

// appendHranaExecuteRow emits one row. A literal null row decodes into the nil
// slice and the legacy conversion publishes it as an empty row.
func appendHranaExecuteRow(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	scanner.skipSpace()
	data := scanner.data
	if scanner.pos >= len(data) {
		return dst, false
	}
	switch data[scanner.pos] {
	case 'n':
		if !scanner.scanLiteral("null") {
			return dst, false
		}
		return append(dst, '[', ']'), true
	case '[':
		scanner.pos++
		scanner.skipSpace()
		dst = append(dst, '[')
		if scanner.pos < len(data) && data[scanner.pos] == ']' {
			scanner.pos++
			return append(dst, ']'), true
		}
		first := true
		for {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			scanner.skipSpace()
			var ok bool
			if dst, ok = appendHranaExecuteCell(dst, scanner); !ok {
				return dst, false
			}
			scanner.skipSpace()
			if scanner.pos >= len(data) {
				return dst, false
			}
			switch data[scanner.pos] {
			case ',':
				scanner.pos++
			case ']':
				scanner.pos++
				return append(dst, ']'), true
			default:
				return dst, false
			}
		}
	default:
		return dst, false
	}
}

// appendHranaExecuteCell mirrors hranaOutputValue followed by the map encoding
// of its result: a string is text, a number with a fraction or exponent is a
// float, every other number keeps its literal as the integer's text value, and
// an array of byte-sized integers is a blob. Everything else is refused so the
// legacy conversion reports its own invalid_native_value.
func appendHranaExecuteCell(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	data := scanner.data
	if scanner.pos >= len(data) {
		return dst, false
	}
	start := scanner.pos
	switch c := data[start]; {
	case c == 'n':
		if !scanner.scanLiteral("null") {
			return dst, false
		}
		return append(dst, `{"type":"null"}`...), true
	case c == '"':
		dst = append(dst, `{"type":"text","value":`...)
		var ok bool
		if dst, ok = appendHranaJSONStringToken(dst, scanner); !ok {
			return dst, false
		}
		return append(dst, '}'), true
	case c == '-' || '0' <= c && c <= '9':
		if !scanner.scanNumber() {
			return dst, false
		}
		literal := data[start:scanner.pos]
		if bytes.IndexAny(literal, ".eE") >= 0 {
			value, err := strconv.ParseFloat(string(literal), 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
				return dst, false
			}
			dst = append(dst, `{"type":"float","value":`...)
			dst = hranaJSONAppendFloat(dst, value)
			return append(dst, '}'), true
		}
		dst = append(dst, `{"type":"integer","value":`...)
		dst = hranaJSONAppendString(dst, string(literal))
		return append(dst, '}'), true
	case c == '[':
		return appendHranaExecuteBlob(dst, scanner)
	default:
		return dst, false
	}
}

func appendHranaExecuteBlob(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	data := scanner.data
	scanner.pos++ // '['
	var scratch [64]byte
	blob := scratch[:0]
	scanner.skipSpace()
	if scanner.pos < len(data) && data[scanner.pos] == ']' {
		scanner.pos++
	} else {
	scanBlob:
		for {
			scanner.skipSpace()
			if scanner.pos >= len(data) {
				return dst, false
			}
			start := scanner.pos
			if !scanner.scanNumber() {
				return dst, false
			}
			integer, err := strconv.ParseInt(string(data[start:scanner.pos]), 10, 64)
			if err != nil || integer < 0 || integer > 255 {
				return dst, false
			}
			blob = append(blob, byte(integer))
			scanner.skipSpace()
			if scanner.pos >= len(data) {
				return dst, false
			}
			switch data[scanner.pos] {
			case ',':
				scanner.pos++
			case ']':
				scanner.pos++
				break scanBlob
			default:
				return dst, false
			}
		}
	}
	dst = append(dst, `{"base64":"`...)
	dst = base64.StdEncoding.AppendEncode(dst, blob)
	return append(dst, `","type":"blob"}`...), true
}

// appendHranaJSONStringToken appends the re-encoded form of the JSON string
// token at scanner.pos and advances past it. Plain content is copied while it
// is scanned; a token with escapes is decoded by encoding/json and re-escaped,
// so escapes, surrogate pairs and invalid UTF-8 follow the decoder exactly.
func appendHranaJSONStringToken(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	data := scanner.data
	start := scanner.pos
	if start >= len(data) || data[start] != '"' {
		return dst, false
	}
	mark := len(dst)
	dst = append(dst, '"')
	pos := start + 1
	run := pos
	for pos < len(data) {
		switch c := data[pos]; {
		case c == '"':
			dst = append(dst, data[run:pos]...)
			dst = append(dst, '"')
			scanner.pos = pos + 1
			return dst, true
		case c == '\\':
			probe := fusedReplyScanner{data: data, pos: start}
			if _, _, ok := probe.scanString(); !ok {
				return dst, false
			}
			var value string
			if json.Unmarshal(data[start:probe.pos], &value) != nil {
				return dst, false
			}
			dst = hranaJSONAppendString(dst[:mark], value)
			scanner.pos = probe.pos
			return dst, true
		case c < 0x20:
			return dst, false
		case c < utf8.RuneSelf:
			if c != '<' && c != '>' && c != '&' {
				pos++
				continue
			}
			dst = append(dst, data[run:pos]...)
			switch c {
			case '<':
				dst = append(dst, `\u003c`...)
			case '>':
				dst = append(dst, `\u003e`...)
			default:
				dst = append(dst, `\u0026`...)
			}
			pos++
			run = pos
		default:
			r, size := utf8.DecodeRune(data[pos:])
			if r == utf8.RuneError && size == 1 {
				dst = append(dst, data[run:pos]...)
				dst = append(dst, "\ufffd"...)
				pos++
				run = pos
				continue
			}
			if r == '\u2028' || r == '\u2029' {
				dst = append(dst, data[run:pos]...)
				dst = append(dst, '\\', 'u', '2', '0', '2')
				if r == '\u2028' {
					dst = append(dst, '8')
				} else {
					dst = append(dst, '9')
				}
				pos += size
				run = pos
				continue
			}
			pos += size
		}
	}
	return dst, false
}

// assembleHranaResponse builds the pipeline reply from entry bytes the loop
// already encoded. The layout is the one json.Marshal produces for
// hranaResponse: baton first, then results in order.
func assembleHranaResponse(baton *string, entries [][]byte) ([]byte, error) {
	encodedBaton, err := json.Marshal(baton)
	if err != nil {
		return nil, err
	}
	const (
		batonPrefix   = `{"baton":`
		resultsPrefix = `,"results":[`
		suffix        = `]}`
	)
	size := len(batonPrefix) + len(encodedBaton) + len(resultsPrefix) + len(suffix)
	for index, entry := range entries {
		if index > 0 {
			size++
		}
		size += len(entry)
	}
	output := make([]byte, 0, size)
	output = append(output, batonPrefix...)
	output = append(output, encodedBaton...)
	output = append(output, resultsPrefix...)
	for index, entry := range entries {
		if index > 0 {
			output = append(output, ',')
		}
		output = append(output, entry...)
	}
	return append(output, suffix...), nil
}
