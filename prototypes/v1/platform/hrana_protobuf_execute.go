package platform

// Single-pass protobuf encoding of one worker statement result for the Hrana
// protobuf adapter. The adapter used to decode the worker's JSON result into
// generic values, convert every cell into a map, and then walk those maps three
// times while wrapping the result for the wire. The published bytes are a pure
// function of the worker result bytes, so one scan can write them directly.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
)

// hranaEncodedStatementResult carries statement-result bytes the adapter wrote
// while scanning the worker reply. The legacy decode path never sees this type:
// shapes the scanner refuses fall back to it, including its error classification.
type hranaEncodedStatementResult struct {
	bytes      []byte
	overBudget bool
}

type hranaProtoExecuteContextKey struct{}

func withHranaProtoExecute(ctx context.Context) context.Context {
	return context.WithValue(ctx, hranaProtoExecuteContextKey{}, true)
}

func hranaProtoExecuteRequested(ctx context.Context) bool {
	requested, _ := ctx.Value(hranaProtoExecuteContextKey{}).(bool)
	return requested
}

type hranaProtoEncodeStatus int

const (
	hranaProtoEncodeUnsupported hranaProtoEncodeStatus = iota
	hranaProtoEncodeComplete
	hranaProtoEncodeOverBudget
)

// encodeHranaProtoStatementResult encodes one StatementResult message. A false
// status is never an error: the caller falls back to decoding and publishing the
// same answer, including the error classification for shapes this scan refuses.
func encodeHranaProtoStatementResult(result json.RawMessage, budget int) (hranaEncodedStatementResult, hranaProtoEncodeStatus) {
	if len(result) == 0 {
		return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
	}
	scanner := fusedReplyScanner{data: result}
	scanner.skipSpace()
	if scanner.pos >= len(result) || result[scanner.pos] != '{' {
		return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
	}
	scanner.pos++
	scanner.skipSpace()
	if scanner.pos < len(result) && result[scanner.pos] == '}' {
		return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
	}
	var spans [hranaExecuteFieldCount]hranaExecuteFieldSpan
scanResult:
	for {
		scanner.skipSpace()
		if scanner.pos >= len(result) || result[scanner.pos] != '"' {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		keyStart, keyEnd, ok := scanner.scanString()
		if !ok {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		field, known, folded := hranaExecuteField(result[keyStart:keyEnd], result[keyStart-1:keyEnd+1])
		if folded {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		scanner.skipSpace()
		if scanner.pos >= len(result) || result[scanner.pos] != ':' {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		scanner.pos++
		scanner.skipSpace()
		valueStart := scanner.pos
		if !scanner.scanValue() {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		if known {
			// The struct decoder applies duplicate keys in order and stops at
			// the first type mismatch. Keeping the last span and validating it
			// later would accept a payload the decoder refuses, so a repeated
			// field is left to the legacy path.
			if spans[field].present {
				return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
			}
			spans[field] = hranaExecuteFieldSpan{start: valueStart, end: scanner.pos, present: true}
		}
		scanner.skipSpace()
		if scanner.pos >= len(result) {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		switch result[scanner.pos] {
		case ',':
			scanner.pos++
		case '}':
			scanner.pos++
			break scanResult
		default:
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
	}
	columns := spans[hranaExecuteFieldCols]
	if !columns.present || hranaExecuteNull(result[columns.start:columns.end]) {
		return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
	}
	rows := spans[hranaExecuteFieldRows]
	if !rows.present || hranaExecuteNull(result[rows.start:rows.end]) {
		return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
	}
	var affectedRowCount int64
	if span := spans[hranaExecuteFieldAffectedRowCount]; span.present && !hranaExecuteNull(result[span.start:span.end]) {
		value, err := strconv.ParseInt(string(result[span.start:span.end]), 10, 64)
		if err != nil {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		affectedRowCount = value
	}
	if affectedRowCount < 0 {
		return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
	}
	// The legacy map keeps a *string here and the encoder's string assertion
	// never matches a pointer, so the value is validated but not emitted.
	if span := spans[hranaExecuteFieldLastInsertRowID]; span.present && !hranaExecuteNull(result[span.start:span.end]) {
		if _, err := strconv.ParseInt(string(result[span.start:span.end]), 10, 64); err != nil {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
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
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		for _, span := range []hranaExecuteFieldSpan{read, written, duration} {
			if hranaExecuteNull(result[span.start:span.end]) {
				return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
			}
		}
		var err error
		if rowsRead, err = strconv.ParseUint(string(result[read.start:read.end]), 10, 64); err != nil {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		if rowsWritten, err = strconv.ParseUint(string(result[written.start:written.end]), 10, 64); err != nil {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
		queryDuration, err = strconv.ParseFloat(string(result[duration.start:duration.end]), 64)
		if err != nil || math.IsNaN(queryDuration) || math.IsInf(queryDuration, 0) || queryDuration < 0 {
			return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
		}
	}
	// The wire message is a little larger than the worker result: every cell
	// gains two nested length prefixes. A small margin keeps the buffer from
	// being grown and copied once the rows have been written.
	output := make([]byte, 0, len(result)+len(result)/16+256)
	var ok bool
	if output, ok = appendHranaProtoColumns(output, result[columns.start:columns.end]); !ok {
		return hranaEncodedStatementResult{}, hranaProtoEncodeUnsupported
	}
	output, status := appendHranaProtoRows(output, result[rows.start:rows.end], budget)
	if status != hranaProtoEncodeComplete {
		return hranaEncodedStatementResult{}, status
	}
	output = appendHranaProtoNumber(output, 3, uint64(affectedRowCount))
	if metricCount == 3 {
		output = appendHranaProtoNumber(output, 6, rowsRead)
		output = appendHranaProtoNumber(output, 7, rowsWritten)
		output = appendHranaProtoDouble(output, 8, queryDuration)
	}
	if budget > 0 && len(output) > budget {
		return hranaEncodedStatementResult{}, hranaProtoEncodeOverBudget
	}
	return hranaEncodedStatementResult{bytes: output}, hranaProtoEncodeComplete
}

// appendHranaProtoColumns writes field 1 for every column. A null column decodes
// into the zero struct, which the legacy path publishes as an empty name and no
// declaration; a column name is always present once decoded, while a declaration
// is omitted when the decoded pointer is nil.
func appendHranaProtoColumns(dst, raw []byte) ([]byte, bool) {
	scanner := fusedReplyScanner{data: raw}
	scanner.skipSpace()
	if scanner.pos >= len(raw) || raw[scanner.pos] != '[' {
		return dst, false
	}
	scanner.pos++
	scanner.skipSpace()
	if scanner.pos < len(raw) && raw[scanner.pos] == ']' {
		scanner.pos++
		return dst, true
	}
	var message []byte
	for {
		message = message[:0]
		var ok bool
		if message, ok = appendHranaProtoColumn(message, &scanner); !ok {
			return dst, false
		}
		dst = appendHranaProtoBytes(dst, 1, message)
		scanner.skipSpace()
		if scanner.pos >= len(raw) {
			return dst, false
		}
		switch raw[scanner.pos] {
		case ',':
			scanner.pos++
		case ']':
			scanner.pos++
			return dst, true
		default:
			return dst, false
		}
	}
}

func appendHranaProtoColumn(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
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
		return appendHranaProtoStringField(dst, 1, ""), true
	case '{':
		return appendHranaProtoColumnObject(dst, scanner)
	default:
		return dst, false
	}
}

func appendHranaProtoColumnObject(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	data := scanner.data
	scanner.pos++
	var name, decltype []byte
	scanner.skipSpace()
	if scanner.pos < len(data) && data[scanner.pos] == '}' {
		scanner.pos++
		return appendHranaProtoStringField(dst, 1, ""), true
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
	nameValue := ""
	if name != nil && !hranaExecuteNull(name) {
		var value string
		if json.Unmarshal(name, &value) != nil {
			return dst, false
		}
		nameValue = value
	}
	dst = appendHranaProtoStringField(dst, 1, nameValue)
	// The legacy map stores the decoded *string and the encoder's string
	// assertion never matches a pointer, so decltype is validated but not
	// emitted. Byte identity with the published wire shape takes priority.
	if decltype != nil && !hranaExecuteNull(decltype) {
		var value string
		if json.Unmarshal(decltype, &value) != nil {
			return dst, false
		}
	}
	return dst, true
}

// appendHranaProtoRows writes field 2 for every row. A literal null row decodes
// into a nil slice, which the legacy conversion publishes as an empty row, so it
// shares the output of an empty array.
func appendHranaProtoRows(dst, raw []byte, budget int) ([]byte, hranaProtoEncodeStatus) {
	scanner := fusedReplyScanner{data: raw}
	scanner.skipSpace()
	if scanner.pos >= len(raw) || raw[scanner.pos] != '[' {
		return dst, hranaProtoEncodeUnsupported
	}
	scanner.pos++
	scanner.skipSpace()
	if scanner.pos < len(raw) && raw[scanner.pos] == ']' {
		scanner.pos++
		return dst, hranaProtoEncodeComplete
	}
	row := make([]byte, 0, 4096)
	for {
		row = row[:0]
		var ok bool
		if row, ok = appendHranaProtoRow(row, &scanner); !ok {
			return dst, hranaProtoEncodeUnsupported
		}
		dst = appendHranaProtoBytes(dst, 2, row)
		if budget > 0 && len(dst) > budget {
			// The legacy encoder charges the same row field before appending it
			// and reports the limit at this point; the partial bytes are never
			// published either way.
			return dst, hranaProtoEncodeOverBudget
		}
		scanner.skipSpace()
		if scanner.pos >= len(raw) {
			return dst, hranaProtoEncodeUnsupported
		}
		switch raw[scanner.pos] {
		case ',':
			scanner.pos++
		case ']':
			scanner.pos++
			return dst, hranaProtoEncodeComplete
		default:
			return dst, hranaProtoEncodeUnsupported
		}
	}
}

func appendHranaProtoRow(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
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
		return dst, true
	case '[':
		scanner.pos++
		scanner.skipSpace()
		if scanner.pos < len(data) && data[scanner.pos] == ']' {
			scanner.pos++
			return dst, true
		}
		for {
			var ok bool
			if dst, ok = appendHranaProtoValueField(dst, scanner); !ok {
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
				return dst, true
			default:
				return dst, false
			}
		}
	default:
		return dst, false
	}
}

// appendHranaProtoValueField appends one Row field (number 1) holding a Value
// message. The nesting is written directly so a text cell is copied from the
// worker reply straight into the row buffer.
func appendHranaProtoValueField(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
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
		return append(dst, 0x0a, 0x02, 0x0a, 0x00), true
	case c == '"':
		tokenStart := scanner.pos
		contentStart, contentEnd, ok := scanner.scanString()
		if !ok {
			return dst, false
		}
		content := data[contentStart:contentEnd]
		if plainJSONContent(content) {
			return appendHranaProtoTextField(dst, content), true
		}
		// Escapes, surrogate pairs and invalid UTF-8 must decode exactly like
		// encoding/json does for the legacy map.
		decoded, ok := fusedDecodeJSONString(data[tokenStart:scanner.pos])
		if !ok {
			return dst, false
		}
		return appendHranaProtoTextField(dst, []byte(decoded)), true
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
			// Value.float is field 3 with fixed64 payload: ten bytes total.
			dst = append(dst, 0x0a, 9, 0x19)
			var bits [8]byte
			binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
			return append(dst, bits[:]...), true
		}
		integer, err := strconv.ParseInt(string(literal), 10, 64)
		if err != nil {
			return dst, false
		}
		zigzag := uint64(integer<<1) ^ uint64(integer>>63)
		var buffer [binary.MaxVarintLen64]byte
		size := binary.PutUvarint(buffer[:], zigzag)
		dst = append(dst, 0x0a, byte(1+size), 0x10)
		return append(dst, buffer[:size]...), true
	case c == '[':
		return appendHranaProtoBlobField(dst, scanner)
	default:
		return dst, false
	}
}

// appendHranaProtoBlobField decodes the worker's byte array into the bytes the
// legacy path base64-round-trips. Every element must be a JSON number that
// strconv.ParseInt accepts and fits in one byte; anything else is left to the
// legacy conversion, which reports invalid_native_value.
func appendHranaProtoBlobField(dst []byte, scanner *fusedReplyScanner) ([]byte, bool) {
	data := scanner.data
	scanner.pos++
	var blob []byte
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
	dst = append(dst, 0x0a)
	dst = appendHranaProtoVarint(dst, uint64(1+hranaProtoVarintLen(uint64(len(blob)))+len(blob)))
	dst = append(dst, 0x2a)
	dst = appendHranaProtoVarint(dst, uint64(len(blob)))
	return append(dst, blob...), true
}

// appendHranaProtoTextField writes the Row field for a text Value whose content
// bytes are already decoded.
func appendHranaProtoTextField(dst, content []byte) []byte {
	dst = append(dst, 0x0a)
	dst = appendHranaProtoVarint(dst, uint64(1+hranaProtoVarintLen(uint64(len(content)))+len(content)))
	dst = append(dst, 0x22)
	dst = appendHranaProtoVarint(dst, uint64(len(content)))
	return append(dst, content...)
}

// appendHranaProtoStringField appends one string field in place.
func appendHranaProtoStringField(dst []byte, number int, value string) []byte {
	dst = append(dst, byte(number<<3|2))
	dst = appendHranaProtoVarint(dst, uint64(len(value)))
	return append(dst, value...)
}

// appendHranaProtoBytes appends one length-delimited field in place.
func appendHranaProtoBytes(dst []byte, number int, value []byte) []byte {
	dst = append(dst, byte(number<<3|2))
	dst = appendHranaProtoVarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendHranaProtoNumber(dst []byte, number int, value uint64) []byte {
	dst = append(dst, byte(number<<3))
	return appendHranaProtoVarint(dst, value)
}

func appendHranaProtoDouble(dst []byte, number int, value float64) []byte {
	dst = append(dst, byte(number<<3|1))
	var bits [8]byte
	binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
	return append(dst, bits[:]...)
}

func appendHranaProtoVarint(dst []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(buffer[:], value)
	return append(dst, buffer[:size]...)
}

func hranaProtoVarintLen(value uint64) int {
	var buffer [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buffer[:], value)
}

func appendHranaProtoFieldHeader(dst []byte, number, length int) []byte {
	dst = append(dst, byte(number<<3|2))
	return appendHranaProtoVarint(dst, uint64(length))
}

// assembleHranaProtoSingleExecute wraps one encoded StatementResult exactly the
// way hranaProtoPipelineResponseWithin wraps the execute entry followed by the
// close entry, without materializing the intermediate copies.
func assembleHranaProtoSingleExecute(result []byte) []byte {
	messageLen := hranaProtoLength(1, len(result))
	innerLen := hranaProtoLength(2, messageLen)
	executeLen := hranaProtoLength(1, innerLen)
	streamExecuteLen := hranaProtoLength(3, executeLen)
	closeInnerLen := hranaProtoLength(1, 0)
	closeStreamLen := hranaProtoLength(1, closeInnerLen)
	closeOuterLen := hranaProtoLength(3, closeStreamLen)
	output := make([]byte, 0, streamExecuteLen+closeOuterLen)
	output = appendHranaProtoFieldHeader(output, 3, executeLen)
	output = appendHranaProtoFieldHeader(output, 1, innerLen)
	output = appendHranaProtoFieldHeader(output, 2, messageLen)
	output = appendHranaProtoFieldHeader(output, 1, len(result))
	output = append(output, result...)
	output = appendHranaProtoFieldHeader(output, 3, closeStreamLen)
	output = appendHranaProtoFieldHeader(output, 1, closeInnerLen)
	output = appendHranaProtoFieldHeader(output, 1, 0)
	return output
}
