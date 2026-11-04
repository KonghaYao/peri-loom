// Statement result validation for a fused open reply. validStatementResult in
// gateway_statement.go stays the authority for the accepted shape; these
// functions walk the same rules without materializing rows or copying the
// payload, which is the whole point of the fused path.
package platform

import (
	"encoding/json"
	"math"
	"math/bits"
)

const (
	fusedStatementFieldCols = iota
	fusedStatementFieldRows
	fusedStatementFieldAffectedRowCount
	fusedStatementFieldLastInsertRowID
	fusedStatementFieldAutocommit
	fusedStatementFieldRowsRead
	fusedStatementFieldRowsWritten
	fusedStatementFieldQueryDurationMS
)

const fusedStatementRequiredFields = 1<<fusedStatementFieldCols |
	1<<fusedStatementFieldRows |
	1<<fusedStatementFieldAffectedRowCount |
	1<<fusedStatementFieldLastInsertRowID |
	1<<fusedStatementFieldAutocommit

// matchStatementResultField is case-sensitive: validStatementResult decodes the
// result into a map, and map keys are compared exactly after unquoting.
func matchStatementResultField(content, quoted []byte) int {
	if plainJSONContent(content) {
		switch {
		case fusedKeyEqual(content, "cols"):
			return fusedStatementFieldCols
		case fusedKeyEqual(content, "rows"):
			return fusedStatementFieldRows
		case fusedKeyEqual(content, "affected_row_count"):
			return fusedStatementFieldAffectedRowCount
		case fusedKeyEqual(content, "last_insert_rowid"):
			return fusedStatementFieldLastInsertRowID
		case fusedKeyEqual(content, "autocommit"):
			return fusedStatementFieldAutocommit
		case fusedKeyEqual(content, "rows_read"):
			return fusedStatementFieldRowsRead
		case fusedKeyEqual(content, "rows_written"):
			return fusedStatementFieldRowsWritten
		case fusedKeyEqual(content, "query_duration_ms"):
			return fusedStatementFieldQueryDurationMS
		}
		return -1
	}
	key, ok := fusedDecodeJSONString(quoted)
	if !ok {
		return -1
	}
	switch key {
	case "cols":
		return fusedStatementFieldCols
	case "rows":
		return fusedStatementFieldRows
	case "affected_row_count":
		return fusedStatementFieldAffectedRowCount
	case "last_insert_rowid":
		return fusedStatementFieldLastInsertRowID
	case "autocommit":
		return fusedStatementFieldAutocommit
	case "rows_read":
		return fusedStatementFieldRowsRead
	case "rows_written":
		return fusedStatementFieldRowsWritten
	case "query_duration_ms":
		return fusedStatementFieldQueryDurationMS
	}
	return -1
}

// validScannedStatementResult accepts exactly the shapes validStatementResult
// accepts. Both decode the same map semantics: duplicate keys keep the last
// value, keys are unquoted before matching, and the unique key count picks the
// five-field or eight-field form. Any unknown key is refused immediately
// because the accepted counts leave no room for one.
func validScannedStatementResult(result []byte) bool {
	if len(result) == 0 {
		return false
	}
	scanner := fusedReplyScanner{data: result}
	scanner.skipSpace()
	if scanner.pos >= len(result) || result[scanner.pos] != '{' {
		return false
	}
	scanner.pos++
	var (
		seen            uint32
		columns         []byte
		columnCount     int
		columnsValid    bool
		columnsRevision uint32
		rowsSpan        []byte
		rowsPresent     bool
		rowsChecked     bool
		rowsValid       bool
		rowsRevision    uint32
		affected        []byte
		rowid           []byte
		autocommit      []byte
		rowsRead        []byte
		rowsWritten     []byte
		duration        []byte
	)
	scanner.skipSpace()
	if scanner.pos < len(result) && result[scanner.pos] == '}' {
		// An empty object has no unique key count the validator accepts.
		return false
	}
scanResult:
	for {
		if scanner.pos >= len(result) || result[scanner.pos] != '"' {
			return false
		}
		keyStart, keyEnd, ok := scanner.scanString()
		if !ok {
			return false
		}
		field := matchStatementResultField(result[keyStart:keyEnd], result[keyStart-1:keyEnd+1])
		if field < 0 {
			return false
		}
		scanner.skipSpace()
		if scanner.pos >= len(result) || result[scanner.pos] != ':' {
			return false
		}
		scanner.pos++
		scanner.skipSpace()
		valueStart := scanner.pos
		seen |= 1 << uint(field)
		switch field {
		case fusedStatementFieldCols:
			// Columns are needed before rows can be checked. The span is kept
			// anyway: a later duplicate cols key makes this value stale, and
			// the last one is the only value the validator would have seen.
			if !scanner.scanValue() {
				return false
			}
			columns = result[valueStart:scanner.pos]
			columnsRevision++
			columnsValid = false
			columnCount = 0
			if parsed, ok := validScannedStatementColumns(columns); ok {
				columnCount = parsed
				columnsValid = true
			}
		case fusedStatementFieldRows:
			// Validating rows here folds the span scan and the width check
			// into one pass over the payload. If cols has not been seen yet,
			// or an earlier cols value was invalid, the span is recorded and
			// checked after the object with the final column count.
			rowsPresent = true
			rowsChecked = columnsValid
			rowsRevision = columnsRevision
			rowsValid = false
			if columnsValid {
				if scanStatementRowsValue(&scanner, columnCount) {
					rowsValid = true
				} else {
					scanner.pos = valueStart
					if !scanner.scanValue() {
						return false
					}
				}
			} else if !scanner.scanValue() {
				return false
			}
			rowsSpan = result[valueStart:scanner.pos]
		default:
			if !scanner.scanValue() {
				return false
			}
			value := result[valueStart:scanner.pos]
			switch field {
			case fusedStatementFieldAffectedRowCount:
				affected = value
			case fusedStatementFieldLastInsertRowID:
				rowid = value
			case fusedStatementFieldAutocommit:
				autocommit = value
			case fusedStatementFieldRowsRead:
				rowsRead = value
			case fusedStatementFieldRowsWritten:
				rowsWritten = value
			case fusedStatementFieldQueryDurationMS:
				duration = value
			}
		}
		scanner.skipSpace()
		if scanner.pos >= len(result) {
			return false
		}
		switch result[scanner.pos] {
		case ',':
			scanner.pos++
			scanner.skipSpace()
		case '}':
			scanner.pos++
			break scanResult
		default:
			return false
		}
	}
	scanner.skipSpace()
	if scanner.pos != len(result) {
		return false
	}
	count := bits.OnesCount32(seen)
	if (count != 5 && count != 8) || seen&fusedStatementRequiredFields != fusedStatementRequiredFields {
		return false
	}
	if columns == nil || !columnsValid || !rowsPresent {
		return false
	}
	if rowsChecked && rowsRevision == columnsRevision {
		if !rowsValid {
			return false
		}
	} else if !validScannedStatementRows(rowsSpan, columnCount) {
		return false
	}
	var affectedRows int64
	if json.Unmarshal(affected, &affectedRows) != nil || affectedRows < 0 {
		return false
	}
	if !fusedKeyEqual(rowid, "null") {
		var inserted int64
		if json.Unmarshal(rowid, &inserted) != nil {
			return false
		}
	}
	if !fusedKeyEqual(autocommit, "true") && !fusedKeyEqual(autocommit, "false") {
		return false
	}
	if count == 8 {
		if fusedKeyEqual(rowsRead, "null") {
			return false
		}
		var read uint64
		if json.Unmarshal(rowsRead, &read) != nil {
			return false
		}
		if fusedKeyEqual(rowsWritten, "null") {
			return false
		}
		var written uint64
		if json.Unmarshal(rowsWritten, &written) != nil {
			return false
		}
		if fusedKeyEqual(duration, "null") {
			return false
		}
		var elapsed float64
		if json.Unmarshal(duration, &elapsed) != nil || math.IsNaN(elapsed) || math.IsInf(elapsed, 0) || elapsed < 0 {
			return false
		}
	}
	return true
}

const (
	fusedStatementColumnName = iota
	fusedStatementColumnDecltype
)

func matchStatementColumnField(content, quoted []byte) int {
	if plainJSONContent(content) {
		switch {
		case fusedKeyEqual(content, "name"):
			return fusedStatementColumnName
		case fusedKeyEqual(content, "decltype"):
			return fusedStatementColumnDecltype
		}
		return -1
	}
	key, ok := fusedDecodeJSONString(quoted)
	if !ok {
		return -1
	}
	switch key {
	case "name":
		return fusedStatementColumnName
	case "decltype":
		return fusedStatementColumnDecltype
	}
	return -1
}

// consumeStringOrNull is the acceptance set of json.Unmarshal into a string:
// a string value, or a null that leaves the zero value behind. Anything else
// is a type error. A decoded string always succeeds once the scan proved its
// escapes, so the value itself is not materialized for metadata fields.
func (scanner *fusedReplyScanner) consumeStringOrNull() bool {
	if scanner.pos >= len(scanner.data) {
		return false
	}
	switch scanner.data[scanner.pos] {
	case '"':
		_, _, ok := scanner.scanString()
		return ok
	case 'n':
		return scanner.scanLiteral("null")
	default:
		return false
	}
}

func validScannedStatementColumns(data []byte) (int, bool) {
	scanner := fusedReplyScanner{data: data}
	scanner.skipSpace()
	if scanner.pos >= len(data) || data[scanner.pos] != '[' {
		return 0, false
	}
	scanner.pos++
	scanner.skipSpace()
	if scanner.pos < len(data) && data[scanner.pos] == ']' {
		scanner.pos++
		scanner.skipSpace()
		if scanner.pos != len(data) {
			return 0, false
		}
		return 0, true
	}
	count := 0
	for {
		scanner.skipSpace()
		if scanner.pos >= len(data) || data[scanner.pos] != '{' {
			return 0, false
		}
		scanner.pos++
		var (
			name     []byte
			decltype []byte
		)
		scanner.skipSpace()
		if scanner.pos < len(data) && data[scanner.pos] == '}' {
			// An empty column object has neither of the two keys.
			return 0, false
		}
	scanColumn:
		for {
			if scanner.pos >= len(data) || data[scanner.pos] != '"' {
				return 0, false
			}
			keyStart, keyEnd, ok := scanner.scanString()
			if !ok {
				return 0, false
			}
			field := matchStatementColumnField(data[keyStart:keyEnd], data[keyStart-1:keyEnd+1])
			if field < 0 {
				return 0, false
			}
			scanner.skipSpace()
			if scanner.pos >= len(data) || data[scanner.pos] != ':' {
				return 0, false
			}
			scanner.pos++
			scanner.skipSpace()
			valueStart := scanner.pos
			if !scanner.consumeStringOrNull() {
				return 0, false
			}
			switch field {
			case fusedStatementColumnName:
				name = data[valueStart:scanner.pos]
			case fusedStatementColumnDecltype:
				decltype = data[valueStart:scanner.pos]
			}
			scanner.skipSpace()
			if scanner.pos >= len(data) {
				return 0, false
			}
			switch data[scanner.pos] {
			case ',':
				scanner.pos++
				scanner.skipSpace()
			case '}':
				scanner.pos++
				break scanColumn
			default:
				return 0, false
			}
		}
		if name == nil || decltype == nil {
			return 0, false
		}
		count++
		scanner.skipSpace()
		if scanner.pos >= len(data) {
			return 0, false
		}
		switch data[scanner.pos] {
		case ',':
			scanner.pos++
		case ']':
			scanner.pos++
			scanner.skipSpace()
			if scanner.pos != len(data) {
				return 0, false
			}
			return count, true
		default:
			return 0, false
		}
	}
}

// scanStatementRow returns the row width. A literal null decodes into the nil
// slice, so it counts as a zero-width row, exactly as the map decode sees it.
func scanStatementRow(scanner *fusedReplyScanner) (int, bool) {
	scanner.skipSpace()
	if scanner.pos >= len(scanner.data) {
		return 0, false
	}
	switch scanner.data[scanner.pos] {
	case '[':
		scanner.pos++
		width := 0
		scanner.skipSpace()
		if scanner.pos < len(scanner.data) && scanner.data[scanner.pos] == ']' {
			scanner.pos++
			return 0, true
		}
		for {
			if !scanner.scanValue() {
				return 0, false
			}
			width++
			scanner.skipSpace()
			if scanner.pos >= len(scanner.data) {
				return 0, false
			}
			switch scanner.data[scanner.pos] {
			case ',':
				scanner.pos++
			case ']':
				scanner.pos++
				return width, true
			default:
				return 0, false
			}
		}
	case 'n':
		if !scanner.scanLiteral("null") {
			return 0, false
		}
		return 0, true
	default:
		return 0, false
	}
}

// scanStatementRowsValue validates the rows array and leaves the scanner after
// its closing bracket. The caller decides whether the surrounding value ends
// there: the standalone validator owns the whole span, while the result scan
// continues with the next key.
//
// Rows are only counted here. A later round that needs cell or row spans can
// record them in this same walk: the scan already visits every cell boundary,
// so recording would not add another pass or an allocation proportional to the
// cell count unless the spans are kept.
func scanStatementRowsValue(scanner *fusedReplyScanner, columnCount int) bool {
	scanner.skipSpace()
	data := scanner.data
	if scanner.pos >= len(data) || data[scanner.pos] != '[' {
		return false
	}
	scanner.pos++
	scanner.skipSpace()
	if scanner.pos < len(data) && data[scanner.pos] == ']' {
		scanner.pos++
		return true
	}
	for {
		width, ok := scanStatementRow(scanner)
		if !ok || width != columnCount {
			return false
		}
		scanner.skipSpace()
		if scanner.pos >= len(data) {
			return false
		}
		switch data[scanner.pos] {
		case ',':
			scanner.pos++
			scanner.skipSpace()
		case ']':
			scanner.pos++
			return true
		default:
			return false
		}
	}
}

func validScannedStatementRows(data []byte, columnCount int) bool {
	scanner := fusedReplyScanner{data: data}
	if !scanStatementRowsValue(&scanner, columnCount) {
		return false
	}
	scanner.skipSpace()
	return scanner.pos == len(data)
}
