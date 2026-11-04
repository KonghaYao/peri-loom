package platform

import (
	"errors"
	"unicode/utf8"
)

// hranaCursorPageCellKind classifies one cursor row cell the way the Hrana
// encoders dispatch after a full json.Decoder pass: the four shapes that can be
// written from their token text, and everything else (blobs, booleans, objects)
// that still needs the decode-then-encode fallback.
type hranaCursorPageCellKind uint8

const (
	hranaCursorPageCellNull hranaCursorPageCellKind = iota
	hranaCursorPageCellInteger
	hranaCursorPageCellFloat
	hranaCursorPageCellText
	hranaCursorPageCellOther
)

// hranaCursorPageCell is one row cell: its kind and the exact source span of
// its JSON token. The span stays valid while the scanned page bytes do.
type hranaCursorPageCell struct {
	kind  hranaCursorPageCellKind
	start int
	end   int
}

// hranaCursorPage is a structurally scanned read cursor page. offsets has one
// entry more than rows, so row i is cells[offsets[i]:offsets[i+1]].
type hranaCursorPage struct {
	source  []byte
	done    bool
	rows    int
	offsets []int
	cells   []hranaCursorPageCell
}

var errHranaCursorPage = errors.New("invalid cursor page")

func (page *hranaCursorPage) row(index int) []hranaCursorPageCell {
	return page.cells[page.offsets[index]:page.offsets[index+1]]
}

func (page *hranaCursorPage) cellBytes(cell hranaCursorPageCell) []byte {
	return page.source[cell.start:cell.end]
}

// hranaCursorPageScanner validates and indexes one worker page without
// materializing rows. Its buffers are reused across the pages of one request;
// a page returned by scan is only valid until the next scan on the scanner.
type hranaCursorPageScanner struct {
	body    []byte
	record  bool
	done    bool
	rows    int
	offsets []int
	cells   []hranaCursorPageCell
}

// hranaCursorPageMaxDepth matches encoding/json's container nesting limit, so
// the scanner accepts and rejects exactly the pages json.Unmarshal accepts.
const hranaCursorPageMaxDepth = 10000

// scan parses body as the read cursor page shape: an object with exactly the
// done and rows members, where every row is an array (or null, which
// json.Unmarshal also accepts for a [][]json.RawMessage row) and rows is an
// array of those. allowMetadata additionally accepts the fused first page's
// cols and autocommit members, which the old adapter decode ignored. scan is a
// strict JSON syntax scan as well, so it can stand in for the json.Unmarshal
// calls that used to accept or reject the page.
func (scanner *hranaCursorPageScanner) scan(body []byte, record, allowMetadata bool) (hranaCursorPage, error) {
	scanner.body = body
	scanner.record = record
	scanner.done = false
	scanner.rows = 0
	scanner.offsets = scanner.offsets[:0]
	scanner.cells = scanner.cells[:0]
	if record {
		scanner.offsets = append(scanner.offsets, 0)
	}
	position, err := scanner.whitespace(0)
	if err != nil || position >= len(body) || body[position] != '{' {
		return hranaCursorPage{}, errHranaCursorPage
	}
	position++
	if position, err = scanner.whitespace(position); err != nil {
		return hranaCursorPage{}, errHranaCursorPage
	}
	distinct := 0
	hasDone, hasRows := false, false
	doneStart, doneEnd := 0, 0
	rowsShapeInvalid := false
	if position < len(body) && body[position] == '}' {
		position++
	} else {
		for {
			if position, err = scanner.whitespace(position); err != nil || position >= len(body) || body[position] != '"' {
				return hranaCursorPage{}, errHranaCursorPage
			}
			keyStart := position
			if position, err = scanner.string(position); err != nil {
				return hranaCursorPage{}, errHranaCursorPage
			}
			key, err := sqlJSONKey(body[keyStart:position])
			if err != nil {
				return hranaCursorPage{}, errHranaCursorPage
			}
			if position, err = scanner.whitespace(position); err != nil || position >= len(body) || body[position] != ':' {
				return hranaCursorPage{}, errHranaCursorPage
			}
			position++
			switch key {
			case "done":
				if !hasDone {
					distinct++
					hasDone = true
				}
				if position, err = scanner.whitespace(position); err != nil {
					return hranaCursorPage{}, errHranaCursorPage
				}
				doneStart = position
				if position, err = scanner.value(position, 2); err != nil {
					return hranaCursorPage{}, errHranaCursorPage
				}
				doneEnd = position
			case "rows":
				if !hasRows {
					distinct++
					hasRows = true
				}
				// A later duplicate key wins, exactly like json.Unmarshal into a
				// map: drop the rows an earlier spelling contributed.
				scanner.rows = 0
				scanner.cells = scanner.cells[:0]
				if record {
					scanner.offsets = scanner.offsets[:1]
				}
				rowsShapeInvalid = false
				if position, err = scanner.whitespace(position); err != nil {
					return hranaCursorPage{}, errHranaCursorPage
				}
				rowsStart := position
				if position < len(body) && body[position] == '[' {
					if position, err = scanner.rowArray(position); err == nil {
						break
					}
				}
				// A valid JSON value with the wrong row shape is only fatal when
				// it is the last rows member; json.Unmarshal would have kept a
				// later one. Re-scan it generically to find its end.
				scanner.rows = 0
				scanner.cells = scanner.cells[:0]
				if record {
					scanner.offsets = scanner.offsets[:1]
				}
				if position, err = scanner.value(rowsStart, 2); err != nil {
					return hranaCursorPage{}, errHranaCursorPage
				}
				rowsShapeInvalid = true
			case "cols", "autocommit":
				if !allowMetadata {
					return hranaCursorPage{}, errHranaCursorPage
				}
				if position, err = scanner.value(position, 2); err != nil {
					return hranaCursorPage{}, errHranaCursorPage
				}
			default:
				return hranaCursorPage{}, errHranaCursorPage
			}
			if position, err = scanner.whitespace(position); err != nil || position >= len(body) {
				return hranaCursorPage{}, errHranaCursorPage
			}
			if body[position] == ',' {
				position++
				continue
			}
			if body[position] == '}' {
				position++
				break
			}
			return hranaCursorPage{}, errHranaCursorPage
		}
	}
	if position, err = scanner.whitespace(position); err != nil || position != len(body) {
		return hranaCursorPage{}, errHranaCursorPage
	}
	if distinct != 2 || !hasDone || !hasRows || rowsShapeInvalid {
		return hranaCursorPage{}, errHranaCursorPage
	}
	// The old validator compared the last done member's raw text, so only the
	// unspaced literals are accepted even though whitespace is legal JSON.
	token := body[doneStart:doneEnd]
	scanner.done = string(token) == "true"
	if string(token) != "true" && string(token) != "false" {
		return hranaCursorPage{}, errHranaCursorPage
	}
	page := hranaCursorPage{source: body, done: scanner.done, rows: scanner.rows}
	if record {
		page.offsets = scanner.offsets
		page.cells = scanner.cells
	}
	return page, nil
}

// rowArray scans the rows member. The page object is depth 1, so rows is depth
// 2, a row is depth 3 and a container cell is depth 4.
func (scanner *hranaCursorPageScanner) rowArray(position int) (int, error) {
	body := scanner.body
	position++
	var err error
	if position, err = scanner.whitespace(position); err != nil {
		return 0, err
	}
	if position >= len(body) {
		return 0, errHranaCursorPage
	}
	if body[position] == ']' {
		return position + 1, nil
	}
	for {
		if position, err = scanner.row(position); err != nil {
			return 0, err
		}
		if position, err = scanner.whitespace(position); err != nil || position >= len(body) {
			return 0, errHranaCursorPage
		}
		if body[position] == ',' {
			position++
			if position, err = scanner.whitespace(position); err != nil {
				return 0, err
			}
			continue
		}
		if body[position] == ']' {
			return position + 1, nil
		}
		return 0, errHranaCursorPage
	}
}

func (scanner *hranaCursorPageScanner) row(position int) (int, error) {
	body := scanner.body
	if position >= len(body) {
		return 0, errHranaCursorPage
	}
	if body[position] == 'n' {
		end, err := hranaCursorPageLiteral(body, position, "null")
		if err != nil {
			return 0, err
		}
		scanner.rows++
		if scanner.record {
			scanner.offsets = append(scanner.offsets, len(scanner.cells))
		}
		return end, nil
	}
	if body[position] != '[' {
		return 0, errHranaCursorPage
	}
	position++
	var err error
	if position, err = scanner.whitespace(position); err != nil {
		return 0, err
	}
	if position < len(body) && body[position] == ']' {
		position++
	} else {
		for {
			start := position
			kind, end, err := scanner.cell(position)
			if err != nil {
				return 0, err
			}
			if scanner.record {
				scanner.cells = append(scanner.cells, hranaCursorPageCell{kind: kind, start: start, end: end})
			}
			position = end
			if position, err = scanner.whitespace(position); err != nil || position >= len(body) {
				return 0, errHranaCursorPage
			}
			if body[position] == ',' {
				position++
				if position, err = scanner.whitespace(position); err != nil {
					return 0, err
				}
				continue
			}
			if body[position] == ']' {
				position++
				break
			}
			return 0, errHranaCursorPage
		}
	}
	scanner.rows++
	if scanner.record {
		scanner.offsets = append(scanner.offsets, len(scanner.cells))
	}
	return position, nil
}

func (scanner *hranaCursorPageScanner) cell(position int) (hranaCursorPageCellKind, int, error) {
	body := scanner.body
	if position >= len(body) {
		return 0, 0, errHranaCursorPage
	}
	switch body[position] {
	case 'n':
		end, err := hranaCursorPageLiteral(body, position, "null")
		return hranaCursorPageCellNull, end, err
	case 't':
		end, err := hranaCursorPageLiteral(body, position, "true")
		return hranaCursorPageCellOther, end, err
	case 'f':
		end, err := hranaCursorPageLiteral(body, position, "false")
		return hranaCursorPageCellOther, end, err
	case '"':
		end, err := scanner.string(position)
		return hranaCursorPageCellText, end, err
	case '[':
		end, err := scanner.array(position, 4)
		return hranaCursorPageCellOther, end, err
	case '{':
		end, err := scanner.object(position, 4)
		return hranaCursorPageCellOther, end, err
	}
	end, float, err := scanner.number(position)
	if err != nil {
		return 0, 0, err
	}
	if float {
		return hranaCursorPageCellFloat, end, nil
	}
	return hranaCursorPageCellInteger, end, nil
}

func (scanner *hranaCursorPageScanner) value(position, depth int) (int, error) {
	body := scanner.body
	var err error
	if position, err = scanner.whitespace(position); err != nil {
		return 0, err
	}
	if position >= len(body) {
		return 0, errHranaCursorPage
	}
	switch body[position] {
	case '{':
		return scanner.object(position, depth)
	case '[':
		return scanner.array(position, depth)
	case '"':
		return scanner.string(position)
	case 't':
		return hranaCursorPageLiteral(body, position, "true")
	case 'f':
		return hranaCursorPageLiteral(body, position, "false")
	case 'n':
		return hranaCursorPageLiteral(body, position, "null")
	}
	end, _, err := scanner.number(position)
	return end, err
}

func (scanner *hranaCursorPageScanner) array(position, depth int) (int, error) {
	if depth > hranaCursorPageMaxDepth {
		return 0, errHranaCursorPage
	}
	body := scanner.body
	position++
	var err error
	if position, err = scanner.whitespace(position); err != nil {
		return 0, err
	}
	if position >= len(body) {
		return 0, errHranaCursorPage
	}
	if body[position] == ']' {
		return position + 1, nil
	}
	for {
		if position, err = scanner.value(position, depth+1); err != nil {
			return 0, err
		}
		if position, err = scanner.whitespace(position); err != nil || position >= len(body) {
			return 0, errHranaCursorPage
		}
		if body[position] == ',' {
			position++
			continue
		}
		if body[position] == ']' {
			return position + 1, nil
		}
		return 0, errHranaCursorPage
	}
}

func (scanner *hranaCursorPageScanner) object(position, depth int) (int, error) {
	if depth > hranaCursorPageMaxDepth {
		return 0, errHranaCursorPage
	}
	body := scanner.body
	position++
	var err error
	if position, err = scanner.whitespace(position); err != nil {
		return 0, err
	}
	if position >= len(body) {
		return 0, errHranaCursorPage
	}
	if body[position] == '}' {
		return position + 1, nil
	}
	for {
		if position, err = scanner.whitespace(position); err != nil || position >= len(body) || body[position] != '"' {
			return 0, errHranaCursorPage
		}
		if position, err = scanner.string(position); err != nil {
			return 0, err
		}
		if position, err = scanner.whitespace(position); err != nil || position >= len(body) || body[position] != ':' {
			return 0, errHranaCursorPage
		}
		position++
		if position, err = scanner.value(position, depth+1); err != nil {
			return 0, err
		}
		if position, err = scanner.whitespace(position); err != nil || position >= len(body) {
			return 0, errHranaCursorPage
		}
		if body[position] == ',' {
			position++
			continue
		}
		if body[position] == '}' {
			return position + 1, nil
		}
		return 0, errHranaCursorPage
	}
}

func (scanner *hranaCursorPageScanner) string(position int) (int, error) {
	body := scanner.body
	position++
	for position < len(body) {
		position += hranaCursorPageIndexSpecial(body[position:])
		if position >= len(body) {
			return 0, errHranaCursorPage
		}
		switch character := body[position]; {
		case character == '"':
			return position + 1, nil
		case character == '\\':
			position++
			if position >= len(body) {
				return 0, errHranaCursorPage
			}
			switch body[position] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				position++
			case 'u':
				if position+4 >= len(body) {
					return 0, errHranaCursorPage
				}
				for offset := 1; offset <= 4; offset++ {
					if !hranaCursorPageHex(body[position+offset]) {
						return 0, errHranaCursorPage
					}
				}
				position += 5
			default:
				return 0, errHranaCursorPage
			}
		case character < 0x20:
			return 0, errHranaCursorPage
		default:
			position++
		}
	}
	return 0, errHranaCursorPage
}

func (scanner *hranaCursorPageScanner) number(position int) (int, bool, error) {
	body := scanner.body
	float := false
	if position < len(body) && body[position] == '-' {
		position++
	}
	if position >= len(body) {
		return 0, false, errHranaCursorPage
	}
	if body[position] == '0' {
		position++
	} else if body[position] >= '1' && body[position] <= '9' {
		for position < len(body) && hranaCursorPageDigit(body[position]) {
			position++
		}
	} else {
		return 0, false, errHranaCursorPage
	}
	if position < len(body) && body[position] == '.' {
		float = true
		position++
		digits := position
		for position < len(body) && hranaCursorPageDigit(body[position]) {
			position++
		}
		if position == digits {
			return 0, false, errHranaCursorPage
		}
	}
	if position < len(body) && (body[position] == 'e' || body[position] == 'E') {
		float = true
		position++
		if position < len(body) && (body[position] == '+' || body[position] == '-') {
			position++
		}
		digits := position
		for position < len(body) && hranaCursorPageDigit(body[position]) {
			position++
		}
		if position == digits {
			return 0, false, errHranaCursorPage
		}
	}
	return position, float, nil
}

func (scanner *hranaCursorPageScanner) whitespace(position int) (int, error) {
	if position < 0 || position > len(scanner.body) {
		return 0, errHranaCursorPage
	}
	for position < len(scanner.body) && sqlJSONSpace(scanner.body[position]) {
		position++
	}
	return position, nil
}

func hranaCursorPageLiteral(body []byte, position int, literal string) (int, error) {
	if len(body)-position < len(literal) || string(body[position:position+len(literal)]) != literal {
		return 0, errHranaCursorPage
	}
	return position + len(literal), nil
}

func hranaCursorPageDigit(character byte) bool {
	return character >= '0' && character <= '9'
}

func hranaCursorPageHex(character byte) bool {
	return hranaCursorPageDigit(character) || character >= 'a' && character <= 'f' || character >= 'A' && character <= 'F'
}

func hranaCursorPageHex4(token []byte) uint32 {
	var value uint32
	for index := 0; index < 4; index++ {
		value <<= 4
		switch character := token[index]; {
		case character >= '0' && character <= '9':
			value |= uint32(character - '0')
		case character >= 'a' && character <= 'f':
			value |= uint32(character-'a') + 10
		default:
			value |= uint32(character-'A') + 10
		}
	}
	return value
}

// hranaCursorPageAppendText appends the decoded value of one text token. It
// reproduces encoding/json's unescaping, including U+FFFD for lone surrogate
// escapes, without allocating the intermediate string.
func hranaCursorPageAppendText(destination, token []byte) []byte {
	for position := 1; position < len(token)-1; {
		next := hranaCursorPageIndexBackslash(token, position)
		if next < 0 {
			destination = append(destination, token[position:len(token)-1]...)
			break
		}
		destination = append(destination, token[position:position+next]...)
		position += next
		switch escape := token[position+1]; escape {
		case '"', '\\', '/':
			destination = append(destination, escape)
			position += 2
		case 'b':
			destination = append(destination, '\b')
			position += 2
		case 'f':
			destination = append(destination, '\f')
			position += 2
		case 'n':
			destination = append(destination, '\n')
			position += 2
		case 'r':
			destination = append(destination, '\r')
			position += 2
		case 't':
			destination = append(destination, '\t')
			position += 2
		case 'u':
			unit := hranaCursorPageHex4(token[position+2:])
			position += 6
			if unit >= 0xd800 && unit <= 0xdbff {
				if position+5 < len(token)-1 && token[position] == '\\' && token[position+1] == 'u' {
					low := hranaCursorPageHex4(token[position+2:])
					if low >= 0xdc00 && low <= 0xdfff {
						destination = utf8.AppendRune(destination, 0x10000+(rune(unit)-0xd800)<<10+(rune(low)-0xdc00))
						position += 6
						continue
					}
				}
				destination = utf8.AppendRune(destination, utf8.RuneError)
				continue
			}
			if unit >= 0xdc00 && unit <= 0xdfff {
				destination = utf8.AppendRune(destination, utf8.RuneError)
				continue
			}
			destination = utf8.AppendRune(destination, rune(unit))
		}
	}
	return destination
}

// hranaJSONAppendPageText appends one text token with exactly the escaping
// hranaJSONAppendString applies to its decoded value: the worker's escapes are
// normalized, and raw HTML and line-separator characters are escaped.
func hranaJSONAppendPageText(destination, token []byte) []byte {
	const hex = "0123456789abcdef"
	destination = append(destination, '"')
	for position := 1; position < len(token)-1; {
		next := hranaCursorPageIndexJSONSpecial(token[position : len(token)-1])
		if next >= len(token)-1-position {
			destination = append(destination, token[position:len(token)-1]...)
			break
		}
		destination = append(destination, token[position:position+next]...)
		position += next
		character := token[position]
		if character != '\\' {
			switch character {
			case '<':
				destination = append(destination, `\u003c`...)
			case '>':
				destination = append(destination, `\u003e`...)
			case '&':
				destination = append(destination, `\u0026`...)
			default:
				if character == 0xe2 && position+2 < len(token)-1 && token[position+1] == 0x80 && (token[position+2] == 0xa8 || token[position+2] == 0xa9) {
					destination = append(destination, '\\', 'u', '2', '0', '2', hex[token[position+2]&0xf])
					position += 3
					continue
				}
				destination = append(destination, character)
			}
			position++
			continue
		}
		switch escape := token[position+1]; escape {
		case '"', '\\':
			destination = append(destination, '\\', escape)
			position += 2
		case '/':
			destination = append(destination, '/')
			position += 2
		case 'b', 'f', 'n', 'r', 't':
			destination = append(destination, '\\', escape)
			position += 2
		case 'u':
			unit := hranaCursorPageHex4(token[position+2:])
			position += 6
			if unit >= 0xd800 && unit <= 0xdbff {
				if position+5 < len(token)-1 && token[position] == '\\' && token[position+1] == 'u' {
					low := hranaCursorPageHex4(token[position+2:])
					if low >= 0xdc00 && low <= 0xdfff {
						destination = utf8.AppendRune(destination, 0x10000+(rune(unit)-0xd800)<<10+(rune(low)-0xdc00))
						position += 6
						continue
					}
				}
				destination = utf8.AppendRune(destination, utf8.RuneError)
				continue
			}
			if unit >= 0xdc00 && unit <= 0xdfff {
				destination = utf8.AppendRune(destination, utf8.RuneError)
				continue
			}
			switch unit {
			case 0x08:
				destination = append(destination, '\\', 'b')
			case 0x0c:
				destination = append(destination, '\\', 'f')
			case 0x0a:
				destination = append(destination, '\\', 'n')
			case 0x0d:
				destination = append(destination, '\\', 'r')
			case 0x09:
				destination = append(destination, '\\', 't')
			case '"':
				destination = append(destination, '\\', '"')
			case '\\':
				destination = append(destination, '\\', '\\')
			case '<':
				destination = append(destination, `\u003c`...)
			case '>':
				destination = append(destination, `\u003e`...)
			case '&':
				destination = append(destination, `\u0026`...)
			case 0x2028:
				destination = append(destination, `\u2028`...)
			case 0x2029:
				destination = append(destination, `\u2029`...)
			default:
				if unit < 0x20 {
					destination = append(destination, '\\', 'u', '0', '0', hex[unit>>4], hex[unit&0xf])
				} else {
					destination = utf8.AppendRune(destination, rune(unit))
				}
			}
		}
	}
	return append(destination, '"')
}
