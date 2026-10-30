package platform

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	errHranaJSONCursorValue  = errors.New("invalid cursor value")
	errHranaJSONCursorEncode = errors.New("unencodable cursor value")
)

// hranaJSONCursorRow writes one cursor row entry:
//
//	{"row":[{"type":...},...],"type":"row"}\n
//
// The shape, key order and string escaping match json.Encoder encoding of
// map[string]any{"type":"row","row":hranaOutputValue(cell)...} byte for byte,
// but a row is appended into one request-local buffer instead of allocating a
// map per cell and reflecting over the whole entry.
type hranaJSONCursorRow struct {
	buffer  []byte
	scratch []byte
}

func (row *hranaJSONCursorRow) begin() {
	row.buffer = row.buffer[:0]
}

func (row *hranaJSONCursorRow) appendCells(cells []any) error {
	row.buffer = append(row.buffer, `{"row":[`...)
	for index, cell := range cells {
		if index != 0 {
			row.buffer = append(row.buffer, ',')
		}
		if err := row.appendCell(cell); err != nil {
			return err
		}
	}
	row.buffer = append(row.buffer, `],"type":"row"}`...)
	return nil
}

func (row *hranaJSONCursorRow) appendCell(cell any) error {
	switch value := cell.(type) {
	case nil:
		row.buffer = append(row.buffer, `{"type":"null"}`...)
	case json.Number:
		if strings.ContainsAny(string(value), ".eE") {
			converted, err := value.Float64()
			if err != nil {
				return errHranaJSONCursorValue
			}
			// json.Encoder rejects NaN and infinities; the cursor path used to
			// surface that rejection as an encode failure after start.
			if math.IsNaN(converted) || math.IsInf(converted, 0) {
				return errHranaJSONCursorEncode
			}
			row.buffer = append(row.buffer, `{"type":"float","value":`...)
			row.buffer = hranaJSONAppendFloat(row.buffer, converted)
			row.buffer = append(row.buffer, '}')
		} else {
			row.buffer = append(row.buffer, `{"type":"integer","value":`...)
			row.buffer = hranaJSONAppendString(row.buffer, string(value))
			row.buffer = append(row.buffer, '}')
		}
	case string:
		row.buffer = append(row.buffer, `{"type":"text","value":`...)
		row.buffer = hranaJSONAppendString(row.buffer, value)
		row.buffer = append(row.buffer, '}')
	case []any:
		blob, err := hranaCursorBlobBytes(value, row.scratch[:0])
		if err != nil {
			return errHranaJSONCursorValue
		}
		row.scratch = blob
		row.buffer = append(row.buffer, `{"base64":"`...)
		row.buffer = base64.StdEncoding.AppendEncode(row.buffer, blob)
		row.buffer = append(row.buffer, `","type":"blob"}`...)
	default:
		return errHranaJSONCursorValue
	}
	return nil
}

func (row *hranaJSONCursorRow) bytes() []byte {
	row.buffer = append(row.buffer, '\n')
	return row.buffer
}

// hranaJSONAppendString mirrors encoding/json's string escaping with HTML
// escaping enabled, which json.Encoder uses by default.
func hranaJSONAppendString(destination []byte, value string) []byte {
	const hex = "0123456789abcdef"
	destination = append(destination, '"')
	start := 0
	for index := 0; index < len(value); {
		if character := value[index]; character < utf8.RuneSelf {
			if character >= 0x20 && character != '"' && character != '\\' && character != '<' && character != '>' && character != '&' {
				index++
				continue
			}
			destination = append(destination, value[start:index]...)
			switch character {
			case '\\', '"':
				destination = append(destination, '\\', character)
			case '\b':
				destination = append(destination, '\\', 'b')
			case '\f':
				destination = append(destination, '\\', 'f')
			case '\n':
				destination = append(destination, '\\', 'n')
			case '\r':
				destination = append(destination, '\\', 'r')
			case '\t':
				destination = append(destination, '\\', 't')
			default:
				destination = append(destination, '\\', 'u', '0', '0', hex[character>>4], hex[character&0xf])
			}
			index++
			start = index
			continue
		}
		character, size := utf8.DecodeRuneInString(value[index:])
		if character == utf8.RuneError && size == 1 {
			destination = append(destination, value[start:index]...)
			destination = append(destination, "\ufffd"...)
			index += size
			start = index
			continue
		}
		if character == '\u2028' || character == '\u2029' {
			destination = append(destination, value[start:index]...)
			destination = append(destination, '\\', 'u', '2', '0', '2', hex[character&0xf])
			index += size
			start = index
			continue
		}
		index += size
	}
	destination = append(destination, value[start:]...)
	return append(destination, '"')
}

// hranaJSONAppendFloat mirrors encoding/json's float64 formatting: decimal
// notation between 1e-6 and 1e21, exponent notation outside it with the
// exponent's leading zero removed.
func hranaJSONAppendFloat(destination []byte, value float64) []byte {
	format := byte('f')
	absolute := math.Abs(value)
	if absolute != 0 && (absolute < 1e-6 || absolute >= 1e21) {
		format = 'e'
	}
	destination = strconv.AppendFloat(destination, value, format, -1, 64)
	if format == 'e' {
		length := len(destination)
		if length >= 4 && destination[length-4] == 'e' && destination[length-3] == '-' && destination[length-2] == '0' {
			destination[length-2] = destination[length-1]
			destination = destination[:length-1]
		}
	}
	return destination
}
