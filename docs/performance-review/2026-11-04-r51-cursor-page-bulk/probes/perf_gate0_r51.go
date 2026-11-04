package platform

// R51 Gate 0: proves the bulk scan/transcode helpers run on live cursor pages
// and produce exactly what the legacy per-byte loops produce. This file exists
// only in the gate worktree and is not part of the measured binary.

import (
	"bytes"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

var (
	r51GateOnce           sync.Once
	r51GateLogPath        string
	r51GateActive         bool
	r51GateMu             sync.Mutex
	r51GateStringCalls    atomic.Int64
	r51GateStringBytes    atomic.Int64
	r51GateStringMismatch atomic.Int64
	r51GateTextCalls      atomic.Int64
	r51GateTextBytes      atomic.Int64
	r51GateTextMismatch   atomic.Int64
	r51GateJSONCalls      atomic.Int64
	r51GateJSONBytes      atomic.Int64
	r51GateJSONMismatch   atomic.Int64
)

func r51GateLoad() {
	r51GateLogPath = os.Getenv("PERI_R51_GATE0_LOG")
	if r51GateLogPath == "" {
		return
	}
	r51GateActive = true
}

func r51GateEnabled() bool {
	r51GateOnce.Do(r51GateLoad)
	return r51GateActive
}

func r51GateWrite(detail string) {
	r51GateMu.Lock()
	defer r51GateMu.Unlock()
	file, err := os.OpenFile(r51GateLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		log.Printf("r51 gate log: %v", err)
		return
	}
	defer file.Close()
	line := "r51gate string_calls=" + strconv.FormatInt(r51GateStringCalls.Load(), 10) +
		" string_bytes=" + strconv.FormatInt(r51GateStringBytes.Load(), 10) +
		" string_mismatch=" + strconv.FormatInt(r51GateStringMismatch.Load(), 10) +
		" text_calls=" + strconv.FormatInt(r51GateTextCalls.Load(), 10) +
		" text_bytes=" + strconv.FormatInt(r51GateTextBytes.Load(), 10) +
		" text_mismatch=" + strconv.FormatInt(r51GateTextMismatch.Load(), 10) +
		" json_calls=" + strconv.FormatInt(r51GateJSONCalls.Load(), 10) +
		" json_bytes=" + strconv.FormatInt(r51GateJSONBytes.Load(), 10) +
		" json_mismatch=" + strconv.FormatInt(r51GateJSONMismatch.Load(), 10) +
		" detail=" + detail + "\n"
	_, _ = file.WriteString(line)
}

func (scanner *hranaCursorPageScanner) string(position int) (int, error) {
	end, err := scanner.stringSpan(position)
	if !r51GateEnabled() {
		return end, err
	}
	legacyEnd, legacyErr := hranaCursorPageStringLegacy(scanner.body, position)
	r51GateStringCalls.Add(1)
	if end > position {
		r51GateStringBytes.Add(int64(end - position))
	}
	detail := "string"
	switch {
	case (err == nil) != (legacyErr == nil):
		r51GateStringMismatch.Add(1)
		detail = "string-error-mismatch span=" + strconv.FormatInt(int64(end), 10) +
			" legacy=" + strconv.FormatInt(int64(legacyEnd), 10)
		log.Printf("r51 gate %s", detail)
	case err == nil && end != legacyEnd:
		r51GateStringMismatch.Add(1)
		detail = "string-position-mismatch span=" + strconv.FormatInt(int64(end), 10) +
			" legacy=" + strconv.FormatInt(int64(legacyEnd), 10)
		log.Printf("r51 gate %s", detail)
	}
	r51GateWrite(detail)
	return end, err
}

func hranaCursorPageAppendText(destination, token []byte) []byte {
	span := hranaCursorPageAppendTextSpan(destination, token)
	if !r51GateEnabled() {
		return span
	}
	legacy := hranaCursorPageAppendTextLegacy(nil, token)
	r51GateTextCalls.Add(1)
	r51GateTextBytes.Add(int64(len(legacy)))
	detail := "text"
	if !bytes.Equal(span[len(destination):], legacy) {
		r51GateTextMismatch.Add(1)
		detail = "text-mismatch span=" + strconv.Itoa(len(span)-len(destination)) + " legacy=" + strconv.Itoa(len(legacy))
		log.Printf("r51 gate %s", detail)
	}
	r51GateWrite(detail)
	return span
}

func hranaJSONAppendPageText(destination, token []byte) []byte {
	span := hranaJSONAppendPageTextSpan(destination, token)
	if !r51GateEnabled() {
		return span
	}
	legacy := hranaJSONAppendPageTextLegacy(nil, token)
	r51GateJSONCalls.Add(1)
	r51GateJSONBytes.Add(int64(len(legacy)))
	detail := "json"
	if !bytes.Equal(span[len(destination):], legacy) {
		r51GateJSONMismatch.Add(1)
		detail = "json-mismatch span=" + strconv.Itoa(len(span)-len(destination)) + " legacy=" + strconv.Itoa(len(legacy))
		log.Printf("r51 gate %s", detail)
	}
	r51GateWrite(detail)
	return span
}

// hranaCursorPageStringLegacy is the pre-R51 per-byte scanner.string.
func hranaCursorPageStringLegacy(body []byte, position int) (int, error) {
	position++
	for position < len(body) {
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

// hranaCursorPageAppendTextLegacy is the pre-R51 per-byte decoded-text writer.
func hranaCursorPageAppendTextLegacy(destination, token []byte) []byte {
	for position := 1; position < len(token)-1; {
		character := token[position]
		if character != '\\' {
			destination = append(destination, character)
			position++
			continue
		}
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

// hranaJSONAppendPageTextLegacy is the pre-R51 per-byte JSON text writer.
func hranaJSONAppendPageTextLegacy(destination, token []byte) []byte {
	const hex = "0123456789abcdef"
	destination = append(destination, '"')
	for position := 1; position < len(token)-1; {
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
