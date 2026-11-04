package platform

import (
	"encoding/binary"
	"encoding/json"
	"strings"
)

// fusedReplyScanner walks JSON with the acceptance rules of encoding/json's
// decoder, but without materializing values. A fused open reply carries the
// statement result exactly once: parsing it used to decode the envelope and
// then decode the multi-megabyte result again, copying both times. The result
// only needs to be proven well-formed and structurally valid, and the statement
// path only needs its bytes, so the scan hands back a subslice of the reply.
type fusedReplyScanner struct {
	data []byte
	pos  int
}

// fusedMaxNestingDepth mirrors encoding/json's scanner limit. A reply nested
// deeper is refused there, so it must be refused here too.
const fusedMaxNestingDepth = 10000

func (scanner *fusedReplyScanner) skipSpace() {
	for scanner.pos < len(scanner.data) {
		switch scanner.data[scanner.pos] {
		case ' ', '\t', '\n', '\r':
			scanner.pos++
		default:
			return
		}
	}
}

// scanString validates one JSON string and returns the offsets of its content
// between the quotes. Values are only validated here; keys are unquoted by the
// caller because encoding/json matches them after decoding.
//
// The inner loop inspects eight bytes at a time: a statement result is mostly
// string payload, and a byte-at-a-time switch there costs more than everything
// else in the parse. The SWAR predicates only need to answer "does this word
// contain a quote, a backslash or a control byte"; a false positive merely
// falls back to the byte loop for that position.
func (scanner *fusedReplyScanner) scanString() (start, end int, ok bool) {
	data := scanner.data
	pos := scanner.pos
	if pos >= len(data) || data[pos] != '"' {
		return 0, 0, false
	}
	pos++
	start = pos
	for {
		for pos+8 <= len(data) {
			if fusedStringWordNeedsScan(binary.LittleEndian.Uint64(data[pos:])) {
				break
			}
			pos += 8
		}
		if pos >= len(data) {
			return 0, 0, false
		}
		switch c := data[pos]; {
		case c == '"':
			scanner.pos = pos + 1
			return start, pos, true
		case c == '\\':
			pos++
			if pos >= len(data) {
				return 0, 0, false
			}
			switch data[pos] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				pos++
			case 'u':
				// encoding/json requires exactly four hex digits; an isolated
				// surrogate is valid here and is replaced when the value is
				// decoded, which never happens for the rows payload.
				if pos+4 >= len(data) {
					return 0, 0, false
				}
				for i := 1; i <= 4; i++ {
					if !fusedHexDigit(data[pos+i]) {
						return 0, 0, false
					}
				}
				pos += 5
			default:
				return 0, 0, false
			}
		case c < 0x20:
			return 0, 0, false
		default:
			pos++
		}
	}
}

const (
	fusedSWAROnes = 0x0101010101010101
	fusedSWARHigh = 0x8080808080808080
)

func fusedSWARHasZeroByte(word uint64) bool {
	return (word-fusedSWAROnes)&^word&fusedSWARHigh != 0
}

func fusedSWARHasByte(word uint64, b byte) bool {
	return fusedSWARHasZeroByte(word ^ (fusedSWAROnes * uint64(b)))
}

// fusedSWARHasLess is the classic "any byte below n" predicate. Bytes with the
// high bit set never match, which is what JSON needs: they are ordinary UTF-8
// continuation or lead bytes inside a string.
func fusedSWARHasLess(word uint64, n byte) bool {
	return (word-fusedSWAROnes*uint64(n))&^word&fusedSWARHigh != 0
}

func fusedStringWordNeedsScan(word uint64) bool {
	return fusedSWARHasByte(word, '"') || fusedSWARHasByte(word, '\\') || fusedSWARHasLess(word, 0x20)
}

func fusedHexDigit(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}

func (scanner *fusedReplyScanner) scanLiteral(literal string) bool {
	if len(scanner.data)-scanner.pos < len(literal) {
		return false
	}
	for i := 0; i < len(literal); i++ {
		if scanner.data[scanner.pos+i] != literal[i] {
			return false
		}
	}
	scanner.pos += len(literal)
	return true
}

// scanNumber accepts exactly the JSON number grammar. Callers reject a value
// that keeps non-delimiter bytes after the number, so a prefix such as "01" is
// refused by the surrounding structure, as it is by the decoder's scanner.
func (scanner *fusedReplyScanner) scanNumber() bool {
	data := scanner.data
	pos := scanner.pos
	if pos >= len(data) {
		return false
	}
	if data[pos] == '-' {
		pos++
		if pos >= len(data) {
			return false
		}
	}
	switch {
	case data[pos] == '0':
		pos++
	case '1' <= data[pos] && data[pos] <= '9':
		pos++
		for pos < len(data) && '0' <= data[pos] && data[pos] <= '9' {
			pos++
		}
	default:
		return false
	}
	if pos < len(data) && data[pos] == '.' {
		pos++
		if pos >= len(data) || data[pos] < '0' || data[pos] > '9' {
			return false
		}
		for pos < len(data) && '0' <= data[pos] && data[pos] <= '9' {
			pos++
		}
	}
	if pos < len(data) && (data[pos] == 'e' || data[pos] == 'E') {
		pos++
		if pos < len(data) && (data[pos] == '+' || data[pos] == '-') {
			pos++
		}
		if pos >= len(data) || data[pos] < '0' || data[pos] > '9' {
			return false
		}
		for pos < len(data) && '0' <= data[pos] && data[pos] <= '9' {
			pos++
		}
	}
	scanner.pos = pos
	return true
}

func (scanner *fusedReplyScanner) scanValue() bool {
	return scanner.scanValueOpen(0)
}

// scanValueOpen validates one complete value with open containers still on the
// stack. The limit is per value, not per reply, because each scan starts from
// either the reply or an already validated statement result.
func (scanner *fusedReplyScanner) scanValueOpen(open int) bool {
	scanner.skipSpace()
	if scanner.pos >= len(scanner.data) {
		return false
	}
	switch c := scanner.data[scanner.pos]; c {
	case '{':
		return scanner.scanObject(open + 1)
	case '[':
		return scanner.scanArray(open + 1)
	case '"':
		_, _, ok := scanner.scanString()
		return ok
	case 't':
		return scanner.scanLiteral("true")
	case 'f':
		return scanner.scanLiteral("false")
	case 'n':
		return scanner.scanLiteral("null")
	case '-':
		return scanner.scanNumber()
	default:
		if '0' <= c && c <= '9' {
			return scanner.scanNumber()
		}
		return false
	}
}

func (scanner *fusedReplyScanner) scanObject(open int) bool {
	if open > fusedMaxNestingDepth {
		return false
	}
	scanner.pos++ // '{'
	scanner.skipSpace()
	if scanner.pos < len(scanner.data) && scanner.data[scanner.pos] == '}' {
		scanner.pos++
		return true
	}
	for {
		scanner.skipSpace()
		if scanner.pos >= len(scanner.data) || scanner.data[scanner.pos] != '"' {
			return false
		}
		if _, _, ok := scanner.scanString(); !ok {
			return false
		}
		scanner.skipSpace()
		if scanner.pos >= len(scanner.data) || scanner.data[scanner.pos] != ':' {
			return false
		}
		scanner.pos++
		if !scanner.scanValueOpen(open) {
			return false
		}
		scanner.skipSpace()
		if scanner.pos >= len(scanner.data) {
			return false
		}
		switch scanner.data[scanner.pos] {
		case ',':
			scanner.pos++
		case '}':
			scanner.pos++
			return true
		default:
			return false
		}
	}
}

func (scanner *fusedReplyScanner) scanArray(open int) bool {
	if open > fusedMaxNestingDepth {
		return false
	}
	scanner.pos++ // '['
	scanner.skipSpace()
	if scanner.pos < len(scanner.data) && scanner.data[scanner.pos] == ']' {
		scanner.pos++
		return true
	}
	for {
		if !scanner.scanValueOpen(open) {
			return false
		}
		scanner.skipSpace()
		if scanner.pos >= len(scanner.data) {
			return false
		}
		switch scanner.data[scanner.pos] {
		case ',':
			scanner.pos++
		case ']':
			scanner.pos++
			return true
		default:
			return false
		}
	}
}

// plainJSONContent reports whether a scanned string needs no unquoting: every
// byte is ASCII and none starts an escape, so the content is the decoded value.
func plainJSONContent(content []byte) bool {
	for _, c := range content {
		if c == '\\' || c >= 0x80 {
			return false
		}
	}
	return true
}

func fusedKeyEqual(key []byte, name string) bool {
	if len(key) != len(name) {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] != name[i] {
			return false
		}
	}
	return true
}

// fusedKeyEqualFold is the ASCII case folding encoding/json applies before its
// Unicode fallback. A plain key is ASCII, so equal fold semantics reduce to it.
func fusedKeyEqualFold(key []byte, name string) bool {
	if len(key) != len(name) {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		d := name[i]
		if 'a' <= d && d <= 'z' {
			d -= 'a' - 'A'
		}
		if c != d {
			return false
		}
	}
	return true
}

func fusedDecodeJSONString(quoted []byte) (string, bool) {
	var key string
	if json.Unmarshal(quoted, &key) != nil {
		return "", false
	}
	return key, true
}

const (
	fusedReplyFieldStatus = iota
	fusedReplyFieldReason
	fusedReplyFieldResult
	fusedReplyFieldError
	fusedReplyFieldExecutionCompleted
)

// matchFusedReplyField mirrors the struct field lookup of encoding/json: an
// exact name wins, then a case-insensitive match using Unicode simple folding.
func matchFusedReplyField(content, quoted []byte) int {
	if plainJSONContent(content) {
		switch {
		case fusedKeyEqualFold(content, "status"):
			return fusedReplyFieldStatus
		case fusedKeyEqualFold(content, "reason"):
			return fusedReplyFieldReason
		case fusedKeyEqualFold(content, "result"):
			return fusedReplyFieldResult
		case fusedKeyEqualFold(content, "error"):
			return fusedReplyFieldError
		case fusedKeyEqualFold(content, "execution_completed"):
			return fusedReplyFieldExecutionCompleted
		}
		return -1
	}
	key, ok := fusedDecodeJSONString(quoted)
	if !ok {
		return -1
	}
	switch {
	case strings.EqualFold(key, "status"):
		return fusedReplyFieldStatus
	case strings.EqualFold(key, "reason"):
		return fusedReplyFieldReason
	case strings.EqualFold(key, "result"):
		return fusedReplyFieldResult
	case strings.EqualFold(key, "error"):
		return fusedReplyFieldError
	case strings.EqualFold(key, "execution_completed"):
		return fusedReplyFieldExecutionCompleted
	}
	return -1
}

const (
	fusedReplyStatusUnknown = iota
	fusedReplyStatusOK
	fusedReplyStatusRequiresIntent
	fusedReplyStatusError
)

func classifyFusedReplyStatus(content, quoted []byte) (int, bool) {
	if plainJSONContent(content) {
		switch {
		case fusedKeyEqual(content, "ok"):
			return fusedReplyStatusOK, true
		case fusedKeyEqual(content, "requires_intent"):
			return fusedReplyStatusRequiresIntent, true
		case fusedKeyEqual(content, "error"):
			return fusedReplyStatusError, true
		}
		return fusedReplyStatusUnknown, true
	}
	status, ok := fusedDecodeJSONString(quoted)
	if !ok {
		return fusedReplyStatusUnknown, false
	}
	switch status {
	case "ok":
		return fusedReplyStatusOK, true
	case "requires_intent":
		return fusedReplyStatusRequiresIntent, true
	case "error":
		return fusedReplyStatusError, true
	}
	return fusedReplyStatusUnknown, true
}

// classifyFusedReplyReason decodes a reason string. An unknown value is kept
// as-is so the branch check refuses it exactly like the struct decode did; a
// plain key that matches no constant is represented by the zero value, which
// is not a valid reason either.
func classifyFusedReplyReason(content, quoted []byte) (guardedIntentReason, bool) {
	if plainJSONContent(content) {
		switch {
		case fusedKeyEqual(content, string(guardedEngineWritable)):
			return guardedEngineWritable, true
		case fusedKeyEqual(content, string(guardedTransactionContext)):
			return guardedTransactionContext, true
		case fusedKeyEqual(content, string(guardedConnectionControl)):
			return guardedConnectionControl, true
		}
		return "", true
	}
	reason, ok := fusedDecodeJSONString(quoted)
	if !ok {
		return "", false
	}
	return guardedIntentReason(reason), true
}

// scanFusedStatementReply is parseFusedStatementAnswer without the envelope
// re-encoding: one pass proves the envelope shape and the statement result,
// and result is a subslice of reply rather than a copy. The reply must be
// treated as immutable for as long as that answer is used.
func scanFusedStatementReply(reply []byte) (fusedStatementAnswer, bool) {
	scanner := fusedReplyScanner{data: reply}
	scanner.skipSpace()
	if scanner.pos >= len(reply) || reply[scanner.pos] != '{' {
		return fusedStatementAnswer{}, false
	}
	scanner.pos++

	var (
		statusKind       int
		reasonValue      guardedIntentReason
		reasonPresent    bool
		result           json.RawMessage
		errorMessage     string
		errorPresent     bool
		completed        bool
		completedPresent bool
	)
	scanner.skipSpace()
scanEnvelope:
	for {
		if scanner.pos >= len(reply) || reply[scanner.pos] != '"' {
			return fusedStatementAnswer{}, false
		}
		keyStart, keyEnd, ok := scanner.scanString()
		if !ok {
			return fusedStatementAnswer{}, false
		}
		field := matchFusedReplyField(reply[keyStart:keyEnd], reply[keyStart-1:keyEnd+1])
		if field < 0 {
			return fusedStatementAnswer{}, false
		}
		scanner.skipSpace()
		if scanner.pos >= len(reply) || reply[scanner.pos] != ':' {
			return fusedStatementAnswer{}, false
		}
		scanner.pos++
		scanner.skipSpace()
		if scanner.pos >= len(reply) {
			return fusedStatementAnswer{}, false
		}
		valueStart := scanner.pos
		switch field {
		case fusedReplyFieldStatus:
			switch reply[valueStart] {
			case 'n':
				if !scanner.scanLiteral("null") {
					return fusedStatementAnswer{}, false
				}
				// A JSON null leaves a string field untouched, including a
				// value an earlier duplicate key already assigned.
			case '"':
				_, valueEnd, ok := scanner.scanString()
				if !ok {
					return fusedStatementAnswer{}, false
				}
				status, ok := classifyFusedReplyStatus(reply[valueStart+1:valueEnd], reply[valueStart:valueEnd+1])
				if !ok {
					return fusedStatementAnswer{}, false
				}
				statusKind = status
			default:
				return fusedStatementAnswer{}, false
			}
		case fusedReplyFieldReason:
			switch reply[valueStart] {
			case 'n':
				if !scanner.scanLiteral("null") {
					return fusedStatementAnswer{}, false
				}
				reasonPresent = false
				reasonValue = ""
			case '"':
				_, valueEnd, ok := scanner.scanString()
				if !ok {
					return fusedStatementAnswer{}, false
				}
				reason, ok := classifyFusedReplyReason(reply[valueStart+1:valueEnd], reply[valueStart:valueEnd+1])
				if !ok {
					return fusedStatementAnswer{}, false
				}
				reasonPresent = true
				reasonValue = reason
			default:
				return fusedStatementAnswer{}, false
			}
		case fusedReplyFieldResult:
			if !scanner.scanValueOpen(1) {
				return fusedStatementAnswer{}, false
			}
			result = json.RawMessage(reply[valueStart:scanner.pos])
		case fusedReplyFieldError:
			switch reply[valueStart] {
			case 'n':
				if !scanner.scanLiteral("null") {
					return fusedStatementAnswer{}, false
				}
				errorPresent = false
				errorMessage = ""
			case '"':
				_, valueEnd, ok := scanner.scanString()
				if !ok {
					return fusedStatementAnswer{}, false
				}
				message, ok := fusedDecodeJSONString(reply[valueStart : valueEnd+1])
				if !ok {
					return fusedStatementAnswer{}, false
				}
				errorPresent = true
				errorMessage = message
			default:
				return fusedStatementAnswer{}, false
			}
		case fusedReplyFieldExecutionCompleted:
			switch reply[valueStart] {
			case 'n':
				if !scanner.scanLiteral("null") {
					return fusedStatementAnswer{}, false
				}
				completedPresent = false
				completed = false
			case 't':
				if !scanner.scanLiteral("true") {
					return fusedStatementAnswer{}, false
				}
				completedPresent = true
				completed = true
			case 'f':
				if !scanner.scanLiteral("false") {
					return fusedStatementAnswer{}, false
				}
				completedPresent = true
				completed = false
			default:
				return fusedStatementAnswer{}, false
			}
		}
		scanner.skipSpace()
		if scanner.pos >= len(reply) {
			return fusedStatementAnswer{}, false
		}
		switch reply[scanner.pos] {
		case ',':
			scanner.pos++
			scanner.skipSpace()
		case '}':
			scanner.pos++
			break scanEnvelope
		default:
			return fusedStatementAnswer{}, false
		}
	}
	scanner.skipSpace()
	if scanner.pos != len(reply) {
		return fusedStatementAnswer{}, false
	}

	switch statusKind {
	case fusedReplyStatusOK:
		if reasonPresent || errorPresent || completedPresent || !validScannedStatementResult(result) {
			return fusedStatementAnswer{}, false
		}
		return fusedStatementAnswer{kind: fusedStatementOK, result: result}, true
	case fusedReplyStatusRequiresIntent:
		if !reasonPresent || result != nil || errorPresent || completedPresent {
			return fusedStatementAnswer{}, false
		}
		switch reasonValue {
		case guardedEngineWritable, guardedTransactionContext, guardedConnectionControl:
			return fusedStatementAnswer{kind: fusedStatementRequiresIntent, reason: reasonValue}, true
		default:
			return fusedStatementAnswer{}, false
		}
	case fusedReplyStatusError:
		if !errorPresent || errorMessage == "" || result != nil || reasonPresent {
			return fusedStatementAnswer{}, false
		}
		answer := fusedStatementAnswer{kind: fusedStatementError, message: errorMessage}
		if completedPresent {
			value := completed
			answer.executionCompleted = &value
		}
		return answer, true
	default:
		return fusedStatementAnswer{}, false
	}
}
