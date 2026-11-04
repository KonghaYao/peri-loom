package platform

import (
	"bytes"
	"encoding/binary"
)

// A cursor page carries tens of megabytes of text, so the per-byte loops in
// hrana_cursor_page.go dominate page validation and row encoding. These
// helpers only locate the next byte that needs individual handling; the caller
// then runs the same per-byte logic on it. They may report a match a little
// early, which costs a rescan and never changes the outcome, so the byte loops
// stay the reference for what counts as a delimiter.

const (
	hranaCursorPageOnes = 0x0101010101010101
	hranaCursorPageHigh = 0x8080808080808080
)

// hranaCursorPageHasZeroByte reports whether any byte of word is zero.
func hranaCursorPageHasZeroByte(word uint64) uint64 {
	return (word - hranaCursorPageOnes) & ^word & hranaCursorPageHigh
}

// hranaCursorPageHasLessThan reports whether any byte of word is below limit.
func hranaCursorPageHasLessThan(word uint64, limit byte) uint64 {
	return (word - hranaCursorPageOnes*uint64(limit)) & ^word & hranaCursorPageHigh
}

// hranaCursorPageIndexSpecial returns the offset of the first byte that the
// page scanner's string() must inspect individually: a quote, a backslash, or a
// control byte below 0x20. It returns len(body) when the whole run is plain.
func hranaCursorPageIndexSpecial(body []byte) int {
	index := 0
	for ; index+8 <= len(body); index += 8 {
		word := binary.LittleEndian.Uint64(body[index:])
		if hranaCursorPageHasZeroByte(word^(hranaCursorPageOnes*'"')) != 0 ||
			hranaCursorPageHasZeroByte(word^(hranaCursorPageOnes*'\\')) != 0 ||
			hranaCursorPageHasLessThan(word, 0x20) != 0 {
			break
		}
	}
	for ; index < len(body); index++ {
		character := body[index]
		if character == '"' || character == '\\' || character < 0x20 {
			return index
		}
	}
	return len(body)
}

// hranaCursorPageIndexJSONSpecial returns the offset of the first byte that
// hranaJSONAppendPageText must inspect individually: a backslash, one of the
// three HTML characters, or the U+2028/U+2029 lead byte.
func hranaCursorPageIndexJSONSpecial(body []byte) int {
	index := 0
	for ; index+8 <= len(body); index += 8 {
		word := binary.LittleEndian.Uint64(body[index:])
		if hranaCursorPageHasZeroByte(word^(hranaCursorPageOnes*'\\')) != 0 ||
			hranaCursorPageHasZeroByte(word^(hranaCursorPageOnes*'<')) != 0 ||
			hranaCursorPageHasZeroByte(word^(hranaCursorPageOnes*'>')) != 0 ||
			hranaCursorPageHasZeroByte(word^(hranaCursorPageOnes*'&')) != 0 ||
			hranaCursorPageHasZeroByte(word^(hranaCursorPageOnes*0xe2)) != 0 {
			break
		}
	}
	for ; index < len(body); index++ {
		switch body[index] {
		case '\\', '<', '>', '&', 0xe2:
			return index
		}
	}
	return len(body)
}

// hranaCursorPageIndexBackslash returns the offset of the next backslash in
// token[position:len(token)-1], or -1 when the remaining text is plain.
func hranaCursorPageIndexBackslash(token []byte, position int) int {
	if position >= len(token)-1 {
		return -1
	}
	return bytes.IndexByte(token[position:len(token)-1], '\\')
}
