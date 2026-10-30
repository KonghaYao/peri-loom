package platform

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
)

// hranaProtoCursorRowFrame builds one Hrana v3 row frame:
//
//	varint payload length
//	field 4 (Row) { repeated field 1 (Value) ResultValue }
//
// The frame and row length prefixes are reserved in front of the cell bytes
// and patched once the row is complete, so the row is written into one buffer
// without the per-cell map[string]any, the nested ResultValue slice and the
// copies that writeFrame used to make. A frame stays valid until the next
// begin call on the same encoder; the encoder is request-local.
type hranaProtoCursorRowFrame struct {
	buffer  []byte
	scratch []byte
}

const (
	hranaProtoCursorFramePrefix = binary.MaxVarintLen64
	hranaProtoCursorRowPrefix   = 1 + binary.MaxVarintLen64
	hranaProtoCursorRowHeader   = hranaProtoCursorFramePrefix + hranaProtoCursorRowPrefix
)

func (frame *hranaProtoCursorRowFrame) begin() {
	if cap(frame.buffer) < hranaProtoCursorRowHeader {
		frame.buffer = make([]byte, hranaProtoCursorRowHeader, 512)
	}
	frame.buffer = frame.buffer[:hranaProtoCursorRowHeader]
}

func (frame *hranaProtoCursorRowFrame) appendCell(cell any) error {
	switch value := cell.(type) {
	case nil:
		// field 1 Value { field 1 null }
		frame.buffer = append(frame.buffer, 0x0a, 0x02, 0x0a, 0x00)
		return nil
	case json.Number:
		if strings.ContainsAny(string(value), ".eE") {
			converted, err := value.Float64()
			if err != nil {
				return err
			}
			return frame.appendFloat(converted)
		}
		integer, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return err
		}
		frame.appendInteger(integer)
		return nil
	case string:
		frame.appendText(value)
		return nil
	case []any:
		blob, err := hranaCursorBlobBytes(value, frame.scratch[:0])
		if err != nil {
			return err
		}
		frame.scratch = blob
		frame.appendBlob(blob)
		return nil
	default:
		return errors.New("invalid cursor value")
	}
}

func (frame *hranaProtoCursorRowFrame) appendInteger(integer int64) {
	zigzag := uint64(integer<<1) ^ uint64(integer>>63)
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], zigzag)
	frame.buffer = append(frame.buffer, 0x0a)
	frame.buffer = binary.AppendUvarint(frame.buffer, uint64(1+size))
	frame.buffer = append(frame.buffer, 0x10)
	frame.buffer = append(frame.buffer, encoded[:size]...)
}

func (frame *hranaProtoCursorRowFrame) appendFloat(value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return errors.New("invalid cursor float")
	}
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
	frame.buffer = append(frame.buffer, 0x0a, 0x09, 0x19)
	frame.buffer = append(frame.buffer, encoded[:]...)
	return nil
}

func (frame *hranaProtoCursorRowFrame) appendText(text string) {
	length := len(text)
	inner := 1 + hranaProtoUvarintSize(uint64(length)) + length
	frame.buffer = append(frame.buffer, 0x0a)
	frame.buffer = binary.AppendUvarint(frame.buffer, uint64(inner))
	frame.buffer = append(frame.buffer, 0x22)
	frame.buffer = binary.AppendUvarint(frame.buffer, uint64(length))
	frame.buffer = append(frame.buffer, text...)
}

func (frame *hranaProtoCursorRowFrame) appendBlob(blob []byte) {
	length := len(blob)
	inner := 1 + hranaProtoUvarintSize(uint64(length)) + length
	frame.buffer = append(frame.buffer, 0x0a)
	frame.buffer = binary.AppendUvarint(frame.buffer, uint64(inner))
	frame.buffer = append(frame.buffer, 0x2a)
	frame.buffer = binary.AppendUvarint(frame.buffer, uint64(length))
	frame.buffer = append(frame.buffer, blob...)
}

// frame patches the reserved length prefixes and returns the complete framed
// row, including the transport varint prefix, so the caller can write it
// without another copy. The returned slice aliases the encoder buffer.
func (frame *hranaProtoCursorRowFrame) frame() []byte {
	var encoded [binary.MaxVarintLen64]byte
	rowLength := len(frame.buffer) - hranaProtoCursorRowHeader
	size := binary.PutUvarint(encoded[:], uint64(rowLength))
	position := hranaProtoCursorRowHeader - 1 - size
	frame.buffer[position] = 0x22
	copy(frame.buffer[position+1:hranaProtoCursorRowHeader], encoded[:size])
	payloadStart := position

	payloadLength := len(frame.buffer) - payloadStart
	size = binary.PutUvarint(encoded[:], uint64(payloadLength))
	position = payloadStart - size
	copy(frame.buffer[position:payloadStart], encoded[:size])
	return frame.buffer[position:]
}

func hranaProtoUvarintSize(value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutUvarint(encoded[:], value)
}

// hranaCursorBlobBytes decodes the worker's byte-array cell into destination,
// validating the same 0..255 range the cursor encoder always enforced.
func hranaCursorBlobBytes(cell []any, destination []byte) ([]byte, error) {
	for _, element := range cell {
		number, ok := element.(json.Number)
		if !ok {
			return nil, errors.New("invalid cursor blob")
		}
		integer, err := number.Int64()
		if err != nil || integer < 0 || integer > 255 {
			return nil, errors.New("invalid cursor blob")
		}
		destination = append(destination, byte(integer))
	}
	return destination, nil
}
