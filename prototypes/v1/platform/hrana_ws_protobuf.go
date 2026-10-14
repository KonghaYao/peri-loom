package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"

	"github.com/gorilla/websocket"
)

func hranaWSRead(connection *websocket.Conn, protobuf bool, message *hranaWSMessage) error {
	if !protobuf {
		frame, data, err := connection.ReadMessage()
		if err != nil {
			return err
		}
		if frame != websocket.TextMessage {
			return errors.New("JSON WebSocket requires text frames")
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(message); err != nil {
			return err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return errors.New("trailing WebSocket JSON")
		}
		return nil
	}
	frame, data, err := connection.ReadMessage()
	if err != nil {
		return err
	}
	if frame != websocket.BinaryMessage {
		return errors.New("protobuf WebSocket requires binary frames")
	}
	*message, err = hranaWSDecodeProto(data)
	return err
}

func hranaWSWrite(connection *websocket.Conn, protobuf bool, response any) error {
	if !protobuf {
		return connection.WriteJSON(response)
	}
	message, ok := response.(map[string]any)
	if !ok {
		return errors.New("invalid WebSocket response")
	}
	encoded, err := hranaWSEncodeProto(message)
	if err != nil {
		return err
	}
	return connection.WriteMessage(websocket.BinaryMessage, encoded)
}

func hranaWSDecodeProto(data []byte) (hranaWSMessage, error) {
	var result hranaWSMessage
	fields, err := hranaProtoRead(data)
	if err != nil || len(fields) != 1 || fields[0].wire != 2 {
		return result, errors.New("invalid WebSocket protobuf message")
	}
	if fields[0].number == 1 {
		inner, err := hranaProtoRead(fields[0].bytes)
		if err != nil || len(inner) != 1 || inner[0].number != 1 {
			return result, errors.New("invalid hello")
		}
		result.JWT, err = hranaProtoText(inner[0])
		result.Type = "hello"
		return result, err
	}
	if fields[0].number != 2 {
		return result, errors.New("unexpected WebSocket protobuf message")
	}
	inner, err := hranaProtoRead(fields[0].bytes)
	if err != nil || len(inner) != 2 || inner[0].number != 1 || inner[0].wire != 0 || inner[0].integer > math.MaxInt32 || inner[1].wire != 2 {
		return result, errors.New("invalid WebSocket protobuf request")
	}
	result.Type = "request"
	result.RequestID = int(inner[0].integer)
	request := &result.Request
	request.Type = map[int]string{2: "open_stream", 3: "close_stream", 4: "execute", 5: "batch", 6: "open_cursor", 7: "close_cursor", 8: "fetch_cursor", 9: "sequence", 10: "describe", 11: "store_sql", 12: "close_sql", 13: "get_autocommit"}[inner[1].number]
	if request.Type == "" {
		return result, errors.New("unsupported WebSocket protobuf request")
	}
	items, err := hranaProtoRead(inner[1].bytes)
	if err != nil {
		return result, err
	}
	streamID := request.Type != "store_sql" && request.Type != "close_sql" && request.Type != "close_cursor" && request.Type != "fetch_cursor"
	if streamID {
		if len(items) == 0 || items[0].number != 1 || items[0].wire != 0 || items[0].integer > math.MaxInt32 {
			return result, errors.New("invalid WebSocket stream ID")
		}
		request.StreamID = int(items[0].integer)
		items = items[1:]
	}
	switch request.Type {
	case "execute", "batch":
		if len(items) != 1 || items[0].number != 2 || items[0].wire != 2 {
			return result, errors.New("invalid WebSocket SQL request")
		}
		var value map[string]any
		if request.Type == "execute" {
			value, err = hranaProtoStatement(items[0].bytes)
		} else {
			value, err = hranaProtoBatch(items[0].bytes)
		}
		if err != nil {
			return result, err
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return result, err
		}
		if request.Type == "execute" {
			err = json.Unmarshal(encoded, &request.Stmt)
		} else {
			request.Batch = encoded
		}
	case "open_cursor":
		if len(items) != 2 || items[0].number != 2 || items[0].wire != 0 || items[0].integer > math.MaxInt32 || items[1].number != 3 || items[1].wire != 2 {
			return result, errors.New("invalid cursor open")
		}
		request.CursorID = int(items[0].integer)
		batch, err := hranaProtoBatch(items[1].bytes)
		if err != nil {
			return result, err
		}
		request.Batch, err = json.Marshal(batch)
	case "close_cursor", "fetch_cursor":
		if len(items) == 0 || items[0].number != 1 || items[0].wire != 0 || items[0].integer > math.MaxInt32 {
			return result, errors.New("invalid cursor ID")
		}
		request.CursorID = int(items[0].integer)
		if request.Type == "fetch_cursor" {
			if len(items) != 2 || items[1].number != 2 || items[1].wire != 0 || items[1].integer > math.MaxUint32 {
				return result, errors.New("invalid cursor fetch limit")
			}
			request.MaxCount = int(items[1].integer)
		} else if len(items) != 1 {
			return result, errors.New("invalid cursor close")
		}
	case "sequence", "describe":
		if len(items) != 1 {
			return result, errors.New("SQL text or ID required")
		}
		if items[0].number == 2 {
			request.SQL, err = hranaProtoText(items[0])
		} else if items[0].number == 3 && items[0].wire == 0 && items[0].integer <= math.MaxInt32 {
			request.SQLID = int(items[0].integer)
		} else {
			return result, errors.New("invalid SQL reference")
		}
	case "store_sql":
		if len(items) != 2 || items[0].number != 1 || items[0].wire != 0 || items[0].integer > math.MaxInt32 || items[1].number != 2 {
			return result, errors.New("invalid stored SQL")
		}
		request.SQLID = int(items[0].integer)
		request.SQL, err = hranaProtoText(items[1])
	case "close_sql":
		if len(items) != 1 || items[0].number != 1 || items[0].wire != 0 || items[0].integer > math.MaxInt32 {
			return result, errors.New("invalid SQL ID")
		}
		request.SQLID = int(items[0].integer)
	default:
		if len(items) != 0 {
			return result, errors.New("unexpected WebSocket request field")
		}
	}
	return result, err
}

func hranaWSEncodeProto(message map[string]any) ([]byte, error) {
	switch message["type"] {
	case "hello_ok":
		return hranaProtoBytes(1, nil), nil
	case "hello_error":
		failure, err := hranaProtoError(message["error"])
		return hranaProtoBytes(2, hranaProtoBytes(1, failure)), err
	}
	identifier, ok := message["request_id"].(int)
	if !ok || identifier < 0 || identifier > math.MaxInt32 {
		return nil, errors.New("invalid request ID")
	}
	encoded := hranaProtoNumber(1, uint64(identifier))
	if message["type"] == "response_error" {
		failure, err := hranaProtoError(message["error"])
		if err != nil {
			return nil, err
		}
		return hranaProtoBytes(4, append(encoded, hranaProtoBytes(2, failure)...)), nil
	}
	if message["type"] != "response_ok" {
		return nil, errors.New("invalid WebSocket response")
	}
	response, ok := message["response"].(map[string]any)
	if !ok {
		return nil, errors.New("missing WebSocket response")
	}
	var field int
	var payload []byte
	var err error
	switch response["type"] {
	case "open_stream":
		field = 2
	case "close_stream":
		field = 3
	case "execute":
		field = 4
		payload, err = hranaProtoStatementResult(response["result"])
	case "batch":
		field = 5
		payload, err = hranaProtoBatchResult(response["result"])
	case "open_cursor":
		field = 6
	case "close_cursor":
		field = 7
	case "fetch_cursor":
		field = 8
		payload, err = hranaWSProtoFetch(response)
	case "sequence":
		field = 9
	case "describe":
		field = 10
		payload, err = hranaProtoDescribeResult(response["result"])
	case "store_sql":
		field = 11
	case "close_sql":
		field = 12
	case "get_autocommit":
		field = 13
		state, valid := response["is_autocommit"].(bool)
		if !valid {
			return nil, errors.New("invalid autocommit state")
		}
		if state {
			payload = hranaProtoNumber(1, 1)
		} else {
			payload = hranaProtoNumber(1, 0)
		}
	default:
		return nil, errors.New("unsupported WebSocket response")
	}
	if err != nil {
		return nil, err
	}
	if field == 4 || field == 5 || field == 10 {
		payload = hranaProtoBytes(1, payload)
	}
	return hranaProtoBytes(3, append(encoded, hranaProtoBytes(field, payload)...)), nil
}
