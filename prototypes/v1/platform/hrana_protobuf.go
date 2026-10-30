package platform

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"unicode/utf8"

	controller "peri-loom/controller"
)

type hranaProtoField struct {
	number  int
	wire    int
	bytes   []byte
	integer uint64
}

func hranaProtoRead(data []byte) ([]hranaProtoField, error) {
	fields := make([]hranaProtoField, 0, 8)
	for len(data) != 0 {
		if len(fields) >= 4096 {
			return nil, errors.New("protobuf field limit")
		}
		header, size := binary.Uvarint(data)
		if size <= 0 || header < 8 || header>>3 > 1024 {
			return nil, errors.New("invalid protobuf tag")
		}
		data = data[size:]
		field := hranaProtoField{number: int(header >> 3), wire: int(header & 7)}
		switch field.wire {
		case 0:
			field.integer, size = binary.Uvarint(data)
			if size <= 0 {
				return nil, errors.New("invalid protobuf varint")
			}
			data = data[size:]
		case 1:
			if len(data) < 8 {
				return nil, errors.New("short protobuf fixed64")
			}
			field.bytes, data = data[:8], data[8:]
		case 2:
			length, count := binary.Uvarint(data)
			if count <= 0 {
				return nil, errors.New("invalid protobuf length")
			}
			data = data[count:]
			if length > uint64(len(data)) {
				return nil, errors.New("short protobuf field")
			}
			field.bytes, data = data[:int(length)], data[int(length):]
		default:
			return nil, errors.New("unsupported protobuf wire type")
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func hranaProtoOne(fields []hranaProtoField, number, wire int) (hranaProtoField, bool, error) {
	var result hranaProtoField
	found := false
	for _, field := range fields {
		if field.number != number {
			continue
		}
		if found || field.wire != wire {
			return result, false, errors.New("duplicate or invalid protobuf field")
		}
		result, found = field, true
	}
	return result, found, nil
}

func hranaProtoText(field hranaProtoField) (string, error) {
	if field.wire != 2 || !utf8.Valid(field.bytes) {
		return "", errors.New("invalid protobuf text")
	}
	return string(field.bytes), nil
}

func hranaProtoValue(data []byte) (map[string]any, error) {
	fields, err := hranaProtoRead(data)
	if err != nil {
		return nil, err
	}
	if len(fields) != 1 {
		return nil, errors.New("protobuf value oneof required")
	}
	field := fields[0]
	switch field.number {
	case 1:
		if field.wire != 2 || len(field.bytes) != 0 {
			break
		}
		return map[string]any{"type": "null"}, nil
	case 2:
		if field.wire != 0 {
			break
		}
		integer := int64(field.integer>>1) ^ -int64(field.integer&1)
		return map[string]any{"type": "integer", "value": strconv.FormatInt(integer, 10)}, nil
	case 3:
		if field.wire != 1 {
			break
		}
		value := math.Float64frombits(binary.LittleEndian.Uint64(field.bytes))
		if math.IsNaN(value) || math.IsInf(value, 0) {
			break
		}
		return map[string]any{"type": "float", "value": value}, nil
	case 4:
		value, err := hranaProtoText(field)
		if err == nil {
			return map[string]any{"type": "text", "value": value}, nil
		}
	case 5:
		if field.wire == 2 {
			return map[string]any{"type": "blob", "base64": base64.StdEncoding.EncodeToString(field.bytes)}, nil
		}
	}
	return nil, errors.New("unsupported protobuf value")
}

func hranaProtoStatement(data []byte) (map[string]any, error) {
	fields, err := hranaProtoRead(data)
	if err != nil {
		return nil, err
	}
	statement := map[string]any{"args": []any{}, "named_args": []any{}}
	var sqlSet, idSet, wantRowsSet bool
	for _, field := range fields {
		switch field.number {
		case 1:
			if sqlSet || field.wire != 2 {
				return nil, errors.New("invalid SQL text")
			}
			statement["sql"], err = hranaProtoText(field)
			sqlSet = true
		case 2:
			if idSet || field.wire != 0 || field.integer > math.MaxInt32 {
				return nil, errors.New("invalid SQL ID")
			}
			statement["sql_id"], idSet = field.integer, true
		case 3:
			if field.wire != 2 || len(statement["args"].([]any)) >= 999 {
				return nil, errors.New("invalid positional arguments")
			}
			var value map[string]any
			value, err = hranaProtoValue(field.bytes)
			if err == nil {
				statement["args"] = append(statement["args"].([]any), value)
			}
		case 4:
			if field.wire != 2 || len(statement["named_args"].([]any)) >= 999 {
				return nil, errors.New("invalid named arguments")
			}
			var named []hranaProtoField
			named, err = hranaProtoRead(field.bytes)
			if err != nil {
				return nil, err
			}
			if len(named) != 2 || named[0].number != 1 || named[1].number != 2 || named[1].wire != 2 {
				return nil, errors.New("invalid named argument")
			}
			var name string
			name, err = hranaProtoText(named[0])
			if err != nil {
				return nil, err
			}
			var value map[string]any
			value, err = hranaProtoValue(named[1].bytes)
			if err == nil {
				statement["named_args"] = append(statement["named_args"].([]any), map[string]any{"name": name, "value": value})
			}
		case 5:
			if wantRowsSet || field.wire != 0 || field.integer > 1 {
				return nil, errors.New("invalid want_rows")
			}
			statement["want_rows"], wantRowsSet = field.integer == 1, true
		default:
			return nil, errors.New("unsupported protobuf statement field")
		}
		if err != nil {
			return nil, err
		}
	}
	if sqlSet == idSet {
		return nil, errors.New("SQL text or ID required")
	}
	return statement, nil
}

func hranaProtoCondition(data []byte, depth int) (map[string]any, error) {
	if depth > 8 {
		return nil, errors.New("condition depth limit")
	}
	fields, err := hranaProtoRead(data)
	if err != nil {
		return nil, err
	}
	if len(fields) != 1 {
		return nil, errors.New("invalid batch condition")
	}
	field := fields[0]
	switch field.number {
	case 1, 2:
		if field.wire != 0 || field.integer > math.MaxInt32 {
			break
		}
		kind := "ok"
		if field.number == 2 {
			kind = "error"
		}
		return map[string]any{"type": kind, "step": field.integer}, nil
	case 3:
		if field.wire != 2 {
			break
		}
		condition, err := hranaProtoCondition(field.bytes, depth+1)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "not", "cond": condition}, nil
	case 4, 5:
		if field.wire != 2 {
			break
		}
		items, err := hranaProtoRead(field.bytes)
		if err != nil || len(items) > 64 {
			return nil, errors.New("invalid batch condition list")
		}
		conditions := make([]any, 0, len(items))
		for _, item := range items {
			if item.number != 1 || item.wire != 2 {
				return nil, errors.New("invalid batch condition entry")
			}
			condition, err := hranaProtoCondition(item.bytes, depth+1)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, condition)
		}
		kind := "and"
		if field.number == 5 {
			kind = "or"
		}
		return map[string]any{"type": kind, "conds": conditions}, nil
	case 6:
		if field.wire == 2 && len(field.bytes) == 0 {
			return map[string]any{"type": "is_autocommit"}, nil
		}
	}
	return nil, errors.New("unsupported batch condition")
}

func hranaProtoBatch(data []byte) (map[string]any, error) {
	fields, err := hranaProtoRead(data)
	if err != nil {
		return nil, err
	}
	steps := make([]any, 0, len(fields))
	for _, field := range fields {
		if field.number != 1 || field.wire != 2 || len(steps) >= 64 {
			return nil, errors.New("invalid batch steps")
		}
		items, err := hranaProtoRead(field.bytes)
		if err != nil {
			return nil, err
		}
		step := map[string]any{}
		for _, item := range items {
			switch item.number {
			case 1:
				if _, found := step["condition"]; found || item.wire != 2 {
					return nil, errors.New("invalid step condition")
				}
				step["condition"], err = hranaProtoCondition(item.bytes, 0)
			case 2:
				if _, found := step["stmt"]; found || item.wire != 2 {
					return nil, errors.New("invalid step statement")
				}
				step["stmt"], err = hranaProtoStatement(item.bytes)
			default:
				return nil, errors.New("unsupported batch step field")
			}
			if err != nil {
				return nil, err
			}
		}
		if step["stmt"] == nil {
			return nil, errors.New("missing batch statement")
		}
		steps = append(steps, step)
	}
	return map[string]any{"steps": steps}, nil
}

func hranaProtoPipeline(data []byte) (map[string]any, error) {
	fields, err := hranaProtoRead(data)
	if err != nil {
		return nil, err
	}
	pipeline := map[string]any{"requests": []any{}}
	baton := false
	for _, field := range fields {
		switch field.number {
		case 1:
			if baton {
				return nil, errors.New("duplicate baton")
			}
			pipeline["baton"], err = hranaProtoText(field)
			baton = true
		case 2:
			if field.wire != 2 || len(pipeline["requests"].([]any)) >= 64 {
				return nil, errors.New("invalid pipeline request")
			}
			var request map[string]any
			request, err = hranaProtoRequest(field.bytes)
			if err == nil {
				pipeline["requests"] = append(pipeline["requests"].([]any), request)
			}
		default:
			return nil, errors.New("unsupported pipeline field")
		}
		if err != nil {
			return nil, err
		}
	}
	if len(pipeline["requests"].([]any)) == 0 {
		return nil, errors.New("empty pipeline")
	}
	return pipeline, nil
}

func hranaProtoRequest(data []byte) (map[string]any, error) {
	fields, err := hranaProtoRead(data)
	if err != nil {
		return nil, err
	}
	if len(fields) != 1 || fields[0].wire != 2 {
		return nil, errors.New("invalid stream request")
	}
	field := fields[0]
	switch field.number {
	case 1:
		if len(field.bytes) == 0 {
			return map[string]any{"type": "close"}, nil
		}
	case 8:
		if len(field.bytes) == 0 {
			return map[string]any{"type": "get_autocommit"}, nil
		}
	case 2, 3:
		fields, err := hranaProtoRead(field.bytes)
		if err != nil {
			return nil, err
		}
		if len(fields) != 1 || fields[0].number != 1 || fields[0].wire != 2 {
			break
		}
		if field.number == 2 {
			stmt, err := hranaProtoStatement(fields[0].bytes)
			return map[string]any{"type": "execute", "stmt": stmt}, err
		}
		batch, err := hranaProtoBatch(fields[0].bytes)
		return map[string]any{"type": "batch", "batch": batch}, err
	case 4, 5, 6, 7:
		fields, err := hranaProtoRead(field.bytes)
		if err != nil {
			return nil, err
		}
		request := map[string]any{}
		if field.number == 4 {
			request["type"] = "sequence"
		} else if field.number == 5 {
			request["type"] = "describe"
		} else if field.number == 6 {
			request["type"] = "store_sql"
		} else {
			request["type"] = "close_sql"
		}
		for _, item := range fields {
			switch item.number {
			case 1:
				if _, found := request["sql"]; (field.number == 4 || field.number == 5) && found {
					return nil, errors.New("duplicate SQL text")
				}
				if _, found := request["sql_id"]; field.number != 4 && field.number != 5 && found {
					return nil, errors.New("duplicate SQL ID")
				}
				if field.number == 4 || field.number == 5 {
					if item.wire != 2 {
						return nil, errors.New("invalid sequence SQL")
					}
					request["sql"], err = hranaProtoText(item)
				} else {
					if item.wire != 0 || item.integer > math.MaxInt32 {
						return nil, errors.New("invalid SQL ID")
					}
					request["sql_id"] = item.integer
				}
			case 2:
				if field.number == 7 {
					return nil, errors.New("invalid close SQL")
				}
				if _, found := request["sql_id"]; (field.number == 4 || field.number == 5) && found {
					return nil, errors.New("duplicate sequence SQL ID")
				}
				if _, found := request["sql"]; field.number != 4 && field.number != 5 && found {
					return nil, errors.New("duplicate stored SQL")
				}
				if field.number == 4 || field.number == 5 {
					if item.wire != 0 || item.integer > math.MaxInt32 {
						return nil, errors.New("invalid sequence SQL ID")
					}
					request["sql_id"] = item.integer
				} else {
					request["sql"], err = hranaProtoText(item)
				}
			default:
				return nil, errors.New("unsupported SQL request field")
			}
			if err != nil {
				return nil, err
			}
		}
		if field.number == 4 || field.number == 5 {
			_, text := request["sql"]
			_, identifier := request["sql_id"]
			if text == identifier {
				return nil, errors.New("sequence SQL text or ID required")
			}
		} else {
			if _, found := request["sql_id"]; !found {
				return nil, errors.New("SQL ID required")
			}
			if field.number == 6 {
				if _, found := request["sql"]; !found {
					return nil, errors.New("stored SQL text required")
				}
			}
		}
		return request, nil
	}
	return nil, errors.New("unsupported stream request")
}

func hranaProtoCursor(data []byte) (map[string]any, error) {
	fields, err := hranaProtoRead(data)
	if err != nil {
		return nil, err
	}
	var baton string
	var batch map[string]any
	for _, field := range fields {
		switch field.number {
		case 1:
			if baton != "" {
				return nil, errors.New("duplicate baton")
			}
			baton, err = hranaProtoText(field)
		case 2:
			if batch != nil || field.wire != 2 {
				return nil, errors.New("duplicate batch")
			}
			batch, err = hranaProtoBatch(field.bytes)
		default:
			return nil, errors.New("unsupported cursor field")
		}
		if err != nil {
			return nil, err
		}
	}
	if batch == nil || len(batch["steps"].([]any)) == 0 {
		return nil, errors.New("cursor batch required")
	}
	return map[string]any{"baton": baton, "requests": []any{map[string]any{"type": "batch", "batch": batch}}}, nil
}

func hranaProtoVar(value uint64) []byte {
	var buffer [10]byte
	return append([]byte(nil), buffer[:binary.PutUvarint(buffer[:], value)]...)
}

func hranaProtoNumber(number int, value uint64) []byte {
	return append(hranaProtoVar(uint64(number<<3)), hranaProtoVar(value)...)
}
func hranaProtoDouble(number int, value float64) []byte {
	encoded := hranaProtoVar(uint64(number<<3 | 1))
	var bits [8]byte
	binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
	return append(encoded, bits[:]...)
}
func hranaProtoBytes(number int, value []byte) []byte {
	encoded := hranaProtoVar(uint64(number<<3 | 2))
	encoded = append(encoded, hranaProtoVar(uint64(len(value)))...)
	return append(encoded, value...)
}
func hranaProtoString(number int, value string) []byte { return hranaProtoBytes(number, []byte(value)) }
func hranaProtoAppend(dst []byte, number int, value []byte) []byte {
	return append(dst, hranaProtoBytes(number, value)...)
}

func hranaProtoResultValue(input any) ([]byte, error) {
	value, ok := input.(map[string]any)
	if !ok {
		return nil, errors.New("invalid result value")
	}
	switch value["type"] {
	case "null":
		return hranaProtoBytes(1, nil), nil
	case "integer":
		text, ok := value["value"].(string)
		if !ok {
			return nil, errors.New("invalid result integer")
		}
		integer, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, err
		}
		return hranaProtoNumber(2, uint64(integer<<1)^uint64(integer>>63)), nil
	case "float":
		converted, err := hranaProtoFloat(value["value"])
		if err != nil {
			return nil, err
		}
		return hranaProtoDouble(3, converted), nil
	case "text":
		text, ok := value["value"].(string)
		if !ok {
			return nil, errors.New("invalid result text")
		}
		return hranaProtoString(4, text), nil
	case "blob":
		text, ok := value["base64"].(string)
		if !ok {
			return nil, errors.New("invalid result blob")
		}
		data, err := base64.StdEncoding.Strict().DecodeString(text)
		if err != nil {
			return nil, err
		}
		return hranaProtoBytes(5, data), nil
	}
	return nil, errors.New("unsupported result value")
}

// hranaProtoFloat accepts both shapes a REAL cell reaches the encoder in:
// json.Number when a worker reply was decoded as JSON, and float64 once
// hranaOutputValue has classified the native cell. Neither NaN nor an infinity
// is representable on the wire, so both shapes are validated here.
func hranaProtoFloat(input any) (float64, error) {
	var converted float64
	switch value := input.(type) {
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid result float %q: %w", string(value), err)
		}
		converted = parsed
	case float64:
		converted = value
	default:
		return 0, fmt.Errorf("invalid result float type %T", input)
	}
	if math.IsNaN(converted) || math.IsInf(converted, 0) {
		return 0, fmt.Errorf("invalid result float %v", converted)
	}
	return converted, nil
}

func hranaProtoError(input any) ([]byte, error) {
	value, ok := input.(map[string]any)
	if !ok {
		return nil, errors.New("invalid Hrana error")
	}
	message, ok := value["message"].(string)
	if !ok {
		return nil, errors.New("error message required")
	}
	encoded := hranaProtoString(1, message)
	if code, ok := value["code"].(string); ok {
		encoded = append(encoded, hranaProtoString(2, code)...)
	}
	return encoded, nil
}

func hranaProtoColumns(input any, fieldNumber int) ([]byte, error) {
	columns, ok := input.([]any)
	if !ok {
		return nil, errors.New("invalid statement columns")
	}
	result := []byte{}
	for _, column := range columns {
		item, ok := column.(map[string]any)
		if !ok {
			return nil, errors.New("invalid statement column")
		}
		field := []byte{}
		if name, ok := item["name"].(string); ok {
			field = append(field, hranaProtoString(1, name)...)
		}
		if decltype, ok := item["decltype"].(string); ok {
			field = append(field, hranaProtoString(2, decltype)...)
		}
		result = hranaProtoAppend(result, fieldNumber, field)
	}
	return result, nil
}

func hranaProtoDescribeResult(input any) ([]byte, error) {
	value, ok := input.(map[string]any)
	if !ok {
		return nil, errors.New("invalid describe result")
	}
	params, ok := value["params"].([]any)
	if !ok {
		return nil, errors.New("invalid describe parameters")
	}
	result := []byte{}
	for _, param := range params {
		item, ok := param.(map[string]any)
		if !ok {
			return nil, errors.New("invalid describe parameter")
		}
		encoded := []byte{}
		if name, ok := item["name"].(string); ok {
			encoded = hranaProtoString(1, name)
		}
		result = hranaProtoAppend(result, 1, encoded)
	}
	columns, err := hranaProtoColumns(value["cols"], 2)
	if err != nil {
		return nil, err
	}
	result = append(result, columns...)
	for _, flag := range []struct {
		name  string
		field int
	}{{"is_explain", 3}, {"is_readonly", 4}} {
		state, ok := value[flag.name].(bool)
		if !ok {
			return nil, errors.New("invalid describe flag")
		}
		if state {
			result = append(result, hranaProtoNumber(flag.field, 1)...)
		} else {
			result = append(result, hranaProtoNumber(flag.field, 0)...)
		}
	}
	return result, nil
}

func hranaProtoStatementResult(input any) ([]byte, error) {
	value, ok := input.(map[string]any)
	if !ok {
		return nil, errors.New("invalid statement result")
	}
	result, err := hranaProtoColumns(value["cols"], 1)
	if err != nil {
		return nil, err
	}
	rows, ok := value["rows"].([]any)
	if !ok {
		typed, native := value["rows"].([][]any)
		if !native {
			return nil, errors.New("invalid statement rows")
		}
		rows = make([]any, len(typed))
		for index := range typed {
			rows[index] = typed[index]
		}
	}
	for _, row := range rows {
		items, ok := row.([]any)
		if !ok {
			return nil, errors.New("invalid statement row")
		}
		var encoded []byte
		for _, item := range items {
			value, err := hranaProtoResultValue(item)
			if err != nil {
				return nil, err
			}
			encoded = hranaProtoAppend(encoded, 1, value)
		}
		result = hranaProtoAppend(result, 2, encoded)
	}
	affected, err := hranaProtoUInt(value["affected_row_count"])
	if err != nil {
		return nil, err
	}
	result = append(result, hranaProtoNumber(3, affected)...)
	if last, ok := value["last_insert_rowid"].(string); ok {
		integer, err := strconv.ParseInt(last, 10, 64)
		if err != nil {
			return nil, err
		}
		result = append(result, hranaProtoNumber(4, uint64(integer<<1)^uint64(integer>>63))...)
	}
	metrics, err := hranaProtoStatementMetrics(value)
	if err != nil {
		return nil, err
	}
	return append(result, metrics...), nil
}

// hranaProtoStatementMetrics encodes the optional execution counters as fields
// 6, 7 and 8 of the official wire schema. Field 5 (replication_index) stays
// untouched: the worker never reports one and the gateway must not invent it.
// An old worker omits all three keys, and the reply keeps its legacy shape.
func hranaProtoStatementMetrics(value map[string]any) ([]byte, error) {
	_, hasRead := value["rows_read"]
	_, hasWritten := value["rows_written"]
	_, hasDuration := value["query_duration_ms"]
	if !hasRead && !hasWritten && !hasDuration {
		return nil, nil
	}
	if !hasRead || !hasWritten || !hasDuration {
		return nil, errors.New("incomplete statement metrics")
	}
	read, err := hranaProtoMetricUInt(value["rows_read"])
	if err != nil {
		return nil, err
	}
	written, err := hranaProtoMetricUInt(value["rows_written"])
	if err != nil {
		return nil, err
	}
	duration, err := hranaProtoMetricFloat(value["query_duration_ms"])
	if err != nil {
		return nil, err
	}
	encoded := hranaProtoNumber(6, read)
	encoded = append(encoded, hranaProtoNumber(7, written)...)
	return append(encoded, hranaProtoDouble(8, duration)...), nil
}

func hranaProtoMetricUInt(input any) (uint64, error) {
	switch value := input.(type) {
	case uint64:
		return value, nil
	case uint:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case int:
		if value < 0 {
			return 0, errors.New("negative statement metric")
		}
		return uint64(value), nil
	case int64:
		if value < 0 {
			return 0, errors.New("negative statement metric")
		}
		return uint64(value), nil
	case float64:
		if value < 0 || value != math.Trunc(value) || value >= math.Ldexp(1, 64) {
			return 0, errors.New("invalid statement metric")
		}
		return uint64(value), nil
	case json.Number:
		return strconv.ParseUint(string(value), 10, 64)
	default:
		return 0, errors.New("invalid statement metric")
	}
}

func hranaProtoMetricFloat(input any) (float64, error) {
	var value float64
	switch typed := input.(type) {
	case float64:
		value = typed
	case json.Number:
		converted, err := typed.Float64()
		if err != nil {
			return 0, err
		}
		value = converted
	case int:
		value = float64(typed)
	case int64:
		value = float64(typed)
	case uint64:
		value = float64(typed)
	default:
		return 0, errors.New("invalid statement duration")
	}
	if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, errors.New("invalid statement duration")
	}
	return value, nil
}

func hranaProtoUInt(value any) (uint64, error) {
	switch typed := value.(type) {
	case json.Number:
		return strconv.ParseUint(string(typed), 10, 64)
	case uint64:
		return typed, nil
	case uint:
		return uint64(typed), nil
	case uint32:
		return uint64(typed), nil
	case int:
		if typed < 0 {
			return 0, errors.New("negative protobuf integer")
		}
		return uint64(typed), nil
	case int64:
		if typed < 0 {
			return 0, errors.New("negative protobuf integer")
		}
		return uint64(typed), nil
	case float64:
		if typed < 0 || typed != math.Trunc(typed) || typed >= math.Ldexp(1, 64) {
			return 0, errors.New("invalid protobuf integer")
		}
		return uint64(typed), nil
	default:
		return 0, errors.New("missing protobuf integer")
	}
}

func hranaProtoBatchResult(input any) ([]byte, error) {
	result, ok := input.(map[string]any)
	if !ok {
		return nil, errors.New("invalid batch result")
	}
	steps, ok := result["step_results"].([]any)
	if !ok {
		return nil, errors.New("invalid batch step results")
	}
	errorsList, ok := result["step_errors"].([]any)
	if !ok || len(steps) != len(errorsList) {
		return nil, errors.New("invalid batch step errors")
	}
	encoded := []byte{}
	for index := range steps {
		if steps[index] != nil {
			statement, err := hranaProtoStatementResult(steps[index])
			if err != nil {
				return nil, err
			}
			item := append(hranaProtoNumber(1, uint64(index)), hranaProtoBytes(2, statement)...)
			encoded = hranaProtoAppend(encoded, 1, item)
		}
		if errorsList[index] != nil {
			failure, err := hranaProtoError(errorsList[index])
			if err != nil {
				return nil, err
			}
			item := append(hranaProtoNumber(1, uint64(index)), hranaProtoBytes(2, failure)...)
			encoded = hranaProtoAppend(encoded, 2, item)
		}
	}
	return encoded, nil
}

func hranaProtoPipelineResponse(value map[string]any) ([]byte, error) {
	encoded := []byte{}
	if baton, ok := value["baton"].(string); ok {
		encoded = append(encoded, hranaProtoString(1, baton)...)
	}
	results, ok := value["results"].([]any)
	if !ok {
		return nil, errors.New("missing pipeline results")
	}
	for _, entry := range results {
		item, ok := entry.(map[string]any)
		if !ok {
			return nil, errors.New("invalid pipeline result")
		}
		var result []byte
		switch item["type"] {
		case "error":
			failure, err := hranaProtoError(item["error"])
			if err != nil {
				return nil, err
			}
			result = hranaProtoBytes(2, failure)
		case "ok":
			response, ok := item["response"].(map[string]any)
			if !ok {
				return nil, errors.New("invalid stream response")
			}
			var message []byte
			var number int
			switch response["type"] {
			case "close":
				number = 1
			case "execute":
				number = 2
				var err error
				message, err = hranaProtoStatementResult(response["result"])
				if err != nil {
					return nil, err
				}
				message = hranaProtoBytes(1, message)
			case "batch":
				number = 3
				var err error
				message, err = hranaProtoBatchResult(response["result"])
				if err != nil {
					return nil, err
				}
				message = hranaProtoBytes(1, message)
			case "sequence":
				number = 4
			case "describe":
				number = 5
				var err error
				message, err = hranaProtoDescribeResult(response["result"])
				if err != nil {
					return nil, err
				}
				message = hranaProtoBytes(1, message)
			case "store_sql":
				number = 6
			case "close_sql":
				number = 7
			case "get_autocommit":
				number = 8
				isAutocommit, ok := response["is_autocommit"].(bool)
				if !ok {
					return nil, errors.New("invalid autocommit response")
				}
				value := uint64(0)
				if isAutocommit {
					value = 1
				}
				message = hranaProtoNumber(1, value)
			default:
				return nil, errors.New("unsupported response type")
			}
			result = hranaProtoBytes(1, hranaProtoBytes(number, message))
		default:
			return nil, errors.New("invalid stream result type")
		}
		encoded = hranaProtoAppend(encoded, 3, result)
	}
	return encoded, nil
}

func hranaProtoCursorFrames(value map[string]any) ([]byte, error) {
	var output []byte
	header := []byte{}
	if baton, ok := value["baton"].(string); ok {
		header = hranaProtoString(1, baton)
	}
	output = append(output, hranaProtoVar(uint64(len(header)))...)
	output = append(output, header...)
	results, ok := value["results"].([]any)
	if !ok || len(results) != 1 {
		return nil, errors.New("invalid cursor result")
	}
	entry, ok := results[0].(map[string]any)
	if !ok || entry["type"] != "ok" {
		return nil, errors.New("invalid cursor batch")
	}
	response, ok := entry["response"].(map[string]any)
	if !ok || response["type"] != "batch" {
		return nil, errors.New("invalid cursor batch response")
	}
	batch, ok := response["result"].(map[string]any)
	if !ok {
		return nil, errors.New("invalid cursor batch result")
	}
	steps, ok := batch["step_results"].([]any)
	if !ok {
		return nil, errors.New("invalid cursor steps")
	}
	errorsList, ok := batch["step_errors"].([]any)
	if !ok || len(steps) != len(errorsList) {
		return nil, errors.New("invalid cursor step errors")
	}
	appendFrame := func(frame []byte) {
		output = append(output, hranaProtoVar(uint64(len(frame)))...)
		output = append(output, frame...)
	}
	for index := range steps {
		if errorsList[index] != nil {
			failure, err := hranaProtoError(errorsList[index])
			if err != nil {
				return nil, err
			}
			item := append(hranaProtoNumber(1, uint64(index)), hranaProtoBytes(2, failure)...)
			appendFrame(hranaProtoBytes(3, item))
			continue
		}
		if steps[index] == nil {
			continue
		}
		result, ok := steps[index].(map[string]any)
		if !ok {
			return nil, errors.New("invalid cursor statement")
		}
		columns, err := hranaProtoColumns(result["cols"], 2)
		if err != nil {
			return nil, err
		}
		begin := append(hranaProtoNumber(1, uint64(index)), columns...)
		appendFrame(hranaProtoBytes(1, begin))
		rows, ok := result["rows"].([]any)
		if !ok {
			return nil, errors.New("invalid cursor rows")
		}
		for _, row := range rows {
			items, ok := row.([]any)
			if !ok {
				return nil, errors.New("invalid cursor row")
			}
			var encoded []byte
			for _, item := range items {
				value, err := hranaProtoResultValue(item)
				if err != nil {
					return nil, err
				}
				encoded = hranaProtoAppend(encoded, 1, value)
			}
			appendFrame(hranaProtoBytes(4, encoded))
		}
		affected, err := hranaProtoUInt(result["affected_row_count"])
		if err != nil || affected > math.MaxUint32 {
			return nil, errors.New("invalid cursor affected count")
		}
		end := hranaProtoNumber(1, affected)
		if last, ok := result["last_insert_rowid"].(string); ok {
			integer, err := strconv.ParseUint(last, 10, 64)
			if err != nil {
				return nil, errors.New("unsupported signed cursor last rowid")
			}
			end = append(end, hranaProtoNumber(2, integer)...)
		}
		appendFrame(hranaProtoBytes(2, end))
	}
	return output, nil
}

func serveHranaV3(writer http.ResponseWriter, request *http.Request, handler *hranaHTTP) bool {
	parts := strings.Split(request.URL.Path, "/")
	if len(parts) < 6 || parts[0] != "" || parts[1] != "db" || parts[5] != "v3-protobuf" {
		return false
	}
	if len(parts) == 6 && request.Method == http.MethodGet && request.URL.RawQuery == "" {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.WriteHeader(http.StatusOK)
		return true
	}
	if len(parts) != 7 || request.Method != http.MethodPost || request.URL.RawQuery != "" || (parts[6] != "pipeline" && parts[6] != "cursor") {
		http.NotFound(writer, request)
		return true
	}
	if request.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(writer, "protobuf required", http.StatusUnsupportedMediaType)
		return true
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, sqlWireLimit))
	if err != nil {
		http.Error(writer, "protobuf request limit", 413)
		return true
	}
	var input map[string]any
	if parts[6] == "cursor" {
		input, err = hranaProtoCursor(body)
	} else {
		input, err = hranaProtoPipeline(body)
	}
	if err != nil {
		http.Error(writer, "invalid Hrana protobuf request", 400)
		return true
	}
	if parts[6] == "cursor" && handler.serveNativeHTTPReadCursor(writer, request, parts, input) {
		return true
	}
	if parts[6] == "pipeline" && handler.serveHranaProtoSingleExecute(writer, request, parts, input) {
		return true
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		http.Error(writer, "invalid protobuf input", 400)
		return true
	}
	forwarded := request.Clone(request.Context())
	forwarded.URL.Path = strings.Join(parts[:5], "/") + "/v2/pipeline"
	forwarded.Body = io.NopCloser(bytes.NewReader(encoded))
	forwarded.ContentLength = int64(len(encoded))
	forwarded.Header = request.Header.Clone()
	forwarded.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, forwarded)
	if recorder.Code != 200 {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.WriteHeader(recorder.Code)
		_, _ = writer.Write(recorder.Body.Bytes())
		return true
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(recorder.Body.Bytes()))
	decoder.UseNumber()
	if decoder.Decode(&result) != nil {
		http.Error(writer, "invalid Hrana backend response", 502)
		return true
	}
	var output []byte
	if parts[6] == "cursor" {
		output, err = hranaProtoCursorFrames(result)
	} else {
		output, err = hranaProtoPipelineResponse(result)
	}
	if err != nil {
		log.Printf("Hrana protobuf response unavailable: path=%s error=%v", request.URL.Path, err)
		http.Error(writer, "Hrana protobuf response unavailable", 502)
		return true
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Content-Type", "application/x-protobuf")
	writer.WriteHeader(200)
	_, _ = writer.Write(output)
	return true
}

// serveHranaProtoSingleExecute answers the execute+close pipeline shape without
// the JSON compatibility round trip. Only a read-only statement that opens and
// closes its own session is handled here; every other shape falls back to the
// JSON path so write settlement, batons and conditions keep one implementation.
func (handler *hranaHTTP) serveHranaProtoSingleExecute(writer http.ResponseWriter, request *http.Request, parts []string, input map[string]any) bool {
	requests, ok := input["requests"].([]any)
	if !ok || len(requests) != 2 {
		return false
	}
	executeRequest, ok := requests[0].(map[string]any)
	if !ok || executeRequest["type"] != "execute" {
		return false
	}
	closeRequest, ok := requests[1].(map[string]any)
	if !ok || closeRequest["type"] != "close" {
		return false
	}
	if baton, exists := input["baton"]; exists {
		if value, ok := baton.(string); !ok || value != "" {
			return false
		}
	}
	if _, exists := executeRequest["condition"]; exists {
		return false
	}
	statementValue, ok := executeRequest["stmt"].(map[string]any)
	if !ok {
		return false
	}
	sql, ok := statementValue["sql"].(string)
	if !ok || !hranaProtoSingleSelect(sql) {
		return false
	}
	encodedStatement, err := json.Marshal(statementValue)
	if err != nil {
		return false
	}
	var statement hranaStatement
	if json.Unmarshal(encodedStatement, &statement) != nil || statement.SQL == "" {
		return false
	}
	database := controller.DatabaseID{Namespace: controller.Namespace{Tenant: parts[2], Database: parts[3]}, Branch: parts[4]}
	if database.Validate() != nil {
		return false
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return false
	}
	credential := strings.TrimPrefix(values[0], "Bearer ")
	principal, err := handler.gateway.config.Identity.Authenticate(credential)
	if err != nil || principal.Subject == "" || principal.Project == "" {
		return false
	}
	quota, err := handler.gateway.authorizeQuota(principal, database)
	if err != nil {
		return false
	}
	resultLimit := min(maxHranaPipelineResultBytes, quota.MaxResultBytes)
	resultReserve := min(maxHranaPipelineSteps*1024, max(128, resultLimit/8))
	if resultLimit <= resultReserve {
		return false
	}
	reply := handler.call(request, credential, "/v1/sql/open", map[string]any{"database": database})
	if reply.status != http.StatusOK || reply.code != "" {
		return false
	}
	var opened struct {
		Session string `json:"session"`
	}
	if json.Unmarshal(reply.result, &opened) != nil || !publicSessionID(opened.Session) {
		if opened.Session != "" {
			handler.cleanupRetiredSession(opened.Session, 0)
		}
		return false
	}
	session := opened.Session
	closed := false
	defer func() {
		if !closed {
			handler.cleanupRetiredSession(session, 0)
		}
	}()
	value, executeReply := handler.execute(request, credential, session, statement)
	if executeReply.code != "" {
		results := []any{map[string]any{"type": "error", "error": map[string]any{"message": hranaErrorMessage(executeReply), "code": executeReply.code}}}
		if closeReply := handler.call(request, credential, "/v1/sql/close", map[string]any{"session": session}); closeReply.status == http.StatusOK && closeReply.code == "" {
			closed = true
			results = append(results, map[string]any{"type": "ok", "response": map[string]any{"type": "close"}})
		}
		output, err := hranaProtoPipelineResponse(map[string]any{"results": results})
		if err != nil {
			http.Error(writer, "Hrana protobuf response unavailable", http.StatusBadGateway)
			return true
		}
		writeHranaProtoResponse(writer, output)
		return true
	}
	closeReply := handler.call(request, credential, "/v1/sql/close", map[string]any{"session": session})
	if closeReply.status != http.StatusOK || closeReply.code != "" {
		http.Error(writer, "cursor session close failed", http.StatusBadGateway)
		return true
	}
	closed = true
	response := map[string]any{
		"results": []any{
			map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": value}},
			map[string]any{"type": "ok", "response": map[string]any{"type": "close"}},
		},
	}
	output, err := hranaProtoPipelineResponse(response)
	if err != nil {
		http.Error(writer, "Hrana protobuf response unavailable", http.StatusBadGateway)
		return true
	}
	if len(output) > resultLimit {
		http.Error(writer, "pipeline_result_limit", http.StatusRequestEntityTooLarge)
		return true
	}
	writeHranaProtoResponse(writer, output)
	return true
}

func hranaProtoSingleSelect(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	return len(trimmed) >= 6 && strings.EqualFold(trimmed[:6], "SELECT")
}

func writeHranaProtoResponse(writer http.ResponseWriter, output []byte) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Content-Type", "application/x-protobuf")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(output)
}
