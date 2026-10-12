package platform

import "encoding/json"

func validCursorOpenResult(encoded json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || len(fields) != 1 {
		return false
	}
	var cols []json.RawMessage
	if json.Unmarshal(fields["cols"], &cols) != nil || cols == nil {
		return false
	}
	for _, col := range cols {
		var parts map[string]json.RawMessage
		var name string
		if json.Unmarshal(col, &parts) != nil || len(parts) != 2 || json.Unmarshal(parts["name"], &name) != nil || name == "" {
			return false
		}
		if string(parts["decltype"]) != "null" {
			var decltype string
			if json.Unmarshal(parts["decltype"], &decltype) != nil {
				return false
			}
		}
	}
	return true
}

func validCursorFetchResult(encoded json.RawMessage, maxRows int) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || len(fields) != 2 {
		return false
	}
	var done bool
	if json.Unmarshal(fields["done"], &done) != nil || string(fields["done"]) != "true" && string(fields["done"]) != "false" {
		return false
	}
	var rows [][]json.RawMessage
	return json.Unmarshal(fields["rows"], &rows) == nil && rows != nil && len(rows) <= maxRows
}

func validCursorFetchResponse(encoded json.RawMessage, maxRows int, writing bool) bool {
	if !writing {
		return validCursorFetchResult(encoded, maxRows)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || len(fields) != 4 {
		return false
	}
	var done bool
	if json.Unmarshal(fields["done"], &done) != nil || string(fields["done"]) != "true" && string(fields["done"]) != "false" {
		return false
	}
	var rows [][]json.RawMessage
	if json.Unmarshal(fields["rows"], &rows) != nil || rows == nil || len(rows) > maxRows {
		return false
	}
	if !done {
		return string(fields["affected_row_count"]) == "null" && string(fields["last_insert_rowid"]) == "null"
	}
	var affected uint32
	if string(fields["affected_row_count"]) == "null" || json.Unmarshal(fields["affected_row_count"], &affected) != nil {
		return false
	}
	if string(fields["last_insert_rowid"]) == "null" {
		return true
	}
	var rowid int64
	return json.Unmarshal(fields["last_insert_rowid"], &rowid) == nil
}
