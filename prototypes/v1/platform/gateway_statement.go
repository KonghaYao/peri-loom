package platform

import "encoding/json"

func validStatementResult(encoded json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || len(fields) != 5 {
		return false
	}
	for _, name := range []string{"cols", "rows", "affected_row_count", "last_insert_rowid", "autocommit"} {
		if _, exists := fields[name]; !exists {
			return false
		}
	}
	var columns []map[string]json.RawMessage
	if json.Unmarshal(fields["cols"], &columns) != nil || columns == nil {
		return false
	}
	for _, column := range columns {
		if len(column) != 2 {
			return false
		}
		var name string
		if json.Unmarshal(column["name"], &name) != nil || column["decltype"] == nil {
			return false
		}
		if string(column["decltype"]) != "null" {
			var declared string
			if json.Unmarshal(column["decltype"], &declared) != nil {
				return false
			}
		}
	}
	var rows [][]json.RawMessage
	if json.Unmarshal(fields["rows"], &rows) != nil || rows == nil {
		return false
	}
	for _, row := range rows {
		if len(row) != len(columns) {
			return false
		}
	}
	var affected int64
	if json.Unmarshal(fields["affected_row_count"], &affected) != nil || affected < 0 {
		return false
	}
	if string(fields["last_insert_rowid"]) != "null" {
		var rowid int64
		if json.Unmarshal(fields["last_insert_rowid"], &rowid) != nil {
			return false
		}
	}
	var autocommit bool
	if json.Unmarshal(fields["autocommit"], &autocommit) != nil || string(fields["autocommit"]) != "true" && string(fields["autocommit"]) != "false" {
		return false
	}
	return true
}
