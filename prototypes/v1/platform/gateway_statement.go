package platform

import (
	"encoding/json"
	"math"
)

func validStatementResult(encoded json.RawMessage) bool {
	valid, _, _ := validStatementResultAutocommit(encoded)
	return valid
}

// validStatementResultAutocommit is validStatementResult plus the autocommit
// verdict it already decoded, so a caller that tracks whether a session still
// holds a transaction does not parse the result a second time.
func validStatementResultAutocommit(encoded json.RawMessage) (valid, autocommit, autocommitKnown bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || (len(fields) != 5 && len(fields) != 8) {
		return false, false, false
	}
	for _, name := range []string{"cols", "rows", "affected_row_count", "last_insert_rowid", "autocommit"} {
		if _, exists := fields[name]; !exists {
			return false, false, false
		}
	}
	var columns []map[string]json.RawMessage
	if json.Unmarshal(fields["cols"], &columns) != nil || columns == nil {
		return false, false, false
	}
	for _, column := range columns {
		if len(column) != 2 {
			return false, false, false
		}
		var name string
		if json.Unmarshal(column["name"], &name) != nil || column["decltype"] == nil {
			return false, false, false
		}
		if string(column["decltype"]) != "null" {
			var declared string
			if json.Unmarshal(column["decltype"], &declared) != nil {
				return false, false, false
			}
		}
	}
	var rows [][]json.RawMessage
	if json.Unmarshal(fields["rows"], &rows) != nil || rows == nil {
		return false, false, false
	}
	for _, row := range rows {
		if len(row) != len(columns) {
			return false, false, false
		}
	}
	var affected int64
	if json.Unmarshal(fields["affected_row_count"], &affected) != nil || affected < 0 {
		return false, false, false
	}
	if string(fields["last_insert_rowid"]) != "null" {
		var rowid int64
		if json.Unmarshal(fields["last_insert_rowid"], &rowid) != nil {
			return false, false, false
		}
	}
	var value bool
	if json.Unmarshal(fields["autocommit"], &value) != nil || string(fields["autocommit"]) != "true" && string(fields["autocommit"]) != "false" {
		return false, false, false
	}
	if len(fields) == 8 && !validStatementMetrics(fields) {
		return false, false, false
	}
	return true, value, true
}

// validStatementMetrics accepts the optional execution counters a worker may
// append to its statement reply. The five-field reply predates them, so both
// shapes are accepted, but a reply that carries some counters and not the rest
// has no agreed meaning and is rejected like any other malformed result.
func validStatementMetrics(fields map[string]json.RawMessage) bool {
	read, readExists := fields["rows_read"]
	written, writtenExists := fields["rows_written"]
	duration, durationExists := fields["query_duration_ms"]
	if !readExists || !writtenExists || !durationExists {
		return false
	}
	return validStatementCounter(read) && validStatementCounter(written) && validStatementDuration(duration)
}

// validStatementCounter rejects null explicitly: json.Unmarshal treats null as
// a successful no-op and would leave the zero value behind.
func validStatementCounter(encoded json.RawMessage) bool {
	if string(encoded) == "null" {
		return false
	}
	var value uint64
	return json.Unmarshal(encoded, &value) == nil
}

func validStatementDuration(encoded json.RawMessage) bool {
	if string(encoded) == "null" {
		return false
	}
	var value float64
	if json.Unmarshal(encoded, &value) != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return false
	}
	return true
}
