package platform

import "encoding/json"

func validDescribeResult(encoded json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil || len(fields) != 4 {
		return false
	}
	var params []map[string]json.RawMessage
	if json.Unmarshal(fields["params"], &params) != nil || params == nil {
		return false
	}
	for _, param := range params {
		if len(param) != 1 {
			return false
		}
		if string(param["name"]) != "null" {
			var name string
			if json.Unmarshal(param["name"], &name) != nil || name == "" {
				return false
			}
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
		if json.Unmarshal(column["name"], &name) != nil || name == "" {
			return false
		}
		if string(column["decltype"]) != "null" {
			var declared string
			if json.Unmarshal(column["decltype"], &declared) != nil {
				return false
			}
		}
	}
	for _, key := range []string{"is_explain", "is_readonly"} {
		var flag bool
		if json.Unmarshal(fields[key], &flag) != nil || string(fields[key]) != "true" && string(fields[key]) != "false" {
			return false
		}
	}
	return true
}
