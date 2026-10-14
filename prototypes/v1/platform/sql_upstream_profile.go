package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"unicode/utf8"
)

func sqlDecodeWriterProfile(ctx context.Context, body []byte, requestID string) error {
	invalid := func() error { return &SQLRPCError{Ambiguous: true} }
	if !utf8.Valid(body) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if sqlJSONValue(ctx, decoder, 0) != nil {
		return invalid()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || len(fields) != 5 {
		return invalid()
	}
	var version uint32
	if json.Unmarshal(fields["version"], &version) != nil || version != 2 {
		return invalid()
	}
	fields["version"] = json.RawMessage("1")
	normalized, err := json.Marshal(fields)
	if err != nil {
		return invalid()
	}
	var result struct {
		Protocol       *uint32 `json:"protocol"`
		WriterProfile  *bool   `json:"writer_profile"`
		MaxInflight    *uint64 `json:"max_inflight"`
		ControlReserve *uint64 `json:"control_reserve"`
		SQLTotal       *uint64 `json:"sql_total"`
		GeneralSQL     *uint64 `json:"general_sql"`
		OwnerSQL       *uint64 `json:"owner_sql"`
		CleanupSQL     *uint64 `json:"cleanup_sql"`
	}
	if err := sqlDecode(ctx, normalized, requestID, &result); err != nil {
		return err
	}
	if result.Protocol == nil || *result.Protocol != 2 || result.WriterProfile == nil || !*result.WriterProfile || result.MaxInflight == nil || result.ControlReserve == nil || result.SQLTotal == nil || result.GeneralSQL == nil || result.OwnerSQL == nil || result.CleanupSQL == nil {
		return invalid()
	}
	if *result.ControlReserve == 0 || *result.MaxInflight < *result.ControlReserve || *result.SQLTotal != *result.MaxInflight-*result.ControlReserve || *result.SQLTotal < 3 || *result.GeneralSQL != *result.SQLTotal-2 || *result.OwnerSQL != 1 || *result.CleanupSQL != 1 {
		return invalid()
	}
	return nil
}
