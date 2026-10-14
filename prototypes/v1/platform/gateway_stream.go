package platform

import (
	"bytes"
	"encoding/json"
	"errors"
)

func gatewayRows(requestID string, rows [][]json.RawMessage, limit int) ([]byte, error) {
	var output bytes.Buffer
	for _, row := range rows {
		encoded, err := json.Marshal(struct {
			Type string            `json:"type"`
			Row  []json.RawMessage `json:"row"`
		}{"row", row})
		if err != nil || output.Len()+len(encoded)+1 > limit {
			return nil, errors.New("result limit")
		}
		output.Write(encoded)
		output.WriteByte('\n')
	}
	complete, err := json.Marshal(struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Rows      int    `json:"rows"`
	}{"complete", requestID, len(rows)})
	if err != nil || output.Len()+len(complete)+1 > limit {
		return nil, errors.New("result limit")
	}
	output.Write(complete)
	output.WriteByte('\n')
	return output.Bytes(), nil
}
