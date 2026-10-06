package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

func validateJSON(payload []byte) error {
	if !utf8.Valid(payload) {
		return errors.New("invalid response encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := validateValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("invalid response suffix")
	}
	return nil
}

func validateValue(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("response nesting limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return nil
	}
	if delimiter != '{' && delimiter != '[' {
		return errors.New("invalid response delimiter")
	}
	keys := map[string]bool{}
	for decoder.More() {
		if delimiter == '{' {
			key, err := decoder.Token()
			name, valid := key.(string)
			if err != nil || !valid || keys[name] {
				return errors.New("invalid or duplicate response key")
			}
			keys[name] = true
		}
		if err := validateValue(decoder, depth+1); err != nil {
			return err
		}
	}
	ending, err := decoder.Token()
	if err != nil || delimiter == '{' && ending != json.Delim('}') || delimiter == '[' && ending != json.Delim(']') {
		return errors.New("invalid response ending")
	}
	return nil
}
