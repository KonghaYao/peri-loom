package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
var subjectPattern = regexp.MustCompile(`^[A-Za-z0-9_.@:-]{1,128}$`)

func strictDecode(encoded []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := jsonValue(decoder, 0); err != nil {
		return errors.New("invalid or ambiguous JSON")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("invalid JSON schema")
	}
	return nil
}

func jsonValue(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("JSON depth exceeded")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	if delimiter == '{' {
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return errors.New("duplicate JSON field")
			}
			seen[name] = true
			if err := jsonValue(decoder, depth+1); err != nil {
				return err
			}
		}
	} else if delimiter == '[' {
		for decoder.More() {
			if err := jsonValue(decoder, depth+1); err != nil {
				return err
			}
		}
	} else {
		return errors.New("unexpected JSON delimiter")
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim('}') && closing != json.Delim(']') {
		return errors.New("missing JSON close")
	}
	return nil
}
