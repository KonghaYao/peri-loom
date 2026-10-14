package faultprobe

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	file    *os.File
	secrets []string
	written int
	failed  bool
}

func (log *Log) Record(value any) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.failed {
		return errors.New("evidence recording stopped")
	}
	encoded, err := sanitize(value, log.secrets)
	if err != nil || len(encoded) > 1<<20 || log.written+len(encoded)+1 > 8<<20 {
		log.failed = true
		return errors.New("evidence bound or encoding failed")
	}
	count, err := log.file.Write(append(encoded, '\n'))
	log.written += count
	if err != nil {
		log.failed = true
		return errors.New("evidence write failed")
	}
	return nil
}

func sanitize(value any, secrets []string) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	redact := func(value string) string {
		for _, secret := range secrets {
			if secret != "" {
				value = strings.ReplaceAll(value, secret, "[REDACTED]")
			}
		}
		return value
	}
	var walk func(any) any
	walk = func(input any) any {
		switch actual := input.(type) {
		case string:
			return redact(actual)
		case []any:
			for index := range actual {
				actual[index] = walk(actual[index])
			}
			return actual
		case map[string]any:
			result := map[string]any{}
			for key, value := range actual {
				name := strings.ReplaceAll(strings.ToLower(key), "_", "")
				private := name == "session"
				for _, marker := range []string{"credential", "token", "apikey", "authorization", "secret", "password", "privatekey", "leaseid", "grantid", "dek"} {
					private = private || strings.Contains(name, marker)
				}
				if private {
					result[redact(key)] = "[REDACTED]"
				} else {
					result[redact(key)] = walk(value)
				}
			}
			return result
		default:
			return input
		}
	}
	return json.Marshal(walk(decoded))
}
