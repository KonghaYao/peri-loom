package platform

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func p6VerifyBytes(encoded json.RawMessage) ([]byte, error) {
	encoded = bytes.TrimSpace(encoded)
	if len(encoded) == 0 {
		return nil, fmt.Errorf("missing persisted bytes")
	}
	switch encoded[0] {
	case '"':
		var text string
		if err := json.Unmarshal(encoded, &text); err != nil {
			return nil, err
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(text)
		if err != nil {
			return nil, err
		}
		if base64.StdEncoding.EncodeToString(decoded) != text {
			return nil, fmt.Errorf("noncanonical persisted base64 bytes")
		}
		return decoded, nil
	case '[':
		var numbers []json.RawMessage
		if err := json.Unmarshal(encoded, &numbers); err != nil {
			return nil, err
		}
		decoded := make([]byte, len(numbers))
		for index, number := range numbers {
			var value *uint8
			if err := json.Unmarshal(number, &value); err != nil || value == nil {
				return nil, fmt.Errorf("invalid persisted byte value at index %d", index)
			}
			decoded[index] = *value
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("persisted bytes must be base64 or a byte array")
	}
}

func p6VerifyArtifacts(test *testing.T, directory, marker string) (int, int, int) {
	test.Helper()
	objects := map[string][]byte{}
	segments, walFrames, caches := 0, 0, 0
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkError error) error {
		if walkError != nil {
			return walkError
		}
		if entry.IsDir() {
			return nil
		}
		extension := filepath.Ext(path)
		if extension != ".obj" && extension != ".seg" {
			return nil
		}
		encoded, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if extension == ".seg" {
			if len(encoded) == 0 || bytes.Contains(encoded, []byte(marker)) {
				test.Fatal("empty or plaintext SSD segment")
			}
			caches++
			return nil
		}
		var envelope struct {
			Key   string          `json:"key"`
			Bytes json.RawMessage `json:"bytes"`
		}
		if err := json.Unmarshal(encoded, &envelope); err != nil {
			return err
		}
		payload, err := p6VerifyBytes(envelope.Bytes)
		if err != nil {
			return err
		}
		if bytes.Contains(payload, []byte(marker)) {
			test.Fatal("plaintext marker in decoded physical object")
		}
		objects[envelope.Key] = payload
		return nil
	})
	if err != nil {
		test.Fatal(err)
	}
	for key, payload := range objects {
		if !strings.HasSuffix(key, "/authority/root") {
			continue
		}
		var envelope struct {
			Root struct {
				Objects map[string]struct {
					Blob string `json:"blob"`
				} `json:"objects"`
			} `json:"root"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil {
			test.Fatal(err)
		}
		test.Logf("decoded authority root logicalObjects=%d physicalObjects=%d", len(envelope.Root.Objects), len(objects))
		prefix := strings.TrimSuffix(key, "root") + "blobs/"
		for logical, reference := range envelope.Root.Objects {
			payload, found := objects[prefix+reference.Blob]
			if !found || len(payload) == 0 {
				test.Fatal("catalog payload missing from actual Local store")
			}
			if strings.HasPrefix(logical, "segments/") {
				segments++
				if bytes.Contains(payload, []byte(marker)) {
					test.Fatal("plaintext native main segment")
				}
			}
			if strings.HasPrefix(logical, "wal/chunks/") {
				var record struct {
					Batch struct {
						Operations []struct {
							Write *struct {
								Bytes json.RawMessage `json:"bytes"`
							} `json:"Write"`
						} `json:"operations"`
					} `json:"batch"`
				}
				if err := json.Unmarshal(payload, &record); err != nil {
					test.Fatal(err)
				}
				for _, operation := range record.Batch.Operations {
					if operation.Write == nil {
						continue
					}
					frame, err := p6VerifyBytes(operation.Write.Bytes)
					if err != nil {
						test.Fatal(err)
					}
					if len(frame) == 0 {
						continue
					}
					if bytes.Contains(frame, []byte(marker)) {
						test.Fatal("plaintext native WAL write bytes")
					}
					walFrames++
				}
			}
		}
	}
	if segments == 0 || walFrames == 0 || caches == 0 {
		test.Fatalf("vacuous payload scan mainSegments=%d nativeWALWrites=%d SSDsegments=%d", segments, walFrames, caches)
	}
	return segments, walFrames, caches
}

func TestP6VerifyArtifactBytesEncoding(test *testing.T) {
	for _, sample := range []struct {
		name    string
		encoded string
		decoded []byte
		invalid bool
	}{
		{name: "base64", encoded: `"AAH/"`, decoded: []byte{0, 1, 255}},
		{name: "legacy_array", encoded: `[0,1,255]`, decoded: []byte{0, 1, 255}},
		{name: "padded_base64", encoded: `"Zg=="`, decoded: []byte{'f'}},
		{name: "empty_base64", encoded: `""`, decoded: []byte{}},
		{name: "empty_array", encoded: `[]`, decoded: []byte{}},
		{name: "missing", invalid: true},
		{name: "null", encoded: `null`, invalid: true},
		{name: "object", encoded: `{}`, invalid: true},
		{name: "boolean", encoded: `true`, invalid: true},
		{name: "number", encoded: `1`, invalid: true},
		{name: "invalid_base64", encoded: `"!!!!"`, invalid: true},
		{name: "missing_padding", encoded: `"Zg"`, invalid: true},
		{name: "nonzero_padding_bits", encoded: `"Zh=="`, invalid: true},
		{name: "base64_newline", encoded: `"AAH/\n"`, invalid: true},
		{name: "urlsafe_base64", encoded: `"AAH_"`, invalid: true},
		{name: "negative", encoded: `[-1]`, invalid: true},
		{name: "negative_zero", encoded: `[-0]`, invalid: true},
		{name: "overflow", encoded: `[256]`, invalid: true},
		{name: "fraction", encoded: `[1.5]`, invalid: true},
		{name: "null_element", encoded: `[null]`, invalid: true},
		{name: "string_element", encoded: `["1"]`, invalid: true},
		{name: "nested_array", encoded: `[[1]]`, invalid: true},
		{name: "malformed_array", encoded: `[1,`, invalid: true},
	} {
		test.Run(sample.name, func(test *testing.T) {
			decoded, err := p6VerifyBytes(json.RawMessage(sample.encoded))
			if sample.invalid {
				if err == nil {
					test.Fatal("invalid persisted bytes accepted")
				}
				return
			}
			if err != nil || !bytes.Equal(decoded, sample.decoded) {
				test.Fatalf("decoded bytes mismatch: got=%v err=%v", decoded, err)
			}
		})
	}
}
