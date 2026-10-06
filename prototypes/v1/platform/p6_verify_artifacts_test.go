package platform

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func p6VerifyBytes(test *testing.T, numbers []int) []byte {
	test.Helper()
	decoded := make([]byte, len(numbers))
	for index, value := range numbers {
		if value < 0 || value > 255 {
			test.Fatal("invalid persisted byte value")
		}
		decoded[index] = byte(value)
	}
	return decoded
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
			Key   string `json:"key"`
			Bytes []int  `json:"bytes"`
		}
		if err := json.Unmarshal(encoded, &envelope); err != nil {
			return err
		}
		payload := p6VerifyBytes(test, envelope.Bytes)
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
								Bytes []int `json:"bytes"`
							} `json:"Write"`
						} `json:"operations"`
					} `json:"batch"`
				}
				if err := json.Unmarshal(payload, &record); err != nil {
					test.Fatal(err)
				}
				for _, operation := range record.Batch.Operations {
					if operation.Write == nil || len(operation.Write.Bytes) == 0 {
						continue
					}
					frame := p6VerifyBytes(test, operation.Write.Bytes)
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
