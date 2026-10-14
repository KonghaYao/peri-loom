package platform

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestKMSRealAESSealOpenNonceAndAADTamper(test *testing.T) {
	kms := securityKMS(test)
	dek := bytes.Repeat([]byte{3}, 32)
	aad := []byte("tenant/domain/root")
	first, err := kms.Seal(dek, aad)
	if err != nil {
		test.Fatal(err)
	}
	second, err := kms.Seal(dek, aad)
	if err != nil {
		test.Fatal(err)
	}
	if bytes.Equal(first, second) {
		test.Fatal("nonce reused")
	}
	actual, err := kms.Open(first, aad)
	if err != nil || !bytes.Equal(actual, dek) {
		test.Fatal("AES round trip")
	}
	if _, err := kms.Open(first, []byte("other-tenant/domain/root")); err == nil {
		test.Fatal("AAD not authenticated")
	}
	var envelope kmsEnvelope
	json.Unmarshal(first, &envelope)
	if envelope.Ciphertext[0] == '0' {
		envelope.Ciphertext = "1" + envelope.Ciphertext[1:]
	} else {
		envelope.Ciphertext = "0" + envelope.Ciphertext[1:]
	}
	tampered, _ := json.Marshal(envelope)
	if _, err := kms.Open(tampered, aad); err == nil {
		test.Fatal("ciphertext tamper accepted")
	}
	other := securityKMS(test)
	if _, err := other.Open(first, aad); err == nil {
		test.Fatal("wrong KEK accepted")
	}
	if _, err := kms.Seal(dek, nil); err == nil {
		test.Fatal("missing AAD")
	}
}

func TestKMSRotationReopenRetainsOldVersionsAndDEK(test *testing.T) {
	path := filepath.Join(test.TempDir(), "ring.json")
	kms, err := NewLocalKMS(path)
	if err != nil {
		test.Fatal(err)
	}
	dek := bytes.Repeat([]byte{8}, 32)
	aad := []byte("domain/root")
	old, err := kms.Seal(dek, aad)
	if err != nil {
		test.Fatal(err)
	}
	if err := RotateLocalKMS(kms); err != nil {
		test.Fatal(err)
	}
	newEnvelope, err := kms.Seal(dek, aad)
	if err != nil {
		test.Fatal(err)
	}
	var wrapped kmsEnvelope
	json.Unmarshal(newEnvelope, &wrapped)
	if wrapped.KeyVersion != "2" {
		test.Fatal("rotation did not change KEK")
	}
	if err := CloseLocalKMS(kms); err != nil {
		test.Fatal(err)
	}
	reopened, err := NewLocalKMS(path)
	if err != nil {
		test.Fatal(err)
	}
	defer CloseLocalKMS(reopened)
	for _, envelope := range []json.RawMessage{old, newEnvelope} {
		actual, err := reopened.Open(envelope, aad)
		if err != nil || !bytes.Equal(actual, dek) {
			test.Fatal("rotation replaced DEK or forgot old KEK")
		}
	}
}

func TestKMSDurableUseCountRollbackAndLimit(test *testing.T) {
	path := filepath.Join(test.TempDir(), "ring.json")
	kms, err := NewLocalKMS(path)
	if err != nil {
		test.Fatal(err)
	}
	old, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	if _, err := kms.Seal(bytes.Repeat([]byte{9}, 32), []byte("domain")); err != nil {
		test.Fatal(err)
	}
	CloseLocalKMS(kms)
	if err := os.WriteFile(path, old, 0600); err != nil {
		test.Fatal(err)
	}
	if _, err := NewLocalKMS(path); err == nil {
		test.Fatal("KEK nonce accounting rollback")
	}
	service := securityKMS(test).(*localKMS)
	service.ring.Uses["1"] = 1000000
	if _, err := service.Seal(bytes.Repeat([]byte{1}, 32), []byte("domain")); err == nil {
		test.Fatal("per-KEK envelope cap bypassed")
	}
}
