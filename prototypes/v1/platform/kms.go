package platform

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
)

type kmsEnvelope struct {
	Version    uint32 `json:"version"`
	KeyVersion string `json:"key_version"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}
type keyRing struct {
	Version  uint32            `json:"version"`
	Revision uint64            `json:"revision"`
	Active   uint64            `json:"active"`
	Keys     map[string]string `json:"keys"`
	Uses     map[string]uint64 `json:"uses"`
}
type localKMS struct {
	disk *durableFile
	ring keyRing
}

func NewLocalKMS(path string) (KMS, error) {
	if path == "" {
		return nil, errors.New("private keyring path required")
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, errors.New("secure KEK generation failed")
	}
	initial := keyRing{Version: 1, Active: 1, Keys: map[string]string{"1": hex.EncodeToString(random[:])}, Uses: map[string]uint64{"1": 0}}
	clear(random[:])
	disk, err := openDurable(path, initial)
	if err != nil {
		return nil, err
	}
	var ring keyRing
	if strictDecode(disk.payload, &ring) != nil || ring.Version != 1 || ring.Revision != disk.revision || ring.Active == 0 || ring.Keys == nil || ring.Uses == nil {
		disk.close()
		return nil, errors.New("invalid keyring")
	}
	if _, ok := ring.Keys[strconv.FormatUint(ring.Active, 10)]; !ok {
		disk.close()
		return nil, errors.New("active KEK missing")
	}
	for version, key := range ring.Keys {
		decoded, err := hex.DecodeString(key)
		number, numberErr := strconv.ParseUint(version, 10, 64)
		uses, ok := ring.Uses[version]
		if err != nil || len(decoded) != 32 || numberErr != nil || number == 0 || strconv.FormatUint(number, 10) != version || number > ring.Active || !ok || uses > 1000000 {
			disk.close()
			return nil, errors.New("invalid KEK version/count")
		}
		clear(decoded)
	}
	if len(ring.Uses) != len(ring.Keys) {
		disk.close()
		return nil, errors.New("invalid KEK accounting")
	}
	return &localKMS{disk: disk, ring: ring}, nil
}

func (kms *localKMS) Seal(plaintext, aad []byte) (json.RawMessage, error) {
	kms.disk.mutex.Lock()
	defer kms.disk.mutex.Unlock()
	if err := kms.disk.available(); err != nil {
		return nil, err
	}
	if len(plaintext) != 32 || len(aad) == 0 || len(aad) > 4096 {
		return nil, errors.New("KMS requires 32-byte DEK and bounded identity AAD")
	}
	version := strconv.FormatUint(kms.ring.Active, 10)
	if kms.ring.Uses[version] >= 1000000 {
		return nil, errors.New("KEK envelope limit exhausted; rotate KEK")
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, errors.New("secure envelope nonce failed")
	}
	key, err := hex.DecodeString(kms.ring.Keys[version])
	if err != nil {
		return nil, errors.New("KEK unavailable")
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("KEK unavailable")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	wrapped := gcm.Seal(nil, nonce[:], plaintext, kmsAAD(version, aad))
	candidate := cloneRing(kms.ring)
	candidate.Revision++
	candidate.Uses[version]++
	if err := kms.disk.commit(candidate.Revision, candidate); err != nil {
		return nil, err
	}
	kms.ring = candidate
	encoded, err := json.Marshal(kmsEnvelope{Version: 1, KeyVersion: version, Nonce: hex.EncodeToString(nonce[:]), Ciphertext: hex.EncodeToString(wrapped)})
	return encoded, err
}

func (kms *localKMS) Open(encoded json.RawMessage, aad []byte) ([]byte, error) {
	kms.disk.mutex.Lock()
	defer kms.disk.mutex.Unlock()
	if err := kms.disk.available(); err != nil {
		return nil, err
	}
	var envelope kmsEnvelope
	if strictDecode(encoded, &envelope) != nil || !validEnvelope(envelope) || len(aad) == 0 || len(aad) > 4096 {
		return nil, errors.New("invalid authenticated envelope")
	}
	hexKey, ok := kms.ring.Keys[envelope.KeyVersion]
	if !ok {
		return nil, errors.New("KEK version unavailable")
	}
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, errors.New("KEK unavailable")
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("KEK unavailable")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce, _ := hex.DecodeString(envelope.Nonce)
	body, _ := hex.DecodeString(envelope.Ciphertext)
	plaintext, err := gcm.Open(nil, nonce, body, kmsAAD(envelope.KeyVersion, aad))
	if err != nil || len(plaintext) != 32 {
		clear(plaintext)
		return nil, errors.New("envelope authentication failed")
	}
	return plaintext, nil
}

func RotateLocalKMS(service KMS) error {
	kms, ok := service.(*localKMS)
	if !ok {
		return errors.New("local KMS required")
	}
	kms.disk.mutex.Lock()
	defer kms.disk.mutex.Unlock()
	if err := kms.disk.available(); err != nil {
		return err
	}
	if kms.ring.Active == ^uint64(0) {
		return errors.New("KEK versions exhausted")
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return errors.New("secure KEK generation failed")
	}
	defer clear(key[:])
	candidate := cloneRing(kms.ring)
	candidate.Revision++
	candidate.Active++
	version := strconv.FormatUint(candidate.Active, 10)
	candidate.Keys[version] = hex.EncodeToString(key[:])
	candidate.Uses[version] = 0
	if err := kms.disk.commit(candidate.Revision, candidate); err != nil {
		return err
	}
	kms.ring = candidate
	return nil
}

func CloseLocalKMS(service KMS) error {
	kms, ok := service.(*localKMS)
	if !ok {
		return errors.New("local KMS required")
	}
	return kms.disk.close()
}
func cloneRing(ring keyRing) keyRing {
	copy := ring
	copy.Keys = map[string]string{}
	copy.Uses = map[string]uint64{}
	for version, key := range ring.Keys {
		copy.Keys[version] = key
	}
	for version, uses := range ring.Uses {
		copy.Uses[version] = uses
	}
	return copy
}
func validEnvelope(envelope kmsEnvelope) bool {
	nonce, err := hex.DecodeString(envelope.Nonce)
	body, bodyErr := hex.DecodeString(envelope.Ciphertext)
	version, versionErr := strconv.ParseUint(envelope.KeyVersion, 10, 64)
	return envelope.Version == 1 && err == nil && len(nonce) == 12 && bodyErr == nil && len(body) == 48 && versionErr == nil && version > 0 && strconv.FormatUint(version, 10) == envelope.KeyVersion
}
func kmsAAD(version string, aad []byte) []byte {
	encoded := appendField(nil, []byte("peri-kms-aes256gcm-v1"))
	encoded = appendField(encoded, []byte(version))
	return appendField(encoded, aad)
}
func appendField(encoded, value []byte) []byte {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	encoded = append(encoded, length[:]...)
	return append(encoded, value...)
}
