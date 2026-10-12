package platform

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type OwnedStopProofConfig struct {
	Generation string            `json:"generation"`
	Directory  string            `json:"directory"`
	KeyFile    string            `json:"key_file"`
	Workers    map[string]string `json:"workers"`
}

type ownedProcessIdentity struct {
	Version    int    `json:"version"`
	Generation string `json:"generation"`
	WorkerID   string `json:"worker_id"`
	Nonce      string `json:"nonce"`
	PID        int    `json:"pid"`
	Started    string `json:"started"`
	Waited     bool   `json:"waited"`
	ExitCode   int    `json:"exit_code"`
}

type ownedSignedProof struct {
	Payload string `json:"payload"`
	MAC     string `json:"mac"`
}

type ownedStopProofVerifier struct {
	config OwnedStopProofConfig
	key    []byte
}

func NewOwnedStopProofVerifier(config OwnedStopProofConfig) (WorkerStopVerifier, error) {
	if !ownedProofToken(config.Generation) || !filepath.IsAbs(config.Directory) || len(config.Workers) == 0 {
		return nil, errors.New("invalid owned worker proof configuration")
	}
	info, err := os.Lstat(config.Directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("private owned worker proof directory required")
	}
	key, err := privateBytes(config.KeyFile, 64)
	if err != nil || len(key) != 32 {
		return nil, errors.New("owned worker proof key unavailable")
	}
	workers := map[string]string{}
	for worker, nonce := range config.Workers {
		if !sqlLabel(worker) || !ownedProofToken(nonce) {
			return nil, errors.New("invalid owned worker incarnation")
		}
		workers[worker] = nonce
	}
	config.Workers = workers
	return &ownedStopProofVerifier{config: config, key: key}, nil
}

func ownedProofToken(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func bindOwnedRunGeneration(store StateStore, config OwnedStopProofConfig) error {
	if !ownedProofToken(config.Generation) {
		return errors.New("invalid owned run generation")
	}
	key, err := privateBytes(config.KeyFile, 32)
	if err != nil || len(key) != 32 {
		return errors.New("owned worker proof key unavailable")
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return errors.New("invalid owned run binding")
	}
	identity := sha256.Sum256(append(encoded, key...))
	binding := hex.EncodeToString(identity[:])
	return store.Update(func(state *State) error {
		if state.OwnedRunGeneration == config.Generation && state.OwnedRunBinding == binding {
			return nil
		}
		if state.OwnedRunGeneration != "" || len(state.Operations) != 0 {
			return errors.New("owned run generation cannot be rebound; new fixture required")
		}
		state.OwnedRunGeneration, state.OwnedRunBinding = config.Generation, binding
		return nil
	})
}

func (verifier *ownedStopProofVerifier) readIdentity(worker, suffix string) (ownedProcessIdentity, error) {
	var identity ownedProcessIdentity
	encoded, err := privateBytes(filepath.Join(verifier.config.Directory, worker+suffix), 8192)
	if err != nil {
		return identity, errors.New("owned worker proof unavailable")
	}
	var signed ownedSignedProof
	if strictDecode(encoded, &signed) != nil {
		return identity, errors.New("invalid owned worker proof")
	}
	actual, err := hex.DecodeString(signed.MAC)
	mac := hmac.New(sha256.New, verifier.key)
	mac.Write([]byte(signed.Payload))
	if err != nil || !hmac.Equal(actual, mac.Sum(nil)) || strictDecode([]byte(signed.Payload), &identity) != nil {
		return identity, errors.New("untrusted owned worker proof")
	}
	if identity.Version != 1 || identity.Generation != verifier.config.Generation || identity.WorkerID != worker || identity.Nonce != verifier.config.Workers[worker] || identity.PID < 1 || !ownedProofToken(identity.Started) {
		return identity, errors.New("owned worker incarnation mismatch")
	}
	return identity, nil
}

func (verifier *ownedStopProofVerifier) VerifyOwnedWorkerStopped(ctx context.Context, worker string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, exists := verifier.config.Workers[worker]; !exists {
		return errors.New("worker not owned by launcher")
	}
	identity, err := verifier.readIdentity(worker, ".identity.json")
	if err != nil {
		return err
	}
	proof, err := verifier.readIdentity(worker, ".stop.json")
	if err != nil {
		return err
	}
	if identity.Waited || !proof.Waited || identity.PID != proof.PID || identity.Started != proof.Started {
		return errors.New("owned worker wait proof mismatch")
	}
	return ctx.Err()
}
