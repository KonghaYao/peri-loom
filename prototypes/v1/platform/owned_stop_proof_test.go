package platform

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func ownedProofTestConfig(test *testing.T) OwnedStopProofConfig {
	test.Helper()
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	keyFile := filepath.Join(directory, "key")
	if err := os.WriteFile(keyFile, make([]byte, 32), 0600); err != nil {
		test.Fatal(err)
	}
	return OwnedStopProofConfig{Generation: p6VerifyToken(test), Directory: directory, KeyFile: keyFile, Workers: map[string]string{"worker-a": p6VerifyToken(test)}}
}

func ownedProofTestWrite(test *testing.T, config OwnedStopProofConfig, suffix string, identity ownedProcessIdentity, forged bool) {
	test.Helper()
	payload, err := json.Marshal(identity)
	if err != nil {
		test.Fatal(err)
	}
	key, err := os.ReadFile(config.KeyFile)
	if err != nil {
		test.Fatal(err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	signed := ownedSignedProof{Payload: string(payload), MAC: hex.EncodeToString(mac.Sum(nil))}
	if forged {
		signed.MAC = p6VerifyToken(test)
	}
	p6VerifyPrivate(test, filepath.Join(config.Directory, "worker-a"+suffix), signed)
}

func TestOwnedStopProofRequiresAuthenticatedMatchingWait(test *testing.T) {
	for _, scenario := range []string{"missing", "forged", "generation", "nonce", "pid", "started", "live", "public_file", "valid"} {
		test.Run(scenario, func(test *testing.T) {
			config := ownedProofTestConfig(test)
			identity := ownedProcessIdentity{Version: 1, Generation: config.Generation, WorkerID: "worker-a", Nonce: config.Workers["worker-a"], PID: 123456789, Started: p6VerifyToken(test)}
			ownedProofTestWrite(test, config, ".identity.json", identity, false)
			proof := identity
			proof.Waited = true
			switch scenario {
			case "generation":
				proof.Generation = p6VerifyToken(test)
			case "nonce":
				proof.Nonce = p6VerifyToken(test)
			case "pid":
				proof.PID++
			case "started":
				proof.Started = p6VerifyToken(test)
			case "live":
				proof.Waited = false
			}
			if scenario != "missing" {
				ownedProofTestWrite(test, config, ".stop.json", proof, scenario == "forged")
			}
			if scenario == "public_file" {
				if err := os.Chmod(filepath.Join(config.Directory, "worker-a.stop.json"), 0644); err != nil {
					test.Fatal(err)
				}
			}
			verifier, err := NewOwnedStopProofVerifier(config)
			if err != nil {
				test.Fatal(err)
			}
			err = verifier.VerifyOwnedWorkerStopped(context.Background(), "worker-a")
			if (err == nil) != (scenario == "valid") {
				test.Fatalf("unexpected wait verification for %s: %v", scenario, err)
			}
			if verifier.VerifyOwnedWorkerStopped(context.Background(), "unowned") == nil {
				test.Fatal("unowned worker accepted")
			}
		})
	}
}

func TestOwnedStopProofGenerationBindingSurvivesReload(test *testing.T) {
	config := ownedProofTestConfig(test)
	directory := test.TempDir()
	store, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	if err := bindOwnedRunGeneration(store, config); err != nil {
		test.Fatal(err)
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	store, err = OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	if err := bindOwnedRunGeneration(store, config); err != nil {
		test.Fatal(err)
	}
	other := config
	other.Generation = p6VerifyToken(test)
	if bindOwnedRunGeneration(store, other) == nil {
		test.Fatal("old state rebound to new generation")
	}
	other = config
	other.Workers = map[string]string{"worker-a": p6VerifyToken(test)}
	if bindOwnedRunGeneration(store, other) == nil {
		test.Fatal("same generation rebound to new worker incarnation")
	}
	legacy := operationDurableStore(test)
	if err := beginOperation(legacy, securityDatabase(), "legacy", "gateway", "open", "open_session", "worker-a", "", nil); err != nil {
		test.Fatal(err)
	}
	if bindOwnedRunGeneration(legacy, config) == nil {
		test.Fatal("legacy operation assigned new ownership retrospectively")
	}
}
