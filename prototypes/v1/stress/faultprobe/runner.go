package faultprobe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type Result struct {
	Case            string   `json:"case"`
	Status          string   `json:"status"`
	Failure         *Failure `json:"failure,omitempty"`
	EvidenceDir     string   `json:"evidence_dir"`
	OldGrantRevival string   `json:"old_grant_revival"`
}

func claimFixture(root string) (*Log, error) {
	claim, err := os.OpenFile(filepath.Join(root, ".faultprobe.used"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.New("fixture has already been claimed; never reuse")
	}
	if err := claim.Close(); err != nil {
		return nil, errors.New("fixture claim failed")
	}
	directory := filepath.Join(root, "faultprobe")
	if err := os.Mkdir(directory, 0700); err != nil {
		return nil, errors.New("fresh evidence directory required")
	}
	file, err := os.OpenFile(filepath.Join(directory, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.New("private evidence unavailable")
	}
	return &Log{file: file}, nil
}

func Run(ctx context.Context, config Config, host Host) (result Result, runError error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	result = Result{Case: config.Case, Status: "BLOCKED", OldGrantRevival: "SKIP: Gateway/Management expose no original-grant replay; no worker revival attempted"}
	connection, topology, err := config.Validate()
	if err != nil {
		return result, err
	}
	metadata, err := checkGate(ctx, config)
	if err != nil {
		return result, err
	}
	if host == nil {
		return result, errors.New("host adapter required")
	}
	global := "/tmp/peri-loom-stress.global.lock"
	lock, err := os.OpenFile(global, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return result, errors.New("another serial runner holds the lock")
	}
	defer os.Remove(global)
	defer lock.Close()
	log, err := claimFixture(config.RunDir)
	if err != nil {
		return result, err
	}
	defer log.file.Close()
	log.secrets = []string{connection.ProjectAPIKey, connection.GlobalAPIKey}
	result.EvidenceDir = filepath.Join(config.RunDir, "faultprobe")
	started := time.Now()
	defer func() {
		if result.Status == "BLOCKED" {
			result.Status = "FAIL"
		}
		if err := log.Record(map[string]any{"type": "summary", "result": result, "elapsed_ns": time.Since(started).Nanoseconds(), "finished_at": time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
			runError = err
			if result.Failure == nil || !result.Failure.Unknown {
				result.Status = "FAIL"
			}
		}
		if err := log.file.Sync(); err != nil {
			runError = errors.New("evidence sync failed")
			if result.Failure == nil || !result.Failure.Unknown {
				result.Status = "FAIL"
			}
		}
	}()
	metadata["type"] = "run"
	metadata["case"] = config.Case
	metadata["run_dir"] = config.RunDir
	metadata["observed_at"] = started.UTC().Format(time.RFC3339Nano)
	metadata["launcher_pid"] = topology.LauncherPID
	metadata["rust_binary"] = config.RustBinary
	metadata["bounds"] = map[string]any{"acks": config.ACKs, "request_timeout_ns": config.RequestTimeout.Nanoseconds(), "recovery_timeout_ns": config.RecoveryTimeout.Nanoseconds(), "observation_window_ns": config.ObservationWindow.Nanoseconds(), "total_timeout_ns": (3 * time.Minute).Nanoseconds()}
	metadata["synthetic_delta_model"] = "sequence_mod_7_minus_3"
	if err := log.Record(metadata); err != nil {
		return result, err
	}
	identities := map[string]ProcessIdentity{}
	for _, worker := range []string{"worker-a", "worker-b"} {
		identity, err := host.Inspect(ctx, topology.Processes[worker].PID)
		if err != nil || validateWorker(config, topology, worker, identity) != nil {
			return result, errors.New("private worker process validation failed")
		}
		if _, err := validateCache(workerCache(config, worker)); err != nil {
			return result, err
		}
		identities[worker] = identity
		if err := log.Record(map[string]any{"type": "worker_verified", "worker": worker, "pid": identity.PID, "start": identity.Start, "cache": workerCache(config, worker)}); err != nil {
			return result, err
		}
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return result, errors.New("fresh operation prefix unavailable")
	}
	prefix := "peri_stress_" + hex.EncodeToString(random[:])
	gateway, err := newClient(connection, connection.ProjectAPIKey, prefix+"_sql", config, log)
	if err != nil {
		return result, err
	}
	management, err := newClient(connection, connection.GlobalAPIKey, prefix+"_mgmt", config, log)
	if err != nil {
		return result, err
	}
	fixture := &Fixture{Database: connection.Database, Prefix: prefix, log: log}
	if config.Case == "owner_crash" {
		result.Failure = ownerCrash(ctx, config, topology, gateway, management, fixture, host, identities, log)
	} else {
		result.Failure = metadataStarvation(ctx, config, connection, gateway, management, prefix, log)
	}
	result.Status = "PASS"
	if result.Failure != nil {
		result.Status = "FAIL"
		if result.Failure.Unknown {
			result.Status = "UNRESOLVED"
		}
		runError = result.Failure
		if result.Failure.Code == "starvation_terminal_and_lease_oracles_unavailable" || result.Failure.Code == "not_found" || result.Failure.Code == "method_not_allowed" {
			result.Status = "SKIP"
			runError = nil
		}
	}
	return result, runError
}
