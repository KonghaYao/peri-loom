package faultprobe

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Gate struct {
	Version     int      `json:"version"`
	JointPass   bool     `json:"joint_pass"`
	ConfirmedBy string   `json:"confirmed_by"`
	SourceGit   string   `json:"source_git"`
	Stages      []string `json:"stages"`
	ObservedAt  string   `json:"observed_at"`
}

func (gate Gate) Validate(head string) error {
	if gate.Version != 1 || !gate.JointPass || gate.ConfirmedBy != "main" || gate.SourceGit != head {
		return errors.New("main joint PASS for matching HEAD required")
	}
	if _, err := time.Parse(time.RFC3339Nano, gate.ObservedAt); err != nil {
		return errors.New("invalid observed date")
	}
	stages := map[string]bool{}
	for _, stage := range gate.Stages {
		stages[stage] = true
	}
	for _, stage := range []string{"P0", "P1", "P2", "P3", "P4", "P5", "P6"} {
		if !stages[stage] {
			return errors.New("all joint stages required")
		}
	}
	return nil
}

func checkGate(ctx context.Context, config Config) (map[string]any, error) {
	head, err := exec.CommandContext(ctx, "git", "-C", config.SourceRoot, "rev-parse", "HEAD").Output()
	if err != nil {
		return nil, errors.New("actual source HEAD unavailable")
	}
	hash := strings.TrimSpace(string(head))
	if _, err := hex.DecodeString(hash); err != nil || len(hash) != 40 && len(hash) != 64 {
		return nil, errors.New("invalid source HEAD")
	}
	var gate Gate
	if readPrivate(config.GateFile, &gate) != nil || gate.Validate(hash) != nil {
		return nil, errors.New("main P0-P6 joint PASS absent or mismatched")
	}
	status, err := exec.CommandContext(ctx, "git", "-C", config.SourceRoot, "status", "--porcelain").Output()
	if err != nil || len(status) > 1<<20 {
		return nil, errors.New("source state unavailable")
	}
	return map[string]any{"source_git": hash, "dirty": len(status) > 0, "joint_gate": gate, "fixture_only": true, "harness_pid": os.Getpid()}, nil
}
