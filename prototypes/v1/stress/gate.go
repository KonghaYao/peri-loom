package stress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"strings"
	"time"
)

type JointGate struct {
	Version     int      `json:"version"`
	JointPass   bool     `json:"joint_pass"`
	ConfirmedBy string   `json:"confirmed_by"`
	SourceGit   string   `json:"source_git"`
	Stages      []string `json:"stages"`
	ObservedAt  string   `json:"observed_at"`
}

type Source struct {
	GitHash          string `json:"git_hash"`
	Dirty            bool   `json:"dirty"`
	DiffSHA256       string `json:"tracked_diff_sha256"`
	StatusSHA256     string `json:"status_sha256"`
	BinarySHA256     string `json:"harness_binary_sha256"`
	UntrackedContent string `json:"untracked_content_provenance"`
}

func ReadGate(path, hash string) (JointGate, error) {
	var gate JointGate
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return gate, errors.New("main joint PASS evidence file required")
	}
	body, err := os.ReadFile(path)
	if err != nil || decodeStrict(body, &gate) != nil {
		return gate, errors.New("invalid joint PASS evidence")
	}
	if err := gate.Validate(hash); err != nil {
		return gate, err
	}
	return gate, nil
}

func (gate JointGate) Validate(hash string) error {
	if gate.Version != 1 || !gate.JointPass || gate.ConfirmedBy != "main" || gate.SourceGit != hash || gate.ObservedAt == "" {
		return errors.New("load prohibited without main confirmation for the matching source hash")
	}
	if _, err := time.Parse(time.RFC3339Nano, gate.ObservedAt); err != nil {
		return errors.New("invalid main gate observed timestamp")
	}
	stages := make(map[string]bool)
	for _, stage := range gate.Stages {
		stages[stage] = true
	}
	for _, stage := range []string{"P0", "P1", "P2", "P3", "P4", "P5", "P6"} {
		if !stages[stage] {
			return errors.New("joint gate must include every P0-P6 stage")
		}
	}
	return nil
}

func CollectSource(ctx context.Context, root string) (Source, error) {
	var result Source
	head, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return result, errors.New("source git metadata unavailable")
	}
	result.GitHash = strings.TrimSpace(string(head))
	if _, err := hex.DecodeString(result.GitHash); err != nil || len(result.GitHash) != 40 && len(result.GitHash) != 64 {
		return result, errors.New("invalid source git hash")
	}
	status, err := exec.CommandContext(ctx, "git", "-C", root, "status", "--porcelain").Output()
	if err != nil {
		return result, errors.New("source status unavailable")
	}
	diff, err := exec.CommandContext(ctx, "git", "-C", root, "diff", "--no-ext-diff", "--no-textconv", "--binary", "HEAD").Output()
	if err != nil {
		return result, errors.New("source diff unavailable")
	}
	statusHash, diffHash := sha256.Sum256(status), sha256.Sum256(diff)
	result.Dirty, result.StatusSHA256, result.DiffSHA256 = len(status) > 0, hex.EncodeToString(statusHash[:]), hex.EncodeToString(diffHash[:])
	binary, err := os.Executable()
	if err == nil {
		if body, err := os.ReadFile(binary); err == nil {
			digest := sha256.Sum256(body)
			result.BinarySHA256 = hex.EncodeToString(digest[:])
		}
	}
	result.UntrackedContent = "status hash only; integrate a main-frozen source manifest before performance comparison"
	return result, nil
}
