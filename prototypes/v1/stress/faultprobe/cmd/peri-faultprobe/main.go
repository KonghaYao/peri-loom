package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"peri-loom/faultprobe"
)

func main() {
	config := faultprobe.DefaultConfig()
	flag.StringVar(&config.RunDir, "run-dir", "", "canonical private fresh peri-stress-* launcher fixture")
	flag.StringVar(&config.ConnectionFile, "connection", "", "private fixture connection JSON")
	flag.StringVar(&config.TopologyFile, "topology", "", "private latest ROOT launcher topology JSON")
	flag.StringVar(&config.SourceRoot, "source-root", "", "actual tested git checkout")
	flag.StringVar(&config.GateFile, "joint-pass", "", "private main P0-P6 joint PASS JSON matching HEAD")
	flag.StringVar(&config.RustBinary, "rust-binary", "", "exact actual Rust peri-loom-v1 binary argv path")
	flag.StringVar(&config.Case, "case", config.Case, "owner_crash or metadata_starvation")
	flag.BoolVar(&config.AllowWorkerKill, "allow-worker-sigkill", false, "authorize only the exact fixture owner worker and its cache")
	flag.IntVar(&config.ACKs, "acks", config.ACKs, "bounded pre-crash transactions, 1..32")
	flag.DurationVar(&config.RequestTimeout, "request-timeout", config.RequestTimeout, "HTTP deadline, 1s..15s")
	flag.DurationVar(&config.RecoveryTimeout, "recovery-timeout", config.RecoveryTimeout, "new epoch observation, 1s..60s")
	flag.DurationVar(&config.ObservationWindow, "observation-window", config.ObservationWindow, "starvation observation, 1s..20s")
	execute := flag.Bool("execute", false, "explicitly enable real probe; main joint PASS still mandatory")
	list := flag.Bool("list", false, "network-free capability catalog")
	flag.Parse()
	if *list {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"owner_crash": "SUPPORTED: ACK-boundary crash only; no in-flight ambiguity claim", "metadata_starvation": "OBSERVATION_ONLY: unknown management outcome stops; missing terminal/authority lease oracles cannot PASS", "old_grant_revived": "SKIP: no Gateway/Management original-grant replay route", "inflight_crash": "SKIP: uncertain commit recovery oracle not implemented"})
		return
	}
	if !*execute {
		fmt.Fprintln(os.Stderr, "BLOCKED: explicit -execute and main joint PASS required")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	result, err := faultprobe.Run(ctx, config, faultprobe.LocalHost{})
	_ = json.NewEncoder(os.Stdout).Encode(result)
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe stopped; inspect private redacted evidence if available")
		os.Exit(1)
	}
}
