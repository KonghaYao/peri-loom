package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	stress "peri-loom/stress"
)

func main() {
	settings := stress.DefaultSettings()
	connection := flag.String("connection", "", "private 0600 JSON connection file; never put keys in flags")
	output := flag.String("output", "", "private output directory; serialized per-host and per-output locks")
	gate := flag.String("joint-pass", "", "main-confirmed P0-P6 joint PASS JSON evidence, matching source git hash")
	list := flag.Bool("list", false, "print scenario capabilities without connecting or executing load")
	cases := flag.String("cases", "all", "comma-separated scenario names or architecture/sql/all; architecture is ordered first")
	concurrency := flag.String("concurrency", "1,2,4,8", "bounded concurrency sweep")
	flag.IntVar(&settings.Rows, "rows", settings.Rows, "initial rows: 1000 or 10000 only")
	flag.Int64Var(&settings.Seed, "seed", settings.Seed, "deterministic synthetic dataset/workload seed")
	flag.DurationVar(&settings.Warmup, "warmup", settings.Warmup, "warmup duration")
	flag.DurationVar(&settings.Smoke, "smoke", settings.Smoke, "smoke duration; shorter is marked gate-only")
	flag.DurationVar(&settings.Measure, "measure", settings.Measure, "measurement duration; shorter than 30s is gate-only")
	flag.IntVar(&settings.OfferedRate, "offered-rate", settings.OfferedRate, "global open-loop offered business operations/second, max 1000")
	flag.DurationVar(&settings.RequestTimeout, "request-timeout", settings.RequestTimeout, "one request deadline; no SQL or HTTP retry")
	flag.Int64Var(&settings.MaxGeneratedBytes, "max-generated-bytes", settings.MaxGeneratedBytes, "conservative cumulative generated request byte ceiling, max 256MiB")
	flag.Int64Var(&settings.MaxArtifactBytes, "max-artifact-bytes", settings.MaxArtifactBytes, "raw JSONL byte ceiling, max 64MiB")
	flag.Int64Var(&settings.MaxRSSBytes, "max-process-rss-bytes", 0, "optional actual per-process RSS abort threshold; no invented default")
	flag.Parse()
	if *list {
		if err := json.NewEncoder(os.Stdout).Encode(stress.Scenarios()); err != nil {
			os.Exit(1)
		}
		return
	}
	if *connection == "" || *output == "" || *gate == "" {
		fmt.Fprintln(os.Stderr, "connection, output and main joint-pass evidence are required; no load executed")
		os.Exit(2)
	}
	settings.Cases = strings.Split(*cases, ",")
	settings.Concurrency = nil
	for _, value := range strings.Split(*concurrency, ",") {
		count, err := strconv.Atoi(value)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid concurrency; no load executed")
			os.Exit(2)
		}
		settings.Concurrency = append(settings.Concurrency, count)
	}
	config, err := stress.ReadConnection(*connection)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	directory, err := (stress.SerialRunner{Config: config, Settings: settings, OutputDir: *output, GatePath: *gate}).Run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness stopped:", err.Error())
		if directory != "" {
			fmt.Fprintln(os.Stderr, "artifacts:", directory)
		}
		os.Exit(1)
	}
	fmt.Fprintln(os.Stdout, "raw run recorded:", directory, "(not a performance conclusion)")
}
