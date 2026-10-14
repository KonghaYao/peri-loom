package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	platform "peri-loom/platform"
)

func main() {
	configuration := flag.String("config", "", "private platform service configuration")
	flag.Parse()
	config, err := platform.ReadServiceConfig(*configuration)
	if err != nil {
		fmt.Fprintln(os.Stderr, "platform configuration unavailable:", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := platform.RunService(ctx, config, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "platform stopped:", err)
		os.Exit(1)
	}
}
