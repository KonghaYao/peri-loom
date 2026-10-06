package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	controller "peri-loom/controller"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "controller stopped:", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "private service configuration file")
	statePath := flag.String("state", "", "private metadata directory")
	address := flag.String("listen", "127.0.0.1:9100", "loopback HTTP address")
	flag.Parse()
	host, _, err := net.SplitHostPort(*address)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("controller listener must be literal loopback")
	}
	config, err := controller.ReadServiceConfig(*configPath)
	if err != nil {
		return err
	}
	store, err := controller.OpenStore(*statePath)
	if err != nil {
		return err
	}
	defer store.Close()
	tokens := map[string]string{}
	for _, worker := range config.Workers {
		tokens[worker.Worker.ID] = worker.ControlToken
		if err := controller.RegisterWorker(store, worker.Worker); err != nil {
			return err
		}
	}
	transport, err := controller.NewTransport(config.AuthorityURL, config.AuthorityToken, tokens)
	if err != nil {
		return err
	}
	control, err := controller.NewController(store, transport, controller.ReconcileConfig{
		LeaseTTL: 30 * time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Second,
	})
	if err != nil {
		return err
	}
	api, err := controller.NewAPI(store, control, config.APIToken)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: api, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 * 1024}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- control.Run(ctx) }()
	go func() {
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = server.Shutdown(shutdown)
	}()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"ready": true, "service": "controller", "address": listener.Addr().String()})
	serveErr := server.Serve(listener)
	cancel()
	runErr := <-finished
	if !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}
