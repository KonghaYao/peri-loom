package platform

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	controller "peri-loom/controller"
)

func RunService(parent context.Context, config ServiceConfig, output io.Writer) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	state, err := OpenStateStore(config.StateDirectory)
	if err != nil {
		return err
	}
	defer state.Close()
	metadata, err := controller.OpenStore(config.MetadataDirectory)
	if err != nil {
		return err
	}
	defer metadata.Close()
	audit, err := OpenAudit(config.AuditPath)
	if err != nil {
		return err
	}
	defer audit.Close()
	master, err := NewLocalKMS(config.KMSKeyring)
	if err != nil {
		return err
	}
	defer CloseLocalKMS(master)
	key, err := privateBytes(config.SigningKeyFile, 32)
	if err != nil || len(key) != 32 {
		return errors.New("signing key unavailable")
	}
	identity, err := NewIdentity(state, key)
	for index := range key {
		key[index] = 0
	}
	if err != nil {
		return err
	}
	snapshot, err := state.Snapshot()
	if err != nil {
		return err
	}
	if len(snapshot.APIKeys) == 0 {
		if err := BootstrapAPIKey(state, config.BootstrapKey); err != nil {
			return err
		}
	}
	workerControl := map[string]string{}
	for _, worker := range config.Controller.Workers {
		workerControl[worker.Worker.ID] = worker.ControlToken
		if err := controller.RegisterWorker(metadata, worker.Worker); err != nil {
			return err
		}
	}
	transport, err := controller.NewTransport(config.Controller.AuthorityURL, config.Controller.AuthorityToken, workerControl)
	if err != nil {
		return err
	}
	control, err := controller.NewController(metadata, transport, controller.ReconcileConfig{LeaseTTL: 30 * time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Second})
	if err != nil {
		return err
	}
	validator, err := NewLeaseValidator(config.Controller.AuthorityURL, config.WorkerAuthorityTokens)
	if err != nil {
		return err
	}
	broker, err := NewKeyBroker(state, master, validator, config.WorkerKeyTokens)
	if err != nil {
		return err
	}
	upstream, err := NewSQLUpstream(config.WorkerSQLTokens)
	if err != nil {
		return err
	}
	gateway, err := NewGateway(GatewayConfig{State: state, Placements: metadata, Controller: control, Identity: identity, Upstream: upstream, Audit: audit})
	if err != nil {
		return err
	}
	defer gateway.Close(context.Background())
	telemetry, err := NewTelemetry(transport, metadata, workerControl)
	if err != nil {
		return err
	}
	management, err := NewManagement(ManagementConfig{State: state, Placements: metadata, Controller: control, Identity: identity, Authority: transport, Keys: broker, Upstream: upstream, Audit: audit, Metrics: gateway, Telemetry: telemetry})
	if err != nil {
		return err
	}
	brokerListener, err := net.Listen("tcp", config.BrokerListen)
	if err != nil {
		return err
	}
	defer brokerListener.Close()
	publicListener, err := net.Listen("tcp", config.GatewayListen)
	if err != nil {
		return err
	}
	defer publicListener.Close()
	publicMux := http.NewServeMux()
	publicMux.Handle("/v1/sql/", gateway)
	publicMux.Handle("/v1/", management)
	publicMux.Handle("/", AdminHandler())
	publicServer := hardenedServer(publicMux)
	publicServer.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	brokerServer := hardenedServer(broker)
	finished := make(chan error, 3)
	go func() { finished <- control.Run(ctx) }()
	go func() { finished <- brokerServer.Serve(brokerListener) }()
	go func() {
		if config.TLSCertificate != "" {
			finished <- publicServer.ServeTLS(publicListener, config.TLSCertificate, config.TLSKey)
		} else {
			finished <- publicServer.Serve(publicListener)
		}
	}()
	if err := json.NewEncoder(output).Encode(map[string]any{"ready": true, "service": "platform", "address": publicListener.Addr().String(), "broker_address": brokerListener.Addr().String(), "tls": config.TLSCertificate != ""}); err != nil {
		return err
	}
	var failure error
	completed := 0
	select {
	case <-ctx.Done():
	case err := <-finished:
		completed++
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			failure = err
		}
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	shutdownError := errors.Join(publicServer.Shutdown(shutdown), brokerServer.Shutdown(shutdown))
	for completed < 3 {
		select {
		case err := <-finished:
			completed++
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
				failure = errors.Join(failure, err)
			}
		case <-shutdown.Done():
			return errors.Join(failure, shutdownError, shutdown.Err())
		}
	}
	return errors.Join(failure, shutdownError, gateway.Close(shutdown))
}

func hardenedServer(handler http.Handler) *http.Server {
	return &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 * 1024}
}
