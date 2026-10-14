package platform

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	controller "peri-loom/controller"
)

func RunService(parent context.Context, config ServiceConfig, output io.Writer) error {
	var verifier WorkerStopVerifier
	if config.OwnedStopProof != nil {
		adapter, err := NewOwnedStopProofVerifier(*config.OwnedStopProof)
		if err != nil {
			return err
		}
		verifier = adapter
	}
	return runService(parent, config, output, verifier)
}

func RunServiceWithWorkerStopVerifier(parent context.Context, config ServiceConfig, output io.Writer, verifier WorkerStopVerifier) error {
	if verifier == nil {
		return errors.New("owned worker stop verifier required")
	}
	return runService(parent, config, output, verifier)
}

func runService(parent context.Context, config ServiceConfig, output io.Writer, verifier WorkerStopVerifier) error {
	requestTimeout := 30 * time.Second
	if setting := os.Getenv("PERI_DEV_SQL_TIMEOUT_SECONDS"); setting != "" {
		seconds, err := strconv.Atoi(setting)
		if err != nil || seconds < 30 || seconds > 300 || !config.InsecureLoopback {
			return errors.New("invalid local SQL timeout")
		}
		requestTimeout = time.Duration(seconds) * time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	state, err := OpenStateStore(config.StateDirectory)
	if err != nil {
		return err
	}
	defer state.Close()
	if config.OwnedStopProof != nil {
		if err := bindOwnedRunGeneration(state, *config.OwnedStopProof); err != nil {
			return err
		}
	} else {
		snapshot, err := state.Snapshot()
		if err != nil {
			return err
		}
		if snapshot.OwnedRunGeneration != "" {
			return errors.New("owned run proof configuration required")
		}
	}
	observation, err := startServiceSQLOpenObservation(ctx, config.StateDirectory, os.Getenv("PERI_SQL_OPEN_OBSERVER"))
	if err != nil {
		return err
	}
	if observation != nil {
		defer observation.stop()
	}
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
	transport = &isolatedControlTransport{ControlTransport: transport, store: state}
	control, err := controller.NewController(metadata, transport, controller.ReconcileConfig{LeaseTTL: localLeaseTTL(requestTimeout), HeartbeatTimeout: 5 * time.Second, Interval: time.Second})
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
	upstream, err := NewSQLUpstreamWithTimeout(config.WorkerSQLTokens, requestTimeout)
	if err != nil {
		return err
	}
	var observer *SQLOpenObserver
	if observation != nil {
		observer = observation.observer
	}
	gateway, err := NewGateway(GatewayConfig{State: state, Placements: metadata, Controller: control, Identity: identity, Upstream: upstream, Audit: audit, WriterAdmission: config.WriterAdmission, OpenObserver: observer, RequestTimeout: requestTimeout})
	if err != nil {
		return err
	}
	defer gateway.Close(context.Background())
	telemetry, err := NewTelemetry(transport, metadata, workerControl)
	if err != nil {
		return err
	}
	capacityTrace, err := newCapacityTraceBridge(config.Controller)
	if err != nil {
		return err
	}
	management, err := NewManagement(ManagementConfig{State: state, Placements: metadata, Controller: control, Identity: identity, Authority: transport, Keys: broker, Upstream: upstream, Audit: audit, Metrics: gateway, Telemetry: telemetry, Resources: gateway, StopProof: verifier, CapacityTrace: capacityTrace})
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
	publicMux := newPublicMux(gateway, management)
	publicServer := hardenedServer(publicMux)
	publicServer.WriteTimeout = requestTimeout + 5*time.Second
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

func newPublicMux(gateway *Gateway, management http.Handler) *http.ServeMux {
	publicMux := http.NewServeMux()
	publicMux.Handle("/v1/sql/", gateway)
	hrana := http.NotFoundHandler()
	hranaEnabled := os.Getenv("PERI_HRANA_EXPERIMENTAL") == "1"
	if hranaEnabled {
		hrana = newHranaHTTP(gateway)
		gateway.mu.Lock()
		gateway.hrana = hrana.(*hranaHTTP)
		gateway.mu.Unlock()
	}
	publicMux.HandleFunc("/v1/platform/capabilities", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodGet || request.URL.RawQuery != "" || strings.ContainsAny(request.Host, "/@?#\\\r\n") {
			http.NotFound(writer, request)
			return
		}
		scheme := "http"
		if request.TLS != nil {
			scheme = "https"
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"version":       1,
			"hrana_http":    hranaEnabled,
			"hrana_ws":      hranaEnabled,
			"public_origin": scheme + "://" + request.Host,
		})
	})
	publicMux.Handle("/db/", hrana)
	publicMux.Handle("/v1/", management)
	publicMux.Handle("/", AdminHandler())
	return publicMux
}

func hardenedServer(handler http.Handler) *http.Server {
	return &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 * 1024}
}

func localLeaseTTL(requestTimeout time.Duration) time.Duration {
	if requestTimeout > 30*time.Second {
		return 60 * time.Second
	}
	return 30 * time.Second
}

type serviceSQLOpenObservation struct {
	observer *SQLOpenObserver
	stream   string
	stopping chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

type serviceSQLOpenRecord struct {
	Stream             string `json:"stream"`
	CallSequence       uint64 `json:"call_sequence"`
	RequestID          string `json:"request_id"`
	Stage              string `json:"stage"`
	Reason             string `json:"reason"`
	Classification     string `json:"classification,omitempty"`
	RPCError           bool   `json:"rpc_error"`
	Ambiguous          bool   `json:"ambiguous"`
	Rejected           bool   `json:"rejected"`
	ExecutionCompleted bool   `json:"execution_completed"`
}

func startServiceSQLOpenObservation(ctx context.Context, directory, enabled string) (*serviceSQLOpenObservation, error) {
	if enabled == "" || enabled == "0" {
		return nil, nil
	}
	if enabled != "1" {
		return nil, errors.New("invalid SQL open observer profile")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("SQL open observer private directory unavailable")
	}
	identity, ok := info.Sys().(*syscall.Stat_t)
	if !ok || identity.Uid != uint32(os.Getuid()) {
		return nil, errors.New("SQL open observer private directory unavailable")
	}
	streamBytes := make([]byte, 16)
	if _, err := rand.Read(streamBytes); err != nil {
		return nil, errors.New("SQL open observer stream unavailable")
	}
	path := filepath.Join(directory, "sql-open-observation.jsonl")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, errors.New("SQL open observer file unavailable")
	}
	actual, statError := file.Stat()
	parent, parentError := os.Lstat(directory)
	if statError != nil || parentError != nil || !os.SameFile(info, parent) || !actual.Mode().IsRegular() || actual.Mode().Perm() != 0600 {
		file.Close()
		return nil, errors.New("SQL open observer file identity unavailable")
	}
	observation := &serviceSQLOpenObservation{observer: NewSQLOpenObserver(), stream: hex.EncodeToString(streamBytes), stopping: make(chan struct{}), done: make(chan struct{})}
	go observation.consume(ctx, file)
	return observation, nil
}

func (observation *serviceSQLOpenObservation) stop() bool {
	observation.stopOnce.Do(func() { close(observation.stopping) })
	select {
	case <-observation.done:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (observation *serviceSQLOpenObservation) consume(ctx context.Context, output io.WriteCloser) {
	defer close(observation.done)
	defer output.Close()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	encoder := json.NewEncoder(output)
	count := 0
	drain := func() bool {
		for count < 64 {
			record, found := observation.observer.TryReceive()
			if !found {
				return true
			}
			count++
			projected := serviceSQLOpenRecord{
				Stream: observation.stream, CallSequence: record.CallSequence, RequestID: record.RequestID,
				Stage: record.Stage, Reason: record.Reason, Classification: record.Classification,
				RPCError: record.RPCError, Ambiguous: record.Ambiguous, Rejected: record.Rejected, ExecutionCompleted: record.ExecutionCompleted,
			}
			if encoder.Encode(projected) != nil {
				return false
			}
		}
		return true
	}
	for {
		select {
		case <-ctx.Done():
			if drain() && count < 64 {
				encoder.Encode(serviceSQLOpenRecord{Stream: observation.stream, Stage: "observer", Reason: "bounded_stop", Classification: "UNPROVEN"})
			}
			return
		case <-observation.stopping:
			if drain() && count < 64 {
				encoder.Encode(serviceSQLOpenRecord{Stream: observation.stream, Stage: "observer", Reason: "bounded_stop", Classification: "UNPROVEN"})
			}
			return
		case <-ticker.C:
			if !drain() {
				return
			}
			if count == 64 {
				return
			}
		}
	}
}
