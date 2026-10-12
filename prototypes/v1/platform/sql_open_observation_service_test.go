package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestServiceOpenObserverDefaultOff(test *testing.T) {
	for _, value := range []string{"", "0"} {
		observation, err := startServiceSQLOpenObservation(context.Background(), "/absent-private-directory", value)
		if err != nil || observation != nil {
			test.Fatalf("default-off result changed: %v", err)
		}
	}
}

func TestServiceOpenObserverStrictLiteral(test *testing.T) {
	for _, value := range []string{"true", "01", " 1", "1\n", "secret-invalid-value"} {
		observation, err := startServiceSQLOpenObservation(context.Background(), test.TempDir(), value)
		if observation != nil || err == nil || err.Error() != "invalid SQL open observer profile" {
			test.Fatal("invalid literal accepted or reflected")
		}
	}
}

func TestServiceOpenObserverExclusivePrivateFile(test *testing.T) {
	directory := test.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		test.Fatal(err)
	}
	observation, err := startServiceSQLOpenObservation(context.Background(), directory, "1")
	if err != nil {
		test.Fatal(err)
	}
	if !observation.stop() {
		test.Fatal("regular-file consumer did not stop")
	}
	path := filepath.Join(directory, "sql-open-observation.jsonl")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		test.Fatal("observation file is not private regular")
	}
	before, err := os.ReadFile(path)
	if err != nil {
		test.Fatal(err)
	}
	duplicate, err := startServiceSQLOpenObservation(context.Background(), directory, "1")
	if duplicate != nil || err == nil {
		test.Fatal("existing file was appended or replaced")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		test.Fatal("existing file was modified")
	}
	var record serviceSQLOpenRecord
	if json.Unmarshal(before, &record) != nil || record.Classification != "UNPROVEN" || record.Stage != "observer" || len(record.Stream) != 32 {
		test.Fatal("bounded stop falsely claimed complete diagnostics")
	}
}

func TestServiceOpenObserverRejectsUnsafeFiles(test *testing.T) {
	for _, kind := range []string{"symlink", "directory", "public-parent", "symlink-parent"} {
		test.Run(kind, func(test *testing.T) {
			directory := test.TempDir()
			if err := os.Chmod(directory, 0700); err != nil {
				test.Fatal(err)
			}
			path := filepath.Join(directory, "sql-open-observation.jsonl")
			switch kind {
			case "symlink":
				if err := os.Symlink(filepath.Join(directory, "absent"), path); err != nil {
					test.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					test.Fatal(err)
				}
			case "public-parent":
				if err := os.Chmod(directory, 0755); err != nil {
					test.Fatal(err)
				}
			case "symlink-parent":
				link := filepath.Join(test.TempDir(), "parent-link")
				if err := os.Symlink(directory, link); err != nil {
					test.Fatal(err)
				}
				directory = link
			}
			observation, err := startServiceSQLOpenObservation(context.Background(), directory, "1")
			if observation != nil || err == nil {
				test.Fatal("unsafe path accepted")
			}
		})
	}
}

type serviceObservationOutput struct {
	bytes.Buffer
	closed atomic.Bool
}

func (output *serviceObservationOutput) Close() error {
	output.closed.Store(true)
	return nil
}

func serviceObservationFixture() *serviceSQLOpenObservation {
	return &serviceSQLOpenObservation{observer: NewSQLOpenObserver(), stream: "synthetic_private_stream", stopping: make(chan struct{}), done: make(chan struct{})}
}

func TestServiceOpenObserverBoundedDrainPrivacy(test *testing.T) {
	observation := serviceObservationFixture()
	ctx := bindSQLOpenObservation(context.Background(), observation.observer, "synthetic_request")
	secret := "private-key namespace SQL raw-error must-not-appear"
	for count := 0; count < 100; count++ {
		observeSQLOpen(ctx, "synthetic_request", sqlOpenTransport, sqlOpenTransportFailure, &SQLRPCError{cause: errors.New(secret), Ambiguous: true}, "outcome_unknown")
	}
	output := &serviceObservationOutput{}
	go observation.consume(context.Background(), output)
	if !observation.stop() {
		test.Fatal("bounded drain did not stop")
	}
	if !output.closed.Load() || strings.Contains(output.String(), secret) {
		test.Fatal("consumer ownership or privacy violation")
	}
	decoder := json.NewDecoder(strings.NewReader(output.String()))
	count := 0
	for {
		var record serviceSQLOpenRecord
		if err := decoder.Decode(&record); err == io.EOF {
			break
		} else if err != nil {
			test.Fatal(err)
		}
		count++
		if record.Stream != observation.stream || record.CallSequence != 1 || record.RequestID != "synthetic_request" || !record.Ambiguous || record.ExecutionCompleted {
			test.Fatal("stream identity or original flags changed")
		}
	}
	if count != 64 {
		test.Fatalf("record budget violated: %d", count)
	}
}

type serviceObservationBlockingOutput struct {
	entered chan struct{}
	release chan struct{}
	closed  atomic.Bool
}

func (output *serviceObservationBlockingOutput) Write(payload []byte) (int, error) {
	select {
	case output.entered <- struct{}{}:
	default:
	}
	<-output.release
	return len(payload), nil
}

func (output *serviceObservationBlockingOutput) Close() error {
	output.closed.Store(true)
	return nil
}

func TestServiceOpenObserverBlockedWriteBoundedStop(test *testing.T) {
	observation := serviceObservationFixture()
	ctx := bindSQLOpenObservation(context.Background(), observation.observer, "synthetic_request")
	observeSQLOpen(ctx, "synthetic_request", sqlOpenGateway, sqlOpenReply, nil, "outcome_unknown")
	output := &serviceObservationBlockingOutput{entered: make(chan struct{}, 1), release: make(chan struct{})}
	defer func() {
		close(output.release)
		select {
		case <-observation.done:
		case <-time.After(time.Second):
			test.Error("released consumer did not close its own file")
		}
	}()
	go observation.consume(context.Background(), output)
	select {
	case <-output.entered:
	case <-time.After(time.Second):
		test.Fatal("consumer did not enter controlled write")
	}
	if observation.stop() || output.closed.Load() {
		test.Fatal("blocked IO falsely joined or was concurrently closed")
	}
}

type serviceObservationFailingOutput struct {
	closed atomic.Bool
}

func (output *serviceObservationFailingOutput) Write(payload []byte) (int, error) {
	return 0, errors.New("synthetic private write failure")
}

func (output *serviceObservationFailingOutput) Close() error {
	output.closed.Store(true)
	return nil
}

func TestServiceOpenObserverWriteFailureClosesOnlyConsumer(test *testing.T) {
	observation := serviceObservationFixture()
	ctx := bindSQLOpenObservation(context.Background(), observation.observer, "synthetic_request")
	observeSQLOpen(ctx, "synthetic_request", sqlOpenGateway, sqlOpenReply, nil, "outcome_unknown")
	output := &serviceObservationFailingOutput{}
	go observation.consume(context.Background(), output)
	if !observation.stop() || !output.closed.Load() {
		test.Fatal("failed writer was not closed by consumer")
	}
}

func TestServiceOpenObserverWrappedErrorFlags(test *testing.T) {
	observation := serviceObservationFixture()
	for flags := 0; flags < 8; flags++ {
		requestID := fmt.Sprintf("synthetic_flags_%d", flags)
		ctx := bindSQLOpenObservation(context.Background(), observation.observer, requestID)
		failure := &SQLRPCError{Ambiguous: flags&1 != 0, Rejected: flags&2 != 0, ExecutionCompleted: flags&4 != 0, cause: errors.New("synthetic private cause")}
		wrapped := fmt.Errorf("synthetic private wrapper: %w", failure)
		if returned := observeSQLOpenError(ctx, requestID, sqlOpenTransport, sqlOpenTransportFailure, wrapped); returned != wrapped {
			test.Fatal("observation replaced original error")
		}
	}
	output := &serviceObservationOutput{}
	go observation.consume(context.Background(), output)
	if !observation.stop() || !output.closed.Load() {
		test.Fatal("consumer did not drain and close")
	}
	decoder := json.NewDecoder(strings.NewReader(output.String()))
	for flags := 0; flags < 8; flags++ {
		var record serviceSQLOpenRecord
		if err := decoder.Decode(&record); err != nil {
			test.Fatal(err)
		}
		if record.Stream != observation.stream || record.CallSequence != uint64(flags+1) || record.RequestID != fmt.Sprintf("synthetic_flags_%d", flags) || record.Stage != "upstream" || record.Reason != "transport" || record.Classification != "" || !record.RPCError || record.Ambiguous != (flags&1 != 0) || record.Rejected != (flags&2 != 0) || record.ExecutionCompleted != (flags&4 != 0) {
			test.Errorf("wrapped error flags/correlation lost for flags=%d: %+v", flags, record)
		}
	}
	var stopped serviceSQLOpenRecord
	if err := decoder.Decode(&stopped); err != nil || stopped.Stream != observation.stream || stopped.Stage != "observer" || stopped.Reason != "bounded_stop" || stopped.Classification != "UNPROVEN" || stopped.RPCError || stopped.Ambiguous || stopped.Rejected || stopped.ExecutionCompleted {
		test.Fatalf("stop marker gained terminal proof: %+v error=%v", stopped, err)
	}
	var extra serviceSQLOpenRecord
	if err := decoder.Decode(&extra); err != io.EOF || strings.Contains(output.String(), "synthetic private") {
		test.Fatal("extra record or private cause leaked")
	}
}

func TestServiceOpenObserverContextCancellation(test *testing.T) {
	observation := serviceObservationFixture()
	ctx, cancel := context.WithCancel(context.Background())
	output := &serviceObservationOutput{}
	cancel()
	go observation.consume(ctx, output)
	select {
	case <-observation.done:
	case <-time.After(time.Second):
		test.Fatal("canceled consumer did not stop")
	}
	if !output.closed.Load() || !strings.Contains(output.String(), "UNPROVEN") {
		test.Fatal("cancellation did not retain incomplete diagnostic semantics")
	}
}
