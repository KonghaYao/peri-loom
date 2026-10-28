package platform

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func nativeWakeManagement(fixture *s02ActivationFixture) *Management {
	return &Management{config: ManagementConfig{
		State:      fixture.base.state,
		Controller: fixture.gateway.config.Controller,
		Placements: fixture.store,
		Identity:   &nativeDiagnosticIdentity{},
		Upstream:   fixture.base.upstream,
		Resources:  fixture.gateway,
	}}
}

func TestManagementNativeDiagnosticPhaseKeepsIdle(t *testing.T) {
	for _, phase := range []string{"idle", "idle_drain"} {
		if got := nativeDiagnosticPhase(phase); got != phase {
			t.Fatalf("settled idle phase was logged as %q", got)
		}
	}
}

func TestManagementNativeViewWakesIdleDatabase(t *testing.T) {
	fixture := s02NewActivationFixture(t, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 0, 10*time.Second)
	management := nativeWakeManagement(fixture)
	result, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "wake-schema", Database: fixture.database}, "/v1/databases/schema")
	if err != nil {
		t.Fatalf("idle schema read did not wake the database: %v", err)
	}
	if result == nil {
		t.Fatal("wake path returned no schema result")
	}
	grants, acquires := fixture.control.counts()
	if grants != 1 || acquires != 1 {
		t.Fatalf("schema wake started %d grants and %d acquires", grants, acquires)
	}
	fixture.waitActive(t)
	if fixture.base.upstream.count("open_session") != 1 || fixture.base.upstream.count("query") != 1 || fixture.base.upstream.count("close_session") != 1 {
		t.Fatal("wake path changed the open/query/close sequence")
	}
	if management.nativeDiagnostics.count != 0 {
		t.Fatal("successful wake emitted an error diagnostic")
	}
}

func TestManagementNativeViewDoesNotWakeSuspendedDatabase(t *testing.T) {
	fixture := s02NewActivationFixture(t, "suspended", controller.SuspendReasonAdmin, controller.WakePolicyManual, "suspended", 0, 10*time.Second)
	management := nativeWakeManagement(fixture)
	_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "wake-suspended", Database: fixture.database}, "/v1/databases/schema")
	if err == nil || err.Error() != "database is not active" {
		t.Fatalf("administrator suspend changed its refusal: %v", err)
	}
	if grants, acquires := fixture.control.counts(); grants != 0 || acquires != 0 {
		t.Fatal("administrator suspend was woken by a metadata read")
	}
	if fixture.base.upstream.count("open_session") != 0 {
		t.Fatal("refused suspended database opened a session")
	}
}

func TestManagementNativeNonViewNeverWakesIdleDatabase(t *testing.T) {
	fixture := s02NewActivationFixture(t, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 0, 10*time.Second)
	management := nativeWakeManagement(fixture)
	_, err := management.nativeTool(context.Background(), Principal{}, managementRequest{RequestID: "wake-checkpoint", Database: fixture.database}, "/v1/databases/checkpoint")
	if err == nil || err.Error() != "database is not active" {
		t.Fatalf("non-view tool changed its refusal: %v", err)
	}
	if grants, acquires := fixture.control.counts(); grants != 0 || acquires != 0 {
		t.Fatal("checkpoint woke an idle database")
	}
	if fixture.base.upstream.count("checkpoint") != 0 {
		t.Fatal("refused checkpoint reached the worker")
	}
}

func TestManagementNativeViewWakeTimeoutKeepsOriginalStage(t *testing.T) {
	fixture := s02NewActivationFixture(t, "idle", controller.SuspendReasonIdle, controller.WakePolicyAuto, "active", 0, 10*time.Second)
	fixture.control.acquireGate = make(chan struct{})
	fixture.control.acquireEntered = make(chan struct{})
	management := nativeWakeManagement(fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	failures := make(chan error, 1)
	go func() {
		_, err := management.nativeTool(ctx, Principal{}, managementRequest{RequestID: "wake-timeout", Database: fixture.database}, "/v1/databases/schema")
		failures <- err
	}()
	select {
	case <-fixture.control.acquireEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("wake never reached the worker acquire")
	}
	err := <-failures
	if err == nil || !strings.Contains(err.Error(), "database wake did not complete") {
		t.Fatalf("wake timeout returned a different failure: %v", err)
	}
	var failure *nativeToolError
	if !errors.As(err, &failure) || failure.stage != "placement" {
		t.Fatalf("wake timeout reported the wrong stage: %v", err)
	}
	if status, code := managementFailure(err); status != 409 || code != "operation_incomplete" {
		t.Fatalf("wake timeout changed the public failure: %d %s", status, code)
	}
	if management.nativeDiagnostics.count != 1 {
		t.Fatal("wake timeout did not emit exactly one diagnostic")
	}
	state, err := fixture.base.state.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Operations) != 0 {
		t.Fatal("a wake registered a management operation")
	}
	close(fixture.control.acquireGate)
	fixture.waitActive(t)
}
