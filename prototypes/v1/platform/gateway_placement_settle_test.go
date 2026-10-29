package platform

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

// placementSettleOverride shortens the open wait for one test. The package
// variable exists so a test does not have to wait out the production budget.
func placementSettleOverride(test *testing.T, timeout time.Duration) {
	test.Helper()
	previous := placementSettleTimeout
	placementSettleTimeout = timeout
	test.Cleanup(func() { placementSettleTimeout = previous })
}

// transientPlacementFixture returns a fixture whose first database sits in one
// placement phase while the gateway holds a controller, so an open may wait for
// convergence. The in-memory store accepts a pending phase without the durable
// pending request the production store demands; the gateway under test only
// reads the phase, and no reconcile loop runs in this fixture.
func transientPlacementFixture(test *testing.T, phase string) *gatewayFixture {
	test.Helper()
	fixture := newGatewayFixture(test)
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		for key, placement := range metadata.Placements {
			placement.TTLMS = 1000
			metadata.Placements[key] = placement
		}
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	reconciler, err := controller.NewController(fixture.placements, s02NewControlStub(), controller.ReconcileConfig{LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Millisecond})
	if err != nil {
		test.Fatal(err)
	}
	fixture.gateway.config.Controller = reconciler
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		placement.Phase = phase
		placement.SuspendReason = ""
		placement.WakePolicy = ""
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	return fixture
}

func gatewayFailureFields(test *testing.T, response *httptest.ResponseRecorder, failure any) {
	test.Helper()
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || len(envelope.Error) == 0 {
		test.Fatalf("invalid failure envelope: %s", response.Body.String())
	}
	if err := json.Unmarshal(envelope.Error, failure); err != nil {
		test.Fatal(err)
	}
}

// TestPlacementRefusalAfterPermitCountsAsAdmittedAndRejected pins the account of
// a request the placement refused after admission. Before this, the refused
// request was neither admitted nor rejected in the counters, and the client got
// a 409 that told it not to retry a placement the controller was already
// converging.
func TestPlacementRefusalAfterPermitCountsAsAdmittedAndRejected(test *testing.T) {
	placementSettleOverride(test, 10*time.Millisecond)
	fixture := transientPlacementFixture(test, "grant_pending")
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if response.Code != http.StatusConflict || gatewayCode(test, response) != "placement_unavailable" {
		test.Fatalf("transient placement was not answered as a placement refusal: %d %s", response.Code, response.Body.String())
	}
	var failure struct {
		Code         string `json:"code"`
		RetryAllowed *bool  `json:"retry_allowed"`
	}
	gatewayFailureFields(test, response, &failure)
	if failure.RetryAllowed == nil || !*failure.RetryAllowed {
		test.Fatalf("transient placement did not publish an actionable retry: %s", response.Body.String())
	}
	if gatewayMetricCount(test, fixture.gateway, "admitted_requests_total") != 1 {
		test.Fatal("a request that held a permit was not counted as admitted")
	}
	assertRefusedReason(test, fixture.gateway, rejectionPlacement)
	if fixture.gateway.Metrics()["active_permits"] != 0 {
		test.Fatal("a placement refusal kept the admission permit")
	}
	if fixture.upstream.count("open_session") != 0 {
		test.Fatal("a placement refusal reached the worker")
	}
}

// TestOpenWaitsForTransientPlacementTransition pins the intended fix: when the
// controller is already converging the placement, the open waits for it instead
// of answering a 409 the client cannot retry.
func TestOpenWaitsForTransientPlacementTransition(test *testing.T) {
	fixture := transientPlacementFixture(test, "grant_pending")
	settled := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		settled <- fixture.placements.Update(func(metadata *controller.Metadata) error {
			// Only the phase was mutated above, so restoring it restores the
			// initial active placement the fixture was built with.
			placement := metadata.Placements[fixture.first.Key()]
			placement.Phase = "active"
			metadata.Placements[fixture.first.Key()] = placement
			return nil
		})
	}()
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if err := <-settled; err != nil {
		test.Fatal(err)
	}
	if response.Code != http.StatusOK {
		test.Fatalf("open did not wait for the converging placement: %d %s", response.Code, response.Body.String())
	}
	if fixture.upstream.count("open_session") != 1 {
		test.Fatalf("open dispatched %d sessions", fixture.upstream.count("open_session"))
	}
}

// TestAdministratorSuspensionStillFailsWithoutWaiting keeps the retry promise
// narrow: an operator's suspension is not a convergence the controller may
// perform, so the open answers immediately and publishes no retry. Only a
// transient grant phase earns the promise.
func TestAdministratorSuspensionStillFailsWithoutWaiting(test *testing.T) {
	fixture := s02NewActivationFixture(test, "suspended", controller.SuspendReasonAdmin, controller.WakePolicyManual, "suspended", 0, time.Second)
	started := time.Now()
	response := gatewayHTTP(test, fixture.gateway, fixture.base.tokenA, "/v1/sql/open", map[string]any{"database": fixture.database})
	elapsed := time.Since(started)
	if response.Code != http.StatusConflict || gatewayCode(test, response) != "placement_unavailable" {
		test.Fatalf("administrator suspension answered %d %s", response.Code, response.Body.String())
	}
	var failure struct {
		RetryAllowed *bool `json:"retry_allowed"`
	}
	gatewayFailureFields(test, response, &failure)
	if failure.RetryAllowed != nil && *failure.RetryAllowed {
		test.Fatalf("administrator suspension promised a retry: %s", response.Body.String())
	}
	if elapsed >= time.Second {
		test.Fatalf("administrator suspension waited %s for a convergence it does not have", elapsed)
	}
	grants, acquires := fixture.control.counts()
	if grants != 0 || acquires != 0 {
		test.Fatalf("an administrator suspension was activated: grants=%d acquires=%d", grants, acquires)
	}

	test.Run("transient grant phase is the retryable class", func(test *testing.T) {
		placementSettleOverride(test, 10*time.Millisecond)
		transient := transientPlacementFixture(test, "grant_pending")
		response := gatewayHTTP(test, transient.gateway, transient.tokenA, "/v1/sql/open", map[string]any{"database": transient.first})
		if response.Code != http.StatusConflict || gatewayCode(test, response) != "placement_unavailable" {
			test.Fatalf("transient grant phase answered %d %s", response.Code, response.Body.String())
		}
		var failure struct {
			RetryAllowed *bool `json:"retry_allowed"`
		}
		gatewayFailureFields(test, response, &failure)
		if failure.RetryAllowed == nil || !*failure.RetryAllowed {
			test.Fatalf("transient grant phase did not publish a retry: %s", response.Body.String())
		}
	})
}

// TestPlacementMetadataFailurePublishes503 keeps a store outage out of the
// refusal account. Nothing about this database was refused, and a caller that
// reads it as a placement decision would wait for a convergence that is not
// happening.
func TestPlacementMetadataFailurePublishes503(test *testing.T) {
	fixture := newGatewayFixture(test)
	store := &gatewayRenewalSnapshots{MetadataStore: fixture.placements}
	store.unavailable.Store(true)
	fixture.gateway.config.Placements = store
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if response.Code != http.StatusServiceUnavailable || gatewayCode(test, response) != "metadata_unavailable" {
		test.Fatalf("metadata outage answered %d %s", response.Code, response.Body.String())
	}
	var failure struct {
		RetryAllowed *bool `json:"retry_allowed"`
	}
	gatewayFailureFields(test, response, &failure)
	if failure.RetryAllowed == nil || !*failure.RetryAllowed {
		test.Fatalf("metadata outage did not publish a retry: %s", response.Body.String())
	}
	if rejected := gatewayMetricCount(test, fixture.gateway, "rejected_requests_total"); rejected != 0 {
		test.Fatalf("metadata outage counted as a refusal: %d", rejected)
	}
	if reasons := gatewayMetricCounts(test, fixture.gateway, "rejection_reasons_total"); len(reasons) != 0 {
		test.Fatalf("metadata outage blamed a refusing layer: %v", reasons)
	}
	if errors := gatewayMetricCounts(test, fixture.gateway, "errors_total"); errors["metadata_unavailable"] != 1 {
		test.Fatalf("metadata outage was not counted as an error: %v", errors)
	}
}

// TestHranaPipelinePublishesTransientPlacementAsRetryable covers the adapter
// half: the pipeline error frame must carry the platform's retry promise,
// because a Hrana client acts on retryable and never sees the platform envelope.
func TestHranaPipelinePublishesTransientPlacementAsRetryable(test *testing.T) {
	placementSettleOverride(test, 10*time.Millisecond)
	fixture := transientPlacementFixture(test, "grant_pending")
	body := `{"requests":[{"type":"batch","batch":{"steps":[{"stmt":{"sql":"SELECT 1","want_rows":true}}]}}]}`
	request := httptest.NewRequest(http.MethodPost, "/db/tenant-a/db-a/main/v2/pipeline", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newHranaHTTP(fixture.gateway).ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		test.Fatalf("transient placement answered %d %s", response.Code, response.Body.String())
	}
	var failure struct {
		Error struct {
			Code         string `json:"code"`
			Retryable    bool   `json:"retryable"`
			RetryAllowed bool   `json:"retry_allowed"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
		test.Fatal(err)
	}
	if failure.Error.Code != "placement_unavailable" || !failure.Error.Retryable || !failure.Error.RetryAllowed {
		test.Fatalf("pipeline error did not publish the retry promise: %s", response.Body.String())
	}
	if fixture.upstream.count("open_session") != 0 {
		test.Fatal("a refused pipeline opened a worker session")
	}
}
