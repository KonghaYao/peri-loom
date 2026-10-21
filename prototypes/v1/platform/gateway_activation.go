package platform

import (
	"context"
	"errors"
	"net/http"
	"time"

	controller "peri-loom/controller"
)

// activationWaitBudget is how long an open request waits for a wake before it
// publishes the durable activation identity. The activation itself keeps going
// in the controller; the HTTP request must leave before the public write
// deadline, and a shorter wait leaves room for the worker open that follows.
func (gateway *Gateway) activationWaitBudget() time.Duration {
	if gateway.config.WakeWaitBudget > 0 {
		return gateway.config.WakeWaitBudget
	}
	timeout := gateway.config.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return min(max(timeout/2, time.Second), 15*time.Second)
}

// awaitActivation wakes a database that was released while idle. It returns a
// result only when the request must answer without opening a session; nil
// means the caller may route again (either no wake was needed or the
// activation completed).
func (gateway *Gateway) awaitActivation(ctx context.Context, database controller.DatabaseID) *gatewayResult {
	if gateway.config.Controller == nil {
		return nil
	}
	placement, _, found, err := controller.ReadRouting(gateway.config.Placements, database)
	if err != nil || !found || !placement.AutoWakeable() {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, gateway.activationWaitBudget())
	defer cancel()
	status, wakeErr := gateway.config.Controller.Wake(waitCtx, database)
	if wakeErr == nil || status.State == controller.ActivationActive {
		return nil
	}
	if ctx.Err() != nil {
		return &gatewayResult{status: http.StatusServiceUnavailable, code: "request_cancelled"}
	}
	if !errors.Is(wakeErr, context.DeadlineExceeded) && !errors.Is(wakeErr, context.Canceled) {
		// A refused or failed activation is not a loading state: let the caller
		// publish the ordinary placement refusal.
		return nil
	}
	// This is an accepted activation, not an opened session. The operation ID
	// is the controller's single-flight identity, so a retry joins the same
	// activation instead of starting another.
	value := map[string]any{
		"loading":      true,
		"state":        status.State,
		"operation_id": status.OperationID,
		"database":     database.Key(),
		"waiters":      status.Waiters,
	}
	if status.Epoch > 0 {
		value["epoch"] = status.Epoch
	}
	return &gatewayResult{
		status:            http.StatusAccepted,
		value:             value,
		operationID:       status.OperationID,
		operationStage:    "activation",
		operationDatabase: database,
	}
}
