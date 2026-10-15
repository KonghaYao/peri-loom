package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Controller struct {
	store      MetadataStore
	transport  ControlTransport
	cfg        ReconcileConfig
	operations chan struct{}
}

var ErrPlacementMissing = errors.New("placement missing")

func NewController(store MetadataStore, transport ControlTransport, cfg ReconcileConfig) (*Controller, error) {
	if store == nil || transport == nil {
		return nil, errors.New("metadata store and transport required")
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 5 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 5 * time.Second
	}
	if cfg.Interval == 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.LeaseTTL < 100*time.Millisecond || cfg.LeaseTTL > 60*time.Second || cfg.LeaseTTL%time.Millisecond != 0 || cfg.HeartbeatTimeout < time.Millisecond || cfg.Interval < time.Millisecond {
		return nil, errors.New("invalid reconciliation timing")
	}
	metadata, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	for key, placement := range metadata.Placements {
		if err := validatePlacement(key, placement); err != nil {
			return nil, err
		}
	}
	return &Controller{store: store, transport: transport, cfg: cfg, operations: make(chan struct{}, 1)}, nil
}

func (controller *Controller) enter(ctx context.Context) (func(), error) {
	select {
	case controller.operations <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-controller.operations
			return nil, err
		}
		return func() { <-controller.operations }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (controller *Controller) Status(database DatabaseID) (Placement, error) {
	if err := database.Validate(); err != nil {
		return Placement{}, err
	}
	placement, _, exists, err := ReadRouting(controller.store, database)
	if err != nil {
		return Placement{}, err
	}
	if !exists {
		return Placement{}, ErrPlacementMissing
	}
	return placement, nil
}

func (controller *Controller) save(placement Placement) error {
	if err := validatePlacement(placement.Database.Key(), placement); err != nil {
		return err
	}
	return controller.store.Update(func(metadata *Metadata) error {
		if metadata.Placements == nil {
			metadata.Placements = make(map[string]Placement)
		}
		metadata.Placements[placement.Database.Key()] = placement
		return nil
	})
}

func (controller *Controller) Place(ctx context.Context, database DatabaseID, region, role string) (Placement, error) {
	release, err := controller.enter(ctx)
	if err != nil {
		return Placement{}, err
	}
	defer release()
	if err := database.Validate(); err != nil {
		return Placement{}, err
	}
	if role != "writer" && role != "read_only" {
		return Placement{}, errors.New("invalid role")
	}
	placement, statusErr := controller.Status(database)
	if statusErr == nil {
		if placement.Role != role || placement.Region != region {
			return placement, errors.New("existing placement policy differs")
		}
		if placement.DesiredState != "active" {
			return placement, errors.New("resume suspended placement explicitly")
		}
		if err := controller.pollWorkers(ctx); err != nil {
			return placement, err
		}
		placement, err = controller.converge(ctx, placement)
		if err != nil {
			return placement, err
		}
		if err := controller.reconcileActive(ctx, &placement); err != nil {
			return placement, err
		}
		return controller.Status(database)
	}
	if !errors.Is(statusErr, ErrPlacementMissing) {
		return Placement{}, statusErr
	}
	if err := controller.pollWorkers(ctx); err != nil {
		return Placement{}, err
	}
	worker, err := controller.selectWorker(region, "")
	if err != nil {
		return Placement{}, err
	}
	placement = Placement{Database: database, DesiredWorker: worker.ID, Region: region, TTLMS: uint64(controller.cfg.LeaseTTL.Milliseconds()), DesiredState: "active", Role: role}
	if err := controller.planGrant(ctx, &placement); err != nil {
		return placement, err
	}
	return controller.converge(ctx, placement)
}

func (controller *Controller) Migrate(ctx context.Context, database DatabaseID, workerID string) (Placement, error) {
	release, err := controller.enter(ctx)
	if err != nil {
		return Placement{}, err
	}
	defer release()
	placement, err := controller.Status(database)
	if err != nil {
		return placement, err
	}
	placement, err = controller.converge(ctx, placement)
	if err != nil {
		return placement, err
	}
	if placement.Phase != "active" {
		return placement, errors.New("placement not active")
	}
	if err := controller.pollWorkers(ctx); err != nil {
		return placement, err
	}
	if _, err := controller.eligibleWorker(workerID, placement.Region); err != nil {
		return placement, err
	}
	if workerID == placement.DesiredWorker {
		return placement, nil
	}
	placement.DesiredWorker = workerID
	placement.Phase = "migrate_drain"
	if err := controller.save(placement); err != nil {
		return placement, err
	}
	return controller.converge(ctx, placement)
}

func (controller *Controller) Suspend(ctx context.Context, database DatabaseID) error {
	release, err := controller.enter(ctx)
	if err != nil {
		return err
	}
	defer release()
	placement, err := controller.Status(database)
	if err != nil {
		return err
	}
	placement, err = controller.converge(ctx, placement)
	if err != nil {
		return err
	}
	if placement.Phase == "suspended" {
		return nil
	}
	if placement.Phase != "active" {
		return errors.New("placement not active")
	}
	placement.DesiredState = "suspended"
	placement.Phase = "suspend_drain"
	if err := controller.save(placement); err != nil {
		return err
	}
	_, err = controller.converge(ctx, placement)
	return err
}

func (controller *Controller) Resume(ctx context.Context, database DatabaseID) (Placement, error) {
	release, err := controller.enter(ctx)
	if err != nil {
		return Placement{}, err
	}
	defer release()
	placement, err := controller.Status(database)
	if err != nil {
		return placement, err
	}
	placement, err = controller.converge(ctx, placement)
	if err != nil {
		return placement, err
	}
	if placement.Phase == "active" {
		return placement, nil
	}
	if placement.Phase != "suspended" {
		return placement, errors.New("suspension not settled")
	}
	if err := controller.pollWorkers(ctx); err != nil {
		return placement, err
	}
	worker, err := controller.selectWorker(placement.Region, "")
	if err != nil {
		return placement, err
	}
	placement.DesiredWorker = worker.ID
	placement.DesiredState = "active"
	if err := controller.planGrant(ctx, &placement); err != nil {
		return placement, err
	}
	return controller.converge(ctx, placement)
}

func (controller *Controller) converge(ctx context.Context, placement Placement) (Placement, error) {
	for transition := 0; transition < 16; transition++ {
		if err := ctx.Err(); err != nil {
			return placement, err
		}
		previous := placement.Phase
		var err error
		switch placement.Phase {
		case "grant_intent", "renew_intent":
			err = controller.challenge(ctx, &placement)
		case "grant_pending", "renew_pending", "revoke_pending":
			err = controller.mutateAuthority(ctx, &placement)
		case "acquire_pending", "renew_worker_pending":
			err = controller.applyWorker(ctx, &placement)
		case "migrate_drain", "suspend_drain":
			err = controller.drain(ctx, &placement)
		case "active", "suspended":
			return placement, nil
		default:
			err = errors.New("unknown placement phase")
		}
		if err != nil {
			placement.LastError = "reconciliation incomplete"
			if saveErr := controller.save(placement); saveErr != nil {
				return placement, saveErr
			}
			return placement, err
		}
		placement.LastError = ""
		if placement.Phase == previous {
			return placement, nil
		}
	}
	return placement, fmt.Errorf("transition limit exceeded")
}

func clearPending(placement *Placement) {
	placement.PendingID = ""
	placement.PendingCommand = nil
	placement.LastError = ""
}

func validatePlacement(key string, placement Placement) error {
	if err := placement.Database.Validate(); err != nil {
		return err
	}
	if key != placement.Database.Key() || placement.DesiredWorker == "" || (placement.Role != "writer" && placement.Role != "read_only") || (placement.DesiredState != "active" && placement.DesiredState != "suspended") || placement.TTLMS < 100 || placement.TTLMS > 60000 {
		return errors.New("invalid placement metadata")
	}
	switch placement.Phase {
	case "active", "suspended", "migrate_drain", "suspend_drain", "acquire_pending", "renew_worker_pending":
		if placement.Grant == nil {
			return errors.New("placement grant missing")
		}
	case "grant_intent", "grant_pending", "renew_intent", "renew_pending", "revoke_pending":
		if placement.PendingID == "" || !json.Valid(placement.PendingCommand) {
			return errors.New("durable pending request missing")
		}
	default:
		return errors.New("invalid placement phase")
	}
	return validatePlacementProtocol(placement)
}
