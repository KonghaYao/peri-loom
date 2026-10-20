package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Controller struct {
	store           MetadataStore
	transport       ControlTransport
	cfg             ReconcileConfig
	operations      chan struct{}
	reconciliations chan struct{}
	lanesMutex      sync.Mutex
	lanes           map[string]*databaseLane
	scheduleMutex   sync.Mutex
	renewals        map[string]time.Time
	// wakes merges concurrent request-driven activations by database identity.
	// The map lock only guards lookup and removal; the call owns its own state.
	wakeMutex sync.Mutex
	wakes     map[string]*wakeCall
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
	if cfg.MaxConcurrentOperations == 0 {
		cfg.MaxConcurrentOperations = 8
	}
	if cfg.ReconcileConcurrency == 0 {
		cfg.ReconcileConcurrency = 8
	}
	if cfg.ControlRPCTimeout == 0 {
		// Half a lease leaves room for the state read and the worker update
		// inside one lease period; the cap keeps a large production TTL from
		// letting one slow control call hold a reconcile worker for a minute.
		cfg.ControlRPCTimeout = min(max(cfg.LeaseTTL/2, 100*time.Millisecond), 2*time.Second)
	}
	if cfg.ActivationTimeout == 0 {
		// A wake may have to replay a WAL before acquire returns. The worker's
		// own load floor is 60s, so the activation budget outlives it while the
		// HTTP waiter keeps its own, shorter budget.
		cfg.ActivationTimeout = min(max(2*cfg.LeaseTTL, 90*time.Second), 10*time.Minute)
	}
	if cfg.LeaseTTL < 100*time.Millisecond || cfg.LeaseTTL > 60*time.Second || cfg.LeaseTTL%time.Millisecond != 0 || cfg.HeartbeatTimeout < time.Millisecond || cfg.Interval < time.Millisecond {
		return nil, errors.New("invalid reconciliation timing")
	}
	if cfg.MaxConcurrentOperations < 1 || cfg.MaxConcurrentOperations > 64 || cfg.ReconcileConcurrency < 1 || cfg.ReconcileConcurrency > 64 || cfg.ControlRPCTimeout < time.Millisecond || cfg.ControlRPCTimeout > 60*time.Second || cfg.ActivationTimeout < time.Millisecond || cfg.ActivationTimeout > 10*time.Minute {
		return nil, errors.New("invalid reconciliation concurrency or control deadline")
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
	return &Controller{
		store:           store,
		transport:       transport,
		cfg:             cfg,
		operations:      make(chan struct{}, cfg.MaxConcurrentOperations),
		reconciliations: make(chan struct{}, cfg.ReconcileConcurrency),
		lanes:           make(map[string]*databaseLane),
		renewals:        make(map[string]time.Time),
		wakes:           make(map[string]*wakeCall),
	}, nil
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
	if err := database.Validate(); err != nil {
		return Placement{}, err
	}
	release, err := controller.enterDatabaseOperation(ctx, database.Key())
	if err != nil {
		return Placement{}, err
	}
	defer release()
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
		if placement.Phase == "idle" {
			// A create retry is an explicit management action, so it may
			// activate an idle database. A request is the only caller that
			// has to respect the manual policy.
			return controller.activateLocked(ctx, placement)
		}
		if err := controller.pollWorkers(ctx); err != nil {
			return placement, err
		}
		placement, err = controller.converge(ctx, placement)
		if err != nil {
			return placement, err
		}
		if err := controller.reconcileActive(ctx, &placement, nil); err != nil {
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
	if err := database.Validate(); err != nil {
		return Placement{}, err
	}
	release, err := controller.enterDatabaseOperation(ctx, database.Key())
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
	if err := database.Validate(); err != nil {
		return err
	}
	release, err := controller.enterDatabaseOperation(ctx, database.Key())
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
	if placement.Phase == "idle" {
		// An idle database has already released its lease. Marking it
		// administrative keeps the durable reason ahead of any queued request.
		placement.Phase = "suspended"
		placement.DesiredState = "suspended"
		placement.SuspendReason = SuspendReasonAdmin
		placement.WakePolicy = WakePolicyManual
		placement.IdleSinceMS = 0
		return controller.save(placement)
	}
	if placement.Phase != "active" {
		return errors.New("placement not active")
	}
	placement.DesiredState = "suspended"
	placement.SuspendReason = SuspendReasonAdmin
	placement.WakePolicy = WakePolicyManual
	placement.IdleSinceMS = 0
	placement.Phase = "suspend_drain"
	if err := controller.save(placement); err != nil {
		return err
	}
	_, err = controller.converge(ctx, placement)
	return err
}

func (controller *Controller) Resume(ctx context.Context, database DatabaseID) (Placement, error) {
	if err := database.Validate(); err != nil {
		return Placement{}, err
	}
	release, err := controller.enterDatabaseOperation(ctx, database.Key())
	if err != nil {
		return Placement{}, err
	}
	defer release()
	placement, _, exists, err := ReadRouting(controller.store, database)
	if err != nil {
		return placement, err
	}
	if !exists {
		return Placement{}, ErrPlacementMissing
	}
	// Resume is an explicit management action: it bypasses the wake policy and
	// shares the same activation path as an automatic wake. Both take the
	// database lane, so an administrator Resume racing a Waking request cannot
	// mint two writer grants.
	return controller.activateLocked(ctx, placement)
}

func (controller *Controller) converge(ctx context.Context, placement Placement) (Placement, error) {
	initial := placement.Phase
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
		case "idle_drain":
			// The worker already dropped the instance; only the lease is left,
			// so there is no drain RPC to wait for.
			err = controller.planRevoke(&placement)
		case "active", "suspended", "idle":
			controller.recordSettledPlacement(placement, initial)
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
			controller.recordSettledPlacement(placement, initial)
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

// clearSuspension drops the idle/admin metadata once the placement is serving
// again. Keeping it on an active record would let a later idle transition
// inherit an administrator's manual policy.
func clearSuspension(placement *Placement) {
	placement.SuspendReason = ""
	placement.WakePolicy = ""
	placement.IdleSinceMS = 0
}

func validatePlacement(key string, placement Placement) error {
	if err := placement.Database.Validate(); err != nil {
		return err
	}
	if key != placement.Database.Key() || placement.DesiredWorker == "" || (placement.Role != "writer" && placement.Role != "read_only") || (placement.DesiredState != "active" && placement.DesiredState != "suspended") || placement.TTLMS < 100 || placement.TTLMS > 60000 {
		return errors.New("invalid placement metadata")
	}
	switch placement.SuspendReason {
	case "", SuspendReasonAdmin, SuspendReasonIdle, SuspendReasonFailure:
	default:
		return errors.New("invalid suspend reason")
	}
	switch placement.WakePolicy {
	case "", WakePolicyAuto, WakePolicyManual:
	default:
		return errors.New("invalid wake policy")
	}
	if placement.WakePolicy == WakePolicyAuto && placement.SuspendReason != SuspendReasonIdle || placement.WakePolicy == WakePolicyManual && placement.SuspendReason == "" || placement.IdleSinceMS < 0 {
		return errors.New("wake policy contradicts suspend reason")
	}
	if placement.Phase == "idle" && (placement.SuspendReason != SuspendReasonIdle || placement.WakePolicy != WakePolicyAuto) {
		return errors.New("idle placement must be auto-wakeable")
	}
	switch placement.Phase {
	case "active", "suspended", "migrate_drain", "suspend_drain", "idle", "idle_drain", "acquire_pending", "renew_worker_pending":
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
