package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

func (management *Management) createDatabase(ctx context.Context, principal Principal, input managementRequest) (any, error) {
	if input.Database.Validate() != nil || input.Database.Branch != "main" {
		return nil, errors.New("database creation denied")
	}
	if principal.Role != "admin" && principal.Role != "developer" || principal.Project != "*" && principal.Project != input.Project {
		return nil, errManagementForbidden
	}
	if input.Role == "" {
		input.Role = "writer"
	}
	if input.Role != "writer" {
		return nil, errors.New("new encrypted namespace requires writer initialization")
	}
	database := Database{ID: input.Database, Project: input.Project, State: "provisioning", Encrypted: true, OperationID: input.RequestID}
	var project Project
	err := management.config.State.Update(func(state *State) error {
		var exists bool
		project, exists = state.Projects[input.Project]
		if !exists || project.Tenant != database.ID.Namespace.Tenant {
			return errors.New("project namespace mismatch")
		}
		if existing, exists := state.Databases[database.ID.Key()]; exists {
			if existing.Project != database.Project || existing.OperationID != database.OperationID || existing.State == "deleted" {
				return errors.New("database identity already reserved")
			}
			return nil
		}
		for _, existing := range state.Databases {
			if existing.ID.Namespace == database.ID.Namespace {
				return errors.New("namespace permanently reserved")
			}
		}
		count := 0
		for _, existing := range state.Databases {
			if existing.Project == project.ID && existing.State != "deleted" {
				count++
			}
		}
		if count >= project.Quotas.MaxDatabases {
			return errDatabaseQuotaExceeded
		}
		state.Databases[database.ID.Key()] = database
		return nil
	})
	if err != nil {
		return nil, err
	}
	if _, err := management.config.Keys.Provision(ctx, database.ID.Namespace, input.RequestID); err != nil {
		return nil, err
	}
	placement, err := management.config.Controller.Place(ctx, database.ID, project.Region, input.Role)
	if err != nil {
		return nil, err
	}
	if err := management.setDatabaseState(database.ID, "active"); err != nil {
		return nil, err
	}
	return placement, nil
}

func (management *Management) setDatabaseState(database controller.DatabaseID, desired string) error {
	placement, placementErr := management.config.Controller.Status(database)
	if desired == "deleted" {
		unlock := lockOperationNamespace(database.Namespace)
		defer unlock()
	}
	return management.config.State.Update(func(state *State) error {
		entry, exists := state.Databases[database.Key()]
		if !exists || entry.State == "deleted" && desired != "deleted" {
			return errors.New("database identity unavailable")
		}
		entry.State = desired
		if desired == "deleted" {
			if err := removeKnownDatabaseOperations(state, database); err != nil {
				return err
			}
		}
		if placementErr == nil && placement.Grant != nil && desired != "deleted" {
			entry.RootIncarnation = placement.Grant.Binding.RootIncarnation
			entry.BranchIncarnation = placement.Grant.Binding.BranchIncarnation
		}
		state.Databases[database.Key()] = entry
		return nil
	})
}

func (management *Management) placement(ctx context.Context, principal Principal, input managementRequest, path string) (any, error) {
	if err := management.config.Identity.Authorize(principal, input.Database, "manage"); err != nil {
		return nil, err
	}
	switch path {
	case "/v1/databases/suspend":
		if err := management.config.Controller.Suspend(ctx, input.Database); err != nil {
			return nil, err
		}
		if err := management.setDatabaseState(input.Database, "suspended"); err != nil {
			return nil, err
		}
	case "/v1/databases/resume":
		if _, err := management.config.Controller.Resume(ctx, input.Database); err != nil {
			return nil, err
		}
		if err := management.setDatabaseState(input.Database, "active"); err != nil {
			return nil, err
		}
	case "/v1/databases/migrate":
		return management.config.Controller.Migrate(ctx, input.Database, input.WorkerID)
	}
	return management.config.Controller.Status(input.Database)
}

func (management *Management) deleteDatabase(ctx context.Context, principal Principal, input managementRequest) (any, error) {
	if err := management.config.Identity.Authorize(principal, input.Database, "manage"); err != nil {
		return nil, err
	}
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	if err := knownDatabaseDeletionPermitted(state, input.Database); err != nil {
		return nil, err
	}
	if err := management.config.Controller.Suspend(ctx, input.Database); err != nil {
		return nil, err
	}
	result, err := management.authorityMaintenance(ctx, input.Database, input.RequestID, map[string]any{"operation": "drop_branch"})
	if err != nil {
		return nil, err
	}
	if err := management.setDatabaseState(input.Database, "deleted"); err != nil {
		return nil, err
	}
	return result, nil
}

func (management *Management) fork(ctx context.Context, principal Principal, input managementRequest, path string) (any, error) {
	if err := management.config.Identity.Authorize(principal, input.Database, "manage"); err != nil {
		return nil, err
	}
	// Validate everything decidable before touching durable state. In
	// particular an invalid role or a missing snapshot must not reserve a
	// descriptor, quota, or an authority branch.
	plan, err := newForkPlan(input, path)
	if err != nil {
		return nil, err
	}
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	parent, exists := state.Databases[plan.parent.Key()]
	if !exists || parent.State == "deleted" {
		return nil, errors.New("source database missing")
	}
	project, exists := state.Projects[parent.Project]
	if !exists {
		return nil, errors.New("project missing")
	}
	plan.project = parent.Project
	plan.digest = plan.canonicalDigest()
	record, err := management.reserveFork(plan)
	if err != nil {
		return nil, err
	}
	if record.Stage == provisioningActive {
		return management.config.Controller.Status(plan.child)
	}
	if record.Stage == provisioningReserved || record.Stage == provisioningUncertain {
		if _, err := management.authorityMaintenance(ctx, plan.parent, plan.requestID, plan.authorityOperation()); err != nil {
			// A lost acknowledgement proves nothing. Ask the catalog before
			// releasing: absence proves the synchronous branch/clone never
			// committed, while presence lets the workflow continue.
			applied, known := false, provisioningNotExecuted(err)
			if !known {
				applied, known = management.forkAuthorityApplied(ctx, plan)
			}
			if !known || !applied {
				if known {
					management.recordProvisioningFailure(plan, provisioningFailed, provisioningErrorAuthorityRejected, true)
				} else {
					management.recordProvisioningFailure(plan, provisioningUncertain, provisioningErrorAuthorityUncertain, false)
				}
				return nil, err
			}
		}
		if err := management.setProvisioningStage(plan, provisioningAuthorityApplied, ""); err != nil {
			return nil, err
		}
	}
	placement, err := management.config.Controller.Place(ctx, plan.child, project.Region, plan.role)
	if err != nil {
		management.recordProvisioningFailure(plan, provisioningAuthorityApplied, provisioningErrorPlacement, false)
		return nil, err
	}
	if err := management.setProvisioningStage(plan, provisioningPlacementReady, ""); err != nil {
		return nil, err
	}
	if err := management.setDatabaseState(plan.child, "active"); err != nil {
		management.recordProvisioningFailure(plan, provisioningPlacementReady, provisioningErrorActivation, false)
		return nil, err
	}
	if err := management.setProvisioningStage(plan, provisioningActive, ""); err != nil {
		return nil, err
	}
	return placement, nil
}
