package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

func (management *Management) createDatabase(ctx context.Context, principal Principal, input managementRequest) (any, error) {
	if input.Database.Validate() != nil || input.Database.Branch != "main" || principal.Role != "admin" && principal.Role != "developer" || principal.Project != "*" && principal.Project != input.Project {
		return nil, errors.New("database creation denied")
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
			return errors.New("database quota exceeded")
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
	return management.config.State.Update(func(state *State) error {
		entry, exists := state.Databases[database.Key()]
		if !exists || entry.State == "deleted" && desired != "deleted" {
			return errors.New("database identity unavailable")
		}
		entry.State = desired
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
	if !labelPattern.MatchString(input.Name) {
		return nil, errors.New("invalid branch name")
	}
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	parent, exists := state.Databases[input.Database.Key()]
	if !exists {
		return nil, errors.New("source database missing")
	}
	child := parent
	child.ID.Branch = input.Name
	child.State = "provisioning"
	child.OperationID = input.RequestID
	err = management.config.State.Update(func(state *State) error {
		if existing, exists := state.Databases[child.ID.Key()]; exists {
			if existing.OperationID != child.OperationID || existing.Project != child.Project || existing.State == "deleted" {
				return errors.New("branch identity already reserved")
			}
			return nil
		}
		project, exists := state.Projects[parent.Project]
		if !exists {
			return errors.New("project missing")
		}
		count := 0
		for _, database := range state.Databases {
			if database.Project == parent.Project && database.State != "deleted" {
				count++
			}
		}
		if count >= project.Quotas.MaxDatabases {
			return errors.New("branch quota exceeded")
		}
		state.Databases[child.ID.Key()] = child
		return nil
	})
	if err != nil {
		return nil, err
	}
	operation := map[string]any{"operation": "branch", "source": input.Database.Branch, "name": input.Name}
	if path == "/v1/databases/clone" {
		operation = map[string]any{"operation": "clone", "snapshot": input.Snapshot, "name": input.Name}
	}
	if _, err := management.authorityMaintenance(ctx, input.Database, input.RequestID, operation); err != nil {
		return nil, err
	}
	project := state.Projects[parent.Project]
	role := input.Role
	if role == "" {
		role = "writer"
	}
	if role != "writer" && role != "read_only" {
		return nil, errors.New("invalid branch role")
	}
	placement, err := management.config.Controller.Place(ctx, child.ID, project.Region, role)
	if err != nil {
		return nil, err
	}
	if err := management.setDatabaseState(child.ID, "active"); err != nil {
		return nil, err
	}
	return placement, nil
}
