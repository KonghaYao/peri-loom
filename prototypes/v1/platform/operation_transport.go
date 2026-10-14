package platform

import (
	"context"
	"encoding/json"
	"errors"

	controller "peri-loom/controller"
)

type isolatedControlTransport struct {
	controller.ControlTransport
	store StateStore
}

func controlIsolationDatabase(command any, workerCommand bool) (controller.DatabaseID, bool, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return controller.DatabaseID{}, false, errors.New("invalid isolation command")
	}
	var action struct {
		Command   string                `json:"command"`
		Database  controller.DatabaseID `json:"database"`
		Operation struct {
			Operation string `json:"operation"`
		} `json:"operation"`
		Request struct {
			Intent   json.RawMessage       `json:"intent"`
			Database controller.DatabaseID `json:"database"`
		} `json:"request"`
		Lease struct {
			Binding controller.Binding `json:"binding"`
		} `json:"lease"`
	}
	if json.Unmarshal(encoded, &action) != nil {
		return controller.DatabaseID{}, false, errors.New("invalid isolation command")
	}
	if !workerCommand && action.Command == "grant" {
		var intent struct {
			Database controller.DatabaseID `json:"database"`
		}
		if json.Unmarshal(action.Request.Intent, &intent) != nil {
			return controller.DatabaseID{}, false, errors.New("invalid isolation grant")
		}
		action.Database = intent.Database
	} else if workerCommand && action.Command == "acquire_database" {
		action.Database = action.Lease.Binding.Database
	} else if !workerCommand && action.Command == "revoke" {
		action.Database = action.Request.Database
	} else if !workerCommand && action.Command == "maintenance" && action.Operation.Operation == "drop_branch" {
		if action.Database.Validate() != nil {
			return controller.DatabaseID{}, false, errors.New("isolation database required")
		}
	} else if !workerCommand || action.Command != "drain_database" && action.Command != "revoke_database" {
		return controller.DatabaseID{}, false, nil
	}
	if action.Database.Validate() != nil {
		return controller.DatabaseID{}, false, errors.New("isolation database required")
	}
	return action.Database, true, nil
}

func (transport *isolatedControlTransport) permitted(command any, workerCommand bool) error {
	database, guarded, err := controlIsolationDatabase(command, workerCommand)
	if err != nil {
		return err
	}
	if !guarded {
		return nil
	}
	state, err := transport.store.Snapshot()
	if err != nil {
		return err
	}
	if _, blocked := isolatedOperation(state, database, true); blocked {
		return ErrDatabaseQuarantined
	}
	var deletion struct {
		Command   string `json:"command"`
		Operation struct {
			Operation string `json:"operation"`
		} `json:"operation"`
	}
	encoded, _ := json.Marshal(command)
	if json.Unmarshal(encoded, &deletion) == nil && deletion.Command == "maintenance" && deletion.Operation.Operation == "drop_branch" {
		return knownDatabaseDeletionPermitted(state, database)
	}
	return nil
}

func (transport *isolatedControlTransport) Authority(ctx context.Context, requestID string, command any, output any) error {
	database, guarded, err := controlIsolationDatabase(command, false)
	if err != nil {
		return err
	}
	if guarded {
		unlock := lockOperationNamespace(database.Namespace)
		defer unlock()
	}
	if err := transport.permitted(command, false); err != nil {
		return err
	}
	return transport.ControlTransport.Authority(ctx, requestID, command, output)
}

func (transport *isolatedControlTransport) Worker(ctx context.Context, worker controller.Worker, requestID string, command any, output any) error {
	database, guarded, err := controlIsolationDatabase(command, true)
	if err != nil {
		return err
	}
	if guarded {
		unlock := lockOperationNamespace(database.Namespace)
		defer unlock()
	}
	if err := transport.permitted(command, true); err != nil {
		return err
	}
	return transport.ControlTransport.Worker(ctx, worker, requestID, command, output)
}
