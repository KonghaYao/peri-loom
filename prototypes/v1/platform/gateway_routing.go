package platform

import (
	"errors"

	controller "peri-loom/controller"
)

func (gateway *Gateway) authorize(principal Principal, database controller.DatabaseID) error {
	_, err := gateway.authorizeQuota(principal, database)
	return err
}

func (gateway *Gateway) authorizeQuota(principal Principal, database controller.DatabaseID) (Quotas, error) {
	if database.Validate() != nil {
		return Quotas{}, errors.New("invalid database")
	}
	switch principal.Role {
	case "admin", "developer", "sql", "writer", "read_only":
	default:
		return Quotas{}, errors.New("SQL role required")
	}
	if principal.Database != nil && *principal.Database != database {
		return Quotas{}, errors.New("database scope mismatch")
	}
	state, err := readAuthorizationState(gateway.config.State, principal.KeyID, principal.Project, database)
	if err != nil {
		return Quotas{}, errors.New("state unavailable")
	}
	project, found := state.Projects[principal.Project]
	if !found || project.Tenant != database.Namespace.Tenant {
		return Quotas{}, errors.New("project tenant mismatch")
	}
	descriptor, found := state.Databases[database.Key()]
	if !found || descriptor.Project != principal.Project || descriptor.ID != database || (descriptor.State != "ready" && descriptor.State != "active") {
		return Quotas{}, errors.New("database unavailable")
	}
	if err := gateway.config.Identity.Authorize(principal, database, "sql"); err != nil {
		return Quotas{}, err
	}
	return project.Quotas, nil
}

func (gateway *Gateway) route(database controller.DatabaseID) (controller.Worker, controller.Binding, error) {
	placement, worker, found, err := controller.ReadRouting(gateway.config.Placements, database)
	if err != nil {
		return controller.Worker{}, controller.Binding{}, errors.New("placement unavailable")
	}
	if !found || placement.Database != database || placement.DesiredState != "active" || placement.Grant == nil {
		return controller.Worker{}, controller.Binding{}, errors.New("database not active")
	}
	switch placement.Phase {
	case "active", "renew_intent", "renew_pending", "renew_worker_pending":
	default:
		return controller.Worker{}, controller.Binding{}, errors.New("database not serving")
	}
	binding := placement.Grant.Binding
	if binding.Database != database || binding.Epoch != placement.Epoch || binding.WorkerID != placement.DesiredWorker || binding.LeaseID != placement.LeaseID || binding.Role != placement.Role || binding.RootIncarnation == "" || binding.AuthorityTerm == 0 || binding.BranchIncarnation == 0 || placement.Grant.RemainingTTLMS == 0 || (binding.Role != "writer" && binding.Role != "read_only") {
		return controller.Worker{}, controller.Binding{}, errors.New("placement identity invalid")
	}
	if worker.ID != binding.WorkerID || !worker.Healthy || worker.Draining {
		return controller.Worker{}, controller.Binding{}, errors.New("worker unavailable")
	}
	return worker, binding, nil
}

func sameGatewayBinding(first, second controller.Binding) bool {
	first.RenewalSequence = second.RenewalSequence
	return first == second
}

func principalBindingValid(principal Principal, binding controller.Binding) bool {
	if principal.Database == nil {
		return true
	}
	return *principal.Database == binding.Database && principal.RootIncarnation != "" && principal.BranchIncarnation > 0 && principal.RootIncarnation == binding.RootIncarnation && principal.BranchIncarnation == binding.BranchIncarnation
}
