package platform

import (
	"context"
	"errors"

	controller "peri-loom/controller"
)

type authorityValidator struct {
	transports  map[string]controller.ControlTransport
	credentials map[string]string
}

func NewLeaseValidator(authorityURL string, workerAuthorityTokens map[string]string) (LeaseValidator, error) {
	if len(workerAuthorityTokens) == 0 {
		return nil, errors.New("worker authority credentials required")
	}
	validator := &authorityValidator{transports: map[string]controller.ControlTransport{}, credentials: map[string]string{}}
	seen := map[string]bool{}
	for worker, token := range workerAuthorityTokens {
		if !identifierPattern.MatchString(worker) || !validCredential(token) || seen[token] {
			return nil, errors.New("invalid or duplicate worker authority credential")
		}
		seen[token] = true
		transport, err := controller.NewTransport(authorityURL, token, nil)
		if err != nil {
			return nil, errors.New("invalid authority endpoint/credential")
		}
		validator.transports[worker] = transport
		validator.credentials[worker] = digestHex([]byte(token))
	}
	return validator, nil
}

func (validator *authorityValidator) Validate(ctx context.Context, binding controller.Binding, role string) (controller.Grant, error) {
	transport, ok := validator.transports[binding.WorkerID]
	if !ok || !validBinding(binding, role) {
		return controller.Grant{}, errors.New("lease invalid")
	}
	id, err := controller.RandomID()
	if err != nil {
		return controller.Grant{}, errors.New("lease validation unavailable")
	}
	command := struct {
		Command string             `json:"command"`
		Binding controller.Binding `json:"binding"`
		Role    string             `json:"role"`
	}{"validate_lease", binding, role}
	var grant controller.Grant
	if transport.Authority(ctx, id, command, &grant) != nil || !validGrant(binding, role, grant) {
		return controller.Grant{}, errors.New("lease invalid")
	}
	return grant, nil
}

func validBinding(binding controller.Binding, role string) bool {
	return binding.Database.Validate() == nil && identifierPattern.MatchString(binding.WorkerID) && identifierPattern.MatchString(binding.RootIncarnation) && identifierPattern.MatchString(binding.LeaseID) && identifierPattern.MatchString(binding.GrantID) && binding.AuthorityTerm > 0 && binding.BranchIncarnation > 0 && binding.Epoch > 0 && (binding.Role == "writer" || binding.Role == "read_only") && (role == "writer" || role == "read_only") && (binding.Role == "writer" || role == "read_only")
}

func validGrant(binding controller.Binding, role string, grant controller.Grant) bool {
	if !validBinding(grant.Binding, role) || grant.RemainingTTLMS == 0 || grant.RemainingTTLMS > 60000 || grant.Binding.RenewalSequence < binding.RenewalSequence {
		return false
	}
	current := grant.Binding
	current.RenewalSequence = binding.RenewalSequence
	return current == binding
}
