package platform

import "errors"

func (management *Management) createOrganization(principal Principal, input managementRequest) (any, error) {
	if principal.Role != "admin" || principal.Project != "*" || !labelPattern.MatchString(input.ID) || len(input.Name) > 128 || input.Name == "" {
		return nil, errors.New("organization creation denied")
	}
	organization := Organization{ID: input.ID, Name: input.Name}
	err := management.config.State.Update(func(state *State) error {
		if existing, found := state.Organizations[organization.ID]; found && existing != organization {
			return errors.New("organization identity already reserved")
		}
		state.Organizations[organization.ID] = organization
		return nil
	})
	return organization, err
}

func defaultQuotas(quotas Quotas) Quotas {
	if quotas.MaxDatabases == 0 {
		quotas.MaxDatabases = 8
	}
	if quotas.MaxSessions == 0 {
		quotas.MaxSessions = 16
	}
	if quotas.MaxConcurrent == 0 {
		quotas.MaxConcurrent = 4
	}
	if quotas.RequestsPerSecond == 0 {
		quotas.RequestsPerSecond = 100
	}
	if quotas.MaxSQLBytes == 0 {
		quotas.MaxSQLBytes = 65536
	}
	if quotas.MaxResultBytes == 0 {
		quotas.MaxResultBytes = 4 * 1024 * 1024
	}
	return quotas
}

func (management *Management) createProject(principal Principal, input managementRequest) (any, error) {
	if principal.Role != "admin" || principal.Project != "*" || !labelPattern.MatchString(input.ID) || !labelPattern.MatchString(input.Organization) || !labelPattern.MatchString(input.Region) {
		return nil, errors.New("project creation denied")
	}
	project := Project{ID: input.ID, Organization: input.Organization, Tenant: input.Organization + "_" + input.ID, Region: input.Region, Quotas: defaultQuotas(input.Quotas)}
	err := management.config.State.Update(func(state *State) error {
		if _, exists := state.Organizations[project.Organization]; !exists {
			return errors.New("organization missing")
		}
		if existing, exists := state.Projects[project.ID]; exists && existing != project {
			return errors.New("project identity already reserved")
		}
		for identity, other := range state.Projects {
			if identity != project.ID && other.Tenant == project.Tenant {
				return errors.New("tenant namespace collision")
			}
		}
		state.Projects[project.ID] = project
		return nil
	})
	return project, err
}

func (management *Management) createAPIKey(principal Principal, input managementRequest) (any, error) {
	if principal.Role != "admin" || principal.Project != "*" && principal.Project != input.Project {
		return nil, errors.New("API key creation denied")
	}
	key, credential, err := NewAPIKey(management.config.State, input.Subject, input.Project, input.Role, input.ServiceAccount)
	if err != nil {
		return nil, err
	}
	return map[string]any{"key": key.ID, "subject": key.Subject, "project": key.Project, "role": key.Role, "credential": credential, "shown_once": true}, nil
}

func (management *Management) revokeAPIKey(principal Principal, input managementRequest) (any, error) {
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	key, exists := state.APIKeys[input.ID]
	if !exists || principal.Role != "admin" || principal.Project != "*" && principal.Project != key.Project {
		return nil, errors.New("API key revocation denied")
	}
	return map[string]bool{"revoked": true}, RevokeAPIKey(management.config.State, key.ID)
}
