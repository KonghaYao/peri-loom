package platform

import "errors"

const (
	projectQuotasPath        = "/v1/projects/quotas"
	projectQuotasMigratePath = "/v1/projects/quotas/migrate"
)

func (management *Management) createOrganization(principal Principal, input managementRequest) (any, error) {
	if !labelPattern.MatchString(input.ID) || len(input.Name) > 128 || input.Name == "" {
		return nil, errors.New("organization creation denied")
	}
	if principal.Role != "admin" || principal.Project != "*" {
		return nil, errManagementForbidden
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
		quotas.RequestsPerSecond = 500
	}
	if quotas.MaxSQLBytes == 0 {
		quotas.MaxSQLBytes = 65536
	}
	if quotas.MaxResultBytes == 0 {
		quotas.MaxResultBytes = 4 * 1024 * 1024
	}
	return quotas
}

// legacyProjectQuotas is the quota set createProject applied before the default
// request rate rose to 500/s. Migration rewrites only the fields that still hold
// these values: every other value is a deliberate operator setting and must
// survive the upgrade. A project that explicitly chose a legacy value cannot be
// distinguished from an untouched default, which is why migration is an
// explicit per-project instruction rather than an automatic rewrite.
var legacyProjectQuotas = Quotas{
	MaxDatabases:      8,
	MaxSessions:       16,
	MaxConcurrent:     4,
	RequestsPerSecond: 100,
	MaxSQLBytes:       65536,
	MaxResultBytes:    4 * 1024 * 1024,
}

var errQuotaRevisionMismatch = errors.New("project quota revision mismatch")

// quotaReplacementValid requires a complete, positive quota set. Update is a
// replacement, not a patch: a field the caller omitted decodes as zero and is
// refused, so a partial body can never silently reset a project to zero
// capacity.
func quotaReplacementValid(quotas Quotas) bool {
	return quotas.MaxDatabases > 0 && quotaValid(quotas)
}

// updateProjectQuotas replaces the quota set of an existing project. Every
// read-modify-write of a project quota goes through the registry revision the
// caller read, so two operators that started from the same version cannot both
// win: the second commit sees a newer revision and is refused.
func (management *Management) updateProjectQuotas(principal Principal, input managementRequest) (any, error) {
	if principal.Role != "admin" || principal.Project != "*" {
		return nil, errManagementForbidden
	}
	if !labelPattern.MatchString(input.ID) || !quotaReplacementValid(input.Quotas) {
		return nil, errors.New("project quota update denied")
	}
	updated := Project{}
	err := management.config.State.Update(func(state *State) error {
		if state.Revision != input.Revision {
			return errQuotaRevisionMismatch
		}
		project, exists := state.Projects[input.ID]
		if !exists {
			return errors.New("project missing")
		}
		project.Quotas = input.Quotas
		state.Projects[input.ID] = project
		updated = project
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"project": updated, "quotas": updated.Quotas, "revision": input.Revision + 1}, nil
}

// migratedProjectQuotas raises fields that still carry a pre-500/s default and
// leaves every other field alone, so an explicit 42 RPS survives the migration.
func migratedProjectQuotas(quotas Quotas) Quotas {
	current := defaultQuotas(Quotas{})
	if quotas.MaxDatabases == legacyProjectQuotas.MaxDatabases {
		quotas.MaxDatabases = current.MaxDatabases
	}
	if quotas.MaxSessions == legacyProjectQuotas.MaxSessions {
		quotas.MaxSessions = current.MaxSessions
	}
	if quotas.MaxConcurrent == legacyProjectQuotas.MaxConcurrent {
		quotas.MaxConcurrent = current.MaxConcurrent
	}
	if quotas.RequestsPerSecond == legacyProjectQuotas.RequestsPerSecond {
		quotas.RequestsPerSecond = current.RequestsPerSecond
	}
	if quotas.MaxSQLBytes == legacyProjectQuotas.MaxSQLBytes {
		quotas.MaxSQLBytes = current.MaxSQLBytes
	}
	if quotas.MaxResultBytes == legacyProjectQuotas.MaxResultBytes {
		quotas.MaxResultBytes = current.MaxResultBytes
	}
	return quotas
}

// migrateProjectQuotas is the explicit migration entry point for projects that
// predate the new defaults. It is revision-guarded like an update and never
// runs on its own: an operator has to name the project.
func (management *Management) migrateProjectQuotas(principal Principal, input managementRequest) (any, error) {
	if principal.Role != "admin" || principal.Project != "*" {
		return nil, errManagementForbidden
	}
	if !labelPattern.MatchString(input.ID) || input.Quotas != (Quotas{}) {
		return nil, errors.New("project quota migration denied")
	}
	updated := Project{}
	migrated := false
	err := management.config.State.Update(func(state *State) error {
		if state.Revision != input.Revision {
			return errQuotaRevisionMismatch
		}
		project, exists := state.Projects[input.ID]
		if !exists {
			return errors.New("project missing")
		}
		next := migratedProjectQuotas(project.Quotas)
		if next != project.Quotas {
			project.Quotas = next
			state.Projects[input.ID] = project
			migrated = true
		}
		updated = project
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"project": updated, "quotas": updated.Quotas, "migrated": migrated, "revision": input.Revision + 1}, nil
}

func (management *Management) createProject(principal Principal, input managementRequest) (any, error) {
	if !labelPattern.MatchString(input.ID) || !labelPattern.MatchString(input.Organization) || !labelPattern.MatchString(input.Region) {
		return nil, errors.New("project creation denied")
	}
	if principal.Role != "admin" || principal.Project != "*" {
		return nil, errManagementForbidden
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
		return nil, errManagementForbidden
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
	if !exists {
		return nil, errors.New("API key revocation denied")
	}
	if principal.Role != "admin" || principal.Project != "*" && principal.Project != key.Project {
		return nil, errManagementForbidden
	}
	return map[string]bool{"revoked": true}, RevokeAPIKey(management.config.State, key.ID)
}
