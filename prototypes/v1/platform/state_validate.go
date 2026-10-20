package platform

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"

	controller "peri-loom/controller"
)

func namespaceKey(namespace controller.Namespace) string {
	return namespace.Tenant + "/" + namespace.Database
}
func validNamespace(namespace controller.Namespace) bool {
	return (controller.DatabaseID{Namespace: namespace, Branch: "main"}).Validate() == nil
}
func validRole(role string) bool {
	return role == "admin" || role == "developer" || role == "sql" || role == "viewer"
}

func validateState(state State, previous *State) error {
	if state.OperationStorageVersion > 1 || previous != nil && state.OperationStorageVersion < previous.OperationStorageVersion {
		return errors.New("operation storage version cannot be reset")
	}
	// The checkpoint names a revision of this state, and it is a record of the
	// past: it can be left behind by later commits but never invented ahead of
	// them or taken back.
	if state.OperationsCheckpoint > state.Revision || previous != nil && state.OperationsCheckpoint < previous.OperationsCheckpoint {
		return errors.New("invalid operation checkpoint")
	}
	if (state.OwnedRunGeneration == "") != (state.OwnedRunBinding == "") || state.OwnedRunBinding != "" && !ownedProofToken(state.OwnedRunBinding) || previous != nil && previous.OwnedRunBinding != "" && state.OwnedRunBinding != previous.OwnedRunBinding {
		return errors.New("owned run binding immutable")
	}
	if state.OwnedRunGeneration != "" && !ownedProofToken(state.OwnedRunGeneration) || previous != nil && previous.OwnedRunGeneration != "" && state.OwnedRunGeneration != previous.OwnedRunGeneration {
		return errors.New("owned run generation immutable")
	}
	if err := validateOperations(state, previous); err != nil {
		return err
	}
	if state.Version != 1 || state.Organizations == nil || state.Projects == nil || state.Databases == nil || state.APIKeys == nil || state.Keys == nil {
		return errors.New("invalid state version/maps")
	}
	for id, organization := range state.Organizations {
		if id != organization.ID || !identifierPattern.MatchString(id) || len(organization.Name) > 256 {
			return errors.New("invalid organization")
		}
	}
	tenants := map[string]bool{}
	for id, project := range state.Projects {
		if id != project.ID || !identifierPattern.MatchString(id) || !identifierPattern.MatchString(project.Organization) || !validNamespace(controller.Namespace{Tenant: project.Tenant, Database: "test"}) || tenants[project.Tenant] {
			return errors.New("invalid or duplicate project tenant")
		}
		if _, ok := state.Organizations[project.Organization]; !ok {
			return errors.New("project organization missing")
		}
		tenants[project.Tenant] = true
		quotas := project.Quotas
		if quotas.MaxDatabases < 0 || quotas.MaxSessions < 0 || quotas.MaxConcurrent < 0 || quotas.RequestsPerSecond < 0 || quotas.MaxSQLBytes < 0 || quotas.MaxResultBytes < 0 {
			return errors.New("invalid project quotas")
		}
		if previous != nil {
			if old, exists := previous.Projects[id]; exists && (old.Tenant != project.Tenant || old.Organization != project.Organization) {
				return errors.New("project security identity immutable")
			}
		}
	}
	for key, database := range state.Databases {
		project, ok := state.Projects[database.Project]
		if database.ID.Validate() != nil || key != database.ID.Key() || !ok || project.Tenant != database.ID.Namespace.Tenant || !identifierPattern.MatchString(database.State) {
			return errors.New("database project/tenant identity invalid")
		}
		if (database.RootIncarnation == "") != (database.BranchIncarnation == 0) || database.RootIncarnation != "" && !identifierPattern.MatchString(database.RootIncarnation) {
			return errors.New("database incarnation binding invalid")
		}
		if previous != nil {
			if old, exists := previous.Databases[key]; exists && (old.Project != database.Project || old.ID != database.ID || old.Encrypted != database.Encrypted || old.OperationID != database.OperationID || (old.RootIncarnation != "" && old.RootIncarnation != database.RootIncarnation) || database.BranchIncarnation < old.BranchIncarnation) {
				return errors.New("database security identity immutable")
			}
		}
	}
	digests := map[string]bool{}
	for id, key := range state.APIKeys {
		decoded, err := hex.DecodeString(key.Digest)
		if id != key.ID || !identifierPattern.MatchString(id) || !subjectPattern.MatchString(key.Subject) || !validRole(key.Role) || err != nil || len(decoded) != 32 || key.Digest != hex.EncodeToString(decoded) {
			return errors.New("invalid API key record")
		}
		if digests[key.Digest] {
			return errors.New("API key digest must be unique")
		}
		digests[key.Digest] = true
		if key.Project == "*" {
			if key.Role != "admin" {
				return errors.New("global key requires admin role")
			}
		} else {
			if _, ok := state.Projects[key.Project]; !ok {
				return errors.New("API key project missing")
			}
		}
		if previous != nil {
			if old, exists := previous.APIKeys[id]; exists {
				normalized := key
				normalized.Revoked = old.Revoked
				if normalized != old || (old.Revoked && !key.Revoked) {
					return errors.New("API key identity immutable and revocation permanent")
				}
			}
		}
	}
	domains := map[string]bool{}
	dekIDs := map[string]bool{}
	descriptorIDs := map[string]bool{}
	for key, descriptor := range state.Keys {
		if key != namespaceKey(descriptor.Namespace) || !validNamespace(descriptor.Namespace) || !identifierPattern.MatchString(descriptor.ID) || !identifierPattern.MatchString(descriptor.ProvisionID) || !identifierPattern.MatchString(descriptor.DomainID) || !identifierPattern.MatchString(descriptor.DEKID) || descriptor.Cipher != "aegis256" || descriptor.Revision == 0 {
			return errors.New("invalid encrypted key descriptor")
		}
		if domains[descriptor.DomainID] || dekIDs[descriptor.DEKID] || descriptorIDs[descriptor.ID] {
			return errors.New("cross-namespace shared key domains forbidden")
		}
		domains[descriptor.DomainID] = true
		dekIDs[descriptor.DEKID] = true
		descriptorIDs[descriptor.ID] = true
		if descriptor.State == "sealed_unbound" {
			if descriptor.RootIncarnation != "" || descriptor.Revision != 1 {
				return errors.New("unbound descriptor identity invalid")
			}
		} else if descriptor.State == "ready" {
			if !identifierPattern.MatchString(descriptor.RootIncarnation) || descriptor.Revision < 2 {
				return errors.New("ready descriptor root missing")
			}
		} else {
			return errors.New("invalid descriptor state")
		}
		var envelope kmsEnvelope
		if err := strictDecode(descriptor.Envelope, &envelope); err != nil || !validEnvelope(envelope) {
			return errors.New("descriptor must contain authenticated wrapped DEK only")
		}
		if previous != nil {
			if old, exists := previous.Keys[key]; exists {
				if old.ID != descriptor.ID || old.ProvisionID != descriptor.ProvisionID || old.Namespace != descriptor.Namespace || old.DomainID != descriptor.DomainID || old.DEKID != descriptor.DEKID || old.Cipher != descriptor.Cipher || (old.State == "ready" && (descriptor.State != "ready" || old.RootIncarnation != descriptor.RootIncarnation)) {
					return errors.New("encryption domain/root identity immutable")
				}
				if !reflect.DeepEqual(old, descriptor) && (old.Revision == ^uint64(0) || descriptor.Revision != old.Revision+1) {
					return errors.New("descriptor revision must increment exactly")
				}
				if old.State == "sealed_unbound" && descriptor.State == "sealed_unbound" && !bytes.Equal(old.Envelope, descriptor.Envelope) {
					return errors.New("unbound DEK reservation cannot be replaced")
				}
			}
		}
	}
	for key, record := range state.Provisionings {
		if key != record.RequestID || !labelPattern.MatchString(key) || record.Database.Validate() != nil || record.Database.Branch != record.Name || !labelPattern.MatchString(record.Name) || (record.Role != "writer" && record.Role != "read_only") || !validProvisioningStage(record.Stage) || len(record.LastError) > 128 {
			return errors.New("invalid provisioning record")
		}
		project, ok := state.Projects[record.Project]
		if !ok || project.Tenant != record.Database.Namespace.Tenant {
			return errors.New("provisioning project/tenant invalid")
		}
		switch record.Operation {
		case "branch":
			if record.Source == "" || record.Snapshot != "" {
				return errors.New("invalid provisioning branch payload")
			}
		case "clone":
			if record.Snapshot == "" || record.Source != "" {
				return errors.New("invalid provisioning clone payload")
			}
		default:
			return errors.New("invalid provisioning operation")
		}
		if !strings.HasPrefix(record.Digest, "provisioning_v1:") {
			return errors.New("invalid provisioning digest")
		}
		decoded, err := hex.DecodeString(strings.TrimPrefix(record.Digest, "provisioning_v1:"))
		if err != nil || len(decoded) != 32 {
			return errors.New("invalid provisioning digest")
		}
		if previous != nil {
			if old, exists := previous.Provisionings[key]; exists {
				normalized := record
				normalized.Stage, normalized.LastError = old.Stage, old.LastError
				if normalized != old {
					return errors.New("provisioning identity immutable")
				}
			}
		}
	}
	for _, database := range state.Databases {
		if database.Encrypted && database.State == "active" {
			descriptor, ok := state.Keys[namespaceKey(database.ID.Namespace)]
			if !ok || !tokenDatabaseReady(database, descriptor) {
				return errors.New("active encrypted database requires ready root-bound key descriptor")
			}
		}
	}
	if previous != nil {
		for id := range previous.APIKeys {
			if _, ok := state.APIKeys[id]; !ok {
				return errors.New("API key revocation records cannot be removed")
			}
		}
		for key := range previous.Keys {
			if _, ok := state.Keys[key]; !ok {
				return errors.New("namespace key reservations cannot be removed")
			}
		}
	}
	return nil
}
