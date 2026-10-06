package platform

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	controller "peri-loom/controller"
)

type identity struct {
	store      StateStore
	signingKey []byte
}
type tokenClaims struct {
	Version           uint32                `json:"version"`
	Issuer            string                `json:"issuer"`
	Audience          string                `json:"audience"`
	KeyID             string                `json:"key_id"`
	Subject           string                `json:"subject"`
	Project           string                `json:"project"`
	Database          controller.DatabaseID `json:"database"`
	Role              string                `json:"role"`
	RootIncarnation   string                `json:"root_incarnation"`
	BranchIncarnation uint64                `json:"branch_incarnation"`
	DomainID          string                `json:"domain_id"`
	IssuedMS          int64                 `json:"issued_ms"`
	ExpiresMS         int64                 `json:"expires_ms"`
}

func NewIdentity(store StateStore, signingKey []byte) (Identity, error) {
	if store == nil || len(signingKey) < 32 {
		return nil, errors.New("identity requires store and strong signing key")
	}
	if _, err := store.Snapshot(); err != nil {
		return nil, err
	}
	return &identity{store: store, signingKey: append([]byte(nil), signingKey...)}, nil
}

func (identity *identity) Authenticate(credential string) (Principal, error) {
	if strings.HasPrefix(credential, "pldb1.") {
		return identity.authenticateToken(credential)
	}
	if !validCredential(credential) {
		return Principal{}, errors.New("unauthenticated")
	}
	state, err := identity.store.Snapshot()
	if err != nil {
		return Principal{}, errors.New("identity unavailable")
	}
	digest := sha256.Sum256([]byte(credential))
	var matched *APIKey
	for _, key := range state.APIKeys {
		decoded, err := hex.DecodeString(key.Digest)
		if err == nil && subtle.ConstantTimeCompare(digest[:], decoded) == 1 {
			record := key
			matched = &record
		}
	}
	if matched == nil || matched.Revoked {
		return Principal{}, errors.New("unauthenticated")
	}
	return Principal{KeyID: matched.ID, Subject: matched.Subject, Project: matched.Project, Role: matched.Role}, nil
}

func (identity *identity) Authorize(principal Principal, database controller.DatabaseID, action string) error {
	state, err := identity.store.Snapshot()
	if err != nil {
		return errors.New("identity unavailable")
	}
	key, ok := state.APIKeys[principal.KeyID]
	if !ok || key.Revoked || key.Subject != principal.Subject || key.Project != principal.Project {
		return errors.New("forbidden")
	}
	if principal.Database != nil {
		if principal.ExpiresMS <= time.Now().UnixMilli() || *principal.Database != database || !canIssue(key.Role, principal.Role) || (action != "sql" && action != "view") {
			return errors.New("forbidden")
		}
	} else if principal.Role != key.Role || principal.ExpiresMS != 0 || principal.RootIncarnation != "" || principal.BranchIncarnation != 0 || principal.DomainID != "" {
		return errors.New("forbidden")
	}
	if key.Project == "*" {
		if key.Role == "admin" && principal.Database == nil && (action == "manage" || action == "view" || action == "keys") {
			return nil
		}
		return errors.New("forbidden")
	}
	if database.Validate() != nil {
		return errors.New("forbidden")
	}
	record, exists := state.Databases[database.Key()]
	project, present := state.Projects[key.Project]
	if !exists || !present || record.Project != key.Project || project.Tenant != database.Namespace.Tenant || record.ID != database {
		return errors.New("forbidden")
	}
	if principal.Database != nil {
		descriptor, ok := state.Keys[namespaceKey(database.Namespace)]
		if !ok || !tokenDatabaseReady(record, descriptor) || principal.RootIncarnation != record.RootIncarnation || principal.BranchIncarnation != record.BranchIncarnation || principal.DomainID != descriptor.DomainID {
			return errors.New("forbidden")
		}
	}
	allowed := false
	switch action {
	case "view":
		allowed = true
	case "sql":
		allowed = principal.Role == "admin" || principal.Role == "developer" || principal.Role == "sql" || principal.Role == "writer" || principal.Role == "read_only"
	case "manage", "checkpoint":
		allowed = principal.Database == nil && (key.Role == "admin" || key.Role == "developer")
	case "keys":
		allowed = principal.Database == nil && key.Role == "admin"
	}
	if !allowed {
		return errors.New("forbidden")
	}
	return nil
}

func (identity *identity) IssueToken(principal Principal, database controller.DatabaseID, role string, expiresMS int64) (string, error) {
	if principal.Database != nil || principal.Project == "*" || !canIssue(principal.Role, role) {
		return "", errors.New("forbidden")
	}
	if err := identity.Authorize(principal, database, "view"); err != nil {
		return "", err
	}
	state, err := identity.store.Snapshot()
	if err != nil {
		return "", errors.New("identity unavailable")
	}
	record := state.Databases[database.Key()]
	descriptor, ok := state.Keys[namespaceKey(database.Namespace)]
	if !ok || !tokenDatabaseReady(record, descriptor) {
		return "", errors.New("encrypted active database binding required")
	}
	now := time.Now().UnixMilli()
	if expiresMS <= now || expiresMS-now > 300000 {
		return "", errors.New("token expiration must be within five minutes")
	}
	claims := tokenClaims{Version: 1, Issuer: "peri-platform", Audience: "peri-sql", KeyID: principal.KeyID, Subject: principal.Subject, Project: principal.Project, Database: database, Role: role, RootIncarnation: record.RootIncarnation, BranchIncarnation: record.BranchIncarnation, DomainID: descriptor.DomainID, IssuedMS: now, ExpiresMS: expiresMS}
	encoded, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(encoded)
	mac := hmac.New(sha256.New, identity.signingKey)
	mac.Write([]byte("pldb1." + payload))
	return "pldb1." + payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (identity *identity) authenticateToken(credential string) (Principal, error) {
	if len(credential) > 4096 {
		return Principal{}, errors.New("unauthenticated")
	}
	parts := strings.Split(credential, ".")
	if len(parts) != 3 || parts[0] != "pldb1" {
		return Principal{}, errors.New("unauthenticated")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Principal{}, errors.New("unauthenticated")
	}
	mac := hmac.New(sha256.New, identity.signingKey)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return Principal{}, errors.New("unauthenticated")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Principal{}, errors.New("unauthenticated")
	}
	var claims tokenClaims
	if strictDecode(encoded, &claims) != nil || claims.Version != 1 || claims.Issuer != "peri-platform" || claims.Audience != "peri-sql" || claims.IssuedMS <= 0 || claims.ExpiresMS <= claims.IssuedMS || claims.ExpiresMS-claims.IssuedMS > 300000 {
		return Principal{}, errors.New("unauthenticated")
	}
	now := time.Now().UnixMilli()
	if now < claims.IssuedMS || now >= claims.ExpiresMS {
		return Principal{}, errors.New("unauthenticated")
	}
	principal := Principal{KeyID: claims.KeyID, Subject: claims.Subject, Project: claims.Project, Role: claims.Role, Database: &claims.Database, ExpiresMS: claims.ExpiresMS, RootIncarnation: claims.RootIncarnation, BranchIncarnation: claims.BranchIncarnation, DomainID: claims.DomainID}
	if err := identity.Authorize(principal, claims.Database, "sql"); err != nil {
		return Principal{}, errors.New("unauthenticated")
	}
	return principal, nil
}

func canIssue(issuer, role string) bool {
	if role == "writer" {
		return issuer == "admin" || issuer == "developer" || issuer == "sql"
	}
	if role == "read_only" {
		return issuer == "admin" || issuer == "developer" || issuer == "sql" || issuer == "viewer"
	}
	return false
}

func tokenDatabaseReady(database Database, descriptor KeyDescriptor) bool {
	return database.Encrypted && database.State == "active" && database.RootIncarnation != "" && database.BranchIncarnation > 0 && descriptor.State == "ready" && descriptor.RootIncarnation == database.RootIncarnation && descriptor.Namespace == database.ID.Namespace && descriptor.DomainID != ""
}

func randomLabel() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", errors.New("secure identifier generation failed")
	}
	return hex.EncodeToString(bytes[:]), nil
}
func validCredential(credential string) bool {
	if len(credential) < 32 || len(credential) > 256 {
		return false
	}
	for _, character := range []byte(credential) {
		if character < 33 || character > 126 {
			return false
		}
	}
	return true
}
