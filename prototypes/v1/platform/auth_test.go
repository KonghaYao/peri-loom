package platform

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func identityFixture(test *testing.T, role string) (StateStore, Identity, APIKey, string) {
	test.Helper()
	store := securityState(test)
	kms := securityKMS(test)
	activeSecurityDatabase(test, store, kms)
	key, credential, err := NewAPIKey(store, "alice", "alpha", role, false)
	if err != nil {
		test.Fatal(err)
	}
	service, err := NewIdentity(store, bytes.Repeat([]byte{17}, 32))
	if err != nil {
		test.Fatal(err)
	}
	return store, service, key, credential
}

func TestBootstrapAndAPIKeysAreHashedAndNeverReset(test *testing.T) {
	store := securityState(test)
	credential := strings.Repeat("bootstrap-test-", 4)
	if err := BootstrapAPIKey(store, credential); err != nil {
		test.Fatal(err)
	}
	if err := BootstrapAPIKey(store, strings.Repeat("replacement-test", 3)); err == nil {
		test.Fatal("bootstrap reset")
	}
	identity, err := NewIdentity(store, bytes.Repeat([]byte{1}, 32))
	if err != nil {
		test.Fatal(err)
	}
	principal, err := identity.Authenticate(credential)
	if err != nil || principal.Project != "*" || principal.Role != "admin" {
		test.Fatalf("bootstrap principal: %+v %v", principal, err)
	}
	if identity.Authorize(principal, securityDatabase(), "manage") != nil {
		test.Fatal("bootstrap cannot manage")
	}
	if identity.Authorize(principal, securityDatabase(), "sql") == nil {
		test.Fatal("bootstrap automatically gained SQL")
	}
	bytes, err := os.ReadFile(store.(*stateStore).disk.path)
	if err != nil || strings.Contains(string(bytes), credential) {
		test.Fatal("plaintext API key persisted")
	}
	if _, err := identity.Authenticate("Bearer " + credential); err == nil {
		test.Fatal("rawcredential API accepted Bearer wrapper")
	}
}

func TestProjectRolesAndReaderTokensDoNotImplyManagement(test *testing.T) {
	for _, role := range []string{"admin", "developer", "sql", "viewer"} {
		test.Run(role, func(test *testing.T) {
			_, identity, _, credential := identityFixture(test, role)
			principal, err := identity.Authenticate(credential)
			if err != nil {
				test.Fatal(err)
			}
			if allowed := identity.Authorize(principal, securityDatabase(), "sql") == nil; allowed != (role != "viewer") {
				test.Fatal("SQL role escalation")
			}
			if allowed := identity.Authorize(principal, securityDatabase(), "keys") == nil; allowed != (role == "admin") {
				test.Fatal("key management role escalation")
			}
			token, err := identity.IssueToken(principal, securityDatabase(), "read_only", time.Now().Add(time.Minute).UnixMilli())
			if err != nil {
				test.Fatal(err)
			}
			reader, err := identity.Authenticate(token)
			if err != nil || reader.Role != "read_only" {
				test.Fatalf("reader token: %v", err)
			}
			if identity.Authorize(reader, securityDatabase(), "sql") != nil {
				test.Fatal("readonly token cannot reach native readonly gate")
			}
			if identity.Authorize(reader, securityDatabase(), "manage") == nil {
				test.Fatal("DB token management escalation")
			}
			_, err = identity.IssueToken(principal, securityDatabase(), "writer", time.Now().Add(time.Minute).UnixMilli())
			if (err == nil) != (role != "viewer") {
				test.Fatal("viewer minted writer")
			}
		})
	}
}

func TestTokensRevocationTenantAndIncarnationCheckedEveryUse(test *testing.T) {
	store, identity, key, credential := identityFixture(test, "sql")
	principal, err := identity.Authenticate(credential)
	if err != nil {
		test.Fatal(err)
	}
	token, err := identity.IssueToken(principal, securityDatabase(), "writer", time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		test.Fatal(err)
	}
	bound, err := identity.Authenticate(token)
	if err != nil {
		test.Fatal(err)
	}
	other := securityDatabase()
	other.Namespace.Tenant = "tenant_b"
	if identity.Authorize(bound, other, "sql") == nil {
		test.Fatal("cross-tenant token")
	}
	if err := store.Update(func(state *State) error {
		database := state.Databases[securityDatabase().Key()]
		database.BranchIncarnation++
		state.Databases[database.ID.Key()] = database
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	if _, err := identity.Authenticate(token); err == nil {
		test.Fatal("restored branch token accepted")
	}
	if identity.Authorize(bound, securityDatabase(), "sql") == nil {
		test.Fatal("cached principal accepted restored branch")
	}
	if err := RevokeAPIKey(store, key.ID); err != nil {
		test.Fatal(err)
	}
	if _, err := identity.Authenticate(credential); err == nil {
		test.Fatal("revoked API key")
	}
	if _, err := identity.Authenticate(token); err == nil {
		test.Fatal("revoked issuer token")
	}
	if err := store.Update(func(state *State) error {
		record := state.APIKeys[key.ID]
		record.Revoked = false
		state.APIKeys[key.ID] = record
		return nil
	}); err == nil {
		test.Fatal("revocation undone")
	}
}

func TestTokenExpiryTamperAndFixedAlgorithm(test *testing.T) {
	_, service, _, credential := identityFixture(test, "developer")
	principal, _ := service.Authenticate(credential)
	for _, expiry := range []int64{time.Now().UnixMilli() - 1, time.Now().Add(6 * time.Minute).UnixMilli()} {
		if _, err := service.IssueToken(principal, securityDatabase(), "writer", expiry); err == nil {
			test.Fatal("invalid lifetime")
		}
	}
	token, err := service.IssueToken(principal, securityDatabase(), "writer", time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		test.Fatal(err)
	}
	parts := strings.Split(token, ".")
	parts[2] = "AAAA"
	if _, err := service.Authenticate(strings.Join(parts, ".")); err == nil {
		test.Fatal("signature tamper")
	}
	encoded, _ := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	var claims tokenClaims
	json.Unmarshal(encoded, &claims)
	claims.ExpiresMS = time.Now().UnixMilli() - 1
	claims.IssuedMS = claims.ExpiresMS - 1000
	encoded, _ = json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(encoded)
	identity := service.(*identity)
	mac := hmac.New(sha256.New, identity.signingKey)
	mac.Write([]byte("pldb1." + payload))
	expired := "pldb1." + payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if _, err := service.Authenticate(expired); err == nil {
		test.Fatal("signed expired token")
	}
	if _, err := NewIdentity(identity.store, []byte("weak")); err == nil {
		test.Fatal("weak signing key")
	}
}
