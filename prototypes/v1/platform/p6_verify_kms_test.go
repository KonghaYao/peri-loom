package platform

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func (fixture *p6VerifyFixture) persisted() State {
	fixture.test.Helper()
	store, err := OpenStateStore(fixture.config.StateDirectory)
	if err != nil {
		fixture.test.Fatal(err)
	}
	defer store.Close()
	state, err := store.Snapshot()
	if err != nil {
		fixture.test.Fatal(err)
	}
	return state
}

func TestP6VerifyKEKRewrapRestartAndAuthenticatedTamper(test *testing.T) {
	fixture := p6VerifyNew(test)
	session := fixture.open(fixture.credential, fixture.database)
	fixture.sql(session, "execute", "CREATE TABLE proof(id INTEGER PRIMARY KEY); INSERT INTO proof VALUES(41)")
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": session})
	fixture.platform.stop(test, true)
	before := fixture.persisted()
	keyID := fixture.database.Namespace.Tenant + "/" + fixture.database.Namespace.Database
	descriptor, found := before.Keys[keyID]
	if !found {
		test.Fatal("persisted namespace descriptor missing")
	}
	digest := sha256.Sum256([]byte(fixture.credential))
	matched := 0
	for _, key := range before.APIKeys {
		if key.Digest == hex.EncodeToString(digest[:]) {
			matched++
		}
	}
	if matched != 1 {
		test.Fatalf("persisted API credential digest matches=%d", matched)
	}
	master, err := NewLocalKMS(fixture.config.KMSKeyring)
	if err != nil {
		test.Fatal(err)
	}
	if err := RotateLocalKMS(master); err != nil {
		test.Fatal(err)
	}
	if err := CloseLocalKMS(master); err != nil {
		test.Fatal(err)
	}
	fixture.startPlatform()
	fixture.must(fixture.credential, "/v1/databases/rewrap", map[string]any{"database": fixture.database})
	fixture.platform.stop(test, true)
	after := fixture.persisted()
	rewrapped := after.Keys[keyID]
	if rewrapped.Revision != descriptor.Revision+1 || rewrapped.DEKID != descriptor.DEKID || rewrapped.DomainID != descriptor.DomainID || bytes.Equal(rewrapped.Envelope, descriptor.Envelope) {
		test.Fatal("KEK rewrap changed encryption identity or failed to advance envelope")
	}
	var oldEnvelope, newEnvelope struct {
		KeyVersion string `json:"key_version"`
	}
	if json.Unmarshal(descriptor.Envelope, &oldEnvelope) != nil || json.Unmarshal(rewrapped.Envelope, &newEnvelope) != nil || oldEnvelope.KeyVersion == newEnvelope.KeyVersion {
		test.Fatal("rewrap did not use rotated KEK")
	}
	fixture.startPlatform()
	recovered := fixture.open(fixture.credential, fixture.database)
	if rows := fixture.sql(recovered, "query", "SELECT id FROM proof"); string(rows.Result) != "[[41]]" {
		test.Fatal("rewrap/restart lost acknowledged native data")
	}
	fixture.must(fixture.credential, "/v1/sql/close", map[string]any{"session": recovered})
	binding := fixture.placement().Grant.Binding
	fixture.platform.stop(test, true)
	store, err := OpenStateStore(fixture.config.StateDirectory)
	if err != nil {
		test.Fatal(err)
	}
	err = store.Update(func(state *State) error {
		entry := state.Keys[keyID]
		var envelope map[string]any
		if err := json.Unmarshal(entry.Envelope, &envelope); err != nil {
			return err
		}
		ciphertext, err := hex.DecodeString(envelope["ciphertext"].(string))
		if err != nil {
			return err
		}
		ciphertext[0] ^= 1
		envelope["ciphertext"] = hex.EncodeToString(ciphertext)
		entry.Envelope, err = json.Marshal(envelope)
		if err != nil {
			return err
		}
		entry.Revision++
		state.Keys[keyID] = entry
		return nil
	})
	if closeError := store.Close(); err == nil {
		err = closeError
	}
	if err != nil {
		test.Fatal(err)
	}
	fixture.startPlatform()
	status, denied := fixture.broker(fixture.config.WorkerKeyTokens[binding.WorkerID], binding)
	if status != 503 || denied.OK || denied.Error.Code != "UNAVAILABLE" || len(denied.Result) > 0 && string(denied.Result) != "null" {
		test.Fatalf("authenticated envelope tamper status=%d code=%s exposed_result=%t", status, denied.Error.Code, len(denied.Result) > 0 && string(denied.Result) != "null")
	}
	fixture.platform.stop(test, true)
	audit, err := os.ReadFile(fixture.config.AuditPath)
	if err != nil {
		test.Fatal(err)
	}
	mutated := bytes.Replace(audit, []byte("verifier"), []byte("tampered"), 1)
	if bytes.Equal(mutated, audit) {
		test.Fatal("audit tamper fixture marker absent")
	}
	if err := os.WriteFile(fixture.config.AuditPath, mutated, 0600); err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenAudit(fixture.config.AuditPath)
	if err == nil {
		_ = reopened.Close()
		test.Fatal("tampered audit chain accepted on restart")
	}
	test.Logf("actual KEK %s->%s rewrap preserves DEK/domain and ACK41 across GoCLI restart; valid disk checksum+mutated GCM rejected by real broker; audit chain tamper refuses reopen", oldEnvelope.KeyVersion, newEnvelope.KeyVersion)
}
