package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	controller "peri-loom/controller"
)

const brokerTestCredential = "worker-to-broker-pure-test-credential-123"

type fakeLeaseValidator struct {
	mutex    sync.Mutex
	calls    int
	failAt   int
	sequence uint64
	hook     func(int)
}

func (validator *fakeLeaseValidator) Validate(ctx context.Context, binding controller.Binding, role string) (controller.Grant, error) {
	validator.mutex.Lock()
	validator.calls++
	call := validator.calls
	failAt := validator.failAt
	sequence := validator.sequence
	hook := validator.hook
	validator.mutex.Unlock()
	if hook != nil {
		hook(call)
	}
	if call == failAt {
		return controller.Grant{}, errors.New("revoked")
	}
	if sequence > binding.RenewalSequence {
		binding.RenewalSequence = sequence
	}
	return controller.Grant{Binding: binding, RemainingTTLMS: 10000}, nil
}
func securityBinding() controller.Binding {
	return controller.Binding{Database: securityDatabase(), RootIncarnation: "root-a", AuthorityTerm: 1, BranchIncarnation: 1, Epoch: 10, WorkerID: "worker-a", LeaseID: "lease", GrantID: "grant", Role: "writer"}
}
func securityBroker(test *testing.T) (StateStore, KMS, *keyBroker, *fakeLeaseValidator) {
	test.Helper()
	store := securityState(test)
	kms := securityKMS(test)
	validator := &fakeLeaseValidator{}
	service, err := NewKeyBroker(store, kms, validator, map[string]string{"worker-a": brokerTestCredential})
	if err != nil {
		if _, clockErr := continuousNanos(); clockErr != nil {
			test.Skip("platform continuous clock unsupported")
		}
		test.Fatal(err)
	}
	return store, kms, service.(*keyBroker), validator
}
func keyCall(test *testing.T, broker KeyBroker, binding controller.Binding, credential, role string) *httptest.ResponseRecorder {
	test.Helper()
	input := keyRequest{Version: 1, RequestID: "key-request", Database: binding.Database, Binding: binding, AccessRole: role}
	encoded, _ := json.Marshal(input)
	request := httptest.NewRequest("POST", "http://127.0.0.1/internal/key", bytes.NewReader(encoded))
	request.Header.Set("Authorization", "Bearer "+credential)
	response := httptest.NewRecorder()
	broker.ServeHTTP(response, request)
	return response
}
func resultKey(test *testing.T, response *httptest.ResponseRecorder) keyResult {
	test.Helper()
	if response.Code != 200 {
		test.Fatalf("key response %d: %s", response.Code, response.Body.String())
	}
	var reply keyReply
	if json.Unmarshal(response.Body.Bytes(), &reply) != nil || !reply.OK || reply.Result == nil || reply.Error != nil {
		test.Fatal("invalid broker reply")
	}
	return *reply.Result
}

func TestBrokerProvisionFirstBindRetryAndRewrapPreserveOneDEK(test *testing.T) {
	store, kms, broker, validator := securityBroker(test)
	namespace := securityDatabase().Namespace
	unbound, err := broker.Provision(context.Background(), namespace, "provision")
	if err != nil {
		test.Fatal(err)
	}
	if unbound.State != "sealed_unbound" || unbound.Revision != 1 {
		test.Fatal("unbound descriptor")
	}
	retry, err := broker.Provision(context.Background(), namespace, "provision")
	if err != nil || retry.DEKID != unbound.DEKID {
		test.Fatal("provision retry regenerated DEK")
	}
	if _, err := broker.Provision(context.Background(), namespace, "other-operation"); err == nil {
		test.Fatal("namespace reused")
	}
	firstResponse := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer")
	first := resultKey(test, firstResponse)
	if first.DescriptorRevision != 2 || first.DEKID != unbound.DEKID || len(first.DEKHex) != 64 || first.ValidForMS == 0 || first.ValidForMS > 1000 {
		test.Fatal("first bind contract")
	}
	if firstResponse.Header().Get("Cache-Control") != "no-store" {
		test.Fatal("plaintext response cache allowed")
	}
	second := resultKey(test, keyCall(test, broker, securityBinding(), brokerTestCredential, "writer"))
	if first.DEKHex != second.DEKHex || validator.calls != 4 {
		test.Fatal("retry cached key or changed DEK")
	}
	if err := RotateLocalKMS(kms); err != nil {
		test.Fatal(err)
	}
	if err := broker.Rewrap(context.Background(), namespace); err != nil {
		test.Fatal(err)
	}
	third := resultKey(test, keyCall(test, broker, securityBinding(), brokerTestCredential, "writer"))
	if third.DEKHex != first.DEKHex || third.DomainID != first.DomainID || third.DescriptorRevision != 3 {
		test.Fatal("rewrap changed encryption identity")
	}
	state, _ := store.Snapshot()
	encoded, _ := json.Marshal(state)
	if strings.Contains(string(encoded), first.DEKHex) {
		test.Fatal("plaintext DEK in metadata")
	}
}

func TestBrokerWorkerIdentityRoleAndRootForgeryDenied(test *testing.T) {
	_, _, broker, validator := securityBroker(test)
	if _, err := broker.Provision(context.Background(), securityDatabase().Namespace, "provision"); err != nil {
		test.Fatal(err)
	}
	forged := securityBinding()
	forged.WorkerID = "worker-b"
	response := keyCall(test, broker, forged, brokerTestCredential, "writer")
	if response.Code != 403 || validator.calls != 0 {
		test.Fatal("worker token substituted binding worker")
	}
	readonly := securityBinding()
	readonly.Role = "read_only"
	if response := keyCall(test, broker, readonly, brokerTestCredential, "writer"); response.Code == 200 {
		test.Fatal("readonly grant elevated")
	}
	reader := resultKey(test, keyCall(test, broker, readonly, brokerTestCredential, "read_only"))
	if reader.AccessRole != "read_only" {
		test.Fatal("narrowed role lost")
	}
	otherRoot := securityBinding()
	otherRoot.RootIncarnation = "other-root"
	if response := keyCall(test, broker, otherRoot, brokerTestCredential, "writer"); response.Code != 403 {
		test.Fatal("descriptor rebound to other root")
	}
}

func TestBrokerRevokeDuringUnwrapWithholdsKeyAfterPersistedFirstBind(test *testing.T) {
	store, _, broker, validator := securityBroker(test)
	broker.Provision(context.Background(), securityDatabase().Namespace, "provision")
	validator.failAt = 2
	response := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer")
	if response.Code != 409 || strings.Contains(response.Body.String(), "dek_hex") {
		test.Fatal("key released after final validation failed")
	}
	state, _ := store.Snapshot()
	if state.Keys[namespaceKey(securityDatabase().Namespace)].State != "ready" {
		test.Fatal("first bind not persisted before validation")
	}
}

func TestBrokerRenewalAndConflictingConcurrentRoots(test *testing.T) {
	_, _, broker, validator := securityBroker(test)
	broker.Provision(context.Background(), securityDatabase().Namespace, "provision")
	validator.sequence = 4
	result := resultKey(test, keyCall(test, broker, securityBinding(), brokerTestCredential, "read_only"))
	if result.ValidatedBinding.RenewalSequence != 4 || result.AccessRole != "read_only" {
		test.Fatal("renewal/narrowing mismatch")
	}
	store := securityState(test)
	kms := securityKMS(test)
	service, err := NewKeyBroker(store, kms, &fakeLeaseValidator{}, map[string]string{"worker-a": brokerTestCredential})
	if err != nil {
		test.Fatal(err)
	}
	service.Provision(context.Background(), securityDatabase().Namespace, "provision")
	var group sync.WaitGroup
	responses := make(chan int, 2)
	for _, root := range []string{"root-a", "root-b"} {
		group.Add(1)
		go func(root string) {
			defer group.Done()
			binding := securityBinding()
			binding.RootIncarnation = root
			responses <- keyCall(test, service, binding, brokerTestCredential, "writer").Code
		}(root)
	}
	group.Wait()
	close(responses)
	successes := 0
	denied := 0
	for status := range responses {
		if status == 200 {
			successes++
		}
		if status == 403 {
			denied++
		}
	}
	if successes != 1 || denied != 1 {
		test.Fatalf("conflicting first bindings: success%d denied%d", successes, denied)
	}
}

func TestBrokerClockRegressionStickyUnavailableAndClockScaling(test *testing.T) {
	_, _, broker, _ := securityBroker(test)
	broker.Provision(context.Background(), securityDatabase().Namespace, "provision")
	samples := []uint64{10, 9}
	position := 0
	broker.clock = &brokerClock{sample: func() (uint64, error) {
		value := samples[position]
		if position == 0 {
			position++
		}
		return value, nil
	}}
	response := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer")
	if response.Code != 503 {
		test.Fatal("clock regression allowed key")
	}
	broker.clock.sample = func() (uint64, error) { return 100, nil }
	if response := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer"); response.Code != 503 {
		test.Fatal("clock uncertainty reopened")
	}
	if value, err := clockScale(9, 2, 3); err != nil || value != 6 {
		test.Fatal("timebase scaling")
	}
	if _, err := clockScale(^uint64(0), 2, 1); err == nil {
		test.Fatal("clock overflow")
	}
	if _, err := clockScale(1, 0, 1); err == nil {
		test.Fatal("invalid timebase")
	}
}

func TestBrokerMalformedRequestDoesNotValidateOrRevealKeys(test *testing.T) {
	_, _, broker, validator := securityBroker(test)
	for _, body := range []string{`{"version":1,"version":1}`, `{"version":1,"path":"authority/root"}`, strings.Repeat("x", 17000)} {
		request := httptest.NewRequest("POST", "http://127.0.0.1/internal/key", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+brokerTestCredential)
		response := httptest.NewRecorder()
		broker.ServeHTTP(response, request)
		if response.Code != 400 || strings.Contains(response.Body.String(), "dek_hex") {
			test.Fatal("malformed key request accepted")
		}
	}
	if validator.calls != 0 {
		test.Fatal("malformed request hit authority")
	}
	if response := keyCall(test, broker, securityBinding(), strings.Repeat("wrong-credential", 3), "writer"); response.Code != http.StatusUnauthorized {
		test.Fatal("unauthenticated key delivery")
	}
}
