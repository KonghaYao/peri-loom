package platform

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lostAckState struct {
	StateStore
	unknown atomic.Bool
	fail    atomic.Bool
}

func (store *lostAckState) Snapshot() (State, error) {
	if store.unknown.Load() {
		return State{}, ErrStateUncertain
	}
	return store.StateStore.Snapshot()
}
func (store *lostAckState) Update(mutation func(*State) error) error {
	if store.unknown.Load() {
		return ErrStateUncertain
	}
	if err := store.StateStore.Update(mutation); err != nil {
		return err
	}
	if store.fail.Swap(false) {
		store.unknown.Store(true)
		return ErrStateUncertain
	}
	return nil
}

func TestBrokerProvisionLostACKAndBindRestartNeverRegenerateDEK(test *testing.T) {
	if _, err := continuousNanos(); err != nil {
		test.Skip("continuous clock unsupported")
	}
	directory := test.TempDir()
	state, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	kmsPath := filepath.Join(test.TempDir(), "ring.json")
	kms, err := NewLocalKMS(kmsPath)
	if err != nil {
		test.Fatal(err)
	}
	defer CloseLocalKMS(kms)
	lost := &lostAckState{StateStore: state}
	lost.fail.Store(true)
	service, err := NewKeyBroker(lost, kms, &fakeLeaseValidator{}, map[string]string{"worker-a": brokerTestCredential})
	if err != nil {
		test.Fatal(err)
	}
	if _, err := service.Provision(context.Background(), securityDatabase().Namespace, "provision"); !errors.Is(err, ErrStateUncertain) {
		test.Fatal("provision ACK loss swallowed")
	}
	if _, err := service.Provision(context.Background(), securityDatabase().Namespace, "provision"); err == nil {
		test.Fatal("poisoned provision reopened")
	}
	state.Close()
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	fresh, err := NewKeyBroker(reopened, kms, &fakeLeaseValidator{}, map[string]string{"worker-a": brokerTestCredential})
	if err != nil {
		test.Fatal(err)
	}
	descriptor, err := fresh.Provision(context.Background(), securityDatabase().Namespace, "provision")
	if err != nil {
		test.Fatal(err)
	}
	first := resultKey(test, keyCall(test, fresh, securityBinding(), brokerTestCredential, "writer"))
	if descriptor.DEKID != first.DEKID {
		test.Fatal("lost ACK regenerated key")
	}
	if kms.(*localKMS).ring.Uses["1"] != 2 {
		test.Fatal("provision was re-encrypted instead of replayed")
	}
	reopened.Close()
	finalStore, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer finalStore.Close()
	finalBroker, err := NewKeyBroker(finalStore, kms, &fakeLeaseValidator{}, map[string]string{"worker-a": brokerTestCredential})
	if err != nil {
		test.Fatal(err)
	}
	second := resultKey(test, keyCall(test, finalBroker, securityBinding(), brokerTestCredential, "writer"))
	if first.DEKHex != second.DEKHex || second.DescriptorRevision != 2 {
		test.Fatal("bound restart regenerated key")
	}
}

func TestBrokerRateLimitAndNoCachedPlaintextResponse(test *testing.T) {
	_, _, broker, validator := securityBroker(test)
	broker.Provision(context.Background(), securityDatabase().Namespace, "provision")
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	validator.hook = func(call int) {
		if call <= 2 {
			entered <- struct{}{}
			<-release
		}
	}
	responses := make(chan *httptest.ResponseRecorder, 2)
	for index := 0; index < 2; index++ {
		go func() { responses <- keyCall(test, broker, securityBinding(), brokerTestCredential, "writer") }()
	}
	for index := 0; index < 2; index++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			test.Fatal("key validation did not pause")
		}
	}
	third := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer")
	if third.Code != 429 || strings.Contains(third.Body.String(), "dek_hex") {
		test.Fatal("broker admission limit not enforced")
	}
	once.Do(func() { close(release) })
	for index := 0; index < 2; index++ {
		resultKey(test, <-responses)
	}
}

func TestBrokerUnsupportedContinuousClockRefusesStartup(test *testing.T) {
	if _, err := continuousNanos(); err == nil {
		test.Skip("continuous clock supported")
	}
	store := securityState(test)
	kms := securityKMS(test)
	if _, err := NewKeyBroker(store, kms, &fakeLeaseValidator{}, map[string]string{"worker-a": brokerTestCredential}); err == nil {
		test.Fatal("unsupported clock accepted")
	}
}

func TestBrokerFirstBindingLostACKWithholdsKeyUntilVerifiedRestart(test *testing.T) {
	store, kms, _, validator := securityBroker(test)
	lost := &lostAckState{StateStore: store}
	broker, err := NewKeyBroker(lost, kms, validator, map[string]string{"worker-a": brokerTestCredential})
	if err != nil {
		test.Fatal(err)
	}
	descriptor, err := broker.Provision(context.Background(), securityDatabase().Namespace, "provision")
	if err != nil {
		test.Fatal(err)
	}
	original, err := kms.Open(descriptor.Envelope, descriptorAAD(descriptor))
	if err != nil {
		test.Fatal(err)
	}
	defer clear(original)
	lost.fail.Store(true)
	response := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer")
	if response.Code != 503 || strings.Contains(response.Body.String(), "dek_hex") {
		test.Fatal("lost first-bind ACK released key")
	}
	if response := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer"); response.Code != 503 {
		test.Fatal("unknown store outcome admitted delivery")
	}
	directory := filepath.Dir(store.(*stateStore).disk.path)
	store.Close()
	reopened, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	fresh, err := NewKeyBroker(reopened, kms, &fakeLeaseValidator{}, map[string]string{"worker-a": brokerTestCredential})
	if err != nil {
		test.Fatal(err)
	}
	result := resultKey(test, keyCall(test, fresh, securityBinding(), brokerTestCredential, "writer"))
	if result.DEKHex != hex.EncodeToString(original) || result.DescriptorRevision != 2 || kms.(*localKMS).ring.Uses["1"] != 2 {
		test.Fatal("lost bind ACK regenerated/rebound DEK")
	}
}

func TestBrokerFinalValidationElapsedTimeCannotExtendDeliveryHint(test *testing.T) {
	_, _, broker, validator := securityBroker(test)
	if _, err := broker.Provision(context.Background(), securityDatabase().Namespace, "provision"); err != nil {
		test.Fatal(err)
	}
	var now atomic.Uint64
	now.Store(1000000)
	broker.clock = &brokerClock{sample: func() (uint64, error) { return now.Load(), nil }}
	validator.hook = func(call int) {
		if call == 2 {
			now.Add(10001000000)
		}
	}
	response := keyCall(test, broker, securityBinding(), brokerTestCredential, "writer")
	if response.Code != 409 || strings.Contains(response.Body.String(), "dek_hex") {
		test.Fatal("slow final validation extended freshness")
	}
}
