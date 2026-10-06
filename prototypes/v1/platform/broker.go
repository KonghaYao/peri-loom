package platform

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"sync"

	controller "peri-loom/controller"
)

var errLeaseInvalid = errors.New("LEASE_INVALID")
var errKeyNotReady = errors.New("KEY_NOT_READY")
var errKeyForbidden = errors.New("FORBIDDEN")
var errBrokerUnavailable = errors.New("UNAVAILABLE")

type keyBroker struct {
	store       StateStore
	kms         KMS
	validator   LeaseValidator
	credentials map[string][32]byte
	inflight    map[string]chan struct{}
	mutex       sync.Mutex
	clock       *brokerClock
}

func NewKeyBroker(store StateStore, kms KMS, validator LeaseValidator, workerKeyTokens map[string]string) (KeyBroker, error) {
	if store == nil || kms == nil || validator == nil || len(workerKeyTokens) == 0 {
		return nil, errors.New("broker dependencies and worker credentials required")
	}
	broker := &keyBroker{store: store, kms: kms, validator: validator, credentials: map[string][32]byte{}, inflight: map[string]chan struct{}{}, clock: &brokerClock{sample: continuousNanos}}
	seen := map[string]bool{}
	for worker, token := range workerKeyTokens {
		if !identifierPattern.MatchString(worker) || !validCredential(token) || seen[token] {
			return nil, errors.New("invalid or duplicate broker credential")
		}
		seen[token] = true
		if authority, ok := validator.(*authorityValidator); ok {
			if _, present := authority.transports[worker]; !present {
				return nil, errors.New("broker worker authority credential missing")
			}
			for _, digest := range authority.credentials {
				if digest == digestHex([]byte(token)) {
					return nil, errors.New("broker and authority credentials must be distinct")
				}
			}
		}
		broker.credentials[worker] = sha256.Sum256([]byte(token))
		broker.inflight[worker] = make(chan struct{}, 2)
	}
	if _, err := broker.clock.now(); err != nil {
		return nil, err
	}
	if _, err := store.Snapshot(); err != nil {
		return nil, err
	}
	return broker, nil
}

func (broker *keyBroker) Provision(ctx context.Context, namespace controller.Namespace, operationID string) (KeyDescriptor, error) {
	if !validNamespace(namespace) || !identifierPattern.MatchString(operationID) {
		return KeyDescriptor{}, errors.New("invalid namespace provisioning identity")
	}
	if ctx.Err() != nil {
		return KeyDescriptor{}, ctx.Err()
	}
	if _, err := broker.clock.now(); err != nil {
		return KeyDescriptor{}, err
	}
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	state, err := broker.store.Snapshot()
	if err != nil {
		return KeyDescriptor{}, err
	}
	key := namespaceKey(namespace)
	if existing, ok := state.Keys[key]; ok {
		if existing.ProvisionID != operationID {
			return KeyDescriptor{}, errors.New("namespace permanently reserved")
		}
		return existing, nil
	}
	for _, existing := range state.Keys {
		if existing.ProvisionID == operationID {
			return KeyDescriptor{}, errors.New("provision operation identity conflict")
		}
	}
	for _, database := range state.Databases {
		if database.ID.Namespace == namespace && (!database.Encrypted || database.RootIncarnation != "" || database.OperationID != operationID) {
			return KeyDescriptor{}, errors.New("known existing namespace cannot be encrypted in place")
		}
	}
	id, err := randomLabel()
	if err != nil {
		return KeyDescriptor{}, err
	}
	domain, err := randomLabel()
	if err != nil {
		return KeyDescriptor{}, err
	}
	dekID, err := randomLabel()
	if err != nil {
		return KeyDescriptor{}, err
	}
	descriptor := KeyDescriptor{ID: id, ProvisionID: operationID, Revision: 1, Namespace: namespace, DomainID: domain, DEKID: dekID, Cipher: "aegis256", State: "sealed_unbound"}
	var dek [32]byte
	if _, err := rand.Read(dek[:]); err != nil {
		return KeyDescriptor{}, errors.New("secure DEK generation failed")
	}
	defer clear(dek[:])
	descriptor.Envelope, err = broker.kms.Seal(dek[:], descriptorAAD(descriptor))
	if err != nil {
		return KeyDescriptor{}, err
	}
	if ctx.Err() != nil {
		return KeyDescriptor{}, ctx.Err()
	}
	err = broker.store.Update(func(state *State) error {
		if _, ok := state.Keys[key]; ok {
			return errors.New("namespace concurrently reserved")
		}
		for _, existing := range state.Keys {
			if existing.ProvisionID == operationID {
				return errors.New("operation concurrently reserved")
			}
		}
		state.Keys[key] = descriptor
		return nil
	})
	if err != nil {
		return KeyDescriptor{}, err
	}
	if ctx.Err() != nil {
		return KeyDescriptor{}, ctx.Err()
	}
	return descriptor, nil
}

func (broker *keyBroker) Rewrap(ctx context.Context, namespace controller.Namespace) error {
	if !validNamespace(namespace) {
		return errors.New("invalid namespace")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, err := broker.clock.now(); err != nil {
		return err
	}
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	state, err := broker.store.Snapshot()
	if err != nil {
		return err
	}
	old, ok := state.Keys[namespaceKey(namespace)]
	if !ok || old.State != "ready" {
		return errKeyNotReady
	}
	dek, err := broker.kms.Open(old.Envelope, descriptorAAD(old))
	if err != nil {
		return err
	}
	defer clear(dek)
	if old.Revision == ^uint64(0) {
		return errors.New("descriptor revisions exhausted")
	}
	next := old
	next.Revision++
	next.Envelope, err = broker.kms.Seal(dek, descriptorAAD(next))
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return broker.store.Update(func(state *State) error {
		current := state.Keys[namespaceKey(namespace)]
		if !sameDescriptor(current, old) {
			return errors.New("descriptor changed")
		}
		state.Keys[namespaceKey(namespace)] = next
		return nil
	})
}

func (broker *keyBroker) resolve(ctx context.Context, binding controller.Binding, role string) (KeyDescriptor, []byte, controller.Grant, uint64, error) {
	if !validBinding(binding, role) {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, errLeaseInvalid
	}
	if _, err := broker.clock.now(); err != nil {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, errBrokerUnavailable
	}
	first, err := broker.validator.Validate(ctx, binding, role)
	if err != nil || ctx.Err() != nil || !validGrant(binding, role, first) {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, errLeaseInvalid
	}
	descriptor, dek, err := broker.unwrapAndBind(ctx, first.Binding)
	if err != nil {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, err
	}
	keep := false
	defer func() {
		if !keep {
			clear(dek)
		}
	}()
	start, err := broker.clock.now()
	if err != nil {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, errBrokerUnavailable
	}
	final, err := broker.validator.Validate(ctx, first.Binding, role)
	if err != nil || ctx.Err() != nil || !validGrant(first.Binding, role, final) {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, errLeaseInvalid
	}
	state, err := broker.store.Snapshot()
	if err != nil {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, errBrokerUnavailable
	}
	current, ok := state.Keys[namespaceKey(binding.Database.Namespace)]
	if !ok || current.State != "ready" || current.RootIncarnation != final.Binding.RootIncarnation || current.ID != descriptor.ID || current.DomainID != descriptor.DomainID || current.DEKID != descriptor.DEKID {
		return KeyDescriptor{}, nil, controller.Grant{}, 0, errKeyNotReady
	}
	keep = true
	return current, dek, final, start, nil
}

func (broker *keyBroker) unwrapAndBind(ctx context.Context, binding controller.Binding) (KeyDescriptor, []byte, error) {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	state, err := broker.store.Snapshot()
	if err != nil {
		return KeyDescriptor{}, nil, errBrokerUnavailable
	}
	descriptor, ok := state.Keys[namespaceKey(binding.Database.Namespace)]
	if !ok {
		return KeyDescriptor{}, nil, errKeyForbidden
	}
	if descriptor.State == "ready" && descriptor.RootIncarnation != binding.RootIncarnation {
		return KeyDescriptor{}, nil, errKeyForbidden
	}
	if descriptor.State != "ready" && descriptor.State != "sealed_unbound" {
		return KeyDescriptor{}, nil, errKeyNotReady
	}
	dek, err := broker.kms.Open(descriptor.Envelope, descriptorAAD(descriptor))
	if err != nil {
		return KeyDescriptor{}, nil, errBrokerUnavailable
	}
	keep := false
	defer func() {
		if !keep {
			clear(dek)
		}
	}()
	if descriptor.State == "sealed_unbound" {
		old := descriptor
		descriptor.State = "ready"
		descriptor.RootIncarnation = binding.RootIncarnation
		descriptor.Revision = 2
		descriptor.Envelope, err = broker.kms.Seal(dek, descriptorAAD(descriptor))
		if err != nil {
			return KeyDescriptor{}, nil, errBrokerUnavailable
		}
		if ctx.Err() != nil {
			return KeyDescriptor{}, nil, errLeaseInvalid
		}
		err = broker.store.Update(func(state *State) error {
			if !sameDescriptor(state.Keys[namespaceKey(binding.Database.Namespace)], old) {
				return errors.New("first binding changed")
			}
			state.Keys[namespaceKey(binding.Database.Namespace)] = descriptor
			return nil
		})
		if err != nil {
			return KeyDescriptor{}, nil, errBrokerUnavailable
		}
	}
	if ctx.Err() != nil {
		return KeyDescriptor{}, nil, errLeaseInvalid
	}
	keep = true
	return descriptor, dek, nil
}

func descriptorAAD(descriptor KeyDescriptor) []byte {
	var encoded []byte
	for _, field := range []string{"peri-dek-domain-v1", descriptor.ID, descriptor.Namespace.Tenant, descriptor.Namespace.Database, descriptor.DomainID, descriptor.DEKID, descriptor.Cipher, descriptor.RootIncarnation} {
		encoded = appendField(encoded, []byte(field))
	}
	return encoded
}
func sameDescriptor(left, right KeyDescriptor) bool {
	leftEnvelope := left.Envelope
	rightEnvelope := right.Envelope
	left.Envelope = nil
	right.Envelope = nil
	return left.ID == right.ID && left.ProvisionID == right.ProvisionID && left.Revision == right.Revision && left.Namespace == right.Namespace && left.RootIncarnation == right.RootIncarnation && left.DomainID == right.DomainID && left.DEKID == right.DEKID && left.Cipher == right.Cipher && left.State == right.State && bytes.Equal(leftEnvelope, rightEnvelope)
}
func (broker *keyBroker) worker(credential string) string {
	digest := sha256.Sum256([]byte(credential))
	found := ""
	for worker, expected := range broker.credentials {
		if subtle.ConstantTimeCompare(digest[:], expected[:]) == 1 {
			found = worker
		}
	}
	return found
}
func keyHex(dek []byte) string { return hex.EncodeToString(dek) }
