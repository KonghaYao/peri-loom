package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
)

func decodeReconcileJSON(payload []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return errors.New("invalid durable protocol payload")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("multiple durable protocol values")
	}
	return nil
}

func validatePlacementProtocol(placement Placement) error {
	if placement.Epoch == 0 || !componentPattern.MatchString(placement.DesiredWorker) || !componentPattern.MatchString(placement.LeaseID) || (placement.Region != "" && !componentPattern.MatchString(placement.Region)) {
		return errors.New("invalid placement identity")
	}
	if placement.Grant != nil {
		binding := placement.Grant.Binding
		if err := validateBinding(binding); err != nil {
			return err
		}
		if binding.Database != placement.Database || binding.Epoch > placement.Epoch {
			return errors.New("placement grant identity mismatch")
		}
		if placement.Phase == "active" || placement.Phase == "acquire_pending" || placement.Phase == "renew_worker_pending" || placement.Phase == "suspended" || placement.Phase == "idle" || placement.Phase == "idle_drain" {
			if binding.Epoch != placement.Epoch || binding.WorkerID != placement.DesiredWorker || binding.LeaseID != placement.LeaseID || binding.Role != placement.Role {
				return errors.New("placement serving grant mismatch")
			}
		}
	}
	switch placement.Phase {
	case "active", "suspended", "migrate_drain", "suspend_drain", "idle", "idle_drain":
		if placement.PendingID != "" || len(placement.PendingCommand) != 0 {
			return errors.New("settled placement retains pending authority request")
		}
		return nil
	case "revoke_pending":
		var command revokeAction
		if err := decodeReconcileJSON(placement.PendingCommand, &command); err != nil {
			return err
		}
		if command.Command != "revoke" || command.Request.RequestID != placement.PendingID || command.Request.Database != placement.Database || command.Request.ExpectedEpoch != placement.Epoch {
			return errors.New("pending revoke identity mismatch")
		}
		return nil
	}
	if !componentPattern.MatchString(placement.PendingID) {
		return errors.New("invalid pending request identifier")
	}
	var command authorityAction
	if err := decodeReconcileJSON(placement.PendingCommand, &command); err != nil {
		return err
	}
	var kind string
	var intent json.RawMessage
	if placement.Phase == "grant_intent" || placement.Phase == "renew_intent" {
		if command.Command != "challenge" || command.Intent == nil || command.Request != nil || command.Database != nil {
			return errors.New("pending challenge shape mismatch")
		}
		kind, intent = command.Intent.Kind, command.Intent.Intent
	} else {
		if command.Request == nil || command.Intent != nil || command.Database != nil || command.Request.RequestID != placement.PendingID || !componentPattern.MatchString(command.Request.Nonce) {
			return errors.New("pending mutator shape mismatch")
		}
		kind, intent = command.Command, command.Request.Intent
	}
	if placement.Phase == "grant_intent" || placement.Phase == "grant_pending" || placement.Phase == "acquire_pending" {
		var grant grantIntent
		if err := decodeReconcileJSON(intent, &grant); err != nil {
			return err
		}
		if kind != "grant" || grant.Database != placement.Database || grant.WorkerID != placement.DesiredWorker || grant.Epoch != placement.Epoch || grant.ExpectedEpoch >= grant.Epoch || grant.LeaseID != placement.LeaseID || grant.TTLMS != placement.TTLMS || grant.Role != placement.Role {
			return errors.New("durable grant intent mismatch")
		}
		if placement.Phase == "acquire_pending" && placement.Grant.Binding.GrantID != placement.PendingID {
			return errors.New("confirmed grant receipt identity mismatch")
		}
		return nil
	}
	var renew renewIntent
	if err := decodeReconcileJSON(intent, &renew); err != nil {
		return err
	}
	if kind != "renew" || placement.Grant == nil || renew.Binding.RenewalSequence == math.MaxUint64 || renew.Sequence != renew.Binding.RenewalSequence+1 || renew.TTLMS != placement.TTLMS {
		return errors.New("durable renewal intent mismatch")
	}
	if placement.Phase == "renew_worker_pending" {
		if !sameLease(renew.Binding, placement.Grant.Binding) || renew.Sequence != placement.Grant.Binding.RenewalSequence {
			return errors.New("confirmed renewal receipt identity mismatch")
		}
	} else if renew.Binding != placement.Grant.Binding {
		return errors.New("pending renewal binding mismatch")
	}
	return nil
}
