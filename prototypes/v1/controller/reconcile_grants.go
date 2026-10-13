package controller

import (
	"context"
	"encoding/json"
	"errors"
	"math"
)

func (controller *Controller) planGrant(ctx context.Context, placement *Placement) error {
	actual, err := controller.authorityState(ctx, placement.Database)
	if err != nil {
		return err
	}
	identifier, err := RandomID()
	if err != nil {
		return err
	}
	leaseID, err := RandomID()
	if err != nil {
		return err
	}
	return controller.store.Update(func(metadata *Metadata) error {
		floor := metadata.Epoch
		if actual.Epoch > floor {
			floor = actual.Epoch
		}
		if placement.Epoch > floor {
			floor = placement.Epoch
		}
		if floor == math.MaxUint64 {
			return errors.New("epoch exhausted")
		}
		candidate := *placement
		candidate.Epoch = floor + 1
		candidate.LeaseID = leaseID
		candidate.PendingID = identifier
		candidate.Phase = "grant_intent"
		candidate.LastError = ""
		intent := grantIntent{Database: candidate.Database, WorkerID: candidate.DesiredWorker, Epoch: candidate.Epoch, ExpectedEpoch: actual.Epoch, LeaseID: leaseID, TTLMS: candidate.TTLMS, Role: candidate.Role}
		payload, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		candidate.PendingCommand, err = json.Marshal(authorityAction{Command: "challenge", Intent: &leaseIntent{Kind: "grant", Intent: payload}})
		if err != nil {
			return err
		}
		if metadata.Placements == nil {
			metadata.Placements = make(map[string]Placement)
		}
		metadata.Epoch = candidate.Epoch
		metadata.Placements[candidate.Database.Key()] = candidate
		*placement = candidate
		return nil
	})
}

func (controller *Controller) planRenew(placement *Placement) error {
	if placement.Grant == nil || placement.Grant.Binding.RenewalSequence == math.MaxUint64 {
		return errors.New("renewal sequence exhausted or grant missing")
	}
	identifier, err := RandomID()
	if err != nil {
		return err
	}
	intent := renewIntent{Binding: placement.Grant.Binding, Sequence: placement.Grant.Binding.RenewalSequence + 1, TTLMS: placement.TTLMS}
	payload, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	command, err := json.Marshal(authorityAction{Command: "challenge", Intent: &leaseIntent{Kind: "renew", Intent: payload}})
	if err != nil {
		return err
	}
	placement.PendingID = identifier
	placement.PendingCommand = command
	placement.Phase = "renew_intent"
	return controller.save(*placement)
}

func (controller *Controller) challenge(ctx context.Context, placement *Placement) error {
	var command authorityAction
	if err := json.Unmarshal(placement.PendingCommand, &command); err != nil {
		return err
	}
	if command.Command != "challenge" || command.Intent == nil {
		return errors.New("pending challenge invalid")
	}
	var reply struct {
		Nonce       string `json:"nonce"`
		ExpiresInMS uint64 `json:"expires_in_ms"`
	}
	if err := controller.transport.Authority(ctx, placement.PendingID, command, &reply); err != nil {
		return err
	}
	if reply.Nonce == "" {
		return errors.New("authority challenge nonce missing")
	}
	request := leaseRequest{RequestID: placement.PendingID, Nonce: reply.Nonce, Intent: command.Intent.Intent}
	mutation := authorityAction{Command: command.Intent.Kind, Request: &request}
	bytes, err := json.Marshal(mutation)
	if err != nil {
		return err
	}
	placement.PendingCommand = bytes
	placement.Phase = command.Intent.Kind + "_pending"
	return controller.save(*placement)
}

func (controller *Controller) mutateAuthority(ctx context.Context, placement *Placement) error {
	if placement.Phase == "revoke_pending" {
		var result json.RawMessage
		if err := controller.transport.Authority(ctx, placement.PendingID, placement.PendingCommand, &result); err != nil {
			return err
		}
		placement.Phase = "suspended"
		clearPending(placement)
		return controller.save(*placement)
	}
	var command authorityAction
	if err := json.Unmarshal(placement.PendingCommand, &command); err != nil {
		return err
	}
	if command.Request == nil || command.Request.RequestID != placement.PendingID {
		return errors.New("pending mutation identity invalid")
	}
	var grant Grant
	if err := controller.transport.Authority(ctx, placement.PendingID, placement.PendingCommand, &grant); err != nil {
		if placement.Phase == "renew_pending" {
			return controller.recoverExpiredRenewal(ctx, placement, err)
		}
		return err
	}
	if grant.RemainingTTLMS == 0 || grant.RemainingTTLMS > 60000 {
		err := errors.New("authority grant is expired or invalid")
		if placement.Phase == "renew_pending" && grant.RemainingTTLMS == 0 {
			return controller.recoverExpiredRenewal(ctx, placement, err)
		}
		return err
	}
	if command.Command == "grant" {
		var intent grantIntent
		if err := json.Unmarshal(command.Request.Intent, &intent); err != nil {
			return err
		}
		if grant.Binding.Database != intent.Database || grant.Binding.WorkerID != intent.WorkerID || grant.Binding.Epoch != intent.Epoch || grant.Binding.LeaseID != intent.LeaseID || grant.Binding.GrantID != placement.PendingID || grant.Binding.Role != intent.Role || grant.Binding.RenewalSequence != 0 {
			return errors.New("authority grant binding mismatch")
		}
		placement.Phase = "acquire_pending"
	} else if command.Command == "renew" {
		var intent renewIntent
		if err := json.Unmarshal(command.Request.Intent, &intent); err != nil {
			return err
		}
		if !sameLease(intent.Binding, grant.Binding) || grant.Binding.RenewalSequence != intent.Sequence {
			return errors.New("authority renewal binding mismatch")
		}
		placement.Phase = "renew_worker_pending"
	} else {
		return errors.New("unsupported pending authority mutation")
	}
	if err := validateBinding(grant.Binding); err != nil {
		return err
	}
	placement.Grant = &grant
	return controller.save(*placement)
}

func (controller *Controller) recoverExpiredRenewal(ctx context.Context, placement *Placement, pendingErr error) error {
	actual, err := controller.authorityState(ctx, placement.Database)
	if err != nil {
		return errors.Join(pendingErr, err)
	}
	if actual.Mode != "revoked" || actual.Binding != nil || actual.RemainingTTLMS != 0 || actual.Epoch < placement.Epoch {
		return pendingErr
	}
	return controller.planGrant(ctx, placement)
}

func (controller *Controller) planRevoke(placement *Placement) error {
	identifier, err := RandomID()
	if err != nil {
		return err
	}
	command := revokeAction{Command: "revoke"}
	command.Request.RequestID = identifier
	command.Request.Database = placement.Database
	command.Request.ExpectedEpoch = placement.Epoch
	bytes, err := json.Marshal(command)
	if err != nil {
		return err
	}
	placement.PendingID = identifier
	placement.PendingCommand = bytes
	placement.Phase = "revoke_pending"
	return controller.save(*placement)
}

func validateBinding(binding Binding) error {
	if err := binding.Database.Validate(); err != nil {
		return err
	}
	if !componentPattern.MatchString(binding.RootIncarnation) || binding.AuthorityTerm == 0 || binding.BranchIncarnation == 0 || binding.Epoch == 0 || !componentPattern.MatchString(binding.WorkerID) || !componentPattern.MatchString(binding.LeaseID) || !componentPattern.MatchString(binding.GrantID) || (binding.Role != "writer" && binding.Role != "read_only") {
		return errors.New("invalid lease binding")
	}
	return nil
}
