package controller

import (
	"encoding/json"
	"errors"
	"strings"
)

func validateMetadata(metadata Metadata, previous *Metadata) error {
	if metadata.Version != 1 || metadata.Workers == nil || metadata.Placements == nil {
		return errors.New("invalid metadata version or maps")
	}
	if previous != nil && metadata.Epoch < previous.Epoch {
		return errors.New("metadata epoch cannot decrease")
	}
	for key, worker := range metadata.Workers {
		if key != worker.ID {
			return errors.New("worker registry key mismatch")
		}
		if err := validateWorker(worker); err != nil {
			return err
		}
		if previous != nil {
			if old, exists := previous.Workers[key]; exists && worker.LastHeartbeatMS < old.LastHeartbeatMS {
				return errors.New("worker heartbeat cannot decrease")
			}
		}
	}
	for key, placement := range metadata.Placements {
		if placement.Database.Validate() != nil || key != placement.Database.Key() {
			return errors.New("placement database key mismatch")
		}
		if placement.Epoch > metadata.Epoch {
			return errors.New("placement epoch exceeds durable allocation")
		}
		if previous != nil {
			if old, exists := previous.Placements[key]; exists && placement.Epoch < old.Epoch {
				return errors.New("placement epoch cannot decrease")
			}
		}
		if placement.Role != "writer" && placement.Role != "read_only" {
			return errors.New("invalid placement role")
		}
		if placement.TTLMS < 100 || placement.TTLMS > 60000 || (placement.DesiredState != "active" && placement.DesiredState != "suspended") || !componentPattern.MatchString(placement.Phase) {
			return errors.New("invalid placement timing, state or phase label")
		}
		if !componentPattern.MatchString(placement.DesiredWorker) {
			return errors.New("invalid desired worker identifier")
		}
		pending := strings.HasSuffix(placement.Phase, "_pending") || strings.HasSuffix(placement.Phase, "_intent")
		if (pending || len(placement.PendingCommand) != 0) && (!componentPattern.MatchString(placement.PendingID) || len(placement.PendingCommand) == 0) {
			return errors.New("durable pending request identity and command required")
		}
		if placement.Grant != nil && (placement.Grant.Binding.Database != placement.Database || placement.Grant.Binding.Epoch > placement.Epoch) {
			return errors.New("placement grant database/epoch mismatch")
		}
		if len(placement.PendingCommand) != 0 {
			var command any
			if err := json.Unmarshal(placement.PendingCommand, &command); err != nil {
				return errors.New("invalid pending command JSON")
			}
			if hasCredentials(command) {
				return errors.New("credentials forbidden in durable metadata")
			}
		}
	}
	return nil
}

func hasCredentials(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for name, child := range typed {
			normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "_", ""), "-", ""))
			switch normalized {
			case "authorization", "password", "secret", "secretkey", "accesskey", "sessiontoken", "token", "credential", "credentials":
				return true
			}
			if hasCredentials(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if hasCredentials(child) {
				return true
			}
		}
	}
	return false
}
