package platform

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
)

func (management *Management) operations(principal Principal, input managementRequest, path string) (any, error) {
	if input.Database.Validate() != nil {
		return nil, errors.New("invalid database")
	}
	if err := management.config.Identity.Authorize(principal, input.Database, "manage"); err != nil {
		return nil, err
	}
	if path == "/v1/databases/operations/clear" {
		return nil, ErrDatabaseQuarantined
	}
	if path == "/v1/databases/operations/decision" {
		if input.Decision != "retain_isolation_new_database_required" || !sqlLabel(input.OperationID) {
			return nil, ErrDatabaseQuarantined
		}
		if err := management.config.State.Update(func(state *State) error {
			key := operationKey(input.Database, input.OperationID)
			record, exists := state.Operations[key]
			if !exists || record.State != "quarantined" {
				return ErrDatabaseQuarantined
			}
			if record.Decision != "" {
				if record.Decision == input.Decision && record.DecisionRequestID == input.RequestID {
					return nil
				}
				return ErrOperationExists
			}
			record.Decision, record.DecisionRequestID = input.Decision, input.RequestID
			state.Operations[key] = record
			return nil
		}); err != nil {
			return nil, err
		}
	}
	state, err := management.config.State.Snapshot()
	if err != nil {
		return nil, err
	}
	records := []OperationStatus{}
	for _, record := range state.Operations {
		if record.Database.Namespace == input.Database.Namespace && management.config.Identity.Authorize(principal, record.Database, "manage") == nil && (input.OperationID == "" || record.OperationID == input.OperationID) {
			records = append(records, record.OperationStatus)
		}
	}
	sort.Slice(records, func(first, second int) bool { return records[first].OperationID < records[second].OperationID })
	return map[string]any{"operations": records, "recovery": "new_database_or_fixture_required", "retention": operationRetentionContract()}, nil
}

func managementUnknownReply(response http.ResponseWriter, requestID string, failure error) {
	stage := "unknown"
	var tool *nativeToolError
	if errors.As(failure, &tool) {
		switch tool.stage {
		case "open_session", "tool_rpc", "close_session":
			stage = tool.stage
		}
	}
	var rpc *SQLRPCError
	flags := map[string]any{"code": "outcome_unknown", "stage": stage, "retry_allowed": false, "business_outcome": "unknown", "cleanup_outcome": "not_proven", "execution_completed": false, "may_have_committed": true}
	if tool != nil && tool.operationID != "" {
		flags["operation_id"] = tool.operationID
	}
	flags["execution_proven"] = false
	if errors.As(failure, &rpc) {
		flags["ambiguous"], flags["rejected"], flags["execution_completed"] = rpc.Ambiguous, rpc.Rejected, rpc.ExecutionCompleted
	}
	if stage == "close_session" {
		flags["business_outcome"], flags["cleanup_outcome"] = "completed", "unknown"
		flags["execution_proven"] = true
	}
	response.WriteHeader(502)
	_ = json.NewEncoder(response).Encode(map[string]any{"version": 1, "request_id": requestID, "ok": false, "result": nil, "error": flags})
}

func managementOutcomeUnknown(failure error) bool {
	var rpc *SQLRPCError
	var tool *nativeToolError
	return errors.As(failure, &rpc) && rpc.Ambiguous || errors.As(failure, &tool) && tool.unknown
}
