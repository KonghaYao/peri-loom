package platform

import (
	"context"
	"sync/atomic"
)

const sqlOpenObservationLimit = 64

type SQLOpenObservation struct {
	CallSequence       uint64 `json:"call_sequence"`
	RequestID          string `json:"request_id"`
	Stage              string `json:"stage"`
	Reason             string `json:"reason"`
	Classification     string `json:"classification,omitempty"`
	RPCError           bool   `json:"rpc_error"`
	Ambiguous          bool   `json:"ambiguous"`
	Rejected           bool   `json:"rejected"`
	ExecutionCompleted bool   `json:"execution_completed"`
}

type SQLOpenObserver struct {
	attempts atomic.Uint64
	events   atomic.Uint64
	queue    chan SQLOpenObservation
}

type sqlOpenObservationKey struct{}

type sqlOpenObservationBinding struct {
	observer *SQLOpenObserver
	sequence uint64
	request  string
}

type sqlOpenObservationStage string
type sqlOpenObservationReason string

const (
	sqlOpenTransport sqlOpenObservationStage = "upstream"
	sqlOpenCodecV1   sqlOpenObservationStage = "codec_v1"
	sqlOpenCodecV2   sqlOpenObservationStage = "codec_v2"
	sqlOpenGateway   sqlOpenObservationStage = "gateway_open"

	sqlOpenRequestValidation sqlOpenObservationReason = "request_validation"
	sqlOpenTransportFailure  sqlOpenObservationReason = "transport"
	sqlOpenHTTPStatus        sqlOpenObservationReason = "http_status"
	sqlOpenBody              sqlOpenObservationReason = "body"
	sqlOpenStrictDecode      sqlOpenObservationReason = "strict_decode"
	sqlOpenWorkerNegative    sqlOpenObservationReason = "worker_negative"
	sqlOpenWriterPresence    sqlOpenObservationReason = "writerstate_settled_presence"
	sqlOpenUpstreamResult    sqlOpenObservationReason = "upstream_classification"
	sqlOpenReply             sqlOpenObservationReason = "openreply"
	sqlOpenWriterBinding     sqlOpenObservationReason = "writerbinding"
	sqlOpenPublicID          sqlOpenObservationReason = "public_id"
	sqlOpenCollision         sqlOpenObservationReason = "registrationcollision"
)

func NewSQLOpenObserver() *SQLOpenObserver {
	return &SQLOpenObserver{queue: make(chan SQLOpenObservation, sqlOpenObservationLimit)}
}

func (observer *SQLOpenObserver) TryReceive() (SQLOpenObservation, bool) {
	if observer != nil {
		select {
		case record := <-observer.queue:
			return record, true
		default:
		}
	}
	return SQLOpenObservation{}, false
}

func bindSQLOpenObservation(ctx context.Context, observer *SQLOpenObserver, requestID string) context.Context {
	if ctx == nil || observer == nil || !sqlLabel(requestID) {
		return ctx
	}
	for {
		previous := observer.attempts.Load()
		if previous >= sqlOpenObservationLimit {
			return ctx
		}
		if observer.attempts.CompareAndSwap(previous, previous+1) {
			binding := sqlOpenObservationBinding{observer: observer, sequence: previous + 1, request: requestID}
			return context.WithValue(ctx, sqlOpenObservationKey{}, binding)
		}
	}
}

func observeSQLOpen(ctx context.Context, requestID string, stage sqlOpenObservationStage, reason sqlOpenObservationReason, cause error, classification string) {
	if ctx == nil {
		return
	}
	binding, found := ctx.Value(sqlOpenObservationKey{}).(sqlOpenObservationBinding)
	if !found || binding.observer == nil || binding.request != requestID || !sqlLabel(requestID) {
		return
	}
	switch stage {
	case sqlOpenTransport, sqlOpenCodecV1, sqlOpenCodecV2, sqlOpenGateway:
	default:
		return
	}
	switch reason {
	case sqlOpenRequestValidation, sqlOpenTransportFailure, sqlOpenHTTPStatus, sqlOpenBody,
		sqlOpenStrictDecode, sqlOpenWorkerNegative, sqlOpenWriterPresence, sqlOpenUpstreamResult,
		sqlOpenReply, sqlOpenWriterBinding, sqlOpenPublicID, sqlOpenCollision:
	default:
		return
	}
	record := SQLOpenObservation{CallSequence: binding.sequence, RequestID: requestID, Stage: string(stage), Reason: string(reason)}
	switch classification {
	case "outcome_unknown", "upstream_rejected", "upstream_unavailable", "sql_error":
		record.Classification = classification
	}
	if failure, ok := cause.(*SQLRPCError); ok && failure != nil {
		record.RPCError = true
		record.Ambiguous = failure.Ambiguous
		record.Rejected = failure.Rejected
		record.ExecutionCompleted = failure.ExecutionCompleted
	}
	for {
		previous := binding.observer.events.Load()
		if previous >= sqlOpenObservationLimit {
			return
		}
		if binding.observer.events.CompareAndSwap(previous, previous+1) {
			select {
			case binding.observer.queue <- record:
			default:
			}
			return
		}
	}
}

func observeSQLOpenError(ctx context.Context, requestID string, stage sqlOpenObservationStage, reason sqlOpenObservationReason, cause error) error {
	observeSQLOpen(ctx, requestID, stage, reason, cause, "")
	return cause
}

func observeSQLOpenResult(ctx context.Context, requestID string, reason sqlOpenObservationReason, result gatewayResult, cause error) gatewayResult {
	observeSQLOpen(ctx, requestID, sqlOpenGateway, reason, cause, result.code)
	return result
}
