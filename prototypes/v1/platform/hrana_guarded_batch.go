package platform

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
)

// A block never spans more statements than one durable audit batch, and its
// command stays well under the worker's request-body limit while leaving room
// for the adapter's JSON envelope.
const guardedBatchSize = 8

const guardedBatchRequestBodyLimit = 128 * 1024

type guardedBatchItemStatus int

const (
	guardedBatchStatusOK guardedBatchItemStatus = iota
	guardedBatchStatusRequiresIntent
	guardedBatchStatusError
)

// guardedBatchItem is one decided entry of a block, as the gateway consumes it
// later in the same pipeline. The raw result is the worker's statement result;
// the failure keeps the worker's own execution evidence. The autocommit flag is
// kept from the same validation that accepted the result, so the session's
// transaction state costs no second parse.
type guardedBatchItem struct {
	status          guardedBatchItemStatus
	result          json.RawMessage
	reason          guardedIntentReason
	failure         *SQLRPCError
	autocommit      bool
	autocommitKnown bool
}

type guardedBatchStatement struct {
	sql       string
	args      []sqlArgument
	namedArgs []namedSQLArgument
	wantRows  bool
}

// guardedBatch is the plan of one contiguous block plus everything the block
// learned when its first entry dispatched it. The pipeline serves its entries
// strictly one after another, so the mutex only keeps the shared state honest
// for the race detector and for a future concurrent reader.
type guardedBatch struct {
	mutex          sync.Mutex
	statements     []guardedBatchStatement
	maxResultBytes int
	reservations   []AuditReservation
	claimed        []bool
	dispatched     bool
	items          []guardedBatchItem
	next           int
	pending        *pendingOperation
	// broken records a dispatched block whose answer cannot be used. Every later
	// entry returns that answer instead of dispatching the entry again.
	broken       bool
	brokenResult gatewayResult
}

type guardedBatchContextKey struct{}

type guardedBatchStep struct {
	batch *guardedBatch
	index int
}

func withGuardedBatch(ctx context.Context, batch *guardedBatch, index int) context.Context {
	return context.WithValue(ctx, guardedBatchContextKey{}, guardedBatchStep{batch: batch, index: index})
}

func guardedBatchStepFrom(ctx context.Context) (guardedBatchStep, bool) {
	step, ok := ctx.Value(guardedBatchContextKey{}).(guardedBatchStep)
	return step, ok && step.batch != nil
}

func (batch *guardedBatch) length() int { return len(batch.statements) }

// claimReservation hands the slot reserved for one entry to that entry's
// request. A slot is claimed at most once; a fallback entry that finds it taken
// reserves its own capacity instead.
func (batch *guardedBatch) claimReservation(index int) (AuditReservation, bool) {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()
	if index < 0 || index >= len(batch.claimed) || batch.claimed[index] || index >= len(batch.reservations) {
		return nil, false
	}
	reservation := batch.reservations[index]
	if reservation == nil {
		return nil, false
	}
	batch.claimed[index] = true
	return reservation, true
}

// Close returns every slot the block did not consume. Release is a no-op for a
// slot a request already recorded, so this is safe on every early return.
func (batch *guardedBatch) Close() {
	batch.mutex.Lock()
	reservations := batch.reservations
	batch.reservations = nil
	batch.mutex.Unlock()
	for _, reservation := range reservations {
		if reservation != nil {
			reservation.Release()
		}
	}
}

// planGuardedBatch returns a block that starts at entries[start], or nil when
// the entry must run on its own. Only top-level execute entries that the
// adapter can send as plain statements take part: a step with conditions,
// stored SQL that is missing, a known write or a statement the adapter would
// serve through a cursor ends the block.
func (handler *hranaHTTP) planGuardedBatch(ctx context.Context, stream *hranaStream, entries []hranaPipelineEntry, start, remaining int) *guardedBatch {
	if remaining <= 0 || stream == nil || stream.session == "" {
		return nil
	}
	worker, writerProtocol, found := handler.gateway.hranaBatchWorker(stream.session)
	if !found || writerProtocol {
		return nil
	}
	statements := make([]guardedBatchStatement, 0, guardedBatchSize)
	bodyBytes := 0
	for offset := 0; offset < guardedBatchSize && start+offset < len(entries); offset++ {
		entry := entries[start+offset]
		if entry.Type != "execute" {
			break
		}
		statement := entry.Stmt
		if statement.SQL == "" {
			statement.SQL = stream.storedSQL[statement.SQLID]
		}
		if statement.SQL == "" {
			break
		}
		// The adapter serves these through its own describe or cursor path
		// inside executeWithin, so they never reach the block command.
		if statement.WantRows && strings.Contains(strings.ToUpper(statement.SQL), "RETURNING") {
			break
		}
		// A statement this platform already proved committing writes takes its
		// durable intent before dispatch; it must not begin a batch.
		key := committingStatementKey{worker: worker.ID, digest: digestHex([]byte(statement.SQL))}
		if handler.gateway.committingStatements.remembered(key) {
			break
		}
		args, namedArgs, ok := hranaStatementArguments(statement)
		if !ok {
			break
		}
		body := guardedBatchStatementBytes(statement.SQL, args, namedArgs)
		if bodyBytes+body > guardedBatchRequestBodyLimit {
			break
		}
		bodyBytes += body
		statements = append(statements, guardedBatchStatement{
			sql:       statement.SQL,
			args:      args,
			namedArgs: namedArgs,
			wantRows:  statement.WantRows,
		})
	}
	if len(statements) < 2 {
		return nil
	}
	// The probe is asked only for a block that would actually be sent, so a
	// single-statement pipeline pays nothing for the capability.
	if !handler.gateway.readonlyBatchSupported(ctx, worker) {
		return nil
	}
	reservations, ok := handler.gateway.reserveBatchCompletions(len(statements))
	if !ok {
		// The audit cannot hold the block's completion rows. Falling back to
		// single statements lets the normal admission answer each one.
		return nil
	}
	return &guardedBatch{
		statements:     statements,
		maxResultBytes: remaining,
		reservations:   reservations,
		claimed:        make([]bool, len(statements)),
	}
}

// guardedBatchStatementBytes is the planner's upper estimate of one statement's
// share of the command body. It only has to stay above the encoded size.
func guardedBatchStatementBytes(sql string, args []sqlArgument, namedArgs []namedSQLArgument) int {
	bytes := len(sql) + 64
	for _, argument := range args {
		bytes += len(argument.Type) + len(argument.Value) + len(argument.Base64) + 24
	}
	for _, argument := range namedArgs {
		bytes += len(argument.Name) + len(argument.Value.Type) + len(argument.Value.Value) + len(argument.Value.Base64) + 40
	}
	return bytes
}
