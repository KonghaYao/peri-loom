package platform

import (
	"context"
	"errors"
	"sync"
)

// pipelineAuditBatchMaximum bounds how many prepared read-only completions a
// pipeline holds before paying for one barrier. It is the size the design was
// measured with; every record still reaches the audit chain.
const pipelineAuditBatchMaximum = 8

type pipelineAuditCollectorContextKey struct{}

// pipelineAuditCollector defers the completion rows of successful read-only
// statements in one pipeline. Only such a row may wait: the statement cannot
// commit, so the client answer proves nothing the row would have to witness
// beyond the row itself. A committing step flushes this collector before its own
// synchronous audit and seals it afterwards, so a later flush failure can never
// rewrite the answer of an operation that already ran.
type pipelineAuditCollector struct {
	mutex    sync.Mutex
	audit    *fileAudit
	prepared []*auditRequest
	failure  error
	sealed   bool
}

func newPipelineAuditCollector() *pipelineAuditCollector {
	return &pipelineAuditCollector{}
}

func withPipelineAuditCollector(ctx context.Context, collector *pipelineAuditCollector) context.Context {
	return context.WithValue(ctx, pipelineAuditCollectorContextKey{}, collector)
}

func pipelineAuditCollectorFrom(ctx context.Context) *pipelineAuditCollector {
	collector, _ := ctx.Value(pipelineAuditCollectorContextKey{}).(*pipelineAuditCollector)
	return collector
}

// Active reports whether this collector still accepts prepared completions. A
// sealed or failed collector sends the caller back to the synchronous path.
func (collector *pipelineAuditCollector) Active() bool {
	if collector == nil {
		return false
	}
	collector.mutex.Lock()
	defer collector.mutex.Unlock()
	return !collector.sealed && collector.failure == nil
}

func (collector *pipelineAuditCollector) Pending() int {
	if collector == nil {
		return 0
	}
	collector.mutex.Lock()
	defer collector.mutex.Unlock()
	return len(collector.prepared)
}

// Add prepares one read-only completion and takes ownership of its reservation,
// including when the preparation itself fails. It reports false only when the
// collector could not take the completion at all: the caller then still owns the
// reservation and must use the synchronous path.
func (collector *pipelineAuditCollector) Add(event AuditEvent, completion AuditReservation) (bool, error) {
	held, ok := completion.(*auditReservation)
	if !ok || held == nil || held.audit == nil {
		return false, nil
	}
	collector.mutex.Lock()
	if collector.sealed || collector.failure != nil || (collector.audit != nil && collector.audit != held.audit) {
		collector.mutex.Unlock()
		return false, nil
	}
	collector.audit = held.audit
	collector.mutex.Unlock()
	request, err := held.audit.prepareCompletionOperation(event, held, nil)
	if err != nil {
		return true, err
	}
	collector.mutex.Lock()
	collector.prepared = append(collector.prepared, request)
	failure := collector.failure
	collector.mutex.Unlock()
	return true, failure
}

// Flush submits every prepared completion as one batch and waits for the whole
// batch. A failed flush poisons the collector: later completions must not be
// presented as durable by a caller that already knows the audit is unusable.
func (collector *pipelineAuditCollector) Flush() error {
	if collector == nil {
		return nil
	}
	collector.mutex.Lock()
	if collector.failure != nil {
		err := collector.failure
		collector.mutex.Unlock()
		return err
	}
	prepared := collector.prepared
	collector.prepared = nil
	audit := collector.audit
	collector.mutex.Unlock()
	if len(prepared) == 0 {
		return nil
	}
	if audit == nil {
		// Add refuses without an audit, so prepared without one is impossible.
		err := errors.New("pipeline audit has no sink")
		collector.mutex.Lock()
		collector.failure = err
		collector.mutex.Unlock()
		return err
	}
	if err := audit.submitCompletionBatch(prepared); err != nil {
		collector.mutex.Lock()
		collector.failure = err
		collector.mutex.Unlock()
		return err
	}
	return nil
}

// Seal stops the collector from accepting new completions. It does not discard
// prepared records: the flush before the sealing step already settled them.
func (collector *pipelineAuditCollector) Seal() {
	if collector == nil {
		return
	}
	collector.mutex.Lock()
	collector.sealed = true
	collector.mutex.Unlock()
}

// Abort releases completions that never reached the audit chain. It runs when
// the pipeline ends without reporting them, including panic unwinding.
func (collector *pipelineAuditCollector) Abort() {
	if collector == nil {
		return
	}
	collector.mutex.Lock()
	prepared := collector.prepared
	collector.prepared = nil
	collector.sealed = true
	audit := collector.audit
	collector.mutex.Unlock()
	if audit == nil {
		return
	}
	for _, request := range prepared {
		audit.discardPrepared(request)
	}
}
