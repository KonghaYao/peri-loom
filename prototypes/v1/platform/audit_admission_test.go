package platform

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openAuditAdmissionFixture(test *testing.T) *fileAudit {
	test.Helper()
	service, err := OpenAudit(filepath.Join(test.TempDir(), "audit.jsonl"))
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { _ = service.Close() })
	return service.(*fileAudit)
}

func waitAuditStatus(test *testing.T, audit *fileAudit, description string, ready func(map[string]any) bool) {
	test.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ready(audit.AuditStatus()) {
			return
		}
		if time.Now().After(deadline) {
			test.Fatalf("audit status did not reach %s: %+v", description, audit.AuditStatus())
		}
		time.Sleep(time.Millisecond)
	}
}

func auditClassStatus(status map[string]any, group, class string) int {
	values, _ := status[group].(map[string]int)
	return values[class]
}

func auditDroppedStatus(status map[string]any, class string) uint64 {
	values, _ := status["dropped_by_class"].(map[string]uint64)
	return values[class]
}

// blockAuditSync holds the writer before its file barrier, so every admitted
// record stays charged while the test observes the queue. Release is idempotent
// because a failing assertion must still unblock the writer for cleanup.
func blockAuditSync(audit *fileAudit) (entered <-chan struct{}, release func()) {
	gate := make(chan struct{})
	started := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	audit.testSyncGate = func() {
		startedOnce.Do(func() { close(started) })
		<-gate
	}
	return started, func() { releaseOnce.Do(func() { close(gate) }) }
}

func TestAuditQueueBoundedUnderSlowFsync(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	entered, release := blockAuditSync(audit)
	test.Cleanup(release)
	const extra = 32
	total := auditRequestQueueMaximum + extra
	results := make(chan error, total)
	for index := 0; index < total; index++ {
		go func() { results <- audit.Record(auditFixtureEvent()) }()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		test.Fatal("audit writer did not reach the barrier")
	}
	waitAuditStatus(test, audit, "a full request queue with refused records", func(status map[string]any) bool {
		return auditClassStatus(status, "admitted_by_class", "request") == auditRequestQueueMaximum &&
			auditDroppedStatus(status, "request") >= extra
	})
	status := audit.AuditStatus()
	if status["admitted_count"].(int) > auditQueueMaximum || status["admitted_bytes"].(int) > auditQueueBytes {
		test.Fatalf("queue exceeded its global budget: %+v", status)
	}
	if status["queue_count"].(int) > auditRequestQueueMaximum {
		test.Fatalf("request queue exceeded its class budget: %+v", status)
	}
	if _, found := status["queue_age_ms"]; !found {
		test.Fatal("queue age is not reported")
	}
	waitAuditStatus(test, audit, "a measurable queue age", func(status map[string]any) bool {
		return status["queue_age_ms"].(int64) > 0
	})
	release()
	admitted, refused := 0, 0
	for index := 0; index < total; index++ {
		switch err := <-results; {
		case err == nil:
			admitted++
		case errors.Is(err, ErrAuditOverloaded):
			refused++
		default:
			test.Fatalf("unexpected audit admission result: %v", err)
		}
	}
	if admitted != auditRequestQueueMaximum || refused != extra {
		test.Fatalf("admitted=%d refused=%d, want %d/%d", admitted, refused, auditRequestQueueMaximum, extra)
	}
	status = audit.AuditStatus()
	if status["admitted_count"].(int) != 0 || status["queue_count"].(int) != 0 {
		test.Fatalf("drained queue still holds capacity: %+v", status)
	}
	if status["dropped_total"].(uint64) < extra {
		test.Fatalf("refused records were not counted: %+v", status)
	}
	if status["fsync_count"].(uint64) == 0 || status["fsync_ns_total"].(uint64) == 0 {
		test.Fatalf("fsync cost is not reported: %+v", status)
	}
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
}

// Rejection traffic has its own budget: filling it must neither consume the
// completion reserve nor the ordinary request budget.
func TestAuditRejectionBudgetIsolatedFromCompletion(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	entered, release := blockAuditSync(audit)
	test.Cleanup(release)
	const extra = 16
	total := auditRejectionQueueMaximum + extra
	results := make(chan error, total)
	for index := 0; index < total; index++ {
		go func() { results <- audit.RecordClass(auditFixtureEvent(), AuditClassRejection) }()
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		test.Fatal("audit writer did not reach the barrier")
	}
	waitAuditStatus(test, audit, "a full rejection budget", func(status map[string]any) bool {
		return auditClassStatus(status, "admitted_by_class", "rejection") == auditRejectionQueueMaximum &&
			auditDroppedStatus(status, "rejection") >= extra
	})
	reservation, err := audit.ReserveCompletion()
	if err != nil {
		test.Fatalf("rejection storm took completion capacity: %v", err)
	}
	completion := make(chan error, 1)
	go func() { completion <- reservation.Record(auditFixtureEvent()) }()
	request := make(chan error, 1)
	go func() { request <- audit.Record(auditFixtureEvent()) }()
	waitAuditStatus(test, audit, "admitted completion and request records", func(status map[string]any) bool {
		return auditClassStatus(status, "admitted_by_class", "completion") == 1 &&
			auditClassStatus(status, "admitted_by_class", "request") == 1
	})
	release()
	admitted, refused := 0, 0
	for index := 0; index < total; index++ {
		switch err := <-results; {
		case err == nil:
			admitted++
		case errors.Is(err, ErrAuditOverloaded):
			refused++
		default:
			test.Fatalf("unexpected rejection admission result: %v", err)
		}
	}
	if admitted != auditRejectionQueueMaximum || refused != extra {
		test.Fatalf("admitted=%d refused=%d, want %d/%d", admitted, refused, auditRejectionQueueMaximum, extra)
	}
	if err := <-completion; err != nil {
		test.Fatalf("reserved completion was not written: %v", err)
	}
	if err := <-request; err != nil {
		test.Fatalf("ordinary request was starved by rejection traffic: %v", err)
	}
	if status := audit.AuditStatus(); status["admitted_count"].(int) != 0 {
		test.Fatalf("drained audit still holds capacity: %+v", status)
	}
}

func TestAuditCompletionReserveIsBounded(test *testing.T) {
	audit := openAuditAdmissionFixture(test)
	held := make([]AuditReservation, 0, auditCompletionMaximum)
	for index := 0; index < auditCompletionMaximum; index++ {
		reservation, err := audit.ReserveCompletion()
		if err != nil {
			test.Fatalf("reserve %d: %v", index, err)
		}
		held = append(held, reservation)
	}
	if _, err := audit.ReserveCompletion(); !errors.Is(err, ErrAuditOverloaded) {
		test.Fatalf("completion reserve accepted work past its bound: %v", err)
	}
	status := audit.AuditStatus()
	if status["reserved_completion_slots"].(int) != auditCompletionMaximum {
		test.Fatalf("reserved slots not reported: %+v", status)
	}
	if auditDroppedStatus(status, "completion") != 1 {
		test.Fatalf("reserve refusal was not counted: %+v", status)
	}
	for _, reservation := range held {
		reservation.Release()
	}
	reservation, err := audit.ReserveCompletion()
	if err != nil {
		test.Fatalf("released slots were not reusable: %v", err)
	}
	reservation.Release()
	if err := audit.Close(); err != nil {
		test.Fatal(err)
	}
}

func TestAuditCompletionReservationDrainsOnClose(test *testing.T) {
	path := filepath.Join(test.TempDir(), "audit.jsonl")
	service, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	audit := service.(*fileAudit)
	reservation, err := audit.ReserveCompletion()
	if err != nil {
		test.Fatal(err)
	}
	audit.mutex.Lock()
	locked := true
	defer func() {
		if locked {
			audit.mutex.Unlock()
		}
	}()
	result := make(chan error, 1)
	go func() { result <- reservation.Record(auditFixtureEvent()) }()
	waitAuditStatus(test, audit, "an accepted completion record", func(status map[string]any) bool {
		return auditClassStatus(status, "admitted_by_class", "completion") == 1
	})
	closed := make(chan error, 1)
	go func() { closed <- audit.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		audit.queueMutex.Lock()
		closing := audit.closing
		audit.queueMutex.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			test.Fatal("close did not stop admission")
		}
		time.Sleep(time.Millisecond)
	}
	audit.mutex.Unlock()
	locked = false
	if err := <-result; err != nil {
		test.Fatalf("close lost an accepted completion: %v", err)
	}
	if err := <-closed; err != nil {
		test.Fatal(err)
	}
	reopened, err := OpenAudit(path)
	if err != nil {
		test.Fatal(err)
	}
	defer reopened.Close()
	if reopened.(*fileAudit).state.Sequence != 1 {
		test.Fatal("accepted completion was not durable after close")
	}
}
