package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// blockingAuthority counts concurrent control calls and can hold a selected
// authority state read (or every call) until the test releases it. Counting
// happens before the block, so a test can prove which work the scheduler
// actually dispatched.
type blockingAuthority struct {
	ControlTransport
	mutex       sync.Mutex
	active      map[string]int
	maxActive   map[string]int
	total       int
	maxTotal    int
	calls       int
	blockKey    string
	blockAll    bool
	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
}

func newBlockingAuthority(base ControlTransport) *blockingAuthority {
	return &blockingAuthority{
		ControlTransport: base,
		active:           make(map[string]int),
		maxActive:        make(map[string]int),
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}
}

func (transport *blockingAuthority) Authority(ctx context.Context, identifier string, input, output any) error {
	var action authorityAction
	_ = reconcileDecode(input, &action)
	key := ""
	if action.Database != nil {
		key = action.Database.Key()
	}
	state := action.Command == "get_database_state" && key != ""
	transport.mutex.Lock()
	transport.calls++
	transport.total++
	if transport.total > transport.maxTotal {
		transport.maxTotal = transport.total
	}
	if state {
		transport.active[key]++
		if transport.active[key] > transport.maxActive[key] {
			transport.maxActive[key] = transport.active[key]
		}
	}
	block := transport.blockAll || (state && key == transport.blockKey)
	transport.mutex.Unlock()
	defer func() {
		transport.mutex.Lock()
		transport.total--
		if state {
			transport.active[key]--
		}
		transport.mutex.Unlock()
	}()
	if block {
		transport.enteredOnce.Do(func() { close(transport.entered) })
		select {
		case <-transport.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return transport.ControlTransport.Authority(ctx, identifier, input, output)
}

func (transport *blockingAuthority) callsStarted() int {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return transport.calls
}

func (transport *blockingAuthority) maximumTotal() int {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return transport.maxTotal
}

func (transport *blockingAuthority) maximumFor(key string) int {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return transport.maxActive[key]
}

func waitForCondition(test *testing.T, timeout time.Duration, condition func() bool, message string) {
	test.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			test.Fatal(message)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A slow database holds one lane and one reconcile worker. The peer's due
// renewal must still complete while that call is blocked past a lease period.
func TestReconcileSlowDatabaseDoesNotDelayPeerRenewal(test *testing.T) {
	fixture := newReconcileFixture(test)
	slow := reconcileDatabase("slow-a")
	peer := reconcileDatabase("peer-b")
	for _, database := range []DatabaseID{slow, peer} {
		if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err != nil {
			test.Fatal(err)
		}
	}
	actual := fixture.fake.authority[peer.Key()]
	actual.RemainingTTLMS = 400 // half of the configured 1s lease
	fixture.fake.authority[peer.Key()] = actual

	transport := newBlockingAuthority(fixture.fake)
	transport.blockKey = slow.Key()
	control, err := NewController(fixture.store, transport, ReconcileConfig{
		LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: 10 * time.Millisecond,
		ControlRPCTimeout: 10 * time.Second, ReconcileConcurrency: 8,
	})
	if err != nil {
		test.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- control.Reconcile(context.Background()) }()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		test.Fatal("slow authority state was not dispatched")
	}
	deadline := time.Now().Add(time.Second)
	for {
		status, err := control.Status(peer)
		if err != nil {
			test.Fatal(err)
		}
		if status.Phase == "active" && status.Grant != nil && status.Grant.Binding.RenewalSequence >= 1 {
			break
		}
		if time.Now().After(deadline) {
			close(transport.release)
			test.Fatal("peer renewal was delayed behind the slow database")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Keep the slow call blocked past one lease period. The peer's renewal is
	// already durable; the pass must still be waiting for only the slow lane.
	time.Sleep(1200 * time.Millisecond)
	select {
	case <-finished:
		test.Fatal("reconcile finished while the slow database was still blocked")
	default:
	}
	status, err := control.Status(peer)
	if err != nil || status.Phase != "active" || status.Grant == nil || status.Grant.Binding.RenewalSequence < 1 {
		test.Fatalf("peer placement did not stay settled: %+v %v", status, err)
	}
	close(transport.release)
	select {
	case err := <-finished:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		test.Fatal("reconcile did not finish after the slow database was released")
	}
	if maximum := transport.maximumFor(slow.Key()); maximum != 1 {
		test.Fatalf("slow database ran %d concurrent state reads", maximum)
	}
}

// A reconcile pass and a management call for the same database must share one
// lane: the second operation may not dispatch until the first releases it.
func TestReconcileAndManagementCallSerializeSameDatabase(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("lane")
	if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	transport := newBlockingAuthority(fixture.fake)
	transport.blockKey = database.Key()
	control, err := NewController(fixture.store, transport, ReconcileConfig{
		LeaseTTL: time.Second, HeartbeatTimeout: 2 * time.Second, Interval: time.Millisecond,
		ControlRPCTimeout: 5 * time.Second,
	})
	if err != nil {
		test.Fatal(err)
	}
	reconciled := make(chan error, 1)
	go func() { reconciled <- control.Reconcile(context.Background()) }()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		test.Fatal("reconcile state read was not dispatched")
	}
	suspended := make(chan error, 1)
	go func() { suspended <- control.Suspend(context.Background(), database) }()
	time.Sleep(150 * time.Millisecond)
	if calls := transport.callsStarted(); calls != 1 {
		test.Fatalf("management call entered the database lane while reconcile held it: calls=%d", calls)
	}
	select {
	case err := <-suspended:
		test.Fatalf("suspend finished while reconcile held the lane: %v", err)
	default:
	}
	close(transport.release)
	select {
	case err := <-reconciled:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		test.Fatal("reconcile did not finish")
	}
	select {
	case err := <-suspended:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		test.Fatal("suspend did not finish after the lane was released")
	}
	status, err := control.Status(database)
	if err != nil || status.Phase != "suspended" {
		test.Fatalf("suspension did not settle: %+v %v", status, err)
	}
	if maximum := transport.maximumFor(database.Key()); maximum != 1 {
		test.Fatalf("same database ran %d concurrent state reads", maximum)
	}
}

func TestReconcileConcurrencyIsBounded(test *testing.T) {
	fixture := newReconcileFixture(test)
	for index := 0; index < 10; index++ {
		if _, err := fixture.controller.Place(context.Background(), reconcileDatabase(fmt.Sprintf("bounded-concurrency-%d", index)), "local", "writer"); err != nil {
			test.Fatal(err)
		}
	}
	transport := newBlockingAuthority(fixture.fake)
	transport.blockAll = true
	control, err := NewController(fixture.store, transport, ReconcileConfig{
		LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Millisecond,
		ReconcileConcurrency: 3, ControlRPCTimeout: 5 * time.Second,
	})
	if err != nil {
		test.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- control.Reconcile(context.Background()) }()
	waitForCondition(test, time.Second, func() bool { return transport.maximumTotal() == 3 }, "reconcile did not fill its concurrency budget")
	time.Sleep(100 * time.Millisecond)
	if maximum := transport.maximumTotal(); maximum != 3 {
		test.Fatalf("reconcile concurrency exceeded its bound: %d", maximum)
	}
	close(transport.release)
	select {
	case err := <-finished:
		if err != nil {
			test.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		test.Fatal("bounded reconcile did not finish")
	}
	if maximum := transport.maximumTotal(); maximum != 3 {
		test.Fatalf("reconcile concurrency exceeded its bound after release: %d", maximum)
	}
}

func TestReconcileCancellationStopsQueuedPlacements(test *testing.T) {
	fixture := newReconcileFixture(test)
	for index := 0; index < 5; index++ {
		if _, err := fixture.controller.Place(context.Background(), reconcileDatabase(fmt.Sprintf("cancel-queued-%d", index)), "local", "writer"); err != nil {
			test.Fatal(err)
		}
	}
	transport := newBlockingAuthority(fixture.fake)
	transport.blockAll = true
	control, err := NewController(fixture.store, transport, ReconcileConfig{
		LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Millisecond,
		ReconcileConcurrency: 1, ControlRPCTimeout: 5 * time.Second,
	})
	if err != nil {
		test.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- control.Reconcile(ctx) }()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		test.Fatal("reconcile state read was not dispatched")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			test.Fatalf("canceled pass reported %v", err)
		}
	case <-time.After(time.Second):
		test.Fatal("canceled pass did not return")
	}
	if calls := transport.callsStarted(); calls != 1 {
		test.Fatalf("%d queued placements reached the authority after cancellation", calls)
	}
}

func TestReconcileControlRPCTimeoutBoundsHangingCall(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("rpc-deadline")
	if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	transport := newBlockingAuthority(fixture.fake)
	transport.blockAll = true
	control, err := NewController(fixture.store, transport, ReconcileConfig{
		LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Millisecond,
		ControlRPCTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		test.Fatal(err)
	}
	started := time.Now()
	err = control.Reconcile(context.Background())
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		test.Fatalf("control deadline was not reported: %v", err)
	}
	if !strings.Contains(err.Error(), database.Key()) {
		test.Fatalf("control failure did not name its placement: %v", err)
	}
	if elapsed > 700*time.Millisecond {
		test.Fatalf("control deadline did not bound the pass: %s", elapsed)
	}
}

func TestManagementOperationConcurrencyIsBounded(test *testing.T) {
	fixture := newReconcileFixture(test)
	transport := newBlockingAuthority(fixture.fake)
	transport.blockAll = true
	control, err := NewController(fixture.store, transport, ReconcileConfig{
		LeaseTTL: time.Second, HeartbeatTimeout: 5 * time.Second, Interval: time.Millisecond,
		MaxConcurrentOperations: 2, ControlRPCTimeout: 5 * time.Second,
	})
	if err != nil {
		test.Fatal(err)
	}
	var group sync.WaitGroup
	failures := make(chan error, 6)
	for index := 0; index < 6; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			if _, err := control.Place(context.Background(), reconcileDatabase(fmt.Sprintf("management-%d", index)), "local", "writer"); err != nil {
				failures <- err
			}
		}(index)
	}
	waitForCondition(test, time.Second, func() bool { return transport.maximumTotal() == 2 }, "management operations did not fill their concurrency budget")
	time.Sleep(100 * time.Millisecond)
	if maximum := transport.maximumTotal(); maximum != 2 {
		test.Fatalf("management concurrency exceeded its bound: %d", maximum)
	}
	close(transport.release)
	group.Wait()
	close(failures)
	for err := range failures {
		test.Fatal(err)
	}
	if maximum := transport.maximumTotal(); maximum != 2 {
		test.Fatalf("management concurrency exceeded its bound after release: %d", maximum)
	}
}

func TestReconcileJobsPrioritizeOverdueRenewals(test *testing.T) {
	fixture := newReconcileFixture(test)
	overdue := reconcileDatabase("overdue")
	unknown := reconcileDatabase("unknown")
	scheduled := reconcileDatabase("scheduled")
	pending := reconcileDatabase("pending")
	metadata := Metadata{Placements: map[string]Placement{
		overdue.Key():   {Database: overdue, Phase: "active"},
		unknown.Key():   {Database: unknown, Phase: "active"},
		scheduled.Key(): {Database: scheduled, Phase: "active"},
		pending.Key():   {Database: pending, Phase: "grant_intent"},
	}}
	now := time.Now()
	fixture.controller.recordRenewalDeadline(overdue.Key(), now.Add(-time.Second))
	fixture.controller.recordRenewalDeadline(scheduled.Key(), now.Add(time.Hour))
	jobs := fixture.controller.reconcileJobs(metadata)
	want := []string{overdue.Key(), unknown.Key(), scheduled.Key(), pending.Key()}
	if len(jobs) != len(want) {
		test.Fatalf("unexpected job count: %+v", jobs)
	}
	for index := range want {
		if jobs[index].key != want[index] {
			test.Fatalf("reconcile priority order = %v, want %v", jobs, want)
		}
	}
}

func TestPlacementRecordsRenewalHint(test *testing.T) {
	fixture := newReconcileFixture(test)
	database := reconcileDatabase("hint")
	if _, err := fixture.controller.Place(context.Background(), database, "local", "writer"); err != nil {
		test.Fatal(err)
	}
	if _, known := fixture.controller.renewalDeadline(database.Key()); !known {
		test.Fatal("settled placement did not record a renewal hint")
	}
}

func TestReconcileConfigValidatesConcurrencyAndControlDeadline(test *testing.T) {
	fixture := newReconcileFixture(test)
	for _, config := range []ReconcileConfig{
		{LeaseTTL: time.Second, HeartbeatTimeout: time.Second, Interval: time.Millisecond, MaxConcurrentOperations: -1},
		{LeaseTTL: time.Second, HeartbeatTimeout: time.Second, Interval: time.Millisecond, ReconcileConcurrency: 65},
		{LeaseTTL: time.Second, HeartbeatTimeout: time.Second, Interval: time.Millisecond, ControlRPCTimeout: time.Nanosecond},
	} {
		if _, err := NewController(fixture.store, fixture.fake, config); err == nil {
			test.Fatalf("invalid reconciliation config accepted: %+v", config)
		}
	}
}
