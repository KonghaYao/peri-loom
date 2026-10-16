package platform

import (
	"testing"
	"time"
)

func TestWriterAdmissionGeneralCannotConsumeOwnerReserve(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 100, MaxSQLBytes: 4096, MaxResultBytes: 4096}
	for count := 0; count < 3; count++ {
		permit, reason := gate.reserve("project", quota, false, false)
		if reason != rejectionNone {
			test.Fatal("general allowance below original quota minus reserve")
		}
		defer permit.finish()
	}
	if _, reason := gate.reserve("project", quota, false, false); reason == rejectionNone {
		test.Fatal("general consumed owner reserve")
	}
	owner, reason := gate.reserveWriter("project", quota, true)
	if reason != rejectionNone {
		test.Fatal("owner could not use its reserved slot")
	}
	defer owner.finish()
	if _, reason := gate.reserveWriter("project", quota, true); reason == rejectionNone {
		test.Fatal("multiple owners exceeded bounded reserve")
	}
	closePermit, reason := gate.reserve("project", quota, false, true)
	if reason != rejectionNone {
		test.Fatal("close control was blocked by SQL slots")
	}
	defer closePermit.finish()
	if _, reason := gate.reserve("project", quota, false, true); reason == rejectionNone {
		test.Fatal("close exceeded independent control bound")
	}
}

func TestWriterAdmissionRateReserveWithinOriginalBurst(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 4, MaxSQLBytes: 4096, MaxResultBytes: 4096}
	for count := 0; count < 3; count++ {
		permit, reason := gate.reserve("project", quota, false, false)
		if reason != rejectionNone {
			test.Fatal("general rate credit unexpectedly unavailable")
		}
		permit.finish()
	}
	if _, reason := gate.reserve("project", quota, false, false); reason == rejectionNone {
		test.Fatal("general drained reserved rate credit")
	}
	owner, reason := gate.reserveWriter("project", quota, true)
	if reason != rejectionNone {
		test.Fatal("owner rate credit unavailable after general exhaustion")
	}
	owner.finish()
	if _, reason := gate.reserveWriter("project", quota, true); reason == rejectionNone {
		test.Fatal("owner minted credit beyond original burst")
	}
	state := gate.projects["project"]
	if state.tokens+state.ownerTokens >= 1 {
		test.Fatal("original four-credit burst was not consumed")
	}
	state.last = time.Now().Add(-time.Second)
	owner, reason = gate.reserveWriter("project", quota, true)
	if reason != rejectionNone {
		test.Fatal("bounded owner credit failed to refill")
	}
	owner.finish()
	if state.tokens+state.ownerTokens > 3 {
		test.Fatal("refill exceeded original rate budget")
	}
}

func TestWriterAdmissionLowQuotasRemainLegacy(test *testing.T) {
	for _, quota := range []Quotas{
		{MaxSessions: 2, MaxConcurrent: 1, RequestsPerSecond: 10, MaxSQLBytes: 64, MaxResultBytes: 64},
		{MaxSessions: 2, MaxConcurrent: 4, RequestsPerSecond: 1, MaxSQLBytes: 64, MaxResultBytes: 64},
	} {
		gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
		permit, reason := gate.reserve("project", quota, false, false)
		if reason != rejectionNone || gate.projects["project"].profile {
			test.Fatal("low quota did not keep original legacy capacity")
		}
		if _, reason := gate.reserveWriter("project", quota, true); reason == rejectionNone {
			test.Fatal("low quota silently gained owner allowance")
		}
		permit.finish()
	}
}

func TestWriterAdmissionUnknownOwnerRetainsOriginalCapacity(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 100, MaxSQLBytes: 64, MaxResultBytes: 64}
	owner, reason := gate.reserveWriter("project", quota, true)
	if reason != rejectionNone {
		test.Fatal("owner reserve unavailable")
	}
	owner.retain()
	owner.finish()
	if _, reason := gate.reserveWriter("project", quota, true); reason == rejectionNone {
		test.Fatal("unknown owner slot was recycled")
	}
	for count := 0; count < 3; count++ {
		permit, reason := gate.reserve("project", quota, false, false)
		if reason != rejectionNone {
			test.Fatal("unknown owner consumed general reserve")
		}
		defer permit.finish()
	}
	_, active, unknown := gate.status()
	if active != 4 || unknown != 1 {
		test.Fatalf("retained totals: active=%d unknown=%d", active, unknown)
	}
	if _, reason := gate.reserve("project", quota, false, false); reason == rejectionNone {
		test.Fatal("unknown owner expanded original total cap")
	}
}

func TestWriterAdmissionDefaultOffAndProjectIsolation(test *testing.T) {
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 4, MaxSQLBytes: 64, MaxResultBytes: 64}
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
	for count := 0; count < 4; count++ {
		permit, reason := gate.reserve("legacy", quota, false, false)
		if reason != rejectionNone {
			test.Fatal("default-off reduced legacy allowance")
		}
		defer permit.finish()
	}
	gate.writerAdmission = true
	owner, reason := gate.reserveWriter("other", quota, true)
	if reason != rejectionNone {
		test.Fatal("another project's permits blocked owner")
	}
	owner.finish()
}

func TestWriterAdmissionQuotaTransitionDoesNotMintCredit(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 1, RequestsPerSecond: 4, MaxSQLBytes: 64, MaxResultBytes: 64}
	permit, _ := gate.reserve("project", quota, false, false)
	permit.finish()
	state := gate.projects["project"]
	state.tokens = 0
	state.last = time.Now()
	quota.MaxConcurrent = 4
	if _, reason := gate.reserveWriter("project", quota, true); reason == rejectionNone {
		test.Fatal("enabling profile minted owner credit")
	}
	quota.MaxConcurrent = 1
	if _, reason := gate.reserve("project", quota, false, false); reason == rejectionNone {
		test.Fatal("disabling profile minted general credit")
	}
}

func frozenWriterRateFixture() (*gatewayAdmission, Quotas) {
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 4, MaxSQLBytes: 64, MaxResultBytes: 64}
	state := &projectAdmission{profile: true, tokens: 3, ownerTokens: 1}
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{"project": state}, writerAdmission: true}
	return gate, quota
}

func reserveAtFrozenWriterRate(gate *gatewayAdmission, quota Quotas, owner bool) (*admissionPermit, rejectionReason) {
	gate.mu.Lock()
	gate.projects["project"].last = time.Now().Add(time.Hour)
	gate.mu.Unlock()
	return gate.reserveWriter("project", quota, owner)
}

func TestWriterAdmissionOwnerBorrowsGeneralBeforeEmergencyWithinTotalBurst(test *testing.T) {
	gate, quota := frozenWriterRateFixture()
	state := gate.projects["project"]
	for count := 0; count < 2; count++ {
		permit, reason := reserveAtFrozenWriterRate(gate, quota, true)
		if reason != rejectionNone {
			test.Fatal("consecutive owner RPC rejected while general rate credits are idle")
		}
		permit.finish()
	}
	if state.tokens != 1 || state.ownerTokens != 1 {
		test.Fatal("trusted owner did not debit general first and preserve emergency credit")
	}
	third, reason := reserveAtFrozenWriterRate(gate, quota, true)
	if reason != rejectionNone {
		test.Fatal("third general-backed owner credit missing")
	}
	third.finish()
	if state.tokens != 0 || state.ownerTokens != 1 {
		test.Fatal("owner minted credit or used emergency before general exhaustion")
	}
	if _, reason := reserveAtFrozenWriterRate(gate, quota, false); reason == rejectionNone {
		test.Fatal("non-owner borrowed emergency rate credit")
	}
	emergency, reason := reserveAtFrozenWriterRate(gate, quota, true)
	if reason != rejectionNone {
		test.Fatal("general exhaustion stranded the emergency owner credit")
	}
	emergency.finish()
	for _, owner := range []bool{true, false} {
		if _, reason := reserveAtFrozenWriterRate(gate, quota, owner); reason == rejectionNone {
			test.Fatal("four-credit original total burst expanded")
		}
	}
	if state.tokens != 0 || state.ownerTokens != 0 || state.active != 0 || state.ownerActive != 0 {
		test.Fatal("finished permits refunded rate credits or retained concurrency")
	}
}

func TestWriterAdmissionThreeGeneralKeepEmergencyOwnerAndOriginalConcurrency(test *testing.T) {
	gate, quota := frozenWriterRateFixture()
	for count := 0; count < 3; count++ {
		permit, reason := reserveAtFrozenWriterRate(gate, quota, false)
		if reason != rejectionNone {
			test.Fatal("general capacity fell below the fixed three-slot contract")
		}
		defer permit.finish()
	}
	if _, reason := reserveAtFrozenWriterRate(gate, quota, false); reason == rejectionNone {
		test.Fatal("fourth general consumed reserved concurrency or emergency credit")
	}
	owner, reason := reserveAtFrozenWriterRate(gate, quota, true)
	if reason != rejectionNone {
		test.Fatal("three occupied general lanes blocked reserved owner")
	}
	defer owner.finish()
	state := gate.projects["project"]
	if state.active != 4 || state.ownerActive != 1 || state.tokens != 0 || state.ownerTokens != 0 {
		test.Fatal("original total concurrency or four-credit burst accounting changed")
	}
	if _, reason := reserveAtFrozenWriterRate(gate, quota, true); reason == rejectionNone {
		test.Fatal("owner borrowing created a second concurrent owner permit")
	}
}

func TestWriterAdmissionFractionalGeneralCannotMintWholeOwnerCredit(test *testing.T) {
	gate, quota := frozenWriterRateFixture()
	state := gate.projects["project"]
	state.tokens = 0.75
	permit, reason := reserveAtFrozenWriterRate(gate, quota, true)
	if reason != rejectionNone {
		test.Fatal("fractional general balance prevented emergency fallback")
	}
	permit.finish()
	if state.tokens != 0.75 || state.ownerTokens != 0 {
		test.Fatal("emergency fallback rounded or transferred general credit")
	}
	for _, owner := range []bool{true, false} {
		if _, reason := reserveAtFrozenWriterRate(gate, quota, owner); reason == rejectionNone {
			test.Fatal("fractional remainder admitted an extra request")
		}
	}
}

func TestWriterAdmissionOwnerBorrowRefillPreservesOriginalAggregateRate(test *testing.T) {
	gate, quota := frozenWriterRateFixture()
	state := gate.projects["project"]
	state.tokens, state.ownerTokens = 0, 0
	state.last = time.Now().Add(-time.Second)
	permit, reason := gate.reserveWriter("project", quota, true)
	if reason != rejectionNone {
		test.Fatal("original aggregate rate failed to refill")
	}
	permit.finish()
	if state.tokens != 2 || state.ownerTokens != 1 {
		test.Fatal("refilled owner failed to preserve emergency and debit shared credit")
	}
	for count := 0; count < 3; count++ {
		permit, reason := reserveAtFrozenWriterRate(gate, quota, true)
		if reason != rejectionNone {
			test.Fatal("refill lost an original aggregate rate credit")
		}
		permit.finish()
	}
	if _, reason := reserveAtFrozenWriterRate(gate, quota, true); reason == rejectionNone {
		test.Fatal("refill minted more than original four total credits")
	}
}
