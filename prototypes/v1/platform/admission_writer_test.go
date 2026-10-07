package platform

import (
	"testing"
	"time"
)

func TestWriterAdmissionGeneralCannotConsumeOwnerReserve(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 100, MaxSQLBytes: 4096, MaxResultBytes: 4096}
	for count := 0; count < 3; count++ {
		permit, ok := gate.reserve("project", quota, false, false)
		if !ok {
			test.Fatal("general allowance below original quota minus reserve")
		}
		defer permit.finish()
	}
	if _, ok := gate.reserve("project", quota, false, false); ok {
		test.Fatal("general consumed owner reserve")
	}
	owner, ok := gate.reserveWriter("project", quota, true)
	if !ok {
		test.Fatal("owner could not use its reserved slot")
	}
	defer owner.finish()
	if _, ok := gate.reserveWriter("project", quota, true); ok {
		test.Fatal("multiple owners exceeded bounded reserve")
	}
	closePermit, ok := gate.reserve("project", quota, false, true)
	if !ok {
		test.Fatal("close control was blocked by SQL slots")
	}
	defer closePermit.finish()
	if _, ok := gate.reserve("project", quota, false, true); ok {
		test.Fatal("close exceeded independent control bound")
	}
}

func TestWriterAdmissionRateReserveWithinOriginalBurst(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 4, MaxSQLBytes: 4096, MaxResultBytes: 4096}
	for count := 0; count < 3; count++ {
		permit, ok := gate.reserve("project", quota, false, false)
		if !ok {
			test.Fatal("general rate credit unexpectedly unavailable")
		}
		permit.finish()
	}
	if _, ok := gate.reserve("project", quota, false, false); ok {
		test.Fatal("general drained reserved rate credit")
	}
	owner, ok := gate.reserveWriter("project", quota, true)
	if !ok {
		test.Fatal("owner rate credit unavailable after general exhaustion")
	}
	owner.finish()
	if _, ok := gate.reserveWriter("project", quota, true); ok {
		test.Fatal("owner minted credit beyond original burst")
	}
	state := gate.projects["project"]
	if state.tokens+state.ownerTokens >= 1 {
		test.Fatal("original four-credit burst was not consumed")
	}
	state.last = time.Now().Add(-time.Second)
	owner, ok = gate.reserveWriter("project", quota, true)
	if !ok {
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
		permit, ok := gate.reserve("project", quota, false, false)
		if !ok || gate.projects["project"].profile {
			test.Fatal("low quota did not keep original legacy capacity")
		}
		if _, ok := gate.reserveWriter("project", quota, true); ok {
			test.Fatal("low quota silently gained owner allowance")
		}
		permit.finish()
	}
}

func TestWriterAdmissionUnknownOwnerRetainsOriginalCapacity(test *testing.T) {
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}, writerAdmission: true}
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 100, MaxSQLBytes: 64, MaxResultBytes: 64}
	owner, ok := gate.reserveWriter("project", quota, true)
	if !ok {
		test.Fatal("owner reserve unavailable")
	}
	owner.retain()
	owner.finish()
	if _, ok := gate.reserveWriter("project", quota, true); ok {
		test.Fatal("unknown owner slot was recycled")
	}
	for count := 0; count < 3; count++ {
		permit, ok := gate.reserve("project", quota, false, false)
		if !ok {
			test.Fatal("unknown owner consumed general reserve")
		}
		defer permit.finish()
	}
	_, active, unknown := gate.status()
	if active != 4 || unknown != 1 {
		test.Fatalf("retained totals: active=%d unknown=%d", active, unknown)
	}
	if _, ok := gate.reserve("project", quota, false, false); ok {
		test.Fatal("unknown owner expanded original total cap")
	}
}

func TestWriterAdmissionDefaultOffAndProjectIsolation(test *testing.T) {
	quota := Quotas{MaxSessions: 8, MaxConcurrent: 4, RequestsPerSecond: 4, MaxSQLBytes: 64, MaxResultBytes: 64}
	gate := &gatewayAdmission{projects: map[string]*projectAdmission{}}
	for count := 0; count < 4; count++ {
		permit, ok := gate.reserve("legacy", quota, false, false)
		if !ok {
			test.Fatal("default-off reduced legacy allowance")
		}
		defer permit.finish()
	}
	gate.writerAdmission = true
	owner, ok := gate.reserveWriter("other", quota, true)
	if !ok {
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
	if _, ok := gate.reserveWriter("project", quota, true); ok {
		test.Fatal("enabling profile minted owner credit")
	}
	quota.MaxConcurrent = 1
	if _, ok := gate.reserve("project", quota, false, false); ok {
		test.Fatal("disabling profile minted general credit")
	}
}
