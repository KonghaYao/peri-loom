package platform

import "peri-loom/platform/sqlcontract"

// testRPCError builds a worker-reply failure for a test. Tests keep naming the
// two directions the worker reports; the proof those directions add up to is
// built by sqlcontract, which is the only place that decides. The raw evidence
// fields no longer exist to be set by hand, so a test fixture cannot invent an
// execution state the platform could not produce.
func testRPCError(executed, rejected bool, fields SQLRPCError) *SQLRPCError {
	fields.evidence = sqlcontract.EvidenceFromDirections(executed, rejected, false)
	return &fields
}
