package faultprobe

import "context"

func ownerCrash(ctx context.Context, config Config, topology Topology, gateway, management *Client, fixture *Fixture, host Host, identities map[string]ProcessIdentity, log *Log) *Failure {
	previous, failure := management.Status(ctx, fixture.Database)
	if failure != nil {
		return failure
	}
	if !writerPlacement(previous) {
		return &Failure{Code: "active_writer_grant_unproven"}
	}
	owner, found := identities[previous.DesiredWorker]
	if !found {
		return &Failure{Code: "owner_not_private_topology"}
	}
	session, failure := gateway.Open(ctx, fixture.Database)
	if failure != nil {
		return failure
	}
	if failure = fixture.Setup(ctx, session); failure != nil {
		return failure
	}
	writerDone := make(chan *Failure, 1)
	go func() {
		for sequence := 0; sequence < config.ACKs; sequence++ {
			if current := fixture.Write(ctx, session); current != nil {
				writerDone <- current
				return
			}
		}
		writerDone <- nil
	}()
	if failure = <-writerDone; failure != nil {
		return failure
	}
	current, failure := management.Status(ctx, fixture.Database)
	if failure != nil {
		return failure
	}
	if !writerPlacement(current) || current.Epoch != previous.Epoch || current.DesiredWorker != previous.DesiredWorker {
		return &Failure{Code: "owner_changed_before_fault", Unknown: true}
	}
	if err := log.Record(map[string]any{"type": "ack_boundary", "acked": len(fixture.ACKs), "inflight_sql": 0, "original_epoch": previous.Epoch, "old_session_fault_cleanup_policy": "abandon_without_close"}); err != nil {
		return &Failure{Code: "fault_evidence_failed"}
	}
	if err := killOwned(ctx, config, topology, previous.DesiredWorker, owner, host, log); err != nil {
		return &Failure{Code: "fault_action_unproven", Unknown: true}
	}
	placement, failure := awaitEpoch(ctx, management, fixture.Database, previous, config.RecoveryTimeout)
	if failure != nil {
		return failure
	}
	if _, found := identities[placement.DesiredWorker]; !found {
		return &Failure{Code: "new_owner_not_private_topology"}
	}
	if err := log.Record(map[string]any{"type": "new_owner", "epoch": placement.Epoch, "worker": placement.DesiredWorker, "diagnostics": "see_actual_status_snapshot_unavailable_accepted"}); err != nil {
		return &Failure{Code: "new_owner_evidence_failed"}
	}
	recovered, failure := gateway.Open(ctx, fixture.Database)
	if failure != nil {
		return failure
	}
	if failure = fixture.Verify(ctx, recovered); failure != nil {
		return failure
	}
	if failure = fixture.Write(ctx, recovered); failure != nil {
		return failure
	}
	if failure = fixture.Verify(ctx, recovered); failure != nil {
		return failure
	}
	return recovered.Close(ctx)
}
