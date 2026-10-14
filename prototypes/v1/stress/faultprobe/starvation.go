package faultprobe

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type migrationOutcome struct {
	Index   int
	Failure *Failure
}

func opposite(worker string) string {
	if worker == "worker-a" {
		return "worker-b"
	}
	if worker == "worker-b" {
		return "worker-a"
	}
	return ""
}

func metadataStarvation(ctx context.Context, config Config, connection Connection, gateway, management *Client, prefix string, log *Log) (failure *Failure) {
	dbs := append([]DatabaseID{connection.Database}, connection.Databases...)
	fixtures := make([]*Fixture, 3)
	sessions := make([]*Session, 3)
	placements := make([]Placement, 3)
	transaction := false
	defer func() {
		if failure != nil && failure.Unknown {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), config.RequestTimeout)
		defer cancel()
		if transaction {
			_, rollback := sessions[0].SQL(cleanup, false, "ROLLBACK")
			if rollback != nil {
				failure = rollback
				return
			}
		}
		for _, session := range sessions {
			if session != nil {
				if closed := session.Close(cleanup); closed != nil {
					failure = closed
					return
				}
			}
		}
	}()
	for index, database := range dbs {
		fixtures[index] = &Fixture{Database: database, Prefix: fmt.Sprintf("%s_db%d", prefix, index), log: log}
		placements[index], failure = management.Status(ctx, database)
		if failure != nil {
			return failure
		}
		if !writerPlacement(placements[index]) || opposite(placements[index].DesiredWorker) == "" {
			return &Failure{Code: "starvation_private_writer_unproven"}
		}
		sessions[index], failure = gateway.Open(ctx, database)
		if failure != nil {
			return failure
		}
		if failure = fixtures[index].Setup(ctx, sessions[index]); failure != nil {
			return failure
		}
		if failure = fixtures[index].Write(ctx, sessions[index]); failure != nil {
			return failure
		}
		if failure = fixtures[index].Verify(ctx, sessions[index]); failure != nil {
			return failure
		}
	}
	if _, failure = sessions[0].SQL(ctx, false, "BEGIN"); failure != nil {
		return failure
	}
	transaction = true
	if _, failure = sessions[0].SQL(ctx, false, fmt.Sprintf("UPDATE %s_account SET balance=balance+17 WHERE id=1", fixtures[0].Prefix)); failure != nil {
		return failure
	}
	observing, cancel := context.WithTimeout(ctx, config.ObservationWindow)
	defer cancel()
	outcomes := make(chan migrationOutcome, 2)
	var pending [2]atomic.Bool
	for index := 0; index < 2; index++ {
		pending[index].Store(true)
		go func(index int) {
			_, current := management.call(observing, "/v1/databases/migrate", map[string]any{"database": dbs[index], "worker_id": opposite(placements[index].DesiredWorker)})
			pending[index].Store(false)
			outcomes <- migrationOutcome{Index: index, Failure: current}
			if current != nil && current.Unknown {
				cancel()
			}
		}(index)
	}
	started := time.Now()
	samples, overlapACKs := 0, 0
	renewalObserved := false
	for samples < 32 && observing.Err() == nil {
		before := pending[0].Load()
		if failure = fixtures[2].Write(observing, sessions[2]); failure != nil {
			break
		}
		if failure = fixtures[2].Verify(observing, sessions[2]); failure != nil {
			break
		}
		current, statusFailure := management.Status(observing, dbs[2])
		if statusFailure != nil {
			failure = statusFailure
			break
		}
		if current.Epoch != placements[2].Epoch || !writerPlacement(current) {
			failure = &Failure{Code: "third_database_lease_changed", Unknown: true}
			break
		}
		renewalObserved = renewalObserved || current.Grant.Binding.RenewalSequence > placements[2].Grant.Binding.RenewalSequence
		after := pending[0].Load()
		if before && after {
			overlapACKs++
		}
		samples++
		if err := log.Record(map[string]any{"type": "starvation_sample", "sample": samples, "observed_at": time.Now().UTC().Format(time.RFC3339Nano), "elapsed_ns": time.Since(started).Nanoseconds(), "db0_client_migration_pending_before": before, "db0_client_migration_pending_after": after, "db1_client_migration_pending": pending[1].Load(), "third_db_acked": len(fixtures[2].ACKs), "placement_renewal_sequence": current.Grant.Binding.RenewalSequence, "renewal_measurement_scope": "controller_placement_metadata_not_authority_lease_execution"}); err != nil {
			failure = &Failure{Code: "starvation_evidence_failed"}
			break
		}
		if !pending[0].Load() && !pending[1].Load() {
			break
		}
		select {
		case <-observing.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	cancel()
	for completed := 0; completed < 2; completed++ {
		outcome := <-outcomes
		if outcome.Failure != nil && (failure == nil || outcome.Failure.Unknown) {
			failure = outcome.Failure
		}
		if outcome.Index == 0 && outcome.Failure == nil && (failure == nil || !failure.Unknown) {
			failure = &Failure{Code: "active_transaction_migration_not_blocked"}
		}
	}
	if err := log.Record(map[string]any{"type": "starvation_observations", "samples": samples, "overlap_acked_samples": overlapACKs, "placement_renewal_observed": renewalObserved, "authority_lease_renewal": "unavailable", "terminal_migration_refusal_oracle": "unavailable_generic_operation_incomplete_is_unknown", "failure": failure}); err != nil && failure == nil {
		failure = &Failure{Code: "starvation_evidence_failed"}
	}
	if failure == nil {
		failure = &Failure{Code: "starvation_terminal_and_lease_oracles_unavailable"}
	}
	return failure
}
