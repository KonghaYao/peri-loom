package stress

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"
)

func lifecycle(ctx context.Context, name string, fixture *Fixture, management *Client, worker string, artifacts *Artifacts) *Failure {
	started := time.Now()
	var failure *Failure
	switch name {
	case "session_cycles":
		for cycle := 0; cycle < 8; cycle++ {
			session, openFailure := fixture.Client.Open(ctx, fixture.Target.Database)
			if openFailure != nil {
				failure = openFailure
				break
			}
			task := fixture.Model.Task("point_read", "", rand.New(rand.NewSource(int64(cycle))))
			rows, _, sqlFailure := session.SQL(ctx, true, task.SQL)
			if sqlFailure != nil {
				failure = sqlFailure
				break
			}
			if !task.Check(rows) {
				failure = &Failure{Class: "correctness", Code: "session_cycle_rows_mismatch"}
				break
			}
			if failure = session.Close(ctx); failure != nil {
				break
			}
		}
	case "checkpoint_reopen":
		actual, callFailure := management.Management(ctx, "checkpoint", fixture.Target.Database, "")
		failure = callFailure
		if failure == nil {
			var confirmed struct {
				Checkpointed bool `json:"checkpointed"`
			}
			if json.Unmarshal(actual, &confirmed) != nil || !confirmed.Checkpointed {
				failure = &Failure{Class: "unknown", Code: "checkpoint_completion_not_proven", Unknown: true}
			}
		}
	case "idle_suspend_resume":
		_, failure = management.Management(ctx, "suspend", fixture.Target.Database, "")
		if failure == nil {
			failure = awaitPlacement(ctx, management, fixture.Target.Database, "suspended", "")
		}
		if failure == nil {
			select {
			case <-ctx.Done():
				failure = &Failure{Class: "timeout", Code: "idle_wait_interrupted", TimedOut: true}
			case <-time.After(100 * time.Millisecond):
			}
		}
		if failure == nil {
			_, failure = management.Management(ctx, "resume", fixture.Target.Database, "")
		}
		if failure == nil {
			failure = awaitPlacement(ctx, management, fixture.Target.Database, "active", "")
		}
	case "graceful_migrate":
		before, statusFailure := management.Management(ctx, "status", fixture.Target.Database, "")
		failure = statusFailure
		if failure == nil {
			var current struct {
				Placement struct {
					DesiredWorker string `json:"desired_worker"`
				} `json:"placement"`
			}
			if json.Unmarshal(before, &current) != nil || current.Placement.DesiredWorker == "" {
				failure = &Failure{Class: "unknown", Code: "migration_source_not_proven", Unknown: true}
			} else if current.Placement.DesiredWorker == worker {
				failure = &Failure{Class: "unsupported", Code: "migration_target_already_owns_fixture"}
			}
			if err := artifacts.Record(map[string]any{"type": "migration_source", "database": fixture.Target.Database, "actual_placement_snapshot": json.RawMessage(before)}); err != nil {
				failure = &Failure{Class: "artifact_error", Code: "migration_source_evidence_unavailable"}
			}
		}
		if failure == nil {
			_, failure = management.Management(ctx, "migrate", fixture.Target.Database, worker)
		}
		if failure == nil {
			failure = awaitPlacement(ctx, management, fixture.Target.Database, "active", worker)
		}
	}
	if err := artifacts.Record(map[string]any{"type": "lifecycle", "scenario": name, "database": fixture.Target.Database, "started_at_observed": started.UTC().Format(time.RFC3339Nano), "finished_at_observed": time.Now().UTC().Format(time.RFC3339Nano), "elapsed_ns": time.Since(started).Nanoseconds(), "failure": failure, "native_empty_cache_proven": false}); err != nil {
		return &Failure{Class: "artifact_error", Code: "lifecycle_artifact_failed"}
	}
	return failure
}

func awaitPlacement(parent context.Context, client *Client, database DatabaseID, phase, worker string) *Failure {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	for {
		actual, failure := client.Management(ctx, "status", database, "")
		if failure != nil {
			return failure
		}
		var status struct {
			Placement struct {
				Phase         string `json:"phase"`
				DesiredWorker string `json:"desired_worker"`
			} `json:"placement"`
		}
		if json.Unmarshal(actual, &status) != nil || status.Placement.Phase == "" {
			return &Failure{Class: "unknown", Code: "invalid_placement_snapshot", Unknown: true}
		}
		if status.Placement.Phase == phase && (worker == "" || status.Placement.DesiredWorker == worker) {
			return nil
		}
		select {
		case <-ctx.Done():
			return &Failure{Class: "timeout", Code: "placement_convergence_not_proven", Unknown: true, TimedOut: true}
		case <-time.After(250 * time.Millisecond):
		}
	}
}
