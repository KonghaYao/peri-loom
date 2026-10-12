package stress

import (
	"context"
	"strings"
	"time"
)

type capacityBindingKey struct{}

func capacityBoundContext(parent context.Context, record OperationRecord) context.Context {
	return context.WithValue(parent, capacityBindingKey{}, map[string]any{"project": record.Project, "database": record.Database,
		"phase": record.Phase, "concurrency": record.Concurrency, "business_operation_id": record.BusinessID})
}

func capacityScenario(name string) bool {
	return strings.HasPrefix(name, "capacity_")
}

func capacityKind(name string) string {
	switch name {
	case "capacity_read":
		return "point_read"
	case "capacity_mixed":
		return "mixed_oltp"
	case "capacity_held_writer":
		return "held_writer"
	case "capacity_io_checkpoint":
		return "io_checkpoint"
	default:
		return "sustained_wal"
	}
}

func capacitySets(fixtures []*Fixture) []struct {
	name     string
	fixtures []*Fixture
} {
	primary, noise := []*Fixture{}, []*Fixture{}
	for _, fixture := range fixtures {
		if fixture.Target.Project == fixtures[0].Target.Project {
			primary = append(primary, fixture)
		} else {
			noise = append(noise, fixture)
		}
	}
	sets := []struct {
		name     string
		fixtures []*Fixture
	}{{"isolated_primary_", primary}}
	if len(noise) > 0 {
		sets = append(sets, struct {
			name     string
			fixtures []*Fixture
		}{"isolated_noise_", noise}, struct {
			name     string
			fixtures []*Fixture
		}{"combined_", fixtures})
	}
	return sets
}

func capacitySlot(sequence, count int, fixtures []*Fixture, victimEvery int) int {
	primary, noise := []int{}, []int{}
	for slot := 0; slot < count; slot++ {
		if fixtures[slot%len(fixtures)].Target.Project == fixtures[0].Target.Project {
			primary = append(primary, slot)
		} else {
			noise = append(noise, slot)
		}
	}
	if len(primary) == 0 || len(noise) == 0 {
		return sequence % count
	}
	if victimEvery == 0 {
		victimEvery = 2
	}
	if sequence%victimEvery == 0 {
		return primary[(sequence/victimEvery)%len(primary)]
	}
	return noise[(sequence-sequence/victimEvery-1)%len(noise)]
}

func capacityPhaseFixtures(fixtures []*Fixture, count int) []*Fixture {
	groups := capacitySets(fixtures)
	if len(groups) == 1 {
		return fixtures
	}
	selected := make([]*Fixture, count)
	primaryCount := (count + 1) / 2
	for slot := range selected {
		if slot < primaryCount {
			selected[slot] = groups[0].fixtures[slot%len(groups[0].fixtures)]
		} else {
			selected[slot] = groups[1].fixtures[(slot-primaryCount)%len(groups[1].fixtures)]
		}
	}
	return selected
}

func capacityPhaseSettings(settings Settings, phase string, dual bool) Settings {
	if !dual || phase == "combined_" {
		return settings
	}
	settings.OfferNumerator, settings.OfferDenominator = 1, 2
	if settings.VictimEvery == 10 {
		settings.OfferDenominator = 10
		if phase == "isolated_noise_" {
			settings.OfferNumerator = 9
		}
	}
	return settings
}

func capacityPhaseSlots(count int, phase string, dual bool) int {
	if dual && phase != "combined_" {
		return count / 2
	}
	return count
}

func offerSchedule(settings Settings, sequence int) time.Duration {
	numerator, denominator := settings.OfferNumerator, settings.OfferDenominator
	if numerator == 0 {
		numerator, denominator = 1, 1
	}
	return time.Duration(sequence) * time.Second * time.Duration(denominator) / time.Duration(settings.OfferedRate*numerator)
}

func plannedOffers(settings Settings, duration time.Duration) int {
	numerator, denominator := settings.OfferNumerator, settings.OfferDenominator
	if numerator == 0 {
		numerator, denominator = 1, 1
	}
	budget := duration.Nanoseconds() * int64(settings.OfferedRate*numerator)
	period := int64(time.Second) * int64(denominator)
	return int((budget + period - 1) / period)
}

func capacityWorkflow(ctx context.Context, session *Session, fixture *Fixture, task Task, kind string, sequence int, timeout time.Duration) ([][]any, []string, []Mutation, *Failure) {
	identifiers := []string{}
	step := func(parent context.Context, query bool, sql string) ([][]any, *Failure) {
		rows, identifier, failure := phaseSQL(parent, session, timeout, query, sql)
		identifiers = append(identifiers, identifier)
		return rows, failure
	}
	if kind != "held_writer" {
		rows, failure := step(ctx, task.Query, task.SQL)
		if failure == nil && kind == "io_checkpoint" {
			if fixture.Management == nil {
				failure = &Failure{Class: "unsupported", Code: "checkpoint_management_required"}
			} else {
				checkpointContext, cancel := context.WithTimeout(ctx, timeout)
				failure = lifecycle(checkpointContext, "checkpoint_reopen", fixture, fixture.Management, "", fixture.Artifacts)
				cancel()
			}
		}
		if failure != nil && len(task.Mutations) > 0 && kind == "io_checkpoint" {
			failure = &Failure{Class: "unknown", Code: "write_checkpoint_workflow_incomplete", Unknown: true}
			session.poisoned = true
		}
		return rows, identifiers, task.Mutations, failure
	}
	if _, failure := step(ctx, false, "BEGIN IMMEDIATE"); failure != nil {
		return nil, identifiers, nil, failure
	}
	statement := strings.TrimSuffix(strings.TrimPrefix(task.SQL, "BEGIN IMMEDIATE;"), "COMMIT")
	if strings.HasPrefix(statement, "BEGIN") || strings.HasSuffix(statement, "COMMIT") {
		return nil, identifiers, nil, &Failure{Class: "correctness", Code: "held_writer_statement_not_unwrapped"}
	}
	_, failure := step(ctx, false, statement)
	if failure == nil {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			failure = &Failure{Class: "canceled", Code: "held_writer_canceled"}
		case <-timer.C:
		}
		timer.Stop()
	}
	if failure != nil && failure.Unknown {
		return nil, identifiers, nil, failure
	}
	terminal := "COMMIT"
	if sequence%2 == 1 || failure != nil {
		terminal = "ROLLBACK"
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if binding := ctx.Value(capacityBindingKey{}); binding != nil {
		cleanup = context.WithValue(cleanup, capacityBindingKey{}, binding)
	}
	_, terminalFailure := step(cleanup, false, terminal)
	cancel()
	if terminalFailure != nil {
		failure = &Failure{Class: "unknown", Code: "held_writer_terminal_not_proven", Unknown: true}
		session.poisoned = true
	}
	if fixture.Artifacts != nil {
		encoded := map[string]any{"type": "held_writer_terminal", "project": fixture.Target.Project, "database": fixture.Target.Database,
			"terminal": terminal, "terminal_acked": terminalFailure == nil, "reserved_cleanup_ns": int64(5 * time.Second), "failure": failure}
		if fixture.Artifacts.Record(encoded) != nil {
			failure = &Failure{Class: "artifact_error", Code: "held_writer_cleanup_evidence_missing"}
		}
	}
	mutations := task.Mutations
	if terminal == "ROLLBACK" {
		mutations = nil
	}
	return nil, identifiers, mutations, failure
}
