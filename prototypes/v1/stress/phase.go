package stress

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type offeredJob struct {
	Sequence   int
	BusinessID string
	Offered    time.Time
}

func runPhase(parent context.Context, name, kind, phase string, count int, duration time.Duration, fixtures []*Fixture, settings Settings, artifacts *Artifacts, ids *IDs) PhaseResult {
	result := PhaseResult{Scenario: name, Phase: phase, Concurrency: count, Status: "COMPLETE", ConfiguredRate: settings.OfferedRate,
		Errors: map[string]int{}, PerProjectAcked: map[string]int{}, PerProjectOffered: map[string]int{}, ShortGate: settings.Smoke < 10*time.Second || settings.Measure < 30*time.Second}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	period := time.Second / time.Duration(settings.OfferedRate)
	result.RequestedDurationNS = duration.Nanoseconds()
	result.PlannedOffers = int((duration + period - 1) / period)
	result.OperationCapReached = result.PlannedOffers > settings.MaxOperations
	activeDatabases := map[DatabaseID]bool{}
	sessions := make([]*Session, count)
	for slot := range sessions {
		fixture := fixtures[slot%len(fixtures)]
		activeDatabases[fixture.Target.Database] = true
		session, failure := fixture.Client.Open(ctx, fixture.Target.Database)
		if failure != nil {
			result.Status, result.Reason = "FAIL", failure.Code
			if failure.Unknown {
				result.Status, result.Unknown = "UNRESOLVED", 1
			}
			result.Errors[failure.Class]++
			closePhaseSessions(sessions, artifacts, &result)
			return result
		}
		sessions[slot] = session
	}
	result.ActiveDatabases = len(activeDatabases)
	started := time.Now()
	deadline := started.Add(duration)
	result.StartedAt = started.UTC().Format(time.RFC3339Nano)
	jobs := make([]chan offeredJob, count)
	events := make(chan OperationRecord, count)
	var workers sync.WaitGroup
	for slot := 0; slot < count; slot++ {
		jobs[slot] = make(chan offeredJob, 1)
		workers.Add(1)
		go func(slot int) {
			defer workers.Done()
			fixture, session := fixtures[slot%len(fixtures)], sessions[slot]
			for {
				select {
				case <-ctx.Done():
					return
				case job, open := <-jobs[slot]:
					if !open {
						return
					}
					dequeued := time.Now()
					if dequeued.After(deadline) {
						continue
					}
					random := rand.New(rand.NewSource(settings.Seed ^ int64(job.Sequence+1)*6364136223846793005))
					task := fixture.Model.Task(kind, job.BusinessID, random)
					began := time.Now()
					record := OperationRecord{Type: "operation", Scenario: name, Phase: phase, Sequence: job.Sequence, BusinessID: job.BusinessID,
						Project: fixture.Target.Project, Database: fixture.Target.Database, Slot: slot, OfferedAt: job.Offered.UTC().Format(time.RFC3339Nano),
						StartedAt: began.UTC().Format(time.RFC3339Nano), QueueNS: dequeued.Sub(job.Offered).Nanoseconds(), PrepareNS: began.Sub(dequeued).Nanoseconds()}
					rows, identifier, failure := session.SQL(ctx, task.Query, task.SQL)
					record.RequestIDs = []string{identifier}
					if task.Flow == "rollback" && failure == nil {
						rows, identifier, failure = session.SQL(ctx, true, "SELECT count(*) FROM "+fixture.Model.Table("probe")+" WHERE id='"+job.BusinessID+"'")
						record.RequestIDs = append(record.RequestIDs, identifier)
					}
					ended := time.Now()
					record.FinishedAt, record.ElapsedNS, record.EndToEndNS = ended.UTC().Format(time.RFC3339Nano), ended.Sub(began).Nanoseconds(), ended.Sub(job.Offered).Nanoseconds()
					record.Failure, record.Acked = failure, failure == nil
					record.Correct = failure == nil && task.Check(rows)
					if task.Expected != nil {
						record.ExpectedRowsHash = HashRows(task.Expected)
						record.ActualRowsHash = HashRows(rows)
					}
					if record.Acked {
						record.Mutations = task.Mutations
					}
					events <- record
					if failure != nil && failure.Class != "admission" || record.Acked && !record.Correct {
						cancel()
						return
					}
				}
			}
		}(slot)
	}
	scheduled := make(chan struct{})
	schedulerFailure := make(chan struct{}, 1)
	var offeringEnded time.Time
	go func() {
		defer close(scheduled)
		defer func() { offeringEnded = time.Now() }()
		defer func() {
			for _, channel := range jobs {
				close(channel)
			}
		}()
		for sequence := 0; sequence < settings.MaxOperations; sequence++ {
			intended := started.Add(time.Duration(sequence) * period)
			if !intended.Before(deadline) {
				break
			}
			if delay := time.Until(intended); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if ctx.Err() != nil || !time.Now().Before(deadline) {
				break
			}
			slot := sequence % count
			fixture := fixtures[slot%len(fixtures)]
			result.Offered++
			result.PerProjectOffered[fixture.Target.Project]++
			job := offeredJob{Sequence: sequence, BusinessID: ids.Next(), Offered: time.Now()}
			select {
			case jobs[slot] <- job:
			default:
				result.ClientAdmissionDropped++
				if artifacts.Record(map[string]any{"type": "client_admission_drop", "scenario": name, "phase": phase, "offered_sequence": sequence, "business_operation_id": job.BusinessID, "project": fixture.Target.Project, "observed_at": job.Offered.UTC().Format(time.RFC3339Nano)}) != nil {
					schedulerFailure <- struct{}{}
					cancel()
					return
				}
			}
		}
		remaining := time.Until(deadline)
		if remaining > 0 {
			timer := time.NewTimer(remaining)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}()
	go func() { workers.Wait(); close(events) }()
	var latency, success, queue []int64
	for event := range events {
		result.Attempted++
		result.Completed++
		latency, queue = append(latency, event.ElapsedNS), append(queue, event.QueueNS)
		if event.Acked {
			result.Acked++
			result.PerProjectAcked[event.Project]++
			success = append(success, event.ElapsedNS)
			fixture := fixtures[event.Slot%len(fixtures)]
			fixture.Acked = append(fixture.Acked, event.Mutations...)
			if !event.Correct {
				result.CorrectnessFailures++
				result.Status, result.Reason = "FAIL", "operation_rows_or_index_plan_mismatch"
				cancel()
			}
		} else if event.Failure != nil {
			result.Errors[event.Failure.Class]++
			if event.Failure.Unknown {
				result.Unknown++
			}
			if event.Failure.TimedOut {
				result.Timeouts++
			}
			if event.Failure.Class != "admission" {
				result.Status, result.Reason = "FAIL", event.Failure.Code
			}
		}
		if artifacts.Record(event) != nil {
			result.Status, result.Reason = "FAIL", "artifact_budget_or_write_failure"
			cancel()
		}
	}
	<-scheduled
	select {
	case <-schedulerFailure:
		result.Status, result.Reason = "FAIL", "scheduler_artifact_budget_or_write_failure"
	default:
	}
	result.OfferingElapsedNS = offeringEnded.Sub(started).Nanoseconds()
	result.MissedOffers = result.PlannedOffers - result.Offered
	result.QueueExpired = result.Offered - result.ClientAdmissionDropped - result.Attempted
	result.ElapsedNS = time.Since(started).Nanoseconds()
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if result.OfferingElapsedNS > 0 {
		result.OfferedRate = float64(result.Offered) * 1e9 / float64(result.OfferingElapsedNS)
	}
	if result.ElapsedNS > 0 {
		result.AchievedRate = float64(result.Acked) * 1e9 / float64(result.ElapsedNS)
	}
	result.AllLatency, result.SuccessLatency, result.QueueLatency = LatencyQuantiles(latency), LatencyQuantiles(success), LatencyQuantiles(queue)
	result.SaturationObserved = result.Errors["admission"] > 0
	closePhaseSessions(sessions, artifacts, &result)
	if result.Unknown > 0 {
		result.Status = "UNRESOLVED"
	}
	if parent.Err() != nil && result.Status == "COMPLETE" {
		result.Status, result.Reason = "FAIL", "run_canceled_or_resource_safety_stop"
	}
	return result
}

func closePhaseSessions(sessions []*Session, artifacts *Artifacts, result *PhaseResult) {
	for slot, session := range sessions {
		if session == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		failure := session.Close(ctx)
		cancel()
		if failure != nil {
			result.Errors["cleanup_"+failure.Class]++
			if failure.Unknown {
				result.CleanupUnknown++
			}
			result.Status, result.Reason = "UNRESOLVED", "session_cleanup_not_proven"
		}
		if artifacts.Record(map[string]any{"type": "session_cleanup", "scenario": result.Scenario, "phase": result.Phase, "slot": slot, "failure": failure, "observed_at": time.Now().UTC().Format(time.RFC3339Nano)}) != nil {
			result.Status, result.Reason = "FAIL", "cleanup_artifact_budget_or_write_failure"
		}
	}
}
