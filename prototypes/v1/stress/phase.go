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
	if settings.MatrixConcurrency != 0 {
		result.Concurrency = settings.MatrixConcurrency
		result.EffectiveSlots = map[string]int{}
		for slot := 0; slot < count; slot++ {
			result.EffectiveSlots[fixtures[slot%len(fixtures)].Target.Project]++
		}
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	dispatch := newPhaseDispatch(ctx, settings.OfferStop)
	defer dispatch.stopOffers()
	result.RequestedDurationNS = duration.Nanoseconds()
	result.PlannedOffers = plannedOffers(settings, duration)
	result.EffectiveRate = float64(time.Second) / float64(offerSchedule(settings, 1))
	result.OperationCapReached = result.PlannedOffers > settings.MaxOperations
	activeDatabases := map[DatabaseID]bool{}
	sessions := make([]*Session, count)
	for slot := range sessions {
		if offerStopRequested(settings.OfferStop) {
			result.Status, result.Reason = "FAIL", "operator_stop_offers"
			closePhaseSessions(sessions, artifacts, &result)
			return result
		}
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
			workflowSequence := 0
			for {
				select {
				case <-settings.OfferStop:
					return
				case <-dispatch.context.Done():
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
					taskKind := kind
					if kind == "held_writer" || kind == "io_checkpoint" {
						taskKind = "sustained_wal"
					}
					task := fixture.Model.Task(taskKind, job.BusinessID, random)
					if !dispatch.admit() {
						return
					}
					began := time.Now()
					record := OperationRecord{Type: "operation", Scenario: name, Phase: phase, Concurrency: result.Concurrency, Sequence: job.Sequence, BusinessID: job.BusinessID,
						Project: fixture.Target.Project, Database: fixture.Target.Database, Slot: slot, OfferedAt: job.Offered.UTC().Format(time.RFC3339Nano),
						StartedAt: began.UTC().Format(time.RFC3339Nano), QueueNS: dequeued.Sub(job.Offered).Nanoseconds(), PrepareNS: began.Sub(dequeued).Nanoseconds()}
					var rows [][]any
					var identifier string
					var failure *Failure
					operationContext := ctx
					if capacityScenario(name) {
						operationContext = capacityBoundContext(ctx, record)
					}
					if kind == "held_writer" || kind == "io_checkpoint" {
						rows, record.RequestIDs, task.Mutations, failure = capacityWorkflow(operationContext, session, fixture, task, kind, workflowSequence, settings.RequestTimeout)
						if kind == "held_writer" && failure == nil {
							record.Terminal = "COMMIT"
							if workflowSequence%2 == 1 {
								record.Terminal = "ROLLBACK"
							}
						}
						workflowSequence++
					} else {
						rows, identifier, failure = phaseSQL(operationContext, session, settings.RequestTimeout, task.Query, task.SQL)
						record.RequestIDs = []string{identifier}
					}
					if task.Flow == "rollback" && failure == nil {
						rows, identifier, failure = phaseSQL(ctx, session, settings.RequestTimeout, true, "SELECT count(*) FROM "+fixture.Model.Table("probe")+" WHERE id='"+job.BusinessID+"'")
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
					stop := failure != nil && failure.Class != "admission" || record.Acked && !record.Correct
					if stop || artifacts.failed.Load() {
						dispatch.stopOffers()
					}
					if artifacts.failed.Load() || failure != nil && failure.Class == "artifact_error" {
						cancel()
					}
					events <- record
					if stop {
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
			intended := started.Add(offerSchedule(settings, sequence))
			if !intended.Before(deadline) {
				break
			}
			if delay := time.Until(intended); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-settings.OfferStop:
					timer.Stop()
					return
				case <-dispatch.context.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if !dispatch.admit() || !time.Now().Before(deadline) {
				break
			}
			slot := sequence % count
			if capacityScenario(name) {
				slot = capacitySlot(sequence, count, fixtures, settings.VictimEvery)
			}
			fixture := fixtures[slot%len(fixtures)]
			result.Offered++
			result.PerProjectOffered[fixture.Target.Project]++
			job := offeredJob{Sequence: sequence, BusinessID: ids.Next(), Offered: time.Now()}
			select {
			case jobs[slot] <- job:
			default:
				result.ClientAdmissionDropped++
				if artifacts.Record(map[string]any{"type": "client_admission_drop", "scenario": name, "phase": phase, "concurrency": result.Concurrency, "offered_sequence": sequence, "business_operation_id": job.BusinessID, "project": fixture.Target.Project, "observed_at": job.Offered.UTC().Format(time.RFC3339Nano)}) != nil {
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
			case <-settings.OfferStop:
				timer.Stop()
			case <-dispatch.context.Done():
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
				dispatch.stopOffers()
			}
		} else if event.Failure != nil {
			result.Errors[event.Failure.Class]++
			if event.Failure.Unknown {
				result.Unknown++
			}
			if event.Failure.TimedOut {
				result.Timeouts++
			}
			if event.Failure.Class != "admission" && result.Status == "COMPLETE" {
				result.Status, result.Reason = "FAIL", event.Failure.Code
			}
		}
		if artifacts.Record(event) != nil {
			result.Status, result.Reason = "FAIL", "artifact_budget_or_write_failure"
			cancel()
		}
	}
	<-scheduled
	if offerStopRequested(settings.OfferStop) && result.Status == "COMPLETE" {
		result.Status, result.Reason = "FAIL", "operator_stop_offers"
	}
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
