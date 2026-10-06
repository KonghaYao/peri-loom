package stress

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type RunReport struct {
	Version               int              `json:"version"`
	RunID                 string           `json:"run_id"`
	Status                string           `json:"status"`
	Reason                string           `json:"reason,omitempty"`
	StartedAt             string           `json:"started_at_observed"`
	FinishedAt            string           `json:"finished_at_observed"`
	ElapsedNS             int64            `json:"elapsed_ns"`
	Source                Source           `json:"source"`
	JointGate             JointGate        `json:"joint_gate"`
	Config                map[string]any   `json:"redacted_connection"`
	Settings              Settings         `json:"settings"`
	Capacity              HostCapacity     `json:"actual_host_capacity"`
	Preflight             []Correctness    `json:"preflight_correctness"`
	Results               []ScenarioResult `json:"scenarios"`
	GeneratedRequestBytes int64            `json:"generated_request_bytes_charged"`
	Limitations           []string         `json:"limitations"`
}

type SerialRunner struct {
	Config    ConnectionConfig
	Settings  Settings
	OutputDir string
	GatePath  string
}

func (runner SerialRunner) Run(parent context.Context) (string, error) {
	if err := runner.Config.Validate(); err != nil {
		return "", err
	}
	if err := runner.Settings.Validate(); err != nil {
		return "", err
	}
	for _, credential := range []string{runner.Config.APIKey, runner.Config.ManagementKey} {
		if credential != "" && strings.Contains(runner.OutputDir, credential) {
			return "", errors.New("output path must not contain a credential")
		}
	}
	if runner.Config.NoisyTenant != nil && strings.Contains(runner.OutputDir, runner.Config.NoisyTenant.APIKey) {
		return "", errors.New("output path must not contain a credential")
	}
	scenarios, err := SelectScenarios(runner.Settings.Cases)
	if err != nil {
		return "", err
	}
	sourceCtx, sourceCancel := context.WithTimeout(parent, 5*time.Second)
	source, err := CollectSource(sourceCtx, runner.Config.SourceRoot)
	sourceCancel()
	if err != nil {
		return "", err
	}
	gate, err := ReadGate(runner.GatePath, source.GitHash)
	if err != nil {
		return "", err
	}
	lock, err := acquireLocks(runner.OutputDir, "/tmp/peri-loom-stress.global.lock")
	if err != nil {
		return "", err
	}
	defer lock.Close()
	var entropy [8]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", errors.New("run identity unavailable")
	}
	identifier := hex.EncodeToString(entropy[:])
	directory := filepath.Join(runner.OutputDir, "run_"+identifier)
	secrets := []string{runner.Config.APIKey, runner.Config.ManagementKey}
	if runner.Config.NoisyTenant != nil {
		secrets = append(secrets, runner.Config.NoisyTenant.APIKey)
	}
	artifacts, err := NewArtifacts(directory, runner.Settings.MaxArtifactBytes, secrets)
	if err != nil {
		return "", err
	}
	defer artifacts.Close()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	started := time.Now()
	report := RunReport{Version: 1, RunID: identifier, Status: "COMPLETE", StartedAt: started.UTC().Format(time.RFC3339Nano), Source: source,
		JointGate: gate, Config: runner.Config.Redacted(), Settings: runner.Settings,
		Limitations: []string{"implementation uses gateway HTTP only; no implicit HTTP or SQL retry", "missing metrics are unavailable/null, never inferred from cache accounting", "closed gateway session and reopened session do not prove an empty native cache", "duration and latency use Go monotonic elapsed; wall timestamps are observations only", "ps percent CPU is an OS lifetime/window metric; RSS is process RSS", "raw results are evidence, not a performance conclusion; short phases are gates only"}}
	capacityCtx, capacityCancel := context.WithTimeout(ctx, 2*time.Second)
	report.Capacity = Capacity(capacityCtx)
	capacityCancel()
	ids := &IDs{Prefix: identifier}
	budget := &DataBudget{Maximum: runner.Settings.MaxGeneratedBytes}
	fixtures, management, err := runner.clients(identifier, ids, budget, scenarios)
	if err != nil {
		return directory, err
	}
	for _, fixture := range fixtures {
		fixture.Client.artifacts, fixture.Artifacts = artifacts, artifacts
	}
	if management != nil {
		management.artifacts = artifacts
	}
	observer := &Observer{Config: runner.Config, Management: management, Artifacts: artifacts, MaxRSS: runner.Settings.MaxRSSBytes, identities: map[int]string{}}
	observeErrors := make(chan error, 1)
	observerDone := make(chan struct{})
	observeCtx, stopObserving := context.WithCancel(ctx)
	go func() {
		defer close(observerDone)
		for {
			sampleCtx, sampleCancel := context.WithTimeout(observeCtx, 2*time.Second)
			err := observer.Sample(sampleCtx)
			sampleCancel()
			if err != nil {
				if observeCtx.Err() == nil {
					observeErrors <- err
					cancel()
				}
				return
			}
			timer := time.NewTimer(time.Second)
			select {
			case <-observeCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	fail := func(reason string) {
		if report.Status != "UNRESOLVED" {
			report.Status = "FAIL"
		}
		report.Reason = reason
	}
	for _, fixture := range fixtures {
		if failure := fixture.Setup(ctx); failure != nil {
			fail(failure.Code)
			if failure.Unknown {
				report.Status = "UNRESOLVED"
			}
			break
		}
		if failure := smokeCorrectness(ctx, fixture, ids); failure != nil {
			fail(failure.Code)
			if failure.Unknown {
				report.Status = "UNRESOLVED"
			}
			break
		}
		verified, failure := fixture.Verify(ctx)
		report.Preflight = append(report.Preflight, verified)
		if failure != nil || verified.Status != "PASS" {
			fail("preflight_correctness_not_proven")
			break
		}
	}
	if report.Status == "COMPLETE" {
		for _, scenario := range scenarios {
			result := runner.scenario(ctx, scenario, fixtures, management, artifacts, ids)
			report.Results = append(report.Results, result)
			if result.Status == "FAIL" || result.Status == "UNRESOLVED" {
				report.Status, report.Reason = result.Status, result.Reason
				break
			}
			if ctx.Err() != nil {
				fail("run_canceled_or_resource_safety_stop")
				break
			}
		}
	}
	stopObserving()
	<-observerDone
	select {
	case <-observeErrors:
		fail("process_or_artifact_safety_stop")
	default:
	}
	if parent.Err() != nil && report.Status == "COMPLETE" {
		fail("run_interrupted")
	}
	if artifacts.failed.Load() {
		fail("artifact_recording_not_complete")
	}
	report.ElapsedNS, report.FinishedAt, report.GeneratedRequestBytes = time.Since(started).Nanoseconds(), time.Now().UTC().Format(time.RFC3339Nano), budget.used.Load()
	if err := artifacts.Summary(report); err != nil {
		return directory, err
	}
	if report.Status != "COMPLETE" {
		return directory, fmt.Errorf("run stopped: %s", report.Reason)
	}
	return directory, nil
}

func (runner SerialRunner) clients(prefix string, ids *IDs, budget *DataBudget, scenarios []Scenario) ([]*Fixture, *Client, error) {
	targets := []Target{runner.Config.Target}
	for _, scenario := range scenarios {
		if scenario.Name == "many_db" {
			for _, database := range runner.Config.Databases {
				target := runner.Config.Target
				target.Database = database
				targets = append(targets, target)
			}
		}
		if scenario.Name == "noisy_tenant" && runner.Config.NoisyTenant != nil {
			targets = append(targets, *runner.Config.NoisyTenant)
		}
	}
	fixtures := make([]*Fixture, 0, len(targets))
	seen := map[DatabaseID]bool{}
	for _, target := range targets {
		if seen[target.Database] {
			continue
		}
		seen[target.Database] = true
		client, err := NewClient(target.Endpoint, target.APIKey, runner.Config.CAFile, runner.Settings, ids, budget)
		if err != nil {
			return nil, nil, err
		}
		fixtures = append(fixtures, &Fixture{Target: target, Client: client, Model: NewModel("stress_"+prefix, runner.Settings.Rows, runner.Settings.Seed+int64(len(fixtures)))})
	}
	var management *Client
	if runner.Config.ManagementKey != "" {
		var err error
		management, err = NewClient(runner.Config.ManagementEndpoint, runner.Config.ManagementKey, runner.Config.CAFile, runner.Settings, ids, budget)
		if err != nil {
			return nil, nil, err
		}
	}
	return fixtures, management, nil
}

func smokeCorrectness(ctx context.Context, fixture *Fixture, ids *IDs) (failure *Failure) {
	session, failure := fixture.Client.Open(ctx, fixture.Target.Database)
	if failure != nil {
		return failure
	}
	defer finishSession(session, &failure)
	for operation := 0; operation < 4; operation++ {
		task := fixture.Model.write(ids.Next(), int64(operation+1), 1, 1)
		if _, _, failure = session.SQL(ctx, false, task.SQL); failure != nil {
			return failure
		}
		fixture.Acked = append(fixture.Acked, task.Mutations...)
		if fixture.Artifacts != nil && fixture.Artifacts.Record(map[string]any{"type": "preflight_write_ack", "business_operation_id": task.Mutations[0].OperationID, "mutations": task.Mutations, "database": fixture.Target.Database}) != nil {
			return &Failure{Class: "artifact_error", Code: "preflight_ledger_evidence_not_recorded"}
		}
	}
	return session.Close(ctx)
}

func (runner SerialRunner) scenario(ctx context.Context, scenario Scenario, fixtures []*Fixture, management *Client, artifacts *Artifacts, ids *IDs) ScenarioResult {
	result := ScenarioResult{Scenario: scenario.Name, Status: "COMPLETE"}
	if scenario.Capability == "SKIP" {
		result.Status, result.Reason = "SKIP", scenario.Reason
		return result
	}
	active := []*Fixture{fixtures[0]}
	switch scenario.Name {
	case "many_db":
		active = nil
		for _, fixture := range fixtures {
			if fixture.Target.Project == runner.Config.Project {
				active = append(active, fixture)
			}
		}
		if len(active) < 2 {
			result.Status, result.Reason = "SKIP", scenario.Reason
			return result
		}
	case "noisy_tenant":
		if runner.Config.NoisyTenant == nil {
			result.Status, result.Reason = "SKIP", scenario.Reason
			return result
		}
		eligible := false
		for _, concurrency := range runner.Settings.Concurrency {
			eligible = eligible || concurrency >= 2
		}
		if !eligible {
			result.Status, result.Reason = "SKIP", "combined interference requires concurrency >=2"
			return result
		}
		for _, fixture := range fixtures {
			if fixture.Target.Project == runner.Config.NoisyTenant.Project {
				active = append(active, fixture)
			}
		}
	case "idle_suspend_resume", "graceful_migrate", "checkpoint_reopen":
		if management == nil || scenario.Name == "graceful_migrate" && (len(runner.Config.WorkerIDs) != 2 || runner.Config.MigrationWorker == "") {
			result.Status, result.Reason = "SKIP", scenario.Reason
			return result
		}
	}
	if scenario.Name == "constraint_error_reuse" || scenario.Name == "session_cycles" || scenario.Name == "idle_suspend_resume" || scenario.Name == "graceful_migrate" {
		var failure *Failure
		if scenario.Name == "constraint_error_reuse" {
			failure = constraintErrorReuse(ctx, active[0], artifacts)
		} else {
			failure = lifecycle(ctx, scenario.Name, active[0], management, runner.Config.MigrationWorker, artifacts)
		}
		if failure != nil {
			result.Status, result.Reason = "FAIL", failure.Code
			if failure.Class == "unsupported" {
				result.Status = "SKIP"
			}
			if failure.Unknown {
				result.Status = "UNRESOLVED"
			}
			return result
		}
	} else {
		kind := scenario.Name
		if kind == "many_db" || kind == "noisy_tenant" || kind == "saturation_admission" {
			kind = "point_read"
		}
		if kind == "checkpoint_reopen" {
			kind = "sustained_wal"
		}
		for _, concurrency := range runner.Settings.Concurrency {
			if scenario.Name == "noisy_tenant" && concurrency == 1 {
				result.Phases = append(result.Phases, PhaseResult{Scenario: scenario.Name, Concurrency: 1, Status: "SKIP", Reason: "combined interference requires at least two independent sessions"})
				continue
			}
			sets := []struct {
				name     string
				fixtures []*Fixture
			}{{"", active}}
			if scenario.Name == "noisy_tenant" {
				sets = []struct {
					name     string
					fixtures []*Fixture
				}{{"isolated_primary_", active[:1]}, {"isolated_noise_", active[1:]}, {"combined_", active}}
			}
			for _, set := range sets {
				for _, phase := range []struct {
					name     string
					duration time.Duration
				}{{"warmup", runner.Settings.Warmup}, {"smoke", runner.Settings.Smoke}, {"measure", runner.Settings.Measure}} {
					if phase.duration == 0 {
						continue
					}
					measured := runPhase(ctx, scenario.Name, kind, set.name+phase.name, concurrency, phase.duration, set.fixtures, runner.Settings, artifacts, ids)
					result.Phases = append(result.Phases, measured)
					if measured.Status != "COMPLETE" {
						result.Status, result.Reason = measured.Status, measured.Reason
						return result
					}
					if phase.name == "smoke" {
						for _, fixture := range set.fixtures {
							verified, failure := fixture.Verify(ctx)
							result.Correctness = append(result.Correctness, verified)
							if failure != nil || verified.Status != "PASS" {
								result.Status, result.Reason = "FAIL", "smoke_correctness_failed_before_measurement"
								if failure != nil && failure.Unknown {
									result.Status = "UNRESOLVED"
								}
								return result
							}
						}
					}
				}
			}
		}
		if scenario.Name == "checkpoint_reopen" {
			if failure := lifecycle(ctx, scenario.Name, active[0], management, "", artifacts); failure != nil {
				result.Status, result.Reason = "FAIL", failure.Code
				if failure.Unknown {
					result.Status = "UNRESOLVED"
				}
				return result
			}
		}
	}
	for _, fixture := range active {
		verified, failure := fixture.Verify(ctx)
		result.Correctness = append(result.Correctness, verified)
		if failure != nil || verified.Status != "PASS" {
			result.Status, result.Reason = "FAIL", "durable_rows_hash_account_sum_or_ledger_not_proven"
			if failure != nil && failure.Unknown {
				result.Status = "UNRESOLVED"
			}
			break
		}
	}
	return result
}
