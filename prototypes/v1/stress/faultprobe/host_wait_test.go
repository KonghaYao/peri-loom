package faultprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

type waitObservation struct {
	identity ProcessIdentity
	failure  error
}

type waitingHost struct {
	original     ProcessIdentity
	observations []waitObservation
	inspections  int
	signals      int
	afterSignal  int
	onInspect    func(*waitingHost, context.Context)
	onSignal     func()
	beforeSignal []waitObservation
}

func (host *waitingHost) Inspect(ctx context.Context, pid int) (ProcessIdentity, error) {
	host.inspections++
	if host.onInspect != nil {
		host.onInspect(host, ctx)
	}
	if host.signals == 0 {
		if host.inspections <= len(host.beforeSignal) {
			observation := host.beforeSignal[host.inspections-1]
			return observation.identity, observation.failure
		}
		return host.original, nil
	}
	index := host.afterSignal
	host.afterSignal++
	if index >= len(host.observations) {
		index = len(host.observations) - 1
	}
	return host.observations[index].identity, host.observations[index].failure
}

func (host *waitingHost) SignalKill(pid int) error {
	host.signals++
	if host.onSignal != nil {
		host.onSignal()
	}
	return nil
}

func assertCacheRetained(test *testing.T, config Config, topology Topology, host *waitingHost) {
	test.Helper()
	for _, worker := range []string{"worker-a", "worker-b"} {
		payload, failure := os.ReadFile(filepath.Join(workerCache(config, worker), "segment"))
		if failure != nil || string(payload) != "synthetic cache" {
			test.Fatalf("cache deletion or modification for %s", worker)
		}
	}
	if host.signals > 1 {
		test.Fatal("signal was repeated")
	}
}

func recordedTypes(test *testing.T, config Config) ([]string, []map[string]any) {
	test.Helper()
	payload, failure := os.ReadFile(filepath.Join(config.RunDir, "unit-events.jsonl"))
	if failure != nil {
		test.Fatal(failure)
	}
	var kinds []string
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(payload)), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil {
			test.Fatal("invalid evidence record")
		}
		kinds = append(kinds, record["type"].(string))
		records = append(records, record)
		if _, found := record["argv"]; found {
			test.Fatal("raw argv in evidence")
		}
	}
	return kinds, records
}

func TestHostWaitZombieThenGoneEvidence(test *testing.T) {
	config, _, topology, fixtureHost, evidence := unitFixture(test)
	original := fixtureHost.identities[topology.Processes["worker-a"].PID]
	zombie := original
	zombie.Args = nil
	host := &waitingHost{original: original, observations: []waitObservation{
		{identity: zombie, failure: ErrProcessZombie}, {failure: ErrProcessGone},
	}}
	if failure := killOwned(context.Background(), config, topology, "worker-a", original, host, evidence); failure != nil {
		test.Fatal(failure)
	}
	kinds, records := recordedTypes(test, config)
	if !reflect.DeepEqual(kinds, []string{"fault_intent", "fault_signal_sent", "fault_process_wait", "fault_process_gone", "fault_completed"}) {
		test.Fatalf("incorrect signal/wait/gone/deletion evidence order: %v", kinds)
	}
	if records[2]["observation"] != "zombie" || records[3]["proof"] != "host_esrch" || host.signals != 1 {
		test.Fatal("zombie was not distinguished from proven absence")
	}
}

func TestHostWaitIncompleteArgvThenGone(test *testing.T) {
	config, _, topology, fixtureHost, evidence := unitFixture(test)
	original := fixtureHost.identities[topology.Processes["worker-a"].PID]
	partial := original
	partial.Args = append([]string{}, original.Args[:2]...)
	host := &waitingHost{original: original, observations: []waitObservation{{identity: partial}, {failure: ErrProcessGone}}}
	if failure := killOwned(context.Background(), config, topology, "worker-a", original, host, evidence); failure != nil {
		test.Fatal(failure)
	}
	_, records := recordedTypes(test, config)
	if records[2]["observation"] != "identity_incomplete" || host.signals != 1 {
		test.Fatal("incomplete argv was mistaken for complete identity or absence")
	}
}

func TestHostWaitVisibleMismatchAfterTransientNeverDeletes(test *testing.T) {
	for _, field := range []string{"pid", "uid", "start", "argv", "extra_argv", "partial_argv", "zombie_uid", "zombie_start", "gone_with_changed_start"} {
		test.Run(field, func(test *testing.T) {
			config, _, topology, fixtureHost, evidence := unitFixture(test)
			original := fixtureHost.identities[topology.Processes["worker-a"].PID]
			changed := original
			changed.Args = append([]string{}, original.Args...)
			var observedError error
			switch field {
			case "pid":
				changed.PID++
			case "uid", "zombie_uid":
				changed.UID++
			case "start", "zombie_start", "gone_with_changed_start":
				changed.Start = "replacement"
			case "argv":
				changed.Args[1] = "authority"
			case "extra_argv":
				changed.Args = append(changed.Args, "extra")
			case "partial_argv":
				changed.Args = []string{"unexpected-binary"}
				observedError = ErrIdentityIncomplete
			}
			if strings.HasPrefix(field, "zombie_") {
				changed.Args = nil
				observedError = ErrProcessZombie
			}
			if field == "gone_with_changed_start" {
				observedError = ErrProcessGone
			}
			host := &waitingHost{original: original, observations: []waitObservation{
				{failure: ErrInspectionUnavailable}, {identity: changed, failure: observedError}, {failure: ErrProcessGone},
			}}
			if failure := killOwned(context.Background(), config, topology, "worker-a", original, host, evidence); failure == nil {
				test.Fatal("observed reuse or mismatch allowed deletion")
			}
			assertCacheRetained(test, config, topology, host)
			if host.afterSignal != 2 || host.signals != 1 {
				test.Fatal("did not stop immediately at observed mismatch")
			}
		})
	}
}

func TestHostWaitPersistentUnavailableAndZombieTimeout(test *testing.T) {
	for _, mode := range []string{"unavailable", "incomplete_argv", "zombie"} {
		test.Run(mode, func(test *testing.T) {
			config, _, topology, fixtureHost, evidence := unitFixture(test)
			original := fixtureHost.identities[topology.Processes["worker-a"].PID]
			observation := waitObservation{failure: ErrInspectionUnavailable}
			if mode == "zombie" {
				observation = waitObservation{identity: original, failure: ErrProcessZombie}
			} else if mode == "incomplete_argv" {
				partial := original
				partial.Args = nil
				observation = waitObservation{identity: partial, failure: ErrIdentityIncomplete}
			}
			host := &waitingHost{original: original, signals: 1, observations: []waitObservation{observation}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if failure := waitOwnedExit(ctx, "worker-a", original, host, evidence); failure == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				test.Fatal("inspection uncertainty did not stop at bounded deadline")
			}
			assertCacheRetained(test, config, topology, host)
			kinds, records := recordedTypes(test, config)
			if kinds[len(kinds)-1] != "fault_exit_uncertain" {
				test.Fatal("deadline missing uncertain evidence")
			}
			if mode == "incomplete_argv" && records[0]["observation"] != "identity_incomplete" {
				test.Fatal("incomplete argv evidence lost its distinction from ps error")
			}
		})
	}
}

func TestHostWaitParentDeadlineStopsWholeFaultWithoutDeletion(test *testing.T) {
	config, _, topology, fixtureHost, evidence := unitFixture(test)
	original := fixtureHost.identities[topology.Processes["worker-a"].PID]
	host := &waitingHost{original: original, observations: []waitObservation{{failure: ErrInspectionUnavailable}}}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if failure := killOwned(ctx, config, topology, "worker-a", original, host, evidence); failure == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		test.Fatal("persistent inspection error advanced past parent deadline")
	}
	assertCacheRetained(test, config, topology, host)
	if host.signals != 1 {
		test.Fatal("expected exactly one mock signal before deadline")
	}
	_, records := recordedTypes(test, config)
	for _, record := range records {
		if record["type"] == "fault_completed" || record["type"] == "fault_process_gone" {
			test.Fatal("unproven disappearance received completion evidence")
		}
	}
}

func TestHostWaitCancellationNeverDeletesEvenWithGone(test *testing.T) {
	for _, gone := range []bool{false, true} {
		test.Run(fmt.Sprint(gone), func(test *testing.T) {
			config, _, topology, fixtureHost, evidence := unitFixture(test)
			original := fixtureHost.identities[topology.Processes["worker-a"].PID]
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			observation := waitObservation{failure: ErrInspectionUnavailable}
			if gone {
				observation.failure = ErrProcessGone
			}
			host := &waitingHost{original: original, observations: []waitObservation{observation},
				onInspect: func(host *waitingHost, ctx context.Context) {
					if host.signals != 0 {
						deadline, bounded := ctx.Deadline()
						if !bounded || time.Until(deadline) > 5*time.Second {
							test.Fatal("wait exceeds five-second bound")
						}
						cancel()
					}
				}}
			if failure := killOwned(ctx, config, topology, "worker-a", original, host, evidence); failure == nil {
				test.Fatal("canceled wait allowed deletion")
			}
			assertCacheRetained(test, config, topology, host)
			if host.signals != 1 {
				test.Fatal("incorrect fake signal count")
			}
		})
	}
}

func TestHostWaitBothPreSignalChecksRemainStrict(test *testing.T) {
	for _, boundary := range []int{1, 2} {
		test.Run(fmt.Sprint(boundary), func(test *testing.T) {
			config, _, topology, fixtureHost, evidence := unitFixture(test)
			original := fixtureHost.identities[topology.Processes["worker-a"].PID]
			before := []waitObservation{{identity: original}, {identity: original}}
			before[boundary-1].failure = ErrIdentityIncomplete
			host := &waitingHost{original: original, beforeSignal: before}
			if failure := killOwned(context.Background(), config, topology, "worker-a", original, host, evidence); failure == nil || host.signals != 0 {
				test.Fatal("pre-signal incomplete identity was retried or signaled")
			}
			assertCacheRetained(test, config, topology, host)
		})
	}
}

func TestHostWaitEvidenceFailureAfterSignalNeverDeletes(test *testing.T) {
	config, _, topology, fixtureHost, evidence := unitFixture(test)
	original := fixtureHost.identities[topology.Processes["worker-a"].PID]
	host := &waitingHost{original: original, observations: []waitObservation{{failure: ErrProcessGone}},
		onSignal: func() { _ = evidence.file.Close() }}
	if failure := killOwned(context.Background(), config, topology, "worker-a", original, host, evidence); failure == nil {
		test.Fatal("missing signal evidence allowed deletion")
	}
	assertCacheRetained(test, config, topology, host)
	if host.signals != 1 || host.afterSignal != 0 {
		test.Fatal("signal evidence failure did not stop workflow")
	}
}

func TestHostWaitOnlyESRCHProvesAbsence(test *testing.T) {
	for _, name := range []string{"ps_error_exists", "ps_error_eperm", "ps_error_esrch", "empty_exists", "empty_esrch", "zombie_exists", "zombie_esrch", "wrong_pid", "canceled"} {
		test.Run(name, func(test *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			observations, probes := 0, 0
			identity, failure := inspectProcess(ctx, 42003, func(ctx context.Context, pid int) ([]byte, error) {
				observations++
				if name == "canceled" {
					cancel()
				}
				if strings.HasPrefix(name, "ps_error") {
					return nil, errors.New("synthetic ps unavailable")
				}
				if strings.HasPrefix(name, "zombie") {
					return []byte(fmt.Sprintf("42003 %d Z Tue Oct 6 01:02:03 2026 dead", os.Getuid())), nil
				}
				if name == "wrong_pid" {
					return []byte("42004 501 S Tue Oct 6 01:02:03 2026 alive"), nil
				}
				return nil, nil
			}, func(pid int) error {
				probes++
				if strings.HasSuffix(name, "esrch") {
					return syscall.ESRCH
				}
				if strings.HasSuffix(name, "eperm") {
					return syscall.EPERM
				}
				return nil
			})
			if errors.Is(failure, ErrProcessGone) != strings.HasSuffix(name, "esrch") {
				test.Fatal("absence classification not tied to actual ESRCH probe")
			}
			if name == "zombie_exists" && (!errors.Is(failure, ErrProcessZombie) || identity.Start == "") {
				test.Fatal("zombie presence metadata lost")
			}
			if name == "zombie_esrch" && identity.PID != 42003 {
				test.Fatal("observed metadata lost before wait can check reuse")
			}
			if (name == "wrong_pid" || name == "canceled") && probes != 0 || observations != 1 {
				test.Fatal("mismatch/cancel was hidden by later probe")
			}
		})
	}
}

func TestHostWaitInvalidPIDNeverInspectedOrProbed(test *testing.T) {
	for _, pid := range []int{-1, 0, 1} {
		_, failure := inspectProcess(context.Background(), pid, func(context.Context, int) ([]byte, error) {
			test.Fatal("invalid PID inspected")
			return nil, nil
		}, func(int) error {
			test.Fatal("invalid PID probed")
			return nil
		})
		if !errors.Is(failure, ErrIdentityMismatch) {
			test.Fatal("unsafe PID accepted")
		}
	}
}

func TestHostWaitTransientThenOriginalThenGone(test *testing.T) {
	config, _, topology, fixtureHost, evidence := unitFixture(test)
	original := fixtureHost.identities[topology.Processes["worker-a"].PID]
	host := &waitingHost{original: original, observations: []waitObservation{
		{failure: errors.New("incomplete process identity")},
		{identity: original},
		{failure: ErrProcessGone},
	}}
	if failure := killOwned(context.Background(), config, topology, "worker-a", original, host, evidence); failure != nil {
		test.Fatalf("bounded transient inspection should reach proven absence: %v", failure)
	}
	if host.signals != 1 || host.inspections != 5 {
		test.Fatal("signal or observation count differs")
	}
	if _, failure := os.Stat(workerCache(config, "worker-a")); !os.IsNotExist(failure) {
		test.Fatal("proven-gone owner cache remained")
	}
	if _, failure := os.ReadFile(filepath.Join(workerCache(config, "worker-b"), "segment")); failure != nil {
		test.Fatal("non-owner cache changed")
	}
}

func TestHostWaitZombieIsNotProcessGone(test *testing.T) {
	encoded := fmt.Sprintf("42003 %d Z Tue Oct 6 01:02:03 2026 dead", os.Getuid())
	identity, failure := parseIdentity(encoded, 42003)
	if failure == nil || errors.Is(failure, ErrProcessGone) {
		test.Fatal("zombie must remain observable, not proof of PID absence")
	}
	if identity.PID != 42003 || identity.UID != os.Getuid() || identity.Start == "" {
		test.Fatal("zombie metadata must remain available for reuse checks")
	}
}
