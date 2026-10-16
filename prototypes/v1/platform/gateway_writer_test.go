package platform

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func TestGatewayWriterGeneralFullOwnerAndCloseProgress(test *testing.T) {
	for _, action := range []string{"query", "execute", "close"} {
		test.Run(action, func(test *testing.T) {
			fixture, _ := newWriterGatewayFixture(test)
			owner := writerGatewaySession(test, fixture)
			general := []string{gatewayOpen(test, fixture, fixture.tokenA, fixture.first), gatewayOpen(test, fixture, fixture.tokenA, fixture.first), gatewayOpen(test, fixture, fixture.tokenA, fixture.first)}
			entered, release := make(chan struct{}, 3), make(chan struct{})
			fixture.upstream.setHook(func(ctx context.Context, worker controller.Worker, command map[string]any) (any, error) {
				if command["sql"] == "blocked-general" {
					entered <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return nil, &SQLRPCError{Ambiguous: true}
					}
				}
				return nil, nil
			})
			var calls sync.WaitGroup
			for _, session := range general {
				calls.Add(1)
				go func(session string) {
					defer calls.Done()
					response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "blocked-general"})
					if response.Code != 200 {
						test.Errorf("general: %s", response.Body.String())
					}
				}(session)
			}
			defer func() { close(release); calls.Wait() }()
			for count := 0; count < 3; count++ {
				select {
				case <-entered:
				case <-time.After(time.Second):
					test.Fatal("general did not reach worker")
				}
			}
			body := map[string]any{"session": owner}
			if action != "close" {
				body["sql"] = "arbitrary-legal-continuation"
			}
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/"+action, body)
			if response.Code != 200 {
				test.Fatalf("reserved owner/close: %s", response.Body.String())
			}
			if action != "close" {
				for _, forbidden := range []string{"owner", "session_state", "sequence", "binding"} {
					var envelope map[string]json.RawMessage
					_ = json.Unmarshal(response.Body.Bytes(), &envelope)
					if _, found := envelope[forbidden]; found {
						test.Fatal("private writer state leaked")
					}
				}
			}
		})
	}
}

func TestGatewayWriterDuplicateSessionBeforeReserve(test *testing.T) {
	fixture, _ := newWriterGatewayFixture(test)
	owner := writerGatewaySession(test, fixture)
	fixture.gateway.sessions[owner].mu.Lock()
	defer fixture.gateway.sessions[owner].mu.Unlock()
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": owner, "sql": "anything"})
	if gatewayCode(test, response) != "session_busy" {
		test.Fatal("duplicate was admitted")
	}
	_, active, _ := fixture.gateway.admission.status()
	if active != 0 {
		test.Fatal("duplicate consumed reserved permit")
	}
}

func TestGatewayWriterConstraintKeepsOwnerThenRollback(test *testing.T) {
	fixture, _ := newWriterGatewayFixture(test)
	owner := writerGatewaySession(test, fixture)
	fixture.upstream.setHook(func(ctx context.Context, worker controller.Worker, command map[string]any) (any, error) {
		if command["sql"] == "constraint" {
			return nil, testRPCError(true, false, SQLRPCError{})
		}
		return nil, nil
	})
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": owner, "sql": "constraint"})
	if gatewayCode(test, response) != "sql_error" || !fixture.gateway.sessions[owner].writerOwner || fixture.gateway.sessions[owner].poisoned {
		test.Fatal("constraint discarded settled owner")
	}
	fixture.gateway.admission.mu.Lock()
	fixture.gateway.admission.projects["project-a"].last = time.Now().Add(-time.Second)
	fixture.gateway.admission.mu.Unlock()
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": owner, "sql": "ROLLBACK"})
	if response.Code != 200 || fixture.gateway.sessions[owner].writerOwner {
		test.Fatal("explicit rollback did not settle owner")
	}
	_, active, unknown := fixture.gateway.admission.status()
	if active != 0 || unknown != 0 {
		test.Fatal("known terminal held unknown quota")
	}
}

func TestGatewayWriterLostAckIgnoresFalseHintAndKeepsPermit(test *testing.T) {
	fixture, upstream := newWriterGatewayFixture(test)
	owner := writerGatewaySession(test, fixture)
	upstream.hook = func(command map[string]any, state *SessionWriterState, err error) (*SessionWriterState, error) {
		state.Owner = false
		return state, &SQLRPCError{Ambiguous: true}
	}
	before := fixture.upstream.count("execute")
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/execute", map[string]any{"session": owner, "sql": "COMMIT"})
	if gatewayCode(test, response) != "outcome_unknown" || !fixture.gateway.sessions[owner].writerOwner {
		test.Fatal("unknown false hint recycled owner")
	}
	response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": owner})
	if gatewayCode(test, response) != "outcome_unknown" || fixture.upstream.count("close_session") != 0 || fixture.upstream.count("execute") != before+1 {
		test.Fatal("unknown was retried or auto-closed")
	}
	_, active, unknown := fixture.gateway.admission.status()
	if active != 1 || unknown != 1 {
		test.Fatal("unknown owner permit released")
	}
}

func TestGatewayWriterBadTrustedHintFailsClosed(test *testing.T) {
	for _, mutate := range []func(*SessionWriterState){
		func(state *SessionWriterState) { state.Sequence = 0 },
		func(state *SessionWriterState) { state.Session = "forged" },
		func(state *SessionWriterState) { state.Binding.WorkerID = "other" },
		func(state *SessionWriterState) { state.Binding.Epoch++ },
		func(state *SessionWriterState) { state.Binding.RootIncarnation = "other" },
		func(state *SessionWriterState) { state.Binding.BranchIncarnation++ },
		func(state *SessionWriterState) { state.Binding.LeaseID = "other" },
		func(state *SessionWriterState) { state.Binding.GrantID = "other" },
		func(state *SessionWriterState) { state.Binding.AuthorityTerm++ },
		func(state *SessionWriterState) { state.Binding.Role = "read_only" },
	} {
		fixture, upstream := newWriterGatewayFixture(test)
		owner := writerGatewaySession(test, fixture)
		upstream.hook = func(command map[string]any, state *SessionWriterState, err error) (*SessionWriterState, error) {
			mutate(state)
			return state, err
		}
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": owner, "sql": "SELECT 1"})
		if gatewayCode(test, response) != "outcome_unknown" || !fixture.gateway.sessions[owner].poisoned {
			test.Fatal("bad native state accepted")
		}
	}
}

func TestGatewayWriterRenewalAdvancesDuringCallWithoutFalseUnknown(test *testing.T) {
	fixture, upstream := newWriterGatewayFixture(test)
	owner := writerGatewaySession(test, fixture)
	upstream.hook = func(command map[string]any, state *SessionWriterState, err error) (*SessionWriterState, error) {
		state.Binding.RenewalSequence++
		return state, err
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": owner, "sql": "SELECT 1"})
	if response.Code != 200 || fixture.gateway.sessions[owner].poisoned {
		test.Fatal("same lease renewal during RPC became false unknown")
	}
}

func TestGatewayWriterAllPlacementIdentityChangesFenceBeforeReserve(test *testing.T) {
	for _, change := range []func(*controller.Metadata, *controller.Placement){
		func(metadata *controller.Metadata, placement *controller.Placement) {
			placement.Epoch++
			placement.Grant.Binding.Epoch++
		},
		func(metadata *controller.Metadata, placement *controller.Placement) {
			placement.Grant.Binding.RootIncarnation = "changed"
		},
		func(metadata *controller.Metadata, placement *controller.Placement) {
			placement.Grant.Binding.BranchIncarnation++
		},
		func(metadata *controller.Metadata, placement *controller.Placement) {
			worker := metadata.Workers[placement.DesiredWorker]
			worker.Address = "http://127.0.0.1:6789"
			metadata.Workers[worker.ID] = worker
		},
		func(metadata *controller.Metadata, placement *controller.Placement) {
			worker := metadata.Workers[placement.DesiredWorker]
			worker.ID = "changed-worker"
			metadata.Workers[worker.ID] = worker
			placement.DesiredWorker = worker.ID
			placement.Grant.Binding.WorkerID = worker.ID
		},
	} {
		fixture, _ := newWriterGatewayFixture(test)
		owner := writerGatewaySession(test, fixture)
		_ = fixture.placements.Update(func(metadata *controller.Metadata) error {
			placement := metadata.Placements[fixture.first.Key()]
			change(metadata, &placement)
			metadata.Placements[fixture.first.Key()] = placement
			return nil
		})
		before := fixture.upstream.count("query")
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": owner, "sql": "SELECT 1"})
		if gatewayCode(test, response) != "session_moved" || fixture.upstream.count("query") != before {
			test.Fatal("reserved owner crossed placement identity boundary")
		}
		_, active, unknown := fixture.gateway.admission.status()
		if active != 0 || unknown != 0 {
			test.Fatal("pre-dispatch migration consumed owner permit")
		}
	}
}

func TestGatewayWriterRenewalAndMigrationStillFenced(test *testing.T) {
	for _, phase := range []string{"renew_intent", "renew_pending", "renew_worker_pending"} {
		fixture, _ := newWriterGatewayFixture(test)
		owner := writerGatewaySession(test, fixture)
		_ = fixture.placements.Update(func(metadata *controller.Metadata) error {
			placement := metadata.Placements[fixture.first.Key()]
			placement.Phase = phase
			placement.Grant.Binding.RenewalSequence++
			metadata.Placements[fixture.first.Key()] = placement
			return nil
		})
		response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": owner, "sql": "SELECT 1"})
		if response.Code != 200 {
			test.Fatalf("normal renewal: %s", response.Body.String())
		}
		before := fixture.upstream.count("query")
		_ = fixture.placements.Update(func(metadata *controller.Metadata) error {
			placement := metadata.Placements[fixture.first.Key()]
			placement.Phase = "active"
			placement.Grant.Binding.BranchIncarnation++
			metadata.Placements[fixture.first.Key()] = placement
			return nil
		})
		response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": owner, "sql": "SELECT 1"})
		if gatewayCode(test, response) != "session_moved" || fixture.upstream.count("query") != before {
			test.Fatal("reserve bypassed incarnation fence")
		}
	}
}
