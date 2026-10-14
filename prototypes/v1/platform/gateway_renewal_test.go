package platform

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	controller "peri-loom/controller"
)

type gatewayRenewalSnapshots struct {
	controller.MetadataStore
	unavailable atomic.Bool
}

func (store *gatewayRenewalSnapshots) Snapshot() (controller.Metadata, error) {
	if store.unavailable.Load() {
		return controller.Metadata{}, errors.New("temporary metadata unavailable")
	}
	return store.MetadataStore.Snapshot()
}

func gatewayRenewalCredential(test *testing.T, fixture *gatewayFixture, databaseToken bool) string {
	test.Helper()
	if !databaseToken {
		return fixture.tokenA
	}
	principal, err := fixture.identity.Authenticate(fixture.tokenA)
	if err != nil {
		test.Fatal(err)
	}
	credential, err := fixture.identity.IssueToken(principal, fixture.first, "writer", time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		test.Fatal(err)
	}
	return credential
}

func TestGatewayStableRenewalPhasesPreserveSessions(test *testing.T) {
	for _, phase := range []string{"renew_intent", "renew_pending", "renew_worker_pending"} {
		for _, databaseToken := range []bool{false, true} {
			credentialKind := "api-key"
			if databaseToken {
				credentialKind = "database-token"
			}
			test.Run(phase+"/"+credentialKind, func(test *testing.T) {
				fixture := newGatewayFixture(test)
				credential := gatewayRenewalCredential(test, fixture, databaseToken)
				session := gatewayOpen(test, fixture, credential, fixture.first)
				if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
					placement := metadata.Placements[fixture.first.Key()]
					placement.Phase = phase
					placement.Grant.Binding.RenewalSequence++
					metadata.Placements[fixture.first.Key()] = placement
					return nil
				}); err != nil {
					test.Fatal(err)
				}
				for _, operation := range []string{"query", "execute"} {
					response := gatewayHTTP(test, fixture.gateway, credential, "/v1/sql/"+operation, map[string]any{"session": session, "sql": "SELECT 1", "request_id": "renewal-" + operation})
					if response.Code != 200 || fixture.upstream.count(operation) != 1 {
						test.Fatalf("same-binding renewal blocked or retried SQL: %s", response.Body.String())
					}
				}
				fresh := gatewayOpen(test, fixture, credential, fixture.first)
				if fresh == session || fixture.upstream.count("open_session") != 2 {
					test.Fatal("renewal failed independent fresh-session admission")
				}
				if fixture.gateway.Metrics()["active_permits"] != 0 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
					test.Fatal("stable renewal poisoned quota")
				}
			})
		}
	}
}

func TestGatewayTransientPlacementFailureAllowsNewBusinessOperation(test *testing.T) {
	for _, failure := range []string{"snapshot", "unhealthy-worker", "draining-worker", "missing-worker", "missing-grant", "inconsistent-binding", "grant_intent", "grant_pending", "acquire_pending", "migrate_drain", "suspend_drain", "revoke_pending", "suspended", "desired-suspended"} {
		for _, databaseToken := range []bool{false, true} {
			credentialKind := "api-key"
			if databaseToken {
				credentialKind = "database-token"
			}
			test.Run(failure+"/"+credentialKind, func(test *testing.T) {
				fixture := newGatewayFixture(test)
				credential := gatewayRenewalCredential(test, fixture, databaseToken)
				session := gatewayOpen(test, fixture, credential, fixture.first)
				initial, err := fixture.placements.Snapshot()
				if err != nil {
					test.Fatal(err)
				}
				store := &gatewayRenewalSnapshots{MetadataStore: fixture.placements}
				fixture.gateway.config.Placements = store
				if failure == "snapshot" {
					store.unavailable.Store(true)
				} else if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
					placement := metadata.Placements[fixture.first.Key()]
					worker := metadata.Workers[placement.DesiredWorker]
					switch failure {
					case "unhealthy-worker":
						worker.Healthy = false
					case "draining-worker":
						worker.Draining = true
					case "missing-grant":
						placement.Grant = nil
					case "inconsistent-binding":
						placement.Grant.Binding.Epoch++
					case "desired-suspended":
						placement.DesiredState = "suspended"
					default:
						if failure != "missing-worker" {
							placement.Phase = failure
						}
					}
					metadata.Workers[worker.ID] = worker
					if failure == "missing-worker" {
						delete(metadata.Workers, worker.ID)
					}
					metadata.Placements[fixture.first.Key()] = placement
					return nil
				}); err != nil {
					test.Fatal(err)
				}
				response := gatewayHTTP(test, fixture.gateway, credential, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1", "request_id": "blocked-business-operation"})
				if response.Code != 409 || gatewayCode(test, response) != "placement_unavailable" || fixture.upstream.count("query") != 0 {
					test.Fatalf("transient placement failure was treated as migration or dispatched: %s", response.Body.String())
				}
				store.unavailable.Store(false)
				if err := fixture.placements.Update(func(metadata *controller.Metadata) error { *metadata = initial; return nil }); err != nil {
					test.Fatal(err)
				}
				response = gatewayHTTP(test, fixture.gateway, credential, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 2", "request_id": "new-business-operation"})
				if response.Code != 200 || fixture.upstream.count("query") != 1 || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
					test.Fatal("recovered placement could not serve explicit new operation or leaked quota")
				}
			})
		}
	}
}

func TestGatewayProvenBindingChangeRemainsTerminal(test *testing.T) {
	for _, change := range []string{"epoch", "worker", "root", "branch", "authority-term", "lease", "grant", "address", "role"} {
		test.Run(change, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			initial, err := fixture.placements.Snapshot()
			if err != nil {
				test.Fatal(err)
			}
			if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
				placement := metadata.Placements[fixture.first.Key()]
				placement.Phase = "renew_worker_pending"
				worker := metadata.Workers[placement.DesiredWorker]
				switch change {
				case "epoch":
					placement.Epoch++
					placement.Grant.Binding.Epoch = placement.Epoch
				case "worker":
					worker.ID = "worker-replacement"
					placement.DesiredWorker = worker.ID
					placement.Grant.Binding.WorkerID = worker.ID
				case "root":
					placement.Grant.Binding.RootIncarnation = "recreated-root"
				case "branch":
					placement.Grant.Binding.BranchIncarnation++
				case "authority-term":
					placement.Grant.Binding.AuthorityTerm++
				case "lease":
					placement.LeaseID = "replacement-lease"
					placement.Grant.Binding.LeaseID = placement.LeaseID
				case "grant":
					placement.Grant.Binding.GrantID = "replacement-grant"
				case "address":
					worker.Address = "http://127.0.0.1:5678"
				case "role":
					placement.Role = "read_only"
					placement.Grant.Binding.Role = placement.Role
				}
				metadata.Workers[worker.ID] = worker
				metadata.Placements[fixture.first.Key()] = placement
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
			if response.Code != 409 || gatewayCode(test, response) != "session_moved" || fixture.upstream.count("query") != 0 {
				test.Fatal("proven identity change did not fence old handle")
			}
			if err := fixture.placements.Update(func(metadata *controller.Metadata) error { *metadata = initial; return nil }); err != nil {
				test.Fatal(err)
			}
			response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 2"})
			if gatewayCode(test, response) != "session_moved" || fixture.upstream.count("query") != 0 {
				test.Fatal("proven-moved session revived after placement restoration")
			}
		})
	}
}

func TestGatewayRenewalKeepsDatabaseTokenIncarnationStrict(test *testing.T) {
	for _, change := range []string{"root", "branch"} {
		test.Run(change, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			credential := gatewayRenewalCredential(test, fixture, true)
			session := gatewayOpen(test, fixture, credential, fixture.first)
			if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
				placement := metadata.Placements[fixture.first.Key()]
				placement.Phase = "renew_pending"
				placement.Grant.Binding.RenewalSequence++
				if change == "root" {
					placement.Grant.Binding.RootIncarnation = "recreated-root"
				} else {
					placement.Grant.Binding.BranchIncarnation++
				}
				metadata.Placements[fixture.first.Key()] = placement
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			response := gatewayHTTP(test, fixture.gateway, credential, "/v1/sql/query", map[string]any{"session": session, "sql": "SELECT 1"})
			if response.Code != 403 || gatewayCode(test, response) != "token_binding_mismatch" || fixture.upstream.count("query") != 0 {
				test.Fatal("renewal relaxed current database-token identity scope")
			}
		})
	}
}
