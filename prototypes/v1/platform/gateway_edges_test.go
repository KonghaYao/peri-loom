package platform

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	controller "peri-loom/controller"
)

func TestGatewayTokenRootChangeAndMissingIncarnationFailClosed(test *testing.T) {
	fixture := newGatewayFixture(test)
	principal, err := fixture.identity.Authenticate(fixture.tokenA)
	if err != nil {
		test.Fatal(err)
	}
	token, err := fixture.identity.IssueToken(principal, fixture.first, "writer", time.Now().Add(time.Minute).UnixMilli())
	if err != nil {
		test.Fatal(err)
	}
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		placement.Grant.Binding.RootIncarnation = "recreated-root"
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, token, "/v1/sql/open", map[string]any{"database": fixture.first})
	if gatewayCode(test, response) != "token_binding_mismatch" || fixture.upstream.count("open_session") != 0 {
		test.Fatal("old root token accepted before management state update")
	}
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	for _, claim := range []Principal{{Database: &fixture.first}, {Database: &fixture.first, RootIncarnation: "recreated-root"}, {Database: &fixture.first, BranchIncarnation: 1}} {
		if principalBindingValid(claim, controller.Binding{Database: fixture.first, RootIncarnation: "recreated-root", BranchIncarnation: 1}) {
			test.Fatal("missing DB token incarnation accepted")
		}
	}
}

func TestGatewayMalformedUnknownAndDuplicateFieldsNeverDispatch(test *testing.T) {
	fixture := newGatewayFixture(test)
	database, _ := json.Marshal(fixture.first)
	for _, body := range []string{`{"database":` + string(database) + `,"database":` + string(database) + `}`, `{"database":` + string(database) + `,"upstream_session":"forged"}`, `{"database":` + string(database) + `} {}`, `{"database":null}`, `{"database":{"namespace":{"tenant":"tenant-a","database":"db-a"},"branch":"main","extra":true}}`} {
		request := httptest.NewRequest("POST", "/v1/sql/open", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
		response := httptest.NewRecorder()
		fixture.gateway.ServeHTTP(response, request)
		if response.Code == 200 {
			test.Fatal("malformed public request accepted")
		}
	}
	if fixture.upstream.count("open_session") != 0 {
		test.Fatal("malformed request caused native open")
	}
}

func TestGatewayNativeReadonlyMismatchAndUnknownOpenRetainSessionQuota(test *testing.T) {
	fixture := newGatewayFixture(test)
	if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
		placement := metadata.Placements[fixture.first.Key()]
		placement.Role = "read_only"
		placement.Grant.Binding.Role = "read_only"
		metadata.Placements[fixture.first.Key()] = placement
		return nil
	}); err != nil {
		test.Fatal(err)
	}
	response := gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/open", map[string]any{"database": fixture.first})
	if gatewayCode(test, response) != "outcome_unknown" || fixture.upstream.count("open_session") != 1 {
		test.Fatal("native Writer flag accepted for ReadOnly placement")
	}
	projects := fixture.gateway.Metrics()["projects"].(map[string]any)
	admitted := projects["project-a"].(map[string]any)
	if admitted["sessions"] != 1 || admitted["unknown_held"] != 1 {
		test.Fatal("unknown native open slot discarded")
	}
}

func TestGatewayShutdownPreDispatchFailureCanBeRetriedWithoutFakeTerminalProof(test *testing.T) {
	fixture := newGatewayFixture(test)
	_ = gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
	fixture.upstream.setHook(func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "close_session" {
			return nil, &SQLRPCError{}
		}
		return nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if fixture.gateway.Close(ctx) == nil || fixture.gateway.Metrics()["unknown_held_permits"] != 0 {
		test.Fatal("pre-dispatch failure misclassified as completed or unknown native mutation")
	}
	fixture.upstream.setHook(nil)
	if err := fixture.gateway.Close(ctx); err != nil {
		test.Fatal("safe close retry blocked after no dispatch")
	}
}
