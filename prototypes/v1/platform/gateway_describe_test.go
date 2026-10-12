package platform

import (
	"context"
	"testing"

	controller "peri-loom/controller"
)

func TestGatewayDescribeMalformedNativeReplyIsUnknownAndNeverReplayed(t *testing.T) {
	fixture := newGatewayFixture(t)
	fixture.upstream.hook = func(_ context.Context, _ controller.Worker, command map[string]any) (any, error) {
		if command["command"] == "describe" {
			return map[string]any{"params": []any{}, "cols": []any{}, "is_explain": false}, nil
		}
		return nil, nil
	}
	session := gatewayOpen(t, fixture, fixture.tokenA, fixture.first)
	request := map[string]any{"session": session, "sql": "INSERT INTO proof VALUES(91)"}
	first := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/describe", request)
	if first.Code != 502 || gatewayCode(t, first) != "outcome_unknown" || fixture.upstream.count("describe") != 1 {
		t.Fatalf("malformed native describe reply accepted: %d %s", first.Code, first.Body.String())
	}
	second := gatewayHTTP(t, fixture.gateway, fixture.tokenA, "/v1/sql/describe", request)
	if second.Code == 200 || fixture.upstream.count("describe") != 1 {
		t.Fatalf("uncertain describe replayed: %d %s", second.Code, second.Body.String())
	}
}
