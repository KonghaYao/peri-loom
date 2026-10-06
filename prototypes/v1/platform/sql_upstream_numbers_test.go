package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	controller "peri-loom/controller"
)

func TestSQLUpstreamExactIntegerLiterals(test *testing.T) {
	const rows = `[[9223372036854775807,-9007199254740993]]`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope struct {
			RequestID string `json:"request_id"`
		}
		if json.NewDecoder(request.Body).Decode(&envelope) != nil {
			test.Error("invalid SQL request")
			return
		}
		_, _ = fmt.Fprintf(writer, `{"version":1,"request_id":%q,"ok":true,"result":%s,"error":null}`, envelope.RequestID, rows)
	}))
	defer server.Close()
	upstream, err := NewSQLUpstream(map[string]string{"worker-a": sqlTestToken(test)})
	if err != nil {
		test.Fatal(err)
	}
	for _, scenario := range []string{"interface", "rows"} {
		test.Run(scenario, func(test *testing.T) {
			var generic any
			var matrix [][]any
			var output any = &generic
			if scenario == "rows" {
				output = &matrix
			}
			if err := upstream.Call(context.Background(), controller.Worker{ID: "worker-a", Address: server.URL}, "exact-integers", map[string]any{"command": "query", "session": strings.Repeat("a", 64), "sql": "SELECT max_i64, negative_bigint FROM t"}, output); err != nil {
				test.Fatal(err)
			}
			encoded, err := json.Marshal(output)
			if err != nil || string(encoded) != rows {
				test.Fatalf("SQL integer literals changed: %s", encoded)
			}
			if scenario == "rows" {
				for column, literal := range []string{"9223372036854775807", "-9007199254740993"} {
					number, valid := matrix[0][column].(json.Number)
					if !valid || number.String() != literal {
						test.Fatal("SQL integer decoded through lossy float instead of json.Number")
					}
				}
			}
		})
	}
}

func TestGatewayExactIntegerJSONAndNDJSON(test *testing.T) {
	for _, format := range []string{"json", "ndjson"} {
		test.Run(format, func(test *testing.T) {
			fixture := newGatewayFixture(test)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var envelope struct {
					RequestID string         `json:"request_id"`
					Command   map[string]any `json:"command"`
				}
				if json.NewDecoder(request.Body).Decode(&envelope) != nil {
					test.Error("invalid SQL request")
					return
				}
				switch envelope.Command["command"] {
				case "open_session":
					_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"session": strings.Repeat("a", 64), "readonly": false}))
				case "query":
					_, _ = fmt.Fprintf(writer, `{"version":1,"request_id":%q,"ok":true,"result":[[9223372036854775807,-9007199254740993]],"error":null}`, envelope.RequestID)
				case "close_session":
					_ = json.NewEncoder(writer).Encode(sqlTestEnvelope(envelope.RequestID, map[string]any{"closed": true}))
				default:
					test.Error("unexpected SQL operation")
				}
			}))
			defer server.Close()
			if err := fixture.placements.Update(func(metadata *controller.Metadata) error {
				worker := metadata.Workers["worker-tenant-a"]
				worker.Address = server.URL
				metadata.Workers[worker.ID] = worker
				return nil
			}); err != nil {
				test.Fatal(err)
			}
			upstream, err := NewSQLUpstream(map[string]string{"worker-tenant-a": sqlTestToken(test)})
			if err != nil {
				test.Fatal(err)
			}
			fixture.gateway.config.Upstream = upstream
			session := gatewayOpen(test, fixture, fixture.tokenA, fixture.first)
			encoded, err := json.Marshal(map[string]any{"session": session, "sql": "SELECT max_i64, negative_bigint FROM t", "request_id": "exact-query"})
			if err != nil {
				test.Fatal(err)
			}
			request := httptest.NewRequest("POST", "/v1/sql/query", strings.NewReader(string(encoded)))
			request.Header.Set("Authorization", "Bearer "+fixture.tokenA)
			if format == "ndjson" {
				request.Header.Set("Accept", "application/x-ndjson")
			}
			response := httptest.NewRecorder()
			fixture.gateway.ServeHTTP(response, request)
			if response.Code != 200 {
				test.Fatalf("query failed: %s", response.Body.String())
			}
			if format == "json" {
				var reply struct {
					Result json.RawMessage `json:"result"`
				}
				if json.Unmarshal(response.Body.Bytes(), &reply) != nil || string(reply.Result) != `[[9223372036854775807,-9007199254740993]]` {
					test.Fatal("gateway JSON changed exact SQL integer literals")
				}
			} else if response.Body.String() != "{\"type\":\"row\",\"row\":[9223372036854775807,-9007199254740993]}\n{\"type\":\"complete\",\"request_id\":\"exact-query\",\"rows\":1}\n" {
				test.Fatal("gateway NDJSON changed exact SQL integer literals")
			}
			response = gatewayHTTP(test, fixture.gateway, fixture.tokenA, "/v1/sql/close", map[string]any{"session": session})
			if response.Code != 200 {
				test.Fatal("exact integer query left session unusable")
			}
		})
	}
}
