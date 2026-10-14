package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTransportExact64BitResultNumbers(test *testing.T) {
	const result = `{"max_i64":9223372036854775807,"max_u64":18446744073709551615,"min_i64":-9223372036854775808,"negative_bigint":-9007199254740993,"nested":{"count":9007199254740993},"rows":[[9223372036854775807,-9007199254740993]]}`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope struct {
			RequestID string `json:"request_id"`
		}
		if json.NewDecoder(request.Body).Decode(&envelope) != nil {
			test.Error("invalid RPC request")
			return
		}
		precondition := ""
		if request.URL.Path == "/" {
			precondition = `,"precondition_failed":false`
		} else if request.URL.Path != "/control" {
			test.Error("invalid RPC path")
			return
		}
		_, _ = fmt.Fprintf(writer, `{"version":1,"request_id":%q,"ok":true,"result":%s,"error":null%s}`, envelope.RequestID, result, precondition)
	}))
	defer server.Close()
	transport := transportInstance(test, server.URL)
	for _, endpoint := range []string{"authority", "worker"} {
		for _, destination := range []string{"interface", "map", "typed"} {
			test.Run(endpoint+"/"+destination, func(test *testing.T) {
				var generic any
				var fields map[string]any
				var typed struct {
					MaxI64         int64  `json:"max_i64"`
					MaxU64         uint64 `json:"max_u64"`
					MinI64         int64  `json:"min_i64"`
					NegativeBigint int64  `json:"negative_bigint"`
					Nested         struct {
						Count uint64 `json:"count"`
					} `json:"nested"`
					Rows [][]json.Number `json:"rows"`
				}
				var output any = &generic
				switch destination {
				case "map":
					output = &fields
				case "typed":
					output = &typed
				}
				var err error
				if endpoint == "authority" {
					err = transport.Authority(context.Background(), "exact-counters", map[string]any{"command": "telemetry"}, output)
				} else {
					err = transport.Worker(context.Background(), Worker{ID: "worker-a", Address: server.URL}, "exact-counters", map[string]any{"command": "get_worker_state"}, output)
				}
				if err != nil {
					test.Fatal(err)
				}
				encoded, err := json.Marshal(output)
				if err != nil || string(encoded) != result {
					test.Fatalf("64-bit RPC literals changed: %s", encoded)
				}
				if destination == "typed" {
					if typed.MaxI64 != 9223372036854775807 || typed.MaxU64 != 18446744073709551615 || typed.MinI64 != -9223372036854775808 || typed.NegativeBigint != -9007199254740993 || typed.Nested.Count != 9007199254740993 {
						test.Fatal("typed RPC integer destination changed")
					}
					return
				}
				if destination == "interface" {
					fields = generic.(map[string]any)
				}
				for name, literal := range map[string]string{"max_i64": "9223372036854775807", "max_u64": "18446744073709551615", "min_i64": "-9223372036854775808", "negative_bigint": "-9007199254740993"} {
					number, valid := fields[name].(json.Number)
					if !valid || number.String() != literal {
						test.Fatal("generic RPC integer not preserved as json.Number")
					}
				}
			})
		}
	}
}
