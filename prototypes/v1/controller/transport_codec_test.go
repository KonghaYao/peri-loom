package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTransportStrictResponseValidation(test *testing.T) {
	for _, scenario := range []string{"version", "identity", "unknown", "missing-result", "missing-error", "missing-precondition", "wrong-ok", "null-ok", "null-precondition", "success-error", "success-precondition", "failure-result", "failure-no-error", "duplicate", "nested-duplicate", "trailing", "malformed", "wrong-output", "unknown-output"} {
		test.Run(scenario, func(test *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var envelope struct {
					RequestID string `json:"request_id"`
				}
				if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
					test.Error("invalid test request")
					return
				}
				reply := transportReply(envelope.RequestID, map[string]any{"value": 42}, true)
				switch scenario {
				case "version":
					reply["version"] = 2
				case "identity":
					reply["request_id"] = "wrong"
				case "unknown":
					reply["unknown"] = "untrusted"
				case "missing-result":
					delete(reply, "result")
				case "missing-error":
					delete(reply, "error")
				case "missing-precondition":
					delete(reply, "precondition_failed")
				case "wrong-ok":
					reply["ok"] = "true"
				case "null-ok":
					reply["ok"] = nil
				case "null-precondition":
					reply["precondition_failed"] = nil
				case "success-error":
					reply["error"] = "untrusted"
				case "success-precondition":
					reply["precondition_failed"] = true
				case "failure-result":
					reply["ok"] = false
					reply["error"] = "untrusted"
				case "failure-no-error":
					reply["ok"] = false
					reply["result"] = nil
				case "wrong-output":
					reply["result"] = map[string]any{"value": "wrong"}
				case "unknown-output":
					reply["result"] = map[string]any{"value": 42, "unknown": true}
				}
				body, _ := json.Marshal(reply)
				switch scenario {
				case "duplicate":
					body = []byte(fmt.Sprintf(`{"version":1,"version":1,"request_id":%q,"ok":true,"result":null,"error":null,"precondition_failed":false}`, envelope.RequestID))
				case "nested-duplicate":
					body = []byte(fmt.Sprintf(`{"version":1,"request_id":%q,"ok":true,"result":{"value":1,"value":2},"error":null,"precondition_failed":false}`, envelope.RequestID))
				case "trailing":
					body = append(body, []byte(` {}`)...)
				case "malformed":
					body = []byte(`{`)
				}
				_, _ = writer.Write(body)
			}))
			defer server.Close()
			output := struct {
				Value int `json:"value"`
			}{Value: 99}
			err := transportInstance(test, server.URL).Authority(context.Background(), "strict-1", map[string]any{"command": "shutdown"}, &output)
			if failure := transportFailure(test, err); !failure.Ambiguous || failure.Rejected {
				test.Fatal("malformed success must be ambiguous")
			}
			if output.Value != 99 {
				test.Fatal("invalid response mutated output")
			}
		})
	}
}

func TestTransportAuthorityAndWorkerResponseShapesAreDistinct(test *testing.T) {
	for _, scenario := range []string{"worker-extra-precondition", "worker-missing-error", "authority-worker-shape"} {
		test.Run(scenario, func(test *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var envelope struct {
					RequestID string `json:"request_id"`
				}
				_ = json.NewDecoder(request.Body).Decode(&envelope)
				reply := transportReply(envelope.RequestID, nil, scenario == "worker-extra-precondition")
				if scenario == "worker-missing-error" {
					delete(reply, "error")
				}
				_ = json.NewEncoder(writer).Encode(reply)
			}))
			defer server.Close()
			transport := transportInstance(test, server.URL)
			var err error
			if scenario == "authority-worker-shape" {
				err = transport.Authority(context.Background(), "shape-1", map[string]any{"command": "shutdown"}, nil)
			} else {
				err = transport.Worker(context.Background(), Worker{ID: "worker-a", Address: server.URL}, "shape-1", map[string]any{"command": "get_worker_state"}, nil)
			}
			if !transportFailure(test, err).Ambiguous {
				test.Fatal("wrong response shape accepted")
			}
		})
	}
}

func TestTransportNullAndDefinitivePrecondition(test *testing.T) {
	secret := transportToken(test)
	for _, rejected := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			var envelope struct {
				RequestID string `json:"request_id"`
			}
			_ = json.NewDecoder(request.Body).Decode(&envelope)
			reply := transportReply(envelope.RequestID, nil, true)
			if rejected {
				reply["ok"] = false
				reply["precondition_failed"] = true
				reply["error"] = secret
			}
			_ = json.NewEncoder(writer).Encode(reply)
		}))
		transport, err := NewTransport(server.URL, secret, nil)
		if err != nil {
			test.Fatal(err)
		}
		output := map[string]bool{"old": true}
		err = transport.Authority(context.Background(), "null-1", map[string]any{"command": "shutdown"}, &output)
		if rejected {
			failure := transportFailure(test, err)
			if !failure.Rejected || !failure.PreconditionFailed || failure.Ambiguous || strings.Contains(fmt.Sprintf("%+v", err), secret) {
				test.Fatal("unsafe precondition rejection")
			}
		} else if err != nil || output != nil {
			test.Fatal("null result not accepted and cleared")
		}
		server.Close()
	}
}

func TestTransportBodyBounds(test *testing.T) {
	for _, advertised := range []bool{false, true} {
		test.Run(fmt.Sprint(advertised), func(test *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				_, _ = io.Copy(io.Discard, request.Body)
				if advertised {
					writer.Header().Set("Content-Length", fmt.Sprint(maxRPCBytes+1))
					writer.WriteHeader(200)
					return
				}
				writer.(http.Flusher).Flush()
				chunk := strings.Repeat("x", 1024*1024)
				for index := 0; index < 33; index++ {
					if _, err := io.WriteString(writer, chunk); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			err := transportInstance(test, server.URL).Authority(context.Background(), "bounds-1", map[string]any{"command": "shutdown"}, nil)
			if !transportFailure(test, err).Ambiguous {
				test.Fatal("oversized response accepted")
			}
		})
	}
	transport := transportInstance(test, "http://127.0.0.1:1")
	err := transport.Authority(context.Background(), "bounds-1", map[string]any{"command": "shutdown", "payload": strings.Repeat("x", maxRPCBytes)}, nil)
	if failure := transportFailure(test, err); failure.Ambiguous {
		test.Fatal("oversized request dispatched")
	}
}

func TestTransportBoundedJSONDepth(test *testing.T) {
	result := strings.Repeat("[", 130) + "null" + strings.Repeat("]", 130)
	body := []byte(`{"version":1,"request_id":"depth-1","ok":true,"result":` + result + `,"error":null,"precondition_failed":false}`)
	if !transportFailure(test, decodeRPCResponse(body, "depth-1", nil, true)).Ambiguous {
		test.Fatal("excessive JSON depth accepted")
	}
}

func TestTransportGenericRPCFailureDoesNotProveMutationAbsent(test *testing.T) {
	for _, message := range []string{"Uncertain backend mutation", "LeaseExpired grant completed too late", "internal failure"} {
		body, err := json.Marshal(map[string]any{"version": 1, "request_id": "uncertain-1", "ok": false, "result": nil, "error": message, "precondition_failed": false})
		if err != nil {
			test.Fatal(err)
		}
		failure := transportFailure(test, decodeRPCResponse(body, "uncertain-1", nil, true))
		if !failure.Ambiguous || failure.Rejected || failure.PreconditionFailed {
			test.Fatal("untyped failure incorrectly proves absence")
		}
		if strings.Contains(failure.Error(), message) {
			test.Fatal("backend detail leaked")
		}
	}
}

func TestTransportRejectsInvalidUTF8(test *testing.T) {
	body := []byte(`{"version":1,"request_id":"utf8-1","ok":true,"result":"`)
	body = append(body, 0xff)
	body = append(body, []byte(`","error":null,"precondition_failed":false}`)...)
	if !transportFailure(test, decodeRPCResponse(body, "utf8-1", nil, true)).Ambiguous {
		test.Fatal("invalid response encoding accepted")
	}
}
