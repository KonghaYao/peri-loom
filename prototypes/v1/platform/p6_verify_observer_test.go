package platform

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

type p6VerifyRefsObservation struct {
	status  int
	ok      bool
	message string
}

type p6VerifyAuthorityObserver struct {
	mutex sync.Mutex
	refs  map[string]p6VerifyRefsObservation
}

func (fixture *p6VerifyFixture) observeAuthority(actual string) string {
	observer := &p6VerifyAuthorityObserver{refs: map[string]p6VerifyRefsObservation{}}
	fixture.observer = observer
	client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, Timeout: 35 * time.Second}
	fixture.test.Cleanup(client.CloseIdleConnections)
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(io.LimitReader(request.Body, 32*1024*1024))
		if err != nil {
			http.Error(writer, "observer request unavailable", 502)
			return
		}
		var envelope struct {
			RequestID string `json:"request_id"`
			Command   struct {
				Command   string `json:"command"`
				Operation struct {
					Operation string `json:"operation"`
				} `json:"operation"`
			} `json:"command"`
		}
		isRefs := json.Unmarshal(body, &envelope) == nil && envelope.Command.Command == "maintenance" && envelope.Command.Operation.Operation == "refs"
		forward, err := http.NewRequestWithContext(request.Context(), request.Method, actual+request.URL.Path, bytes.NewReader(body))
		if err != nil {
			http.Error(writer, "observer request unavailable", 502)
			return
		}
		forward.GetBody = nil
		forward.Header = request.Header.Clone()
		response, err := client.Do(forward)
		if err != nil {
			http.Error(writer, "observer transport unavailable", 502)
			return
		}
		defer response.Body.Close()
		encoded, err := io.ReadAll(io.LimitReader(response.Body, 32*1024*1024))
		if err != nil {
			http.Error(writer, "observer response unavailable", 502)
			return
		}
		if isRefs {
			var reply struct {
				OK    bool    `json:"ok"`
				Error *string `json:"error"`
			}
			if json.Unmarshal(encoded, &reply) == nil {
				observation := p6VerifyRefsObservation{status: response.StatusCode, ok: reply.OK}
				if reply.Error != nil {
					observation.message = *reply.Error
				}
				observer.mutex.Lock()
				observer.refs[envelope.RequestID] = observation
				observer.mutex.Unlock()
			}
		}
		for name, values := range response.Header {
			for _, value := range values {
				writer.Header().Add(name, value)
			}
		}
		writer.WriteHeader(response.StatusCode)
		_, _ = writer.Write(encoded)
	}))
	fixture.test.Cleanup(proxy.Close)
	return proxy.URL
}

func (fixture *p6VerifyFixture) captureOriginalRefs(identifier string) {
	fixture.test.Helper()
	if fixture.observer == nil {
		return
	}
	fixture.observer.mutex.Lock()
	observation, found := fixture.observer.refs[identifier]
	fixture.observer.mutex.Unlock()
	message := observation.message
	for _, secret := range fixture.secrets {
		message = strings.ReplaceAll(message, secret, "[redacted]")
	}
	if len(message) > 4096 {
		message = message[:4096]
	}
	fixture.test.Logf("failure diagnostic: original authority refs request_id=%s captured=%t status=%d ok=%t error=%q", identifier, found, observation.status, observation.ok, message)
}
