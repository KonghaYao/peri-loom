package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type apiTestTransport struct{}

func (apiTestTransport) Authority(context.Context, string, any, any) error {
	return errors.New("unexpected authority dispatch")
}

func (apiTestTransport) Worker(context.Context, Worker, string, any, any) error {
	return errors.New("unexpected worker dispatch")
}

func TestAPIRejectsUnauthenticatedAndMalformedManagement(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	token := strings.Repeat("m", 32)
	control, err := NewController(store, apiTestTransport{}, ReconcileConfig{})
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPI(store, control, token)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		method, path, body, auth string
		status                   int
	}{
		{http.MethodGet, "/v1/state", "", "", 403},
		{http.MethodGet, "/v1/state", "", "Bearer wrong", 403},
		{http.MethodGet, "/v1/state", "", "Bearer " + token, 200},
		{http.MethodPost, "/v1/workers/register", `{"unknown":true}`, "Bearer " + token, 400},
		{http.MethodPost, "/v1/workers/register", `{}`, "Bearer " + token, 400},
		{http.MethodPost, "/v1/workers/register", `{} {}`, "Bearer " + token, 400},
		{http.MethodPost, "/v1/unknown", `{}`, "Bearer " + token, 404},
	} {
		request := httptest.NewRequest(testCase.method, testCase.path, strings.NewReader(testCase.body))
		request.Header.Set("Authorization", testCase.auth)
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		if response.Code != testCase.status {
			t.Fatalf("%s got %d want %d", testCase.path, response.Code, testCase.status)
		}
		if strings.Contains(response.Body.String(), token) {
			t.Fatal("credential leaked in response")
		}
	}
}
