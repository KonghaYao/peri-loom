package controller

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type API struct {
	store      MetadataStore
	controller *Controller
	token      [32]byte
}

func NewAPI(store MetadataStore, controller *Controller, token string) (*API, error) {
	if store == nil || controller == nil {
		return nil, errors.New("controller and metadata store required")
	}
	if err := validateServiceToken(token); err != nil {
		return nil, err
	}
	return &API{store: store, controller: controller, token: sha256.Sum256([]byte(token))}, nil
}

func (api *API) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	provided := sha256.Sum256([]byte(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")))
	if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(provided[:], api.token[:]) != 1 {
		apiReply(response, http.StatusForbidden, map[string]string{"error": "authorization denied"})
		return
	}
	if request.Method == http.MethodGet && request.URL.Path == "/v1/state" {
		metadata, err := api.store.Snapshot()
		if err != nil {
			apiReply(response, http.StatusServiceUnavailable, map[string]string{"error": "metadata unavailable"})
			return
		}
		apiReply(response, http.StatusOK, metadata)
		return
	}
	if request.Method != http.MethodPost {
		apiReply(response, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var input struct {
		Database DatabaseID `json:"database"`
		Worker   *Worker    `json:"worker,omitempty"`
		WorkerID string     `json:"worker_id,omitempty"`
		Region   string     `json:"region,omitempty"`
		Role     string     `json:"role,omitempty"`
		Draining bool       `json:"draining,omitempty"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		apiReply(response, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}
	var result any = map[string]bool{"ok": true}
	var err error
	switch request.URL.Path {
	case "/v1/workers/register":
		if input.Worker == nil {
			apiReply(response, http.StatusBadRequest, map[string]string{"error": "worker required"})
			return
		}
		err = RegisterWorker(api.store, *input.Worker)
	case "/v1/workers/drain":
		err = SetWorkerDraining(api.store, input.WorkerID, input.Draining)
	case "/v1/databases/place":
		role := input.Role
		if role == "" {
			role = "writer"
		}
		result, err = api.controller.Place(request.Context(), input.Database, input.Region, role)
	case "/v1/databases/migrate":
		result, err = api.controller.Migrate(request.Context(), input.Database, input.WorkerID)
	case "/v1/databases/suspend":
		err = api.controller.Suspend(request.Context(), input.Database)
	case "/v1/databases/resume":
		result, err = api.controller.Resume(request.Context(), input.Database)
	case "/v1/reconcile":
		err = api.controller.Reconcile(request.Context())
	default:
		apiReply(response, http.StatusNotFound, map[string]string{"error": "unknown operation"})
		return
	}
	if err != nil {
		apiReply(response, http.StatusConflict, map[string]string{"error": "operation did not converge; inspect authenticated controller state"})
		return
	}
	apiReply(response, http.StatusOK, result)
}

func apiReply(response http.ResponseWriter, status int, value any) {
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
