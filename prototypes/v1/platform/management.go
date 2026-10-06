package platform

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	controller "peri-loom/controller"
)

type MetricsProvider interface{ Metrics() map[string]any }

type ManagementConfig struct {
	State      StateStore
	Placements controller.MetadataStore
	Controller *controller.Controller
	Identity   Identity
	Authority  controller.ControlTransport
	Keys       KeyBroker
	Upstream   SQLUpstream
	Audit      Audit
	Metrics    MetricsProvider
	Telemetry  *Telemetry
}

type Management struct{ config ManagementConfig }

type managementRequest struct {
	RequestID      string                `json:"request_id"`
	Database       controller.DatabaseID `json:"database"`
	Project        string                `json:"project"`
	Organization   string                `json:"organization"`
	ID             string                `json:"id"`
	Name           string                `json:"name"`
	Region         string                `json:"region"`
	Role           string                `json:"role"`
	Subject        string                `json:"subject"`
	ServiceAccount bool                  `json:"service_account"`
	WorkerID       string                `json:"worker_id"`
	Snapshot       string                `json:"snapshot"`
	Sequence       uint64                `json:"sequence"`
	TimestampMS    uint64                `json:"timestamp_ms"`
	SafetyWindowMS uint64                `json:"safety_window_ms"`
	ExpiresMS      int64                 `json:"expires_ms"`
	Quotas         Quotas                `json:"quotas"`
}

var labelPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func NewManagement(config ManagementConfig) (*Management, error) {
	if config.State == nil || config.Placements == nil || config.Controller == nil || config.Identity == nil || config.Authority == nil || config.Keys == nil || config.Upstream == nil || config.Audit == nil {
		return nil, errors.New("management dependencies required")
	}
	return &Management{config: config}, nil
}

func (management *Management) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") {
		managementReply(response, 403, "", nil, "unauthorized")
		return
	}
	principal, err := management.config.Identity.Authenticate(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
	if err != nil || principal.Database != nil {
		managementReply(response, 403, "", nil, "unauthorized")
		return
	}
	if request.Method == http.MethodGet {
		management.read(response, request, principal)
		return
	}
	if request.Method != http.MethodPost {
		managementReply(response, 405, "", nil, "method_not_allowed")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(response, request.Body, 1024*1024))
	decoder.DisallowUnknownFields()
	var input managementRequest
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || !labelPattern.MatchString(input.RequestID) {
		managementReply(response, 400, "", nil, "invalid_request")
		return
	}
	event := AuditEvent{RequestID: input.RequestID, Subject: principal.Subject, Project: principal.Project, Action: request.URL.Path, Outcome: "admitted", TimestampMS: time.Now().UnixMilli()}
	if input.Database.Validate() == nil {
		event.Database = input.Database.Key()
	}
	if management.config.Audit.Record(event) != nil {
		managementReply(response, 503, input.RequestID, nil, "audit_unavailable")
		return
	}
	result, err := management.dispatch(request, principal, input)
	event.Outcome = "complete"
	if err != nil {
		event.Outcome = "incomplete"
	}
	event.TimestampMS = time.Now().UnixMilli()
	if management.config.Audit.Record(event) != nil {
		managementReply(response, 503, input.RequestID, nil, "outcome_unknown")
		return
	}
	if err != nil {
		managementReply(response, 409, input.RequestID, nil, "operation_incomplete")
		return
	}
	managementReply(response, 200, input.RequestID, result, "")
}

func (management *Management) dispatch(request *http.Request, principal Principal, input managementRequest) (any, error) {
	switch request.URL.Path {
	case "/v1/reconcile":
		if principal.Role != "admin" || principal.Project != "*" {
			return nil, errors.New("controller reconciliation denied")
		}
		return map[string]bool{"ok": true}, management.config.Controller.Reconcile(request.Context())
	case "/v1/organizations/create":
		return management.createOrganization(principal, input)
	case "/v1/projects/create":
		return management.createProject(principal, input)
	case "/v1/keys/create":
		return management.createAPIKey(principal, input)
	case "/v1/keys/revoke":
		return management.revokeAPIKey(principal, input)
	case "/v1/tokens":
		return management.config.Identity.IssueToken(principal, input.Database, input.Role, input.ExpiresMS)
	case "/v1/databases/create":
		return management.createDatabase(request.Context(), principal, input)
	case "/v1/databases/delete":
		return management.deleteDatabase(request.Context(), principal, input)
	case "/v1/databases/suspend", "/v1/databases/resume", "/v1/databases/migrate":
		return management.placement(request.Context(), principal, input, request.URL.Path)
	case "/v1/databases/branch", "/v1/databases/clone":
		return management.fork(request.Context(), principal, input, request.URL.Path)
	case "/v1/databases/snapshot", "/v1/databases/restore", "/v1/databases/pitr", "/v1/databases/drop-snapshot", "/v1/databases/retain-after", "/v1/databases/gc":
		return management.maintenance(request.Context(), principal, input, request.URL.Path)
	case "/v1/databases/restore-point":
		return management.maintenance(request.Context(), principal, input, request.URL.Path)
	case "/v1/databases/status", "/v1/databases/refs", "/v1/databases/gc-inspect":
		return management.inspect(request.Context(), principal, input, request.URL.Path)
	case "/v1/databases/schema", "/v1/databases/checkpoint", "/v1/databases/compact":
		return management.nativeTool(request.Context(), principal, input, request.URL.Path)
	case "/v1/databases/rewrap":
		if err := management.config.Identity.Authorize(principal, input.Database, "keys"); err != nil {
			return nil, err
		}
		return map[string]bool{"ok": true}, management.config.Keys.Rewrap(request.Context(), input.Database.Namespace)
	default:
		return nil, errors.New("unknown management operation")
	}
}

func (management *Management) read(response http.ResponseWriter, request *http.Request, principal Principal) {
	if request.URL.Path == "/v1/metrics" {
		if principal.Role != "admin" || principal.Project != "*" || management.config.Metrics == nil {
			managementReply(response, 403, "", nil, "forbidden")
			return
		}
		metrics := management.config.Metrics.Metrics()
		if management.config.Telemetry != nil {
			metrics["native"] = management.config.Telemetry.Collect(request.Context())
		}
		managementReply(response, 200, "", metrics, "")
		return
	}
	if request.URL.Path != "/v1/catalog" {
		managementReply(response, 404, "", nil, "not_found")
		return
	}
	state, err := management.config.State.Snapshot()
	if err != nil {
		managementReply(response, 503, "", nil, "unavailable")
		return
	}
	projects := map[string]Project{}
	databases := map[string]Database{}
	for identity, project := range state.Projects {
		if principal.Project == identity || principal.Role == "admin" && principal.Project == "*" {
			projects[identity] = project
		}
	}
	for identity, database := range state.Databases {
		if _, allowed := projects[database.Project]; allowed {
			databases[identity] = database
		}
	}
	managementReply(response, 200, "", map[string]any{"revision": state.Revision, "projects": projects, "databases": databases}, "")
}

func managementReply(response http.ResponseWriter, status int, requestID string, result any, code string) {
	var failure any
	if code != "" {
		failure = map[string]string{"code": code}
	}
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]any{"version": 1, "request_id": requestID, "ok": code == "", "result": result, "error": failure})
}
