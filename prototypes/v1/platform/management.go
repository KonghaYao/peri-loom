package platform

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	controller "peri-loom/controller"
)

type MetricsProvider interface{ Metrics() map[string]any }

type ManagementConfig struct {
	State         StateStore
	Placements    controller.MetadataStore
	Controller    *controller.Controller
	Identity      Identity
	Authority     controller.ControlTransport
	Keys          KeyBroker
	Upstream      SQLUpstream
	Audit         Audit
	Metrics       MetricsProvider
	Telemetry     *Telemetry
	Resources     *Gateway
	StopProof     WorkerStopVerifier
	Fence         IsolationFenceVerifier
	CapacityTrace *capacityTraceBridge
}

type Management struct {
	config            ManagementConfig
	nativeDiagnostics nativeDiagnosticBudget
}

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
	OperationID    string                `json:"operation_id"`
	Decision       string                `json:"decision"`
	Quotas         Quotas                `json:"quotas"`
}

var labelPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// managementReconcileBudget bounds the RPCs of every reconcile pass and stops
// new passes once it expires. It is not a strict wall-clock bound: a pass that
// already converged may return success just after the boundary, and
// context-free local metadata reads are not interruptible. A cold restart of a
// large placement set can exceed the old 10s window while still converging, so
// the budget leaves headroom but stays below the launcher's 30s request timeout.
const managementReconcileBudget = 20 * time.Second

const managementLogDetailLimit = 512

var errManagementForbidden = errors.New("forbidden")
var errDatabaseQuotaExceeded = errors.New("database quota exceeded")

func NewManagement(config ManagementConfig) (*Management, error) {
	if config.State == nil || config.Placements == nil || config.Controller == nil || config.Identity == nil || config.Authority == nil || config.Keys == nil || config.Upstream == nil || config.Audit == nil {
		return nil, errors.New("management dependencies required")
	}
	if config.Fence == nil {
		// The release path needs the authority's own attestation; the control
		// transport is already the component that talks to it.
		config.Fence = authorityIsolationFence{transport: config.Authority}
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
		if managementOutcomeUnknown(err) {
			event.Outcome = "outcome_unknown"
		}
	}
	event.TimestampMS = time.Now().UnixMilli()
	if management.config.Audit.Record(event) != nil {
		if managementOutcomeUnknown(err) {
			managementUnknownReply(response, input.RequestID, err)
			return
		}
		managementReply(response, 503, input.RequestID, nil, "outcome_unknown")
		return
	}
	if err != nil {
		if managementOutcomeUnknown(err) {
			log.Printf("管理请求失败：path=%s request_id=%s 隔离原因=%s 底层错误=%v", request.URL.Path, input.RequestID, "outcome_unknown", err)
			managementUnknownReply(response, input.RequestID, err)
			return
		}
		status, code := managementFailure(err)
		// 对外只保留折叠后的稳定错误码，底层原因必须留在私有日志里，
		// 否则 /v1/reconcile 与只读检查失败无法区分，也无法定位控制面根因。
		detail := "-"
		var rejected *controller.RPCError
		if errors.As(err, &rejected) {
			detail = managementLogDetail(rejected.Detail)
		}
		log.Printf("管理请求失败：path=%s request_id=%s 隔离原因=%s 底层错误=%v 底层错误详情=%s", request.URL.Path, input.RequestID, code, err, detail)
		managementReply(response, status, input.RequestID, nil, code)
		return
	}
	managementReply(response, 200, input.RequestID, result, "")
}

// managementLogDetail bounds and quotes backend error text before it reaches
// the private log. Truncation is byte-bounded, and ToValidUTF8 repairs a split
// rune; Quote escapes newlines and control characters so a backend message
// cannot forge a log line.
func managementLogDetail(detail string) string {
	if detail == "" {
		return "-"
	}
	if len(detail) > managementLogDetailLimit {
		detail = detail[:managementLogDetailLimit]
	}
	return strconv.Quote(strings.ToValidUTF8(detail, "\uFFFD"))
}

// managementFailure maps a dispatch error to the public status and code. The
// order preserves the contract the error matrix tests already pin.
func managementFailure(err error) (int, string) {
	if errors.Is(err, ErrDatabaseQuarantined) || errors.Is(err, ErrOperationExists) {
		return 409, "database_isolated"
	}
	// The release refusals name the missing proof instead of collapsing into the
	// generic isolation code, so an operator can tell where the path is stuck.
	if errors.Is(err, errReleaseNotRetired) {
		return 409, "release_not_retired"
	}
	if errors.Is(err, errReleaseProofMissing) {
		return 409, "release_stop_proof_missing"
	}
	if errors.Is(err, errReleaseFenceUnavailable) {
		return 409, "release_fencing_proof_unavailable"
	}
	if errors.Is(err, errReleaseScopeMismatch) {
		return 409, "release_scope_mismatch"
	}
	var native *nativeToolError
	if !errors.As(err, &native) {
		if errors.Is(err, errManagementForbidden) {
			return 403, "forbidden"
		}
		if errors.Is(err, errDatabaseQuotaExceeded) {
			return 429, "quota_exceeded"
		}
	}
	return 409, "operation_incomplete"
}

func (management *Management) dispatch(request *http.Request, principal Principal, input managementRequest) (any, error) {
	if strings.HasPrefix(request.URL.Path, "/v1/databases/") {
		if request.URL.Path == "/v1/databases/isolation/release" {
			// The release is the one path that must run while the database is still
			// isolated, so it is dispatched before the isolation gate below.
			return management.releaseIsolation(request.Context(), principal, input)
		}
		if request.URL.Path == "/v1/databases/operations" || request.URL.Path == "/v1/databases/operations/decision" || request.URL.Path == "/v1/databases/operations/clear" || request.URL.Path == "/v1/databases/operations/retire-local" {
			if request.URL.Path == "/v1/databases/operations/retire-local" {
				return management.retireLocal(request.Context(), principal, input)
			}
			return management.operations(principal, input, request.URL.Path)
		}
		if input.Database.Validate() == nil && request.URL.Path != "/v1/databases/status" && request.URL.Path != "/v1/databases/refs" && request.URL.Path != "/v1/databases/gc-inspect" && request.URL.Path != "/v1/databases/schema" && request.URL.Path != "/v1/databases/diagnostics" {
			state, err := management.config.State.Snapshot()
			if err != nil {
				return nil, err
			}
			if request.URL.Path == "/v1/databases/create" {
				project, exists := state.Projects[input.Project]
				if !exists {
					return nil, errors.New("project missing")
				}
				if project.Tenant != input.Database.Namespace.Tenant || principal.Role != "admin" && principal.Role != "developer" || principal.Project != "*" && principal.Project != input.Project {
					return nil, errManagementForbidden
				}
			} else if err := management.config.Identity.Authorize(principal, input.Database, "manage"); err != nil {
				return nil, err
			}
			if request.URL.Path != "/v1/databases/suspend" {
				// Stopping service is the one placement change that stays allowed while
				// a database is isolated. It can only take serving away: the drain asks
				// the owning worker to detach and then records a revoke so the authority
				// drops the branch ownership. That is exactly the fencing evidence the
				// isolation release needs, and it never puts old data back in service,
				// because resume stays behind the isolation gate.
				if _, blocked := isolatedOperation(state, input.Database, true); blocked {
					return nil, ErrDatabaseQuarantined
				}
			}
		}
	}
	switch request.URL.Path {
	case "/v1/reconcile":
		if principal.Role != "admin" || principal.Project != "*" {
			return nil, errManagementForbidden
		}
		// 启动收敛是期望状态操作，不是 SQL；同一预算传入每轮 pass 的 RPC 上下文，
		// deadline 会取消进行中的 transport 调用，预算耗尽返回最后一次底层错误。
		return map[string]bool{"ok": true}, management.config.Controller.ReconcileWithin(request.Context(), time.Now().Add(managementReconcileBudget))
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
	case "/v1/databases/schema", "/v1/databases/checkpoint", "/v1/databases/compact", "/v1/databases/diagnostics":
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
	if request.URL.Path == "/v1/capacity-trace" {
		management.capacityTrace(response, request, principal)
		return
	}
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
