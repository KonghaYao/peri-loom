package platform

import (
	"encoding/json"
	"net/http"
	"strings"

	controller "peri-loom/controller"
)

// hranaFusionCandidate decides whether a top-level execute may ride along with
// the implicit open. The answer must be a statement the normal path would send
// straight to /v1/sql/statement: the arguments have to be valid here so a bad
// entry still fails before it reaches a worker, and a RETURNING probe keeps its
// own describe round trip instead of being fused into the open.
func hranaFusionCandidate(statement hranaStatement) (fusedOpenStatement, bool) {
	if statement.SQL == "" {
		return fusedOpenStatement{}, false
	}
	if statement.WantRows && strings.Contains(strings.ToUpper(statement.SQL), "RETURNING") {
		return fusedOpenStatement{}, false
	}
	args, namedArgs, ok := hranaStatementArguments(statement)
	if !ok {
		return fusedOpenStatement{}, false
	}
	if guardedBatchStatementBytes(statement.SQL, args, namedArgs) > guardedBatchRequestBodyLimit {
		// The ordinary statement path keeps its own error for an oversized
		// request; fusing it would turn a transport refusal into an unknown open.
		return fusedOpenStatement{}, false
	}
	return fusedOpenStatement{SQL: statement.SQL, Args: args, NamedArgs: namedArgs, WantRows: statement.WantRows}, true
}

type hranaOpenResult struct {
	Session string
	// FusedStatement is the statement reply the gateway received from the same
	// worker request, or nil when the worker did not fuse it.
	FusedStatement json.RawMessage
	// FusedClose records that the same worker request also retired the session.
	// The adapter still sends its close entry so the platform keeps its own
	// records; this field only tells it that the worker session is gone.
	FusedClose bool
}

// hranaOpen is the implicit open of a Hrana pipeline. It asks the gateway to
// fuse the given statement into the open when the worker supports it; the
// gateway performs the capability probe, so the adapter only supplies the
// candidate. A worker that does not fuse the statement answers with the usual
// session-only result and the entry takes the ordinary statement path.
//
// closeAfter says the adapter's very next entry is the session's close, so the
// gateway may ask the worker to retire the session in the same request. That
// answer is still the gateway's decision: a worker that does not advertise the
// close level keeps the session.
func (handler *hranaHTTP) hranaOpen(request *http.Request, credential string, database controller.DatabaseID, statement *hranaStatement, closeAfter bool) (hranaOpenResult, hranaReply) {
	openRequest := request
	candidate := false
	if statement != nil {
		if fused, ok := hranaFusionCandidate(*statement); ok {
			fused.CloseAfter = closeAfter
			candidate = true
			openRequest = request.WithContext(withFusedOpenStatement(request.Context(), fused))
		}
	}
	reply := handler.call(openRequest, credential, "/v1/sql/open", map[string]any{"database": database})
	if reply.status != http.StatusOK || reply.code != "" {
		return hranaOpenResult{}, reply
	}
	var opened struct {
		Session        string          `json:"session"`
		FusedStatement json.RawMessage `json:"fused_statement"`
		FusedClose     bool            `json:"fused_close"`
	}
	if json.Unmarshal(reply.result, &opened) != nil {
		return hranaOpenResult{}, hranaReply{status: http.StatusBadGateway, code: "invalid_native_open", unknown: true}
	}
	if opened.FusedStatement != nil {
		// The gateway validates this payload before publishing it, so a shape
		// this adapter cannot read is a protocol error, not a statement outcome.
		// The session is already registered, hence its idle sweep can retire it.
		if !candidate {
			handler.cleanupRetiredSession(opened.Session, 0)
			return hranaOpenResult{Session: opened.Session}, hranaReply{status: http.StatusBadGateway, code: "invalid_native_open", unknown: true}
		}
		if _, valid := parseFusedStatementAnswer(opened.FusedStatement); !valid {
			handler.cleanupRetiredSession(opened.Session, 0)
			return hranaOpenResult{Session: opened.Session}, hranaReply{status: http.StatusBadGateway, code: "invalid_native_open", unknown: true}
		}
	} else if opened.FusedClose {
		// A close without the statement it followed cannot be spent: the close
		// entry would skip the worker call for a session nothing proved closed.
		handler.cleanupRetiredSession(opened.Session, 0)
		return hranaOpenResult{Session: opened.Session}, hranaReply{status: http.StatusBadGateway, code: "invalid_native_open", unknown: true}
	}
	return hranaOpenResult{Session: opened.Session, FusedStatement: opened.FusedStatement, FusedClose: opened.FusedClose}, hranaReply{status: http.StatusOK}
}

// hranaFusedStatementResult is the marker the next statement call consumes. It
// carries the statement text so the gateway can prove the answer belongs to
// this entry before it uses it.
func hranaFusedStatementResult(session string, statement hranaStatement, reply json.RawMessage, closed bool) *fusedStatementResult {
	if reply == nil {
		return nil
	}
	return &fusedStatementResult{Session: session, SQL: statement.SQL, Reply: reply, Closed: closed}
}
