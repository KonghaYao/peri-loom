package platform

import (
	"net/http"
	"testing"

	"peri-loom/platform/sqlcontract"
)

// TestHranaAgreesWithTheFailureContract drives the Hrana adapter's only failure
// decode with the conformance kit: every published code, every evidence shape
// and every status the platform answers with. The adapter must reach exactly the
// answer sqlcontract reaches, because that answer decides whether the adapter
// aborts a pipeline, drops a connection or ends a cursor stream.
//
// This is the check the contract review did not have. Its absence is what let a
// rewrite make the platform's own proof for a non-committing operation — a
// describe or a read cursor — read as "no evidence", so that any failure of
// those was answered as an unknown outcome and aborted the pipeline.
func TestHranaAgreesWithTheFailureContract(t *testing.T) {
	vectors := sqlcontract.Vectors()
	t.Logf("failure contract vectors: %d codes x evidence x status", len(vectors))
	for _, vector := range vectors {
		failure := vector.Failure()
		reply := hranaGatewayReply(vector.Status, vector.Body(), "/v1/sql/statement")
		if reply.code != vector.Code {
			t.Fatalf("%s: code %q", vector.Name, reply.code)
		}
		if reply.status != vector.Status {
			t.Fatalf("%s: status %d", vector.Name, reply.status)
		}
		if reply.unknown != failure.Unknown(vector.Status) {
			t.Fatalf("%s: unknown=%t want %t", vector.Name, reply.unknown, failure.Unknown(vector.Status))
		}
		if reply.readOnlyCandidate != failure.ReadOnlyCandidate() {
			t.Fatalf("%s: readOnlyCandidate=%t", vector.Name, reply.readOnlyCandidate)
		}
	}
}

// TestHranaAbortsOnlyWithoutProof keeps the other half of the rule: an answer
// that proves nothing, the platform's own unresolved outcome, or an answer the
// adapter could not decode are the only things that may abort a pipeline, and an
// answer the platform answered with a platform failure status is never treated
// as a definite one.
func TestHranaAbortsOnlyWithoutProof(t *testing.T) {
	proven := `{"version":1,"request_id":"r","ok":false,"result":null,"error":{"code":"sql_timeout","execution_completed":true,"may_have_committed":false}}`
	if reply := hranaGatewayReply(http.StatusGatewayTimeout, []byte(proven), "/v1/sql/describe"); reply.unknown {
		t.Fatalf("a settled non-committing failure aborted the pipeline: %+v", reply)
	}
	unproven := `{"version":1,"request_id":"r","ok":false,"result":null,"error":{"code":"sql_timeout"}}`
	if reply := hranaGatewayReply(504, []byte(unproven), "/v1/sql/describe"); !reply.unknown {
		t.Fatalf("an unproven failure was answered as definite: %+v", reply)
	}
	if reply := hranaGatewayReply(502, []byte(`{`), "/v1/sql/describe"); !reply.unknown || reply.code != "invalid_gateway_reply" {
		t.Fatalf("a reply the adapter could not decode was answered as definite: %+v", reply)
	}
	if reply := hranaGatewayReply(502, []byte(`{"ok":false,"result":null,"error":null}`), "/v1/sql/describe"); !reply.unknown || reply.code != "invalid_gateway_reply" {
		t.Fatalf("a failure without a failure object was answered as definite: %+v", reply)
	}
	refused := `{"version":1,"request_id":"r","ok":false,"result":null,"error":{"code":"session_moved"}}`
	if reply := hranaGatewayReply(409, []byte(refused), "/v1/sql/statement"); reply.unknown {
		t.Fatalf("a proven refusal became an unknown outcome: %+v", reply)
	}
}
