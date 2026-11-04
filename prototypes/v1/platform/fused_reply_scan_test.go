package platform

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
)

// fusedStatementReply is the envelope shape the old decoder targeted. It lives
// with the oracle because production code no longer decodes the envelope into
// a struct.
type fusedStatementReply struct {
	Status             string               `json:"status"`
	Reason             *guardedIntentReason `json:"reason"`
	Result             json.RawMessage      `json:"result"`
	Error              *string              `json:"error"`
	ExecutionCompleted *bool                `json:"execution_completed"`
}

// parseFusedStatementAnswerLegacy is the implementation R48 replaced, kept
// verbatim as the oracle. It must stay a faithful copy of the decoder path:
// the differential test exists to prove both acceptance sets are identical,
// including duplicate keys, null-versus-absent fields and trailing bytes.
func parseFusedStatementAnswerLegacy(reply json.RawMessage) (fusedStatementAnswer, bool) {
	var parsed fusedStatementReply
	decoder := json.NewDecoder(bytes.NewReader(reply))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if decoder.Decode(&parsed) != nil || decoder.Decode(new(any)) != io.EOF {
		return fusedStatementAnswer{}, false
	}
	switch parsed.Status {
	case "ok":
		if parsed.Reason != nil || parsed.Error != nil || parsed.ExecutionCompleted != nil ||
			!validStatementResult(parsed.Result) {
			return fusedStatementAnswer{}, false
		}
		return fusedStatementAnswer{kind: fusedStatementOK, result: parsed.Result}, true
	case "requires_intent":
		if parsed.Reason == nil || parsed.Result != nil || parsed.Error != nil || parsed.ExecutionCompleted != nil {
			return fusedStatementAnswer{}, false
		}
		switch *parsed.Reason {
		case guardedEngineWritable, guardedTransactionContext, guardedConnectionControl:
		default:
			return fusedStatementAnswer{}, false
		}
		return fusedStatementAnswer{kind: fusedStatementRequiresIntent, reason: *parsed.Reason}, true
	case "error":
		if parsed.Error == nil || *parsed.Error == "" || parsed.Result != nil || parsed.Reason != nil {
			return fusedStatementAnswer{}, false
		}
		return fusedStatementAnswer{
			kind:               fusedStatementError,
			message:            *parsed.Error,
			executionCompleted: parsed.ExecutionCompleted,
		}, true
	default:
		return fusedStatementAnswer{}, false
	}
}

const (
	fusedFiveFieldResult = `{"cols":[{"name":"x","decltype":"INTEGER"}],"rows":[[1]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`
	fusedRealResultPath  = "/tmp/peri-perf/r48-result-1000.json"
)

func fusedOKReply(result string) string {
	return `{"status":"ok","result":` + result + `}`
}

func fusedReplyWithResult(result []byte) []byte {
	reply := make([]byte, 0, len(result)+24)
	reply = append(reply, `{"status":"ok","result":`...)
	reply = append(reply, result...)
	reply = append(reply, '}')
	return reply
}

// fusedRealStatementResult returns the captured R1.limit-1000 statement result
// when it is on this machine. Tests fall back to a synthetic payload instead of
// depending on a developer's /tmp.
func fusedRealStatementResult(tb testing.TB) []byte {
	tb.Helper()
	data, err := os.ReadFile(fusedRealResultPath)
	if err != nil {
		return nil
	}
	if !json.Valid(data) {
		tb.Fatalf("%s is not valid JSON", fusedRealResultPath)
	}
	return data
}

func syntheticFusedStatementResult(rows int) []byte {
	var builder strings.Builder
	builder.Grow(rows*64 + 256)
	builder.WriteString(`{"affected_row_count":0,"autocommit":true,"cols":[{"decltype":"TEXT","name":"content"}],"last_insert_rowid":null,"query_duration_ms":2.5,"rows":[`)
	for i := 0; i < rows; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(`["issue28/`)
		builder.WriteString(fmt.Sprintf("%05d", i))
		builder.WriteString(`/issue28/payload"]`)
	}
	builder.WriteString(`],"rows_read":`)
	builder.WriteString(strconv.Itoa(rows))
	builder.WriteString(`,"rows_written":0}`)
	return []byte(builder.String())
}

func fusedBenchmarkStatementResult(tb testing.TB) []byte {
	tb.Helper()
	if result := fusedRealStatementResult(tb); result != nil {
		return result
	}
	tb.Logf("%s is unavailable; using a synthetic 1,000-row statement result", fusedRealResultPath)
	return syntheticFusedStatementResult(1000)
}

func fusedSampleForLog(sample []byte) string {
	const limit = 400
	if len(sample) <= limit {
		return fmt.Sprintf("%q", sample)
	}
	return fmt.Sprintf("%q... (%d bytes)", sample[:limit], len(sample))
}

func requireSameFusedAnswer(t *testing.T, label string, sample []byte) {
	t.Helper()
	got, gotOK := scanFusedStatementReply(sample)
	want, wantOK := parseFusedStatementAnswerLegacy(sample)
	if gotOK != wantOK {
		t.Fatalf("%s: scanner ok = %v, legacy ok = %v for %s", label, gotOK, wantOK, fusedSampleForLog(sample))
	}
	if !gotOK {
		return
	}
	if got.kind != want.kind || got.reason != want.reason || got.message != want.message {
		t.Fatalf("%s: scanner answer = %+v, legacy answer = %+v for %s", label, got, want, fusedSampleForLog(sample))
	}
	if !bytes.Equal(got.result, want.result) {
		t.Fatalf("%s: scanner result = %s, legacy result = %s", label, fusedSampleForLog(got.result), fusedSampleForLog(want.result))
	}
	if (got.executionCompleted == nil) != (want.executionCompleted == nil) {
		t.Fatalf("%s: scanner completed = %v, legacy completed = %v", label, got.executionCompleted, want.executionCompleted)
	}
	if got.executionCompleted != nil && *got.executionCompleted != *want.executionCompleted {
		t.Fatalf("%s: scanner completed = %v, legacy completed = %v", label, *got.executionCompleted, *want.executionCompleted)
	}
}

func fusedDifferentialCorpus(t *testing.T) []struct {
	name string
	body []byte
} {
	t.Helper()
	corpus := []struct {
		name string
		body []byte
	}{
		{"ok five fields", []byte(fusedOKReply(fusedFiveFieldResult))},
		{"ok escaped status", []byte(`{"status":"o\u006b","result":` + fusedFiveFieldResult + `}`)},
		{"ok case-insensitive key", []byte(`{"Status":"ok","result":` + fusedFiveFieldResult + `}`)},
		{"ok unicode folded key", []byte(`{"\u017ftatu\u017f":"ok","result":` + fusedFiveFieldResult + `}`)},
		{"ok escaped result key", []byte(`{"status":"ok","res\u0075lt":` + fusedFiveFieldResult + `}`)},
		{"ok duplicate result last wins", []byte(`{"status":"ok","result":null,"result":` + fusedFiveFieldResult + `}`)},
		{"ok duplicate result last null", []byte(`{"status":"ok","result":` + fusedFiveFieldResult + `,"result":null}`)},
		{"ok duplicate status null keeps value", []byte(`{"status":"ok","status":null,"result":` + fusedFiveFieldResult + `}`)},
		{"ok reason null", []byte(`{"status":"ok","reason":null,"result":` + fusedFiveFieldResult + `}`)},
		{"ok error null", []byte(`{"status":"ok","error":null,"result":` + fusedFiveFieldResult + `}`)},
		{"ok completed null", []byte(`{"status":"ok","execution_completed":null,"result":` + fusedFiveFieldResult + `}`)},
		{"ok completed false", []byte(`{"status":"ok","execution_completed":false,"result":` + fusedFiveFieldResult + `}`)},
		{"ok missing result", []byte(`{"status":"ok"}`)},
		{"ok result null", []byte(`{"status":"ok","result":null}`)},
		{"ok unknown field", []byte(`{"status":"ok","extra":1,"result":` + fusedFiveFieldResult + `}`)},
		{"ok trailing content", []byte(fusedOKReply(fusedFiveFieldResult) + `x`)},
		{"ok second value", []byte(fusedOKReply(fusedFiveFieldResult) + `{}`)},
		{"ok trailing space", []byte(" \n" + fusedOKReply(fusedFiveFieldResult) + "\t\r ")},
		{"status null", []byte(`{"status":null}`)},
		{"status number", []byte(`{"status":1}`)},
		{"status empty object", []byte(`{}`)},
		{"top level null", []byte(`null`)},
		{"top level array", []byte(`[]`)},
		{"empty input", nil},
		{"only whitespace", []byte(" \n\t")},
		{"requires intent", []byte(`{"status":"requires_intent","reason":"engine_writable"}`)},
		{"requires intent transaction", []byte(`{"status":"requires_intent","reason":"transaction_context"}`)},
		{"requires intent duplicate reason", []byte(`{"status":"requires_intent","reason":"connection_control","reason":"engine_writable"}`)},
		{"requires intent result null", []byte(`{"status":"requires_intent","reason":"engine_writable","result":null}`)},
		{"requires intent reason null", []byte(`{"status":"requires_intent","reason":null}`)},
		{"requires intent unknown reason", []byte(`{"status":"requires_intent","reason":"other"}`)},
		{"requires intent completed", []byte(`{"status":"requires_intent","reason":"engine_writable","execution_completed":true}`)},
		{"error", []byte(`{"status":"error","error":"boom"}`)},
		{"error empty", []byte(`{"status":"error","error":""}`)},
		{"error null", []byte(`{"status":"error","error":null}`)},
		{"error result null", []byte(`{"status":"error","error":"boom","result":null}`)},
		{"error reason", []byte(`{"status":"error","error":"boom","reason":"engine_writable"}`)},
		{"error completed true", []byte(`{"status":"error","error":"boom","execution_completed":true}`)},
		{"error completed null", []byte(`{"status":"error","error":"boom","execution_completed":null}`)},
		{"error duplicate message", []byte(`{"status":"error","error":"one","error":"two"}`)},
		{"unknown status", []byte(`{"status":"maybe"}`)},
		{"legacy statement result", []byte(fusedOKReply(`{"cols":[{"name":"id","decltype":"INTEGER"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"metrics statement result", []byte(fusedOKReply(`{"cols":[{"name":"id","decltype":"INTEGER"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":8625,"rows_written":0,"query_duration_ms":955.25}`))},
		{"six fields", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1}`))},
		{"seven fields", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0}`))},
		{"nine fields", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":1,"extra":null}`))},
		{"eight fields without duration", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"extra":1}`))},
		{"null rows read", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":null,"rows_written":0,"query_duration_ms":1}`))},
		{"negative rows read", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":-1,"rows_written":0,"query_duration_ms":1}`))},
		{"fractional rows read", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1.5,"rows_written":0,"query_duration_ms":1}`))},
		{"overflow rows read", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":18446744073709551616,"rows_written":0,"query_duration_ms":1}`))},
		{"negative duration", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":-0.5}`))},
		{"negative zero duration", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":-0}`))},
		{"overflow duration", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":1e400}`))},
		{"quoted duration", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":"1.5"}`))},
		{"null affected", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":null,"last_insert_rowid":null,"autocommit":true}`))},
		{"negative affected", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":-1,"last_insert_rowid":null,"autocommit":true}`))},
		{"fractional affected", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":1.0,"last_insert_rowid":null,"autocommit":true}`))},
		{"null cols", []byte(fusedOKReply(`{"cols":null,"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"null rows", []byte(fusedOKReply(`{"cols":[],"rows":null,"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"empty arrays", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"null row", []byte(fusedOKReply(`{"cols":[],"rows":[null],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"empty row", []byte(fusedOKReply(`{"cols":[],"rows":[[]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"width mismatch", []byte(fusedOKReply(`{"cols":[],"rows":[[1]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"row object", []byte(fusedOKReply(`{"cols":[],"rows":[{}],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column three keys", []byte(fusedOKReply(`{"cols":[{"name":"x","decltype":null,"extra":1}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column one key", []byte(fusedOKReply(`{"cols":[{"name":"x"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column name null", []byte(fusedOKReply(`{"cols":[{"name":null,"decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column name number", []byte(fusedOKReply(`{"cols":[{"name":1,"decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column decltype null", []byte(fusedOKReply(`{"cols":[{"name":"x","decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column decltype string", []byte(fusedOKReply(`{"cols":[{"name":"x","decltype":"TEXT"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column decltype number", []byte(fusedOKReply(`{"cols":[{"name":"x","decltype":1}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column missing decltype", []byte(fusedOKReply(`{"cols":[{"name":"x"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column duplicate name", []byte(fusedOKReply(`{"cols":[{"name":"x","name":"y","decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column escaped key", []byte(fusedOKReply(`{"cols":[{"\u006eame":"x","decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"duplicate cols last wins", []byte(fusedOKReply(`{"cols":null,"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"escaped cols key", []byte(fusedOKReply(`{"\u0063ols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"autocommit null", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":null}`))},
		{"autocommit number", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":1}`))},
		{"autocommit quoted", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":"true"}`))},
		{"rowid missing", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"autocommit":true}`))},
		{"rowid null", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"rowid integer", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":7,"autocommit":true}`))},
		{"rowid fractional", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":1.0,"autocommit":true}`))},
		{"rowid quoted null", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":"null","autocommit":true}`))},
		{"unknown statement key", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"extra":1}`))},
		{"duplicate statement key", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"autocommit":true}`))},
		{"row cells of every shape", []byte(fusedOKReply(`{"cols":[{"name":"a","decltype":null},{"name":"b","decltype":"X"}],"rows":[[null,true],["x",{}],[[],1.5]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":false}`))},
		{"rows before cols", []byte(fusedOKReply(`{"rows":[],"cols":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"rows before cols width match", []byte(fusedOKReply(`{"rows":[[1]],"cols":[{"name":"a","decltype":null}],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"rows checked then cols shrinks", []byte(fusedOKReply(`{"cols":[{"name":"a","decltype":null}],"rows":[[1]],"cols":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"rows checked then cols grows", []byte(fusedOKReply(`{"cols":[],"rows":[[1]],"cols":[{"name":"a","decltype":null}],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"duplicate rows last valid", []byte(fusedOKReply(`{"cols":[],"rows":[[1]],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"duplicate rows last invalid", []byte(fusedOKReply(`{"cols":[],"rows":[],"rows":[[1]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"duplicate cols escaped", []byte(fusedOKReply(`{"cols":null,"\u0063ols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"envelope duplicate status escaped", []byte(`{"status":"error","error":"x","\u0073tatus":"ok","error":null,"result":` + fusedFiveFieldResult + `}`)},
		{"statement result case variant", []byte(fusedOKReply(`{"Cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column key case variant", []byte(fusedOKReply(`{"cols":[{"NAME":"a","decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"column escaped metadata", []byte(fusedOKReply(`{"cols":[{"name":"a\u0062","decltype":"\u0054EXT"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"whitespace around result", []byte(`{ "status" : "ok" , "result" :  ` + fusedFiveFieldResult + ` , "execution_completed" : null }`)},
		{"surrogate cell", []byte(fusedOKReply(`{"cols":[{"name":"a","decltype":null}],"rows":[["\ud800"]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"control escape cell", []byte(fusedOKReply(`{"cols":[{"name":"a","decltype":null}],"rows":[["\u0001"]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"raw newline in cell", []byte(fusedOKReply("{\"cols\":[{\"name\":\"a\",\"decltype\":null}],\"rows\":[[\"a\nb\"]],\"affected_row_count\":0,\"last_insert_rowid\":null,\"autocommit\":true}"))},
		{"affected negative zero", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":-0,"last_insert_rowid":null,"autocommit":true}`))},
		{"affected overflow", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":9223372036854775808,"last_insert_rowid":null,"autocommit":true}`))},
		{"rowid negative zero", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":-0,"autocommit":true}`))},
		{"rowid overflow", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":9223372036854775808,"autocommit":true}`))},
		{"duration exponent", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":1E+2}`))},
		{"autocommit false", []byte(fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":false}`))},
		{"multi column multi row", []byte(fusedOKReply(`{"cols":[{"name":"a","decltype":null},{"name":"b","decltype":"TEXT"}],"rows":[[1,"x"],[2,"y"]],"affected_row_count":2,"last_insert_rowid":2,"autocommit":true}`))},
		{"empty key", []byte(`{"":1}`)},
		{"non-string key", []byte(`{1:2}`)},
		{"invalid escape status", []byte(`{"status":"o\x6b"}`)},
		{"short unicode escape", []byte(`{"status":"o\u12"}`)},
		{"requires intent with first result null", []byte(`{"result":null,"status":"requires_intent","reason":"engine_writable"}`)},
		{"error duplicate completed", []byte(`{"status":"error","error":"x","execution_completed":true,"execution_completed":null}`)},
		{"column unicode folded key is not exact", []byte(fusedOKReply(`{"cols":[{"\u017fname":"a","decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"statement folded key is not exact", []byte(fusedOKReply(`{"\u017fols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"exponent cell", []byte(fusedOKReply(`{"cols":[{"name":"a","decltype":null}],"rows":[[1e400,-0]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"trailing comma envelope", []byte(`{"status":"ok",}`)},
		{"trailing comma columns", []byte(fusedOKReply(`{"cols":[{"name":"a","decltype":null},],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
		{"trailing comma rows", []byte(fusedOKReply(`{"cols":[],"rows":[[1],],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`))},
	}
	if real := fusedRealStatementResult(t); real != nil {
		corpus = append(corpus, struct {
			name string
			body []byte
		}{"real R1 statement result", fusedReplyWithResult(real)})
		// A handful of structural edits over the captured payload, so the
		// differential path also validates the multi-megabyte result shape.
		edited := bytes.Replace(real, []byte(`"last_insert_rowid":null`), []byte(`"last_insert_rowid":1.0`), 1)
		corpus = append(corpus, struct {
			name string
			body []byte
		}{"real R1 rowid edited", fusedReplyWithResult(edited)})
		edited = bytes.Replace(real, []byte(`"query_duration_ms":2.209209`), []byte(`"query_duration_ms":null`), 1)
		corpus = append(corpus, struct {
			name string
			body []byte
		}{"real R1 duration null", fusedReplyWithResult(edited)})
		edited = bytes.Replace(real, []byte(`,"rows_read":1000,"rows_written":0`), nil, 1)
		corpus = append(corpus, struct {
			name string
			body []byte
		}{"real R1 metrics removed", fusedReplyWithResult(edited)})
		corpus = append(corpus, struct {
			name string
			body []byte
		}{"real R1 truncated", fusedReplyWithResult(real[:len(real)/2])})
	}
	return corpus
}

func TestScanFusedReplyDifferentialCorpus(t *testing.T) {
	for _, sample := range fusedDifferentialCorpus(t) {
		sample := sample
		t.Run(sample.name, func(t *testing.T) {
			requireSameFusedAnswer(t, sample.name, sample.body)
		})
	}
}

func TestScanFusedReplyDifferentialMutations(t *testing.T) {
	seeds := [][]byte{
		[]byte(fusedOKReply(fusedFiveFieldResult)),
		[]byte(fusedOKReply(`{"cols":[{"name":"id","decltype":"INTEGER"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":8625,"rows_written":0,"query_duration_ms":955.25}`)),
		[]byte(`{"status":"requires_intent","reason":"engine_writable"}`),
		[]byte(`{"status":"error","error":"boom","execution_completed":true}`),
		[]byte(fusedOKReply(`{"cols":[{"name":"x","decltype":"TEXT"}],"rows":[["a","b"],["c","d"]],"affected_row_count":2,"last_insert_rowid":3,"autocommit":false}`)),
		fusedReplyWithResult(syntheticFusedStatementResult(200)),
	}
	rng := rand.New(rand.NewSource(0x48))
	const samples = 3500
	for i := 0; i < samples; i++ {
		seed := seeds[rng.Intn(len(seeds))]
		sample := fusedMutate(seed, rng)
		requireSameFusedAnswer(t, fmt.Sprintf("mutation %d", i), sample)
	}
	t.Logf("compared %d deterministic mutations", samples)
}

func fusedMutate(seed []byte, rng *rand.Rand) []byte {
	out := append([]byte(nil), seed...)
	switch rng.Intn(8) {
	case 0:
		if len(out) > 0 {
			start := rng.Intn(len(out))
			n := 1 + rng.Intn(4)
			if start+n > len(out) {
				n = len(out) - start
			}
			out = append(out[:start], out[start+n:]...)
		}
	case 1:
		pos := rng.Intn(len(out) + 1)
		out = append(out, 0)
		copy(out[pos+1:], out[pos:])
		out[pos] = byte(rng.Intn(256))
	case 2:
		if len(out) > 0 {
			out[rng.Intn(len(out))] = byte(rng.Intn(256))
		}
	case 3:
		if len(out) > 1 {
			out = out[:rng.Intn(len(out))]
		}
	case 4:
		pos := rng.Intn(len(out) + 1)
		out = append(out, 0)
		copy(out[pos+1:], out[pos:])
		out[pos] = []byte(" \t\r\n")[rng.Intn(4)]
	case 5:
		out = append(out, []byte("x")...)
	case 6:
		if len(out) > 8 {
			start := rng.Intn(len(out) - 4)
			n := 1 + rng.Intn(4)
			chunk := append([]byte(nil), out[start:start+n]...)
			pos := rng.Intn(len(out) + 1)
			out = append(out, make([]byte, len(chunk))...)
			copy(out[pos+len(chunk):], out[pos:])
			copy(out[pos:], chunk)
		}
	case 7:
		index := bytes.Index(out, []byte("null"))
		if index < 0 {
			index = bytes.Index(out, []byte("true"))
		}
		if index >= 0 {
			replacement := []string{"null", "0", `""`, "", "1.0", "[]", "{}"}[rng.Intn(7)]
			out = append(out[:index:index], append([]byte(replacement), out[index+4:]...)...)
		}
	}
	return out
}

func TestScanFusedReplySemantics(t *testing.T) {
	completedTrue := true
	completedFalse := false
	for _, scenario := range []struct {
		name      string
		body      string
		valid     bool
		kind      fusedStatementAnswerKind
		reason    guardedIntentReason
		message   string
		result    string
		completed *bool
	}{
		{name: "ok", body: fusedOKReply(fusedFiveFieldResult), valid: true, kind: fusedStatementOK, result: fusedFiveFieldResult},
		{name: "ok duplicate result last wins", body: `{"status":"ok","result":null,"result":` + fusedFiveFieldResult + `}`, valid: true, kind: fusedStatementOK, result: fusedFiveFieldResult},
		{name: "ok duplicate result last null", body: `{"status":"ok","result":` + fusedFiveFieldResult + `,"result":null}`, valid: false},
		{name: "ok status null keeps earlier value", body: `{"status":"ok","status":null,"result":` + fusedFiveFieldResult + `}`, valid: true, kind: fusedStatementOK, result: fusedFiveFieldResult},
		{name: "ok unknown key", body: `{"status":"ok","result":` + fusedFiveFieldResult + `,"extra":1}`, valid: false},
		{name: "ok trailing content", body: fusedOKReply(fusedFiveFieldResult) + `{}`, valid: false},
		{name: "ok trailing whitespace", body: fusedOKReply(fusedFiveFieldResult) + " \n\t", valid: true, kind: fusedStatementOK, result: fusedFiveFieldResult},
		{name: "requires intent duplicate reason last wins", body: `{"status":"requires_intent","reason":"connection_control","reason":"engine_writable"}`, valid: true, kind: fusedStatementRequiresIntent, reason: guardedEngineWritable},
		{name: "requires intent result null", body: `{"status":"requires_intent","reason":"engine_writable","result":null}`, valid: false},
		{name: "requires intent reason null", body: `{"status":"requires_intent","reason":null}`, valid: false},
		{name: "requires intent unknown reason", body: `{"status":"requires_intent","reason":"other"}`, valid: false},
		{name: "error duplicate message last wins", body: `{"status":"error","error":"one","error":"two"}`, valid: true, kind: fusedStatementError, message: "two"},
		{name: "error empty message", body: `{"status":"error","error":""}`, valid: false},
		{name: "error null", body: `{"status":"error","error":null}`, valid: false},
		{name: "error completed true", body: `{"status":"error","error":"boom","execution_completed":true}`, valid: true, kind: fusedStatementError, message: "boom", completed: &completedTrue},
		{name: "error completed false", body: `{"status":"error","error":"boom","execution_completed":false}`, valid: true, kind: fusedStatementError, message: "boom", completed: &completedFalse},
		{name: "error completed null", body: `{"status":"error","error":"boom","execution_completed":null}`, valid: true, kind: fusedStatementError, message: "boom"},
		{name: "error completed string", body: `{"status":"error","error":"boom","execution_completed":"true"}`, valid: false},
		{name: "ok completed null", body: `{"status":"ok","execution_completed":null,"result":` + fusedFiveFieldResult + `}`, valid: true, kind: fusedStatementOK, result: fusedFiveFieldResult},
		{name: "ok completed false", body: `{"status":"ok","execution_completed":false,"result":` + fusedFiveFieldResult + `}`, valid: false},
		{name: "ok with metrics", body: fusedOKReply(`{"cols":[{"name":"id","decltype":"INTEGER"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":1}`), valid: true, kind: fusedStatementOK},
		{name: "metrics partial", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0}`), valid: false},
		{name: "metrics null counter", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":null,"rows_written":0,"query_duration_ms":1}`), valid: false},
		{name: "metrics negative duration", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":-1}`), valid: false},
		{name: "metrics negative zero duration", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":-0}`), valid: true, kind: fusedStatementOK},
		{name: "metrics infinite duration", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true,"rows_read":1,"rows_written":0,"query_duration_ms":1e400}`), valid: false},
		{name: "negative affected", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":-1,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "null affected", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":null,"last_insert_rowid":null,"autocommit":true}`), valid: true, kind: fusedStatementOK},
		{name: "row width mismatch", body: fusedOKReply(`{"cols":[{"name":"a","decltype":null}],"rows":[[1,2]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "rows checked then cols grows", body: fusedOKReply(`{"cols":[],"rows":[[1]],"cols":[{"name":"a","decltype":null}],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: true, kind: fusedStatementOK},
		{name: "rows checked then cols shrinks", body: fusedOKReply(`{"cols":[{"name":"a","decltype":null}],"rows":[[1]],"cols":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "duplicate rows last valid", body: fusedOKReply(`{"cols":[],"rows":[[1]],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: true, kind: fusedStatementOK},
		{name: "duplicate rows last invalid", body: fusedOKReply(`{"cols":[],"rows":[],"rows":[[1]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "null row is zero width", body: fusedOKReply(`{"cols":[],"rows":[null],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: true, kind: fusedStatementOK},
		{name: "column three keys", body: fusedOKReply(`{"cols":[{"name":"a","decltype":null,"x":1}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "column missing decltype", body: fusedOKReply(`{"cols":[{"name":"a"}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "column null decltype", body: fusedOKReply(`{"cols":[{"name":"a","decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: true, kind: fusedStatementOK},
		{name: "column number decltype", body: fusedOKReply(`{"cols":[{"name":"a","decltype":1}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "column null name", body: fusedOKReply(`{"cols":[{"name":null,"decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: true, kind: fusedStatementOK},
		{name: "column number name", body: fusedOKReply(`{"cols":[{"name":1,"decltype":null}],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`), valid: false},
		{name: "autocommit number", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":1}`), valid: false},
		{name: "autocommit quoted", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null,"autocommit":"true"}`), valid: false},
		{name: "rowid fractional", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":1.0,"autocommit":true}`), valid: false},
		{name: "rowid missing", body: fusedOKReply(`{"cols":[],"rows":[],"affected_row_count":0,"autocommit":true}`), valid: false},
		{name: "status number", body: `{"status":1}`, valid: false},
		{name: "status null", body: `{"status":null}`, valid: false},
		{name: "empty object", body: `{}`, valid: false},
	} {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			answer, valid := scanFusedStatementReply([]byte(scenario.body))
			if valid != scenario.valid {
				t.Fatalf("valid = %v, want %v for %s", valid, scenario.valid, scenario.body)
			}
			if !valid {
				return
			}
			if answer.kind != scenario.kind || answer.reason != scenario.reason || answer.message != scenario.message {
				t.Fatalf("answer = %+v, want kind=%v reason=%q message=%q", answer, scenario.kind, scenario.reason, scenario.message)
			}
			if scenario.result != "" && string(answer.result) != scenario.result {
				t.Fatalf("result = %s, want %s", answer.result, scenario.result)
			}
			if (answer.executionCompleted == nil) != (scenario.completed == nil) {
				t.Fatalf("completed = %v, want %v", answer.executionCompleted, scenario.completed)
			}
			if answer.executionCompleted != nil && *answer.executionCompleted != *scenario.completed {
				t.Fatalf("completed = %v, want %v", *answer.executionCompleted, *scenario.completed)
			}
		})
	}
}

func TestFusedStringWordNeedsScan(t *testing.T) {
	scalar := func(word [8]byte) bool {
		for _, c := range word {
			if c == '"' || c == '\\' || c < 0x20 {
				return true
			}
		}
		return false
	}
	for value := 0; value < 256; value++ {
		for position := 0; position < 8; position++ {
			var word [8]byte
			for i := range word {
				word[i] = 'a'
			}
			word[position] = byte(value)
			if got, want := fusedStringWordNeedsScan(binary.LittleEndian.Uint64(word[:])), scalar(word); got != want {
				t.Fatalf("value %#x at position %d: swar=%v scalar=%v", value, position, got, want)
			}
		}
	}
	rng := rand.New(rand.NewSource(0x48))
	for i := 0; i < 100000; i++ {
		var word [8]byte
		for j := range word {
			word[j] = byte(rng.Intn(256))
		}
		if got, want := fusedStringWordNeedsScan(binary.LittleEndian.Uint64(word[:])), scalar(word); got != want {
			t.Fatalf("random word %x: swar=%v scalar=%v", word, got, want)
		}
	}
}

func TestFusedScanStringMatchesEncodingJSON(t *testing.T) {
	samples := []string{
		`""`,
		`"plain"`,
		`"issue28/00000/payload"`,
		`"a\"b"`,
		`"a\\b"`,
		`"a\/b"`,
		`"a\nb"`,
		`"\u0041\u017f\ud800"`,
		`"\u0000"`,
		"\"a\x7fb\"",
		"\"a\xffb\"", // invalid UTF-8 is accepted by encoding/json and replaced when decoded
		"\"a\nb\"",   // unescaped control byte
		`"a\x6b"`,    // invalid escape
		`"a\u12"`,    // short unicode escape
		`"unterminated`,
	}
	for _, sample := range samples {
		scanner := fusedReplyScanner{data: []byte(sample)}
		_, _, scanned := scanner.scanString()
		var decoded string
		decodedByJSON := json.Unmarshal([]byte(sample), &decoded) == nil
		if scanned != decodedByJSON {
			t.Fatalf("scanString(%q) = %v, encoding/json = %v", sample, scanned, decodedByJSON)
		}
		if scanned && scanner.pos != len(sample) {
			t.Fatalf("scanString(%q) stopped at %d, want %d", sample, scanner.pos, len(sample))
		}
	}
}

func TestScanFusedReplyDepthLimit(t *testing.T) {
	build := func(depth int) []byte {
		cell := strings.Repeat("[", depth) + strings.Repeat("]", depth)
		result := `{"cols":[{"name":"x","decltype":null}],"rows":[[` + cell + `]],"affected_row_count":0,"last_insert_rowid":null,"autocommit":true}`
		return []byte(fusedOKReply(result))
	}
	// The envelope object, the result object, the rows array and the row array
	// are four containers; the cell contributes the rest of the nesting.
	for _, scenario := range []struct {
		name   string
		depth  int
		wantOK bool
	}{
		{"at limit", fusedMaxNestingDepth - 4, true},
		{"over limit", fusedMaxNestingDepth - 3, false},
	} {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			sample := build(scenario.depth)
			if _, ok := scanFusedStatementReply(sample); ok != scenario.wantOK {
				t.Fatalf("scanner accepted = %v, want %v", ok, scenario.wantOK)
			}
			requireSameFusedAnswer(t, scenario.name, sample)
		})
	}
}

var fusedBenchmarkSink json.RawMessage

func BenchmarkScanFusedStatementReply(b *testing.B) {
	result := fusedBenchmarkStatementResult(b)
	reply := fusedReplyWithResult(result)
	b.SetBytes(int64(len(reply)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		answer, ok := scanFusedStatementReply(reply)
		if !ok || answer.kind != fusedStatementOK || len(answer.result) != len(result) {
			b.Fatal("benchmark payload was refused")
		}
		fusedBenchmarkSink = answer.result
	}
}

func BenchmarkParseFusedStatementAnswerLegacy(b *testing.B) {
	result := fusedBenchmarkStatementResult(b)
	reply := fusedReplyWithResult(result)
	b.SetBytes(int64(len(reply)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		answer, ok := parseFusedStatementAnswerLegacy(reply)
		if !ok || answer.kind != fusedStatementOK || len(answer.result) != len(result) {
			b.Fatal("benchmark payload was refused")
		}
		fusedBenchmarkSink = answer.result
	}
}
