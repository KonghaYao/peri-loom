package stress

import (
	"context"
	"fmt"
	"time"
)

type constraintStep struct {
	Name      string   `json:"name"`
	RequestID string   `json:"request_id"`
	Acked     bool     `json:"acked"`
	Failure   *Failure `json:"failure,omitempty"`
	RowsHash  string   `json:"rows_hash,omitempty"`
}

func constraintErrorReuse(ctx context.Context, fixture *Fixture, artifacts *Artifacts) (failure *Failure) {
	started := time.Now()
	steps := []constraintStep{}
	var session *Session
	inTransaction := false
	operationContext := ctx
	step := func(name string, query bool, sql string) ([][]any, *Failure) {
		rows, identifier, current := session.SQL(operationContext, query, sql)
		entry := constraintStep{Name: name, RequestID: identifier, Acked: current == nil, Failure: current}
		if query && current == nil {
			entry.RowsHash = HashRows(rows)
		}
		steps = append(steps, entry)
		return rows, current
	}
	defer func() {
		if session != nil {
			if inTransaction && !session.poisoned {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				operationContext = cleanup
				_, rollbackFailure := step("cleanup_explicit_rollback", false, "ROLLBACK")
				cancel()
				if rollbackFailure != nil && (failure == nil || rollbackFailure.Unknown) {
					failure = rollbackFailure
				}
			}
			finishSession(session, &failure)
		}
		status := "PASS"
		if failure != nil {
			status = "FAIL"
			if failure.Unknown {
				status = "UNRESOLVED"
			}
		}
		if artifacts != nil {
			if err := artifacts.Record(map[string]any{"type": "constraint_error_reuse", "status": status,
				"observed_at": started.UTC().Format(time.RFC3339Nano), "elapsed_ns": time.Since(started).Nanoseconds(),
				"steps": steps, "failure": failure, "implicit_rollback_assumed": false}); err != nil && failure == nil {
				failure = &Failure{Class: "artifact_safety", Code: "constraint_proof_record_failed"}
			}
		}
	}()
	session, failure = fixture.Client.Open(ctx, fixture.Target.Database)
	if failure != nil {
		return failure
	}
	table := fixture.Model.Table("constraint_accounts")
	if _, failure = step("seed", false, fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY,balance INTEGER NOT NULL); INSERT INTO %s VALUES(1,100000)", table, table)); failure != nil {
		return failure
	}
	check := func(name string, expected int64, sql string) *Failure {
		rows, current := step(name, true, sql)
		if current != nil {
			return current
		}
		if HashRows(rows) != HashRows([][]any{{expected}}) {
			return &Failure{Class: "correctness", Code: name + "_mismatch"}
		}
		return nil
	}
	selectBalance := fmt.Sprintf("SELECT balance FROM %s WHERE id=1", table)
	if failure = check("seed_balance", 100000, selectBalance); failure != nil {
		return failure
	}
	if _, failure = step("begin", false, "BEGIN"); failure != nil {
		return failure
	}
	inTransaction = true
	if _, failure = step("update_in_transaction", false, fmt.Sprintf("UPDATE %s SET balance=balance+17 WHERE id=1", table)); failure != nil {
		return failure
	}
	_, negative := step("duplicate_primary_key", false, fmt.Sprintf("INSERT INTO %s VALUES(1,100000)", table))
	if negative == nil {
		return &Failure{Class: "correctness", Code: "duplicate_primary_key_unexpected_success"}
	}
	if negative.Unknown {
		return negative
	}
	if negative.Class != "sql_error" || negative.Code != "sql_error" || !negative.ExecutionCompleted || !negative.MayHaveCommitted {
		return &Failure{Class: "correctness", Code: "expected_terminal_sql_error_missing"}
	}
	inTransaction = false
	if _, failure = step("explicit_rollback", false, "ROLLBACK"); failure != nil {
		return failure
	}
	if failure = check("rolled_back_seed_balance", 100000, selectBalance); failure != nil {
		return failure
	}
	task := fixture.Model.write(fixture.Client.ids.Next(), 1, 1, 1)
	if _, failure = step("subsequent_ledger_write", false, task.SQL); failure != nil {
		return failure
	}
	fixture.Acked = append(fixture.Acked, task.Mutations...)
	expected := fixture.Model.Accounts[0].Balance
	for _, mutation := range fixture.Acked {
		if mutation.Account == 1 {
			expected += mutation.Delta
		}
	}
	return check("subsequent_query", expected, fmt.Sprintf("SELECT balance FROM %s WHERE id=1", fixture.Model.Table("accounts")))
}
