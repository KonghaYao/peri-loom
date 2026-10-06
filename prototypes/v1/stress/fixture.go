package stress

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Fixture struct {
	Target    Target
	Client    *Client
	Model     *Model
	Acked     []Mutation
	Artifacts *Artifacts
}

type Correctness struct {
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Accounts           int    `json:"accounts"`
	LedgerRows         int    `json:"ledger_rows"`
	ExpectedHash       string `json:"expected_hash,omitempty"`
	ActualHash         string `json:"actual_hash,omitempty"`
	ExpectedAccountSum int64  `json:"expected_account_sum"`
	ActualAccountSum   int64  `json:"actual_account_sum"`
	ExpectedLedgerHash string `json:"expected_ledger_hash,omitempty"`
	ActualLedgerHash   string `json:"actual_ledger_hash,omitempty"`
}

func (fixture *Fixture) Setup(ctx context.Context) (failure *Failure) {
	session, failure := fixture.Client.Open(ctx, fixture.Target.Database)
	if failure != nil {
		return failure
	}
	defer finishSession(session, &failure)
	model := fixture.Model
	ddl := fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY,grp INTEGER NOT NULL,score INTEGER NOT NULL,balance INTEGER NOT NULL,payload TEXT NOT NULL); CREATE INDEX %s ON %s(grp,score,id); CREATE TABLE %s(op_id TEXT NOT NULL,ordinal INTEGER NOT NULL,account INTEGER NOT NULL,delta INTEGER NOT NULL,PRIMARY KEY(op_id,ordinal)); CREATE TABLE %s(id INTEGER PRIMARY KEY); CREATE TABLE %s(id INTEGER PRIMARY KEY,payload TEXT NOT NULL); CREATE TABLE %s(id TEXT PRIMARY KEY)",
		model.Table("accounts"), model.Table("group_index"), model.Table("accounts"), model.Table("ledger"), model.Table("dimension"), model.Table("large"), model.Table("probe"))
	if _, _, failure = session.SQL(ctx, false, ddl); failure != nil {
		return failure
	}
	for start := 0; start < len(model.Accounts); start += 100 {
		stop := start + 100
		if stop > len(model.Accounts) {
			stop = len(model.Accounts)
		}
		var sql strings.Builder
		fmt.Fprintf(&sql, "BEGIN IMMEDIATE; INSERT INTO %s VALUES", model.Table("accounts"))
		for index, account := range model.Accounts[start:stop] {
			if index > 0 {
				sql.WriteByte(',')
			}
			fmt.Fprintf(&sql, "(%d,%d,%d,%d,'%s')", account.ID, account.Group, account.Score, account.Balance, account.Payload)
		}
		sql.WriteString("; COMMIT")
		if _, _, failure = session.SQL(ctx, false, sql.String()); failure != nil {
			return failure
		}
	}
	var sql strings.Builder
	fmt.Fprintf(&sql, "BEGIN IMMEDIATE; INSERT INTO %s VALUES", model.Table("dimension"))
	for group := 0; group < 97; group++ {
		if group > 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "(%d)", group)
	}
	fmt.Fprintf(&sql, "; INSERT INTO %s VALUES(1,'%s'); COMMIT", model.Table("large"), model.Large)
	if _, _, failure = session.SQL(ctx, false, sql.String()); failure != nil {
		return failure
	}
	return session.Close(ctx)
}

func (fixture *Fixture) Verify(ctx context.Context) (result Correctness, failure *Failure) {
	result = Correctness{Status: "FAIL", Accounts: len(fixture.Model.Accounts), LedgerRows: len(fixture.Acked)}
	session, failure := fixture.Client.Open(ctx, fixture.Target.Database)
	if failure != nil {
		result.Reason = failure.Code
		return result, failure
	}
	defer func() {
		finishSession(session, &failure)
		if failure != nil && failure.Unknown {
			result.Status, result.Reason = "UNRESOLVED", failure.Code
		}
	}()
	model := fixture.Model
	deltas := make(map[int64]int64)
	ledger := make([][]any, 0, len(fixture.Acked))
	for _, mutation := range fixture.Acked {
		deltas[mutation.Account] += mutation.Delta
		ledger = append(ledger, []any{mutation.OperationID, int64(mutation.Ordinal), mutation.Account, mutation.Delta})
	}
	sort.Slice(ledger, func(first, second int) bool {
		if ledger[first][0] == ledger[second][0] {
			return ledger[first][1].(int64) < ledger[second][1].(int64)
		}
		return ledger[first][0].(string) < ledger[second][0].(string)
	})
	expected := make([][]any, 0, len(model.Accounts))
	for _, account := range model.Accounts {
		balance := account.Balance + deltas[account.ID]
		result.ExpectedAccountSum += balance
		expected = append(expected, []any{account.ID, account.Group, account.Score, balance, account.Payload})
	}
	actual, failure := paged(ctx, session, "id,grp,score,balance,payload", model.Table("accounts"), "id", len(expected)+1)
	if failure != nil {
		result.Reason = failure.Code
		return result, failure
	}
	result.ExpectedHash, result.ActualHash = HashRows(expected), HashRows(actual)
	for _, row := range actual {
		if len(row) != 5 {
			result.Reason = "invalid_account_shape"
			return result, nil
		}
		balance, valid := integer(row[3])
		if !valid {
			result.Reason = "invalid_account_balance"
			return result, nil
		}
		result.ActualAccountSum += balance
	}
	actualLedger, failure := paged(ctx, session, "op_id,ordinal,account,delta", model.Table("ledger"), "op_id,ordinal", len(ledger)+1)
	if failure != nil {
		result.Reason = failure.Code
		return result, failure
	}
	result.ExpectedLedgerHash, result.ActualLedgerHash = HashRows(ledger), HashRows(actualLedger)
	if result.ExpectedHash != result.ActualHash || result.ExpectedAccountSum != result.ActualAccountSum || HashRows(ledger) != HashRows(actualLedger) {
		result.Reason = "rows_hash_account_sum_or_write_ledger_mismatch"
	} else {
		result.Status = "PASS"
	}
	if failure = session.Close(ctx); failure != nil {
		result.Status, result.Reason = "UNRESOLVED", failure.Code
	}
	return result, failure
}

func finishSession(session *Session, failure **Failure) {
	if session.closed || session.poisoned {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if cleanup := session.Close(ctx); cleanup != nil && (*failure == nil || cleanup.Unknown) {
		*failure = cleanup
	}
}

func paged(ctx context.Context, session *Session, columns, table, order string, maximum int) ([][]any, *Failure) {
	rows := make([][]any, 0)
	for offset := 0; offset < maximum; offset += 256 {
		limit := maximum - offset
		if limit > 256 {
			limit = 256
		}
		page, _, failure := session.SQL(ctx, true, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d OFFSET %d", columns, table, order, limit, offset))
		if failure != nil {
			return nil, failure
		}
		rows = append(rows, page...)
		if len(page) < limit {
			break
		}
	}
	return rows, nil
}
