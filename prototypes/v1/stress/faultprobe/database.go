package faultprobe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type Placement struct {
	Database      DatabaseID `json:"database"`
	DesiredWorker string     `json:"desired_worker"`
	Epoch         uint64     `json:"epoch"`
	Phase         string     `json:"phase"`
	TTLMS         uint64     `json:"ttl_ms"`
	Grant         *struct {
		Binding struct {
			WorkerID        string `json:"worker_id"`
			Epoch           uint64 `json:"epoch"`
			RenewalSequence uint64 `json:"renewal_sequence"`
			Role            string `json:"role"`
		} `json:"binding"`
	} `json:"grant"`
}

func (client *Client) Status(ctx context.Context, database DatabaseID) (Placement, *Failure) {
	body, failure := client.call(ctx, "/v1/databases/status", map[string]any{"database": database})
	if failure != nil {
		return Placement{}, failure
	}
	var snapshot struct {
		Placement Placement `json:"placement"`
	}
	if json.Unmarshal(body, &snapshot) != nil || snapshot.Placement.Database != database || snapshot.Placement.Epoch == 0 {
		return Placement{}, &Failure{Code: "placement_identity_unproven", Unknown: true}
	}
	return snapshot.Placement, nil
}

func writerPlacement(placement Placement) bool {
	return placement.Phase == "active" && placement.Grant != nil && placement.Grant.Binding.WorkerID == placement.DesiredWorker && placement.Grant.Binding.Epoch == placement.Epoch && placement.Grant.Binding.Role == "writer"
}

func awaitEpoch(ctx context.Context, management *Client, database DatabaseID, previous Placement, timeout time.Duration) (Placement, *Failure) {
	limit, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		current, failure := management.Status(limit, database)
		if failure != nil {
			return Placement{}, failure
		}
		if current.Epoch < previous.Epoch {
			return Placement{}, &Failure{Code: "epoch_regression"}
		}
		if current.Epoch > previous.Epoch && current.DesiredWorker != previous.DesiredWorker && writerPlacement(current) {
			return current, nil
		}
		select {
		case <-limit.Done():
			return Placement{}, &Failure{Code: "new_owner_epoch_not_observed", Unknown: true}
		case <-time.After(200 * time.Millisecond):
		}
	}
}

type ACK struct {
	Sequence   int    `json:"sequence"`
	BusinessID string `json:"business_id"`
	Delta      int64  `json:"delta"`
}

type Fixture struct {
	Database DatabaseID
	Prefix   string
	ACKs     []ACK
	log      *Log
}

func (fixture *Fixture) Setup(ctx context.Context, session *Session) *Failure {
	sql := fmt.Sprintf("CREATE TABLE %s_account(id INTEGER PRIMARY KEY,balance INTEGER NOT NULL); INSERT INTO %s_account VALUES(1,100000); CREATE TABLE %s_ledger(seq INTEGER PRIMARY KEY,business_id TEXT NOT NULL UNIQUE,delta INTEGER NOT NULL)", fixture.Prefix, fixture.Prefix, fixture.Prefix)
	_, failure := session.SQL(ctx, false, sql)
	return failure
}

func (fixture *Fixture) Write(ctx context.Context, session *Session) *Failure {
	sequence := len(fixture.ACKs) + 1
	ack := ACK{Sequence: sequence, BusinessID: fmt.Sprintf("%s_business_%04d", fixture.Prefix, sequence), Delta: int64(sequence%7) - 3}
	sql := fmt.Sprintf("BEGIN IMMEDIATE; INSERT INTO %s_ledger VALUES(%d,'%s',%d); UPDATE %s_account SET balance=balance+(%d) WHERE id=1; COMMIT", fixture.Prefix, sequence, ack.BusinessID, ack.Delta, fixture.Prefix, ack.Delta)
	if _, failure := session.SQL(ctx, false, sql); failure != nil {
		return failure
	}
	fixture.ACKs = append(fixture.ACKs, ack)
	if err := fixture.log.Record(map[string]any{"type": "writer_ack", "database": fixture.Database, "ack": ack, "observed_at": time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		return &Failure{Code: "ack_evidence_failed", Unknown: true}
	}
	return nil
}

func hashRows(rows [][]any) string {
	encoded, _ := json.Marshal(rows)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (fixture *Fixture) Verify(ctx context.Context, session *Session) *Failure {
	rows, failure := session.SQL(ctx, true, fmt.Sprintf("SELECT seq,business_id,delta FROM %s_ledger ORDER BY seq LIMIT 34", fixture.Prefix))
	if failure != nil {
		return failure
	}
	expected := make([][]any, 0, len(fixture.ACKs))
	balance := int64(100000)
	for _, ack := range fixture.ACKs {
		expected = append(expected, []any{ack.Sequence, ack.BusinessID, ack.Delta})
		balance += ack.Delta
	}
	if hashRows(rows) != hashRows(expected) {
		return &Failure{Code: "ack_ledger_mismatch"}
	}
	balances, failure := session.SQL(ctx, true, fmt.Sprintf("SELECT id,balance FROM %s_account ORDER BY id LIMIT 2", fixture.Prefix))
	if failure != nil {
		return failure
	}
	if hashRows(balances) != hashRows([][]any{{1, balance}}) {
		return &Failure{Code: "ack_balance_mismatch"}
	}
	integrity, failure := session.SQL(ctx, true, "PRAGMA integrity_check")
	if failure != nil {
		return failure
	}
	if hashRows(integrity) != hashRows([][]any{{"ok"}}) {
		return &Failure{Code: "integrity_check_failed"}
	}
	if err := fixture.log.Record(map[string]any{"type": "recovery_oracle", "database": fixture.Database, "acked": len(fixture.ACKs), "expected_ledger_hash": hashRows(expected), "actual_ledger_hash": hashRows(rows), "balance": balance, "integrity": integrity}); err != nil {
		return &Failure{Code: "oracle_evidence_failed"}
	}
	return nil
}
