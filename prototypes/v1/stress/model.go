package stress

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

type Account struct {
	ID, Group, Score, Balance int64
	Payload                   string
}
type Mutation struct {
	OperationID string `json:"business_operation_id"`
	Ordinal     int    `json:"ordinal"`
	Account     int64  `json:"account"`
	Delta       int64  `json:"delta"`
}

type Task struct {
	Query     bool
	SQL       string
	Expected  [][]any
	Mutations []Mutation
	Flow      string
	PlanIndex string
}

type Model struct {
	Prefix   string
	Accounts []Account
	Large    string
}

func NewModel(prefix string, rows int, seed int64) *Model {
	model := &Model{Prefix: prefix, Accounts: make([]Account, rows)}
	for index := range model.Accounts {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", seed, index+1)))
		score := int64(digest[0])<<8 | int64(digest[1])
		model.Accounts[index] = Account{ID: int64(index + 1), Group: int64((index + 1) % 97), Score: score, Balance: 100000, Payload: hex.EncodeToString(digest[:])}
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("large:%d", seed)))
	model.Large = strings.Repeat(hex.EncodeToString(digest[:]), 256)
	return model
}

func (model *Model) Table(name string) string { return model.Prefix + "_" + name }
func (account Account) ReadRow() []any {
	return []any{account.ID, account.Group, account.Score, account.Payload}
}

func (model *Model) Task(kind, operation string, random *rand.Rand) Task {
	account := model.Accounts[random.Intn(len(model.Accounts))]
	table := model.Table("accounts")
	columns := "id,grp,score,payload"
	switch kind {
	case "mixed_oltp":
		if random.Intn(100) < 70 {
			return model.Task("point_read", operation, random)
		}
		return model.write(operation, account.ID, int64(random.Intn(9)-4), 1)
	case "hot_keys":
		return model.write(operation, int64(1+random.Intn(4)), 1, 1)
	case "bulk_tx":
		return model.write(operation, account.ID, 1, 8)
	case "sustained_wal":
		return model.write(operation, account.ID, 1, 1)
	case "index_range":
		minimum := int64(random.Intn(65536))
		filtered := make([]Account, 0)
		for _, candidate := range model.Accounts {
			if candidate.Group == account.Group && candidate.Score >= minimum {
				filtered = append(filtered, candidate)
			}
		}
		sort.Slice(filtered, func(first, second int) bool {
			if filtered[first].Score == filtered[second].Score {
				return filtered[first].ID < filtered[second].ID
			}
			return filtered[first].Score < filtered[second].Score
		})
		if len(filtered) > 20 {
			filtered = filtered[:20]
		}
		expected := make([][]any, 0, len(filtered))
		for _, candidate := range filtered {
			expected = append(expected, candidate.ReadRow())
		}
		return Task{Query: true, SQL: fmt.Sprintf("SELECT %s FROM %s WHERE grp=%d AND score>=%d ORDER BY score,id LIMIT 20", columns, table, account.Group, minimum), Expected: expected}
	case "pagination":
		offset := random.Intn(len(model.Accounts) - 20 + 1)
		expected := make([][]any, 0, 20)
		for _, candidate := range model.Accounts[offset : offset+20] {
			expected = append(expected, candidate.ReadRow())
		}
		return Task{Query: true, SQL: fmt.Sprintf("SELECT %s FROM %s ORDER BY id LIMIT 20 OFFSET %d", columns, table, offset), Expected: expected}
	case "sort_aggregate_join":
		expected := make([][]any, 97)
		for group := 0; group < 97; group++ {
			var count, total int64
			for _, candidate := range model.Accounts {
				if candidate.Group == int64(group) {
					count++
					total += candidate.Score
				}
			}
			expected[group] = []any{int64(group), count, total}
		}
		return Task{Query: true, SQL: fmt.Sprintf("SELECT a.grp,count(*),sum(a.score) FROM %s a JOIN %s d ON a.grp=d.id GROUP BY a.grp ORDER BY a.grp", table, model.Table("dimension")), Expected: expected}
	case "index_plan":
		return Task{Query: true, SQL: fmt.Sprintf("EXPLAIN QUERY PLAN SELECT id FROM %s WHERE grp=%d AND score>100", table, account.Group), PlanIndex: model.Table("group_index")}
	case "large_values":
		return Task{Query: true, SQL: "SELECT payload FROM " + model.Table("large") + " WHERE id=1", Expected: [][]any{{model.Large}}}
	case "rollback":
		return Task{Flow: "rollback", SQL: fmt.Sprintf("BEGIN; INSERT INTO %s(id) VALUES('%s'); ROLLBACK", model.Table("probe"), operation), Expected: [][]any{{int64(0)}}}
	default:
		return Task{Query: true, SQL: fmt.Sprintf("SELECT %s FROM %s WHERE id=%d", columns, table, account.ID), Expected: [][]any{account.ReadRow()}}
	}
}

func (model *Model) write(operation string, account, delta int64, count int) Task {
	var sql strings.Builder
	sql.WriteString("BEGIN IMMEDIATE;")
	mutations := make([]Mutation, count)
	for ordinal := range mutations {
		identity := (account-1+int64(ordinal))%int64(len(model.Accounts)) + 1
		mutations[ordinal] = Mutation{OperationID: operation, Ordinal: ordinal, Account: identity, Delta: delta}
		fmt.Fprintf(&sql, "INSERT INTO %s(op_id,ordinal,account,delta) VALUES('%s',%d,%d,%d); UPDATE %s SET balance=balance+(%d) WHERE id=%d;", model.Table("ledger"), operation, ordinal, identity, delta, model.Table("accounts"), delta, identity)
	}
	sql.WriteString("COMMIT")
	return Task{SQL: sql.String(), Mutations: mutations}
}

func HashRows(rows [][]any) string {
	encoded, _ := json.Marshal(rows)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (task Task) Check(rows [][]any) bool {
	if task.PlanIndex != "" {
		for _, row := range rows {
			for _, value := range row {
				if text, found := value.(string); found && strings.Contains(text, task.PlanIndex) && strings.Contains(strings.ToUpper(text), "USING") {
					return true
				}
			}
		}
		return false
	}
	if task.Expected == nil {
		return true
	}
	return HashRows(rows) == HashRows(task.Expected)
}
