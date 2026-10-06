package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type p5VerifyWorker struct {
	process        *p5VerifyProcess
	worker         Worker
	controlToken   string
	sqlToken       string
	authorityToken string
	cache          string
}

type p5VerifyCluster struct {
	test           *testing.T
	binary         string
	root           string
	authority      *p5VerifyProcess
	authorityToken string
	workers        map[string]*p5VerifyWorker
	transport      ControlTransport
	tokens         []string
}

func p5VerifyNewCluster(test *testing.T) *p5VerifyCluster {
	test.Helper()
	cluster := &p5VerifyCluster{test: test, binary: p5VerifyBinary(test), root: test.TempDir(), authorityToken: p5VerifyToken(test), workers: make(map[string]*p5VerifyWorker)}
	cluster.tokens = append(cluster.tokens, cluster.authorityToken)
	workerCredentials := make(map[string]string)
	for _, identity := range []string{"verify-a", "verify-b"} {
		worker := &p5VerifyWorker{controlToken: p5VerifyToken(test), sqlToken: p5VerifyToken(test), authorityToken: p5VerifyToken(test), cache: filepath.Join(cluster.root, identity+"-cache")}
		worker.worker = Worker{ID: identity, Region: "local", Capacity: 8, Healthy: true, LastHeartbeatMS: time.Now().UnixMilli()}
		cluster.workers[identity] = worker
		workerCredentials[identity] = worker.authorityToken
		cluster.tokens = append(cluster.tokens, worker.controlToken, worker.sqlToken, worker.authorityToken)
	}
	configuration := filepath.Join(cluster.root, "synthetic-worker-auth.json")
	encoded, err := json.Marshal(workerCredentials)
	if err != nil {
		test.Fatal(err)
	}
	if err := os.WriteFile(configuration, encoded, 0600); err != nil {
		test.Fatal(err)
	}
	content, err := os.ReadFile(cluster.binary)
	if err != nil {
		test.Fatal(err)
	}
	digest := sha256.Sum256(content)
	test.Logf("Rust executable SHA256=%s", hex.EncodeToString(digest[:]))
	cluster.authority = p5VerifyStart(test, cluster.binary, []string{"authority", filepath.Join(cluster.root, "backend"), "127.0.0.1:0", filepath.Join(cluster.root, "authority-state")}, []string{"PERI_AUTHORITY_CONTROL_TOKEN=" + cluster.authorityToken, "PERI_AUTHORITY_WORKERS_FILE=" + configuration}, cluster.tokens)
	for _, identity := range []string{"verify-a", "verify-b"} {
		cluster.startWorker(identity)
	}
	credentials := map[string]string{}
	for identity, worker := range cluster.workers {
		credentials[identity] = worker.controlToken
	}
	cluster.transport, err = NewTransport(cluster.authority.address, cluster.authorityToken, credentials)
	if err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() {
		for _, worker := range cluster.workers {
			worker.process.kill()
			if diagnostics := worker.process.diagnostics.text(cluster.tokens); diagnostics != "" {
				test.Logf("worker %s bounded redacted stderr: %s", worker.worker.ID, diagnostics)
			}
		}
		cluster.authority.kill()
		if diagnostics := cluster.authority.diagnostics.text(cluster.tokens); diagnostics != "" {
			test.Logf("authority bounded redacted stderr: %s", diagnostics)
		}
	})
	return cluster
}

func (cluster *p5VerifyCluster) startWorker(identity string) {
	cluster.test.Helper()
	worker := cluster.workers[identity]
	worker.process = p5VerifyStart(cluster.test, cluster.binary, []string{"owned-worker", cluster.authority.address, worker.cache, identity, "127.0.0.1:0"}, []string{"PERI_WORKER_AUTHORITY_TOKEN=" + worker.authorityToken, "PERI_WORKER_CONTROL_TOKEN=" + worker.controlToken, "PERI_WORKER_SQL_TOKEN=" + worker.sqlToken}, cluster.tokens)
	worker.worker.Address = worker.process.address
	worker.worker.LastHeartbeatMS = time.Now().UnixMilli()
}

func (cluster *p5VerifyCluster) register(store MetadataStore) {
	cluster.test.Helper()
	for _, identity := range []string{"verify-a", "verify-b"} {
		if err := RegisterWorker(store, cluster.workers[identity].worker); err != nil {
			cluster.test.Fatal(err)
		}
	}
}

func (cluster *p5VerifyCluster) sql(identity string, command any) p5VerifyRPCReply {
	cluster.test.Helper()
	worker := cluster.workers[identity]
	reply, status, err := p5VerifyRPC(worker.process.address, "/sql", worker.sqlToken, command)
	if err != nil || status != 200 {
		cluster.test.Fatalf("worker %s SQL RPC failed status=%d err=%v; authority diagnostics=%s", identity, status, err, cluster.authority.diagnostics.text(cluster.tokens))
	}
	return reply
}

func (cluster *p5VerifyCluster) open(identity string, database DatabaseID) string {
	cluster.test.Helper()
	reply := cluster.sql(identity, map[string]any{"command": "open_session", "database": database})
	var result struct {
		Session  string `json:"session"`
		Readonly bool   `json:"readonly"`
	}
	if !reply.OK || json.Unmarshal(reply.Result, &result) != nil || result.Session == "" {
		cluster.test.Fatalf("worker %s session open refused", identity)
	}
	return result.Session
}

func (cluster *p5VerifyCluster) execute(identity, session, statement string) {
	cluster.test.Helper()
	reply := cluster.sql(identity, map[string]any{"command": "execute", "session": session, "sql": statement})
	if !reply.OK {
		cluster.test.Fatalf("worker %s execute refused", identity)
	}
}

func (cluster *p5VerifyCluster) rows(identity, session, statement string) json.RawMessage {
	cluster.test.Helper()
	reply := cluster.sql(identity, map[string]any{"command": "query", "session": session, "sql": statement})
	if !reply.OK {
		cluster.test.Fatalf("worker %s query refused", identity)
	}
	return reply.Result
}

func p5VerifyDatabase(name string) DatabaseID {
	return DatabaseID{Namespace: Namespace{Tenant: "independent-p5", Database: name}, Branch: "main"}
}

func p5VerifyContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 15*time.Second)
}
