package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type p5VerifyLostACK struct {
	mutex  sync.Mutex
	grants [][]byte
	lost   atomic.Bool
	server *httptest.Server
}

func p5VerifyGrantACKProxy(test *testing.T, cluster *p5VerifyCluster) (*p5VerifyLostACK, ControlTransport) {
	test.Helper()
	proxy := &p5VerifyLostACK{}
	upstream := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	client := &http.Client{Transport: upstream, Timeout: 5 * time.Second}
	proxy.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		payload, err := io.ReadAll(io.LimitReader(request.Body, 32*1024*1024+1))
		if err != nil || len(payload) > 32*1024*1024 {
			http.Error(response, "proxy input unavailable", 502)
			return
		}
		var envelope struct {
			Command struct {
				Command string `json:"command"`
			} `json:"command"`
		}
		if json.Unmarshal(payload, &envelope) != nil {
			http.Error(response, "proxy protocol unavailable", 502)
			return
		}
		grant := envelope.Command.Command == "grant"
		if grant {
			proxy.mutex.Lock()
			proxy.grants = append(proxy.grants, append([]byte{}, payload...))
			proxy.mutex.Unlock()
		}
		ctx, cancel := context.WithTimeout(request.Context(), 5*time.Second)
		defer cancel()
		forward, err := http.NewRequestWithContext(ctx, http.MethodPost, cluster.authority.address+"/", bytes.NewReader(payload))
		if err != nil {
			http.Error(response, "proxy request unavailable", 502)
			return
		}
		forward.Header.Set("Authorization", request.Header.Get("Authorization"))
		forward.Header.Set("Content-Type", "application/json")
		actual, err := client.Do(forward)
		if err != nil {
			http.Error(response, "proxy authority unavailable", 502)
			return
		}
		defer actual.Body.Close()
		body, err := io.ReadAll(io.LimitReader(actual.Body, 32*1024*1024+1))
		if err != nil || len(body) > 32*1024*1024 {
			http.Error(response, "proxy response unavailable", 502)
			return
		}
		var outcome struct {
			OK bool `json:"ok"`
		}
		if grant && actual.StatusCode == 200 && json.Unmarshal(body, &outcome) == nil && outcome.OK && proxy.lost.CompareAndSwap(false, true) {
			connection, _, err := response.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(actual.StatusCode)
		_, _ = response.Write(body)
	}))
	test.Cleanup(func() { proxy.server.Close(); upstream.CloseIdleConnections() })
	credentials := map[string]string{}
	for identity, worker := range cluster.workers {
		credentials[identity] = worker.controlToken
	}
	transport, err := NewTransport(proxy.server.URL, cluster.authorityToken, credentials)
	if err != nil {
		test.Fatal(err)
	}
	return proxy, transport
}

func (proxy *p5VerifyLostACK) requests() [][]byte {
	proxy.mutex.Lock()
	defer proxy.mutex.Unlock()
	result := make([][]byte, len(proxy.grants))
	for index, payload := range proxy.grants {
		result[index] = append([]byte{}, payload...)
	}
	return result
}

func TestP5VerifyRealGrantSocketLostACKRestartsAndReplaysExactDurablePending(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	directory := filepath.Join(cluster.root, "controller-state")
	store := p5VerifyStore(test, directory)
	cluster.register(store)
	proxy, transport := p5VerifyGrantACKProxy(test, cluster)
	control := p5VerifyController(test, store, transport)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	database := p5VerifyDatabase("grant-lost-ack")
	_, err := control.Place(ctx, database, "local", "writer")
	var unknown *RPCError
	if !errors.As(err, &unknown) || !unknown.Ambiguous || unknown.Rejected || !proxy.lost.Load() {
		test.Fatalf("actual socket-close grant was not uncertain: %v", err)
	}
	pending, err := control.Status(database)
	if err != nil || pending.Phase != "grant_pending" || pending.PendingID == "" || len(pending.PendingCommand) == 0 {
		test.Fatal("lost grant ACK discarded durable exact pending intent")
	}
	first := proxy.requests()
	if len(first) != 1 {
		test.Fatal("transport automatically retried an uncertain grant")
	}
	actual := p5VerifyActual(test, cluster, database)
	if actual.Binding == nil || actual.Epoch != pending.Epoch || actual.Binding.GrantID != pending.PendingID {
		test.Fatal("authority had not actually committed the lost-ACK grant")
	}
	if err := store.Close(); err != nil {
		test.Fatal(err)
	}
	reopened := p5VerifyStore(test, directory)
	restarted := p5VerifyController(test, reopened, transport)
	loaded, err := restarted.Status(database)
	if err != nil || loaded.PendingID != pending.PendingID || !bytes.Equal(loaded.PendingCommand, pending.PendingCommand) || loaded.Epoch != pending.Epoch {
		test.Fatal("restart changed the uncertain durable payload/identity/epoch")
	}
	if err := restarted.Reconcile(ctx); err != nil {
		test.Fatalf("exact pending receipt replay failed: %v", err)
	}
	settled, err := restarted.Status(database)
	requests := proxy.requests()
	if err != nil || settled.Phase != "active" || settled.Epoch != pending.Epoch || settled.PendingID != "" || len(requests) != 2 || !bytes.Equal(requests[0], requests[1]) {
		test.Fatal("recovery did not replay exact wire bytes once without epoch replacement")
	}
	session := cluster.open(settled.DesiredWorker, database)
	cluster.execute(settled.DesiredWorker, session, "CREATE TABLE records(id INTEGER PRIMARY KEY); INSERT INTO records VALUES(4)")
	p5VerifyRequireRows(test, cluster.rows(settled.DesiredWorker, session, "SELECT id FROM records"), "[[4]]")
	test.Logf("real Authority grant%d committed then proxy closed socket before ACK; reopened metadata replayed identical wire payload/requestID, same epoch, actual SQL writable", settled.Epoch)
}

type p5VerifyEpochFailureStore struct {
	MetadataStore
	failed atomic.Bool
}

func (store *p5VerifyEpochFailureStore) Update(change func(*Metadata) error) error {
	return store.MetadataStore.Update(func(candidate *Metadata) error {
		previous := candidate.Epoch
		if err := change(candidate); err != nil {
			return err
		}
		if candidate.Epoch > previous && store.failed.CompareAndSwap(false, true) {
			return errors.New("independent pre-commit epoch allocation failure")
		}
		return nil
	})
}

func TestP5VerifyMetadataAllocationFailureNeverIssuesActualGrant(test *testing.T) {
	cluster := p5VerifyNewCluster(test)
	store := p5VerifyStore(test, filepath.Join(cluster.root, "controller-state"))
	cluster.register(store)
	fault := &p5VerifyEpochFailureStore{MetadataStore: store}
	control := p5VerifyController(test, fault, cluster.transport)
	ctx, cancel := p5VerifyContext()
	defer cancel()
	database := p5VerifyDatabase("metadata-failure")
	if _, err := control.Place(ctx, database, "local", "writer"); err == nil || !fault.failed.Load() {
		test.Fatal("allocation persistence failure was not surfaced")
	}
	metadata, err := store.Snapshot()
	if err != nil || metadata.Epoch != 0 || len(metadata.Placements) != 0 {
		test.Fatal("failed allocation partially published durable placement")
	}
	actual := p5VerifyActual(test, cluster, database)
	if actual.Epoch != 0 || actual.Binding != nil {
		test.Fatal("Rust grant preceded successful durable epoch allocation")
	}
	placed, err := control.Place(ctx, database, "local", "writer")
	if err != nil || placed.Phase != "active" || placed.Epoch != 1 {
		test.Fatalf("safe allocation retry did not recover: %v", err)
	}
	test.Log("real authority remained unowned after failed metadata allocation; next successfully persisted epoch1 acquired")
}
