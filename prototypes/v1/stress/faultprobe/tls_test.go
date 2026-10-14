package faultprobe

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHTTPSRequiresVerifiedCA(t *testing.T) {
	config, connection, _, _, log := unitFixture(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var input testRequest
		_ = json.NewDecoder(request.Body).Decode(&input)
		wireReply(writer, input, grantSnapshot(input.Database, 1, "worker-a"), "", 200)
	}))
	defer server.Close()
	connection.Endpoint = server.URL
	client, err := newClient(connection, connection.GlobalAPIKey, "untrusted", config, log)
	if err != nil {
		t.Fatal(err)
	}
	if _, failure := client.Status(context.Background(), connection.Database); failure == nil {
		t.Fatal("untrusted TLS accepted")
	}
	certificate, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	connection.CAFile = filepath.Join(config.RunDir, "gateway-ca.pem")
	if os.WriteFile(connection.CAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0600) != nil {
		t.Fatal("CA fixture")
	}
	client, err = newClient(connection, connection.GlobalAPIKey, "trusted", config, log)
	if err != nil {
		t.Fatal(err)
	}
	if _, failure := client.Status(context.Background(), connection.Database); failure != nil {
		t.Fatalf("verified TLS rejected: %v", failure)
	}
}

func TestCanceledFaultNeverSignals(t *testing.T) {
	config, _, topology, host, log := unitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	original := host.identities[topology.Processes["worker-a"].PID]
	if err := killOwned(ctx, config, topology, "worker-a", original, host, log); err == nil || host.killed() {
		t.Fatal("canceled probe sent signal")
	}
}
