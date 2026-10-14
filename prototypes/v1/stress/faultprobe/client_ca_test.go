package faultprobe

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const caPrivateKey = "private_ca_fixture_credential_0123456789"

func boundedCAClient(t *testing.T, path string) (*Client, error) {
	t.Helper()
	type outcome struct {
		client *Client
		err    error
	}
	completed := make(chan outcome, 1)
	go func() {
		connection := Connection{Endpoint: "https://127.0.0.1:9443", CAFile: path}
		client, err := newClient(connection, caPrivateKey, "ca_unit", DefaultConfig(), nil)
		completed <- outcome{client: client, err: err}
	}()
	select {
	case result := <-completed:
		return result.client, result.err
	case <-time.After(2 * time.Second):
		t.Fatal("CA preflight blocked without a bounded completion")
		return nil, nil
	}
}

func fixtureCAPEM(t *testing.T) []byte {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	certificate, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
}

func TestBoundedCARejectsActualFIFOWithoutWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), caPrivateKey+"-fifo")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	client, err := boundedCAClient(t, path)
	if client != nil || err == nil || err.Error() != "CA file unavailable or invalid" || strings.Contains(err.Error(), caPrivateKey) {
		t.Fatalf("FIFO accepted or error leaked its path: %v", err)
	}
}

func TestBoundedCARejectsSymlinkDeviceDirectoryAndOversize(t *testing.T) {
	directory := t.TempDir()
	valid := filepath.Join(directory, "valid.pem")
	if err := os.WriteFile(valid, fixtureCAPEM(t), 0600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(directory, caPrivateKey+"-symlink")
	if err := os.Symlink(valid, symlink); err != nil {
		t.Fatal(err)
	}
	oversize := filepath.Join(directory, "oversize.pem")
	file, err := os.OpenFile(oversize, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxCABytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"symlink": symlink, "device": "/dev/null", "directory": directory, "one_byte_over_limit": oversize, "missing": filepath.Join(directory, caPrivateKey+"-missing")} {
		t.Run(name, func(t *testing.T) {
			client, err := boundedCAClient(t, path)
			if client != nil || err == nil || err.Error() != "CA file unavailable or invalid" || strings.Contains(err.Error(), caPrivateKey) {
				t.Fatalf("unsafe CA accepted or unsanitized error: %v", err)
			}
		})
	}
}

func TestBoundedCAExactBoundaryAndTLSNoRetryPolicy(t *testing.T) {
	pemBytes := fixtureCAPEM(t)
	if len(pemBytes) >= maxCABytes {
		t.Fatal("certificate fixture exceeds test bound")
	}
	for _, size := range []int{len(pemBytes), maxCABytes, maxCABytes + 1} {
		path := filepath.Join(t.TempDir(), "certificate.pem")
		body := append(append([]byte{}, pemBytes...), bytes.Repeat([]byte("\n"), size-len(pemBytes))...)
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		client, err := boundedCAClient(t, path)
		if size > maxCABytes {
			if client != nil || err == nil {
				t.Fatal("valid PEM one byte beyond boundary accepted")
			}
			continue
		}
		if err != nil || client == nil {
			t.Fatalf("valid bounded PEM rejected at %d bytes: %v", size, err)
		}
		transport := client.http.Transport.(*http.Transport)
		if transport.TLSClientConfig.InsecureSkipVerify || transport.TLSClientConfig.RootCAs == nil || !transport.DisableKeepAlives || transport.Proxy != nil || client.http.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
			t.Fatal("TLS verification or no-retry transport policy changed")
		}
	}
}

func TestBoundedCAMalformedExactLimitRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), maxCABytes), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := boundedCAClient(t, path)
	if client != nil || err == nil || err.Error() != "CA file unavailable or invalid" {
		t.Fatal("malformed certificate at exact limit accepted")
	}
}
