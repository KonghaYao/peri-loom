package controller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type p5VerifyDiagnostics struct {
	mutex sync.Mutex
	bytes []byte
}

func (diagnostics *p5VerifyDiagnostics) Write(input []byte) (int, error) {
	diagnostics.mutex.Lock()
	defer diagnostics.mutex.Unlock()
	remaining := 64*1024 - len(diagnostics.bytes)
	if remaining > len(input) {
		remaining = len(input)
	}
	if remaining > 0 {
		diagnostics.bytes = append(diagnostics.bytes, input[:remaining]...)
	}
	return len(input), nil
}

func (diagnostics *p5VerifyDiagnostics) text(tokens []string) string {
	diagnostics.mutex.Lock()
	defer diagnostics.mutex.Unlock()
	result := string(diagnostics.bytes)
	for _, token := range tokens {
		result = strings.ReplaceAll(result, token, "[redacted]")
	}
	return result
}

type p5VerifyProcess struct {
	command     *exec.Cmd
	address     string
	diagnostics p5VerifyDiagnostics
	done        chan struct{}
	exit        error
}

func p5VerifyToken(test *testing.T) string {
	test.Helper()
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		test.Fatal(err)
	}
	return hex.EncodeToString(random[:])
}

func p5VerifyBinary(test *testing.T) string {
	test.Helper()
	if configured := os.Getenv("PERI_P5_RUST_BINARY"); configured != "" {
		absolute, err := filepath.Abs(configured)
		if err != nil {
			test.Fatal(err)
		}
		return absolute
	}
	directory, err := os.Getwd()
	if err != nil {
		test.Fatal(err)
	}
	for {
		candidate := filepath.Join(directory, "prototypes/v1/target/debug/peri-loom-v1")
		if information, err := os.Stat(candidate); err == nil && information.Mode().IsRegular() {
			return candidate
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			break
		}
		directory = parent
	}
	test.Fatal("actual Rust binary missing: build prototypes/v1 or set PERI_P5_RUST_BINARY to its executable")
	return ""
}

func p5VerifyStart(test *testing.T, binary string, arguments, environment, tokens []string) *p5VerifyProcess {
	test.Helper()
	process := &p5VerifyProcess{command: exec.Command(binary, arguments...), done: make(chan struct{})}
	process.command.Env = append([]string{}, environment...)
	process.command.Stderr = &process.diagnostics
	output, err := process.command.StdoutPipe()
	if err != nil {
		test.Fatal(err)
	}
	if err := process.command.Start(); err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { process.kill() })
	ready := make(chan []byte, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		if scanner.Scan() {
			ready <- append([]byte{}, scanner.Bytes()...)
		}
		for scanner.Scan() {
		}
		close(ready)
	}()
	go func() {
		process.exit = process.command.Wait()
		close(process.done)
	}()
	select {
	case line, available := <-ready:
		var response struct {
			Ready   bool   `json:"ready"`
			Service string `json:"service"`
			Address string `json:"address"`
		}
		if !available || json.Unmarshal(line, &response) != nil || !response.Ready || response.Address == "" || response.Service != arguments[0] {
			test.Fatalf("Rust process did not become ready: %s", process.diagnostics.text(tokens))
		}
		process.address = "http://" + response.Address
	case <-time.After(8 * time.Second):
		test.Fatalf("Rust process readiness timeout: %s", process.diagnostics.text(tokens))
	}
	test.Logf("actual Rust %s ready pid=%d endpoint=%s explicit_env_names=%v", arguments[0], process.command.Process.Pid, process.address, p5VerifyEnvNames(environment))
	return process
}

func p5VerifyEnvNames(environment []string) []string {
	names := make([]string, 0, len(environment))
	for _, entry := range environment {
		names = append(names, strings.SplitN(entry, "=", 2)[0])
	}
	return names
}

func (process *p5VerifyProcess) kill() {
	select {
	case <-process.done:
		return
	default:
	}
	_ = process.command.Process.Kill()
	select {
	case <-process.done:
	case <-time.After(8 * time.Second):
	}
}

func (process *p5VerifyProcess) wait() error {
	select {
	case <-process.done:
		return process.exit
	case <-time.After(8 * time.Second):
		return fmt.Errorf("Rust process exit timeout")
	}
}

type p5VerifyRPCReply struct {
	Version   uint32          `json:"version"`
	RequestID string          `json:"request_id"`
	OK        bool            `json:"ok"`
	Result    json.RawMessage `json:"result"`
	Error     *string         `json:"error"`
}

func p5VerifyRPC(address, path, token string, command any) (p5VerifyRPCReply, int, error) {
	var reply p5VerifyRPCReply
	identity, err := RandomID()
	if err != nil {
		return reply, 0, err
	}
	body, err := json.Marshal(map[string]any{"version": 1, "request_id": identity, "command": command})
	if err != nil {
		return reply, 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, address+path, bytes.NewReader(body))
	if err != nil {
		return reply, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return reply, 0, fmt.Errorf("process RPC transport failed")
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 32*1024*1024+1))
	if err != nil || len(payload) > 32*1024*1024 {
		return reply, response.StatusCode, fmt.Errorf("process RPC response unreadable")
	}
	if response.StatusCode != http.StatusOK {
		return reply, response.StatusCode, fmt.Errorf("process RPC HTTP rejection")
	}
	if json.Unmarshal(payload, &reply) != nil || reply.Version != 1 || reply.RequestID != identity {
		return reply, response.StatusCode, fmt.Errorf("process RPC response identity invalid")
	}
	return reply, response.StatusCode, nil
}
