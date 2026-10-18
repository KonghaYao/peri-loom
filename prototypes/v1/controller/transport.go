package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const maxRPCBytes = 32 * 1024 * 1024

type RPCError struct {
	Message            string
	Ambiguous          bool
	Rejected           bool
	PreconditionFailed bool
	StatusCode         int
	// Detail keeps the backend's error text out of Error(), so it is never
	// returned to a client, while private diagnostics can still log the cause.
	Detail string
	cause  error
}

func (failure *RPCError) Error() string { return failure.Message }
func (failure *RPCError) Unwrap() error { return failure.cause }

type httpControlTransport struct {
	authority    string
	controlToken string
	workerTokens map[string]string
	client       *http.Client
}

func RandomID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", &RPCError{Message: "request identifier generation failed"}
	}
	return hex.EncodeToString(bytes[:]), nil
}

func NewTransport(authorityURL, controlToken string, workerTokens map[string]string) (ControlTransport, error) {
	endpoint, err := loopbackEndpoint(authorityURL, "/")
	if err != nil {
		return nil, err
	}
	if !validToken(controlToken) {
		return nil, &RPCError{Message: "invalid control credential"}
	}
	tokens := make(map[string]string, len(workerTokens))
	seen := map[string]bool{controlToken: true}
	for worker, token := range workerTokens {
		if !validRPCLabel(worker) || !validToken(token) || seen[token] {
			return nil, &RPCError{Message: "invalid or duplicate worker credential"}
		}
		seen[token] = true
		tokens[worker] = token
	}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		DisableKeepAlives:      true,
		ForceAttemptHTTP2:      false,
		ResponseHeaderTimeout:  10 * time.Second,
		MaxResponseHeaderBytes: 16 * 1024,
	}
	return &httpControlTransport{
		authority: endpoint, controlToken: controlToken, workerTokens: tokens,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (transport *httpControlTransport) Authority(ctx context.Context, requestID string, command any, output any) error {
	encoded, err := json.Marshal(command)
	if err != nil || !validAuthorityID(encoded, requestID) {
		return &RPCError{Message: "invalid authority command"}
	}
	return transport.call(ctx, transport.authority, transport.controlToken, requestID, json.RawMessage(encoded), output, true)
}

func (transport *httpControlTransport) Worker(ctx context.Context, worker Worker, requestID string, ownershipAction any, output any) error {
	token, found := transport.workerTokens[worker.ID]
	if !found {
		return &RPCError{Message: "worker credential unavailable"}
	}
	endpoint, err := loopbackEndpoint(worker.Address, "/control")
	if err != nil {
		return err
	}
	command := struct {
		Command string `json:"command"`
		Action  any    `json:"action"`
	}{Command: "ownership", Action: ownershipAction}
	return transport.call(ctx, endpoint, token, requestID, command, output, false)
}

func (transport *httpControlTransport) call(ctx context.Context, endpoint, token, requestID string, command, output any, authority bool) error {
	if ctx == nil || !validRPCLabel(requestID) || !validOutput(output) {
		return &RPCError{Message: "invalid RPC request"}
	}
	if err := ctx.Err(); err != nil {
		return &RPCError{Message: "RPC context expired before dispatch", cause: err}
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	envelope := struct {
		Version   uint32 `json:"version"`
		RequestID string `json:"request_id"`
		Command   any    `json:"command"`
	}{Version: 1, RequestID: requestID, Command: command}
	body, err := json.Marshal(envelope)
	if err != nil || len(body) > maxRPCBytes {
		return &RPCError{Message: "invalid or oversized RPC request"}
	}
	var dispatched atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { dispatched.Store(true) }}
	request, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return &RPCError{Message: "invalid RPC request"}
	}
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := transport.client.Do(request)
	if err != nil {
		cause := ctx.Err()
		if cause == nil && errors.Is(err, context.DeadlineExceeded) {
			cause = context.DeadlineExceeded
		}
		return &RPCError{Message: "RPC transport failed", Ambiguous: dispatched.Load(), cause: cause}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		rejected := response.StatusCode >= 400 && response.StatusCode < 500
		return &RPCError{Message: "RPC HTTP request rejected", StatusCode: response.StatusCode, Rejected: rejected, Ambiguous: !rejected}
	}
	if response.ContentLength > maxRPCBytes {
		return &RPCError{Message: "RPC response exceeds limit", Ambiguous: true}
	}
	body, err = io.ReadAll(io.LimitReader(response.Body, maxRPCBytes+1))
	if err != nil || len(body) > maxRPCBytes {
		return &RPCError{Message: "RPC response unreadable or oversized", Ambiguous: true, cause: ctx.Err()}
	}
	return decodeRPCResponseContext(ctx, body, requestID, output, authority)
}

func loopbackEndpoint(address, path string) (string, error) {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(address, "#") || parsed.RawPath != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", &RPCError{Message: "invalid RPC endpoint"}
	}
	ip := net.ParseIP(parsed.Hostname())
	if ip == nil || !ip.IsLoopback() || strings.HasSuffix(parsed.Host, ":") {
		return "", &RPCError{Message: "RPC endpoint must be literal loopback HTTP"}
	}
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return "", &RPCError{Message: "invalid RPC endpoint port"}
		}
	}
	parsed.Path = path
	return parsed.String(), nil
}

func validToken(token string) bool {
	if len(token) < 32 || len(token) > 256 {
		return false
	}
	for _, byte := range []byte(token) {
		if byte < 33 || byte > 126 {
			return false
		}
	}
	return true
}

func validRPCLabel(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, byte := range []byte(value) {
		if !(byte >= 'a' && byte <= 'z' || byte >= 'A' && byte <= 'Z' || byte >= '0' && byte <= '9' || byte == '-' || byte == '_' || byte == '.') {
			return false
		}
	}
	return true
}
