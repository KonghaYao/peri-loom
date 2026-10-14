package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	controller "peri-loom/controller"
)

const capacityTracePageBytes = 64 * 1024

var capacityTraceStream = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)
var capacityTraceDecimal = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

type capacityTraceTarget struct {
	endpoint string
	token    string
}

type capacityTraceBridge struct {
	authority capacityTraceTarget
	workers   map[string]capacityTraceTarget
	client    *http.Client
	slots     chan struct{}
}

type capacityTraceQuery struct {
	role    string
	worker  string
	stream  string
	after   uint64
	maximum uint64
}

func capacityTraceEndpoint(address, path string) (string, error) {
	endpoint, err := url.Parse(address)
	if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" || (endpoint.Path != "" && endpoint.Path != "/") || (endpoint.Hostname() != "127.0.0.1" && endpoint.Hostname() != "::1") || endpoint.Port() == "" {
		return "", errors.New("invalid capacity trace endpoint")
	}
	endpoint.Path = path
	return endpoint.String(), nil
}

func newCapacityTraceBridge(config controller.ServiceConfig) (*capacityTraceBridge, error) {
	endpoint, err := capacityTraceEndpoint(config.AuthorityURL, "/")
	if err != nil || len(config.AuthorityToken) < 32 {
		return nil, errors.New("capacity trace authority unavailable")
	}
	bridge := &capacityTraceBridge{authority: capacityTraceTarget{endpoint, config.AuthorityToken}, workers: map[string]capacityTraceTarget{}, slots: make(chan struct{}, 8)}
	for _, worker := range config.Workers {
		endpoint, err := capacityTraceEndpoint(worker.Worker.Address, "/control")
		if err != nil || !identifierPattern.MatchString(worker.Worker.ID) || len(worker.ControlToken) < 32 {
			return nil, errors.New("invalid capacity trace worker")
		}
		if _, exists := bridge.workers[worker.Worker.ID]; exists {
			return nil, errors.New("duplicate capacity trace worker")
		}
		bridge.workers[worker.Worker.ID] = capacityTraceTarget{endpoint, worker.ControlToken}
	}
	bridge.client = &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 8192, ResponseHeaderTimeout: 2 * time.Second}, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return bridge, nil
}

func parseCapacityTraceQuery(raw string) (capacityTraceQuery, error) {
	var query capacityTraceQuery
	invalid := errors.New("invalid capacity trace query")
	if len(raw) > 1024 {
		return query, invalid
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return query, invalid
	}
	for key, entries := range values {
		if len(entries) != 1 || (key != "role" && key != "worker_id" && key != "stream_id" && key != "after_sequence" && key != "max_events") {
			return query, invalid
		}
	}
	query.role, query.worker, query.stream = values.Get("role"), values.Get("worker_id"), values.Get("stream_id")
	_, workerPresent := values["worker_id"]
	if query.role != "authority" && query.role != "worker" || query.role == "authority" && workerPresent || query.role == "worker" && !identifierPattern.MatchString(query.worker) || !capacityTraceStream.MatchString(query.stream) || !capacityTraceDecimal.MatchString(values.Get("after_sequence")) || !capacityTraceDecimal.MatchString(values.Get("max_events")) {
		return query, invalid
	}
	query.after, err = strconv.ParseUint(values.Get("after_sequence"), 10, 64)
	if err != nil {
		return query, invalid
	}
	query.maximum, err = strconv.ParseUint(values.Get("max_events"), 10, 64)
	if err != nil || query.maximum < 1 || query.maximum > 128 {
		return query, invalid
	}
	return query, nil
}

func (bridge *capacityTraceBridge) page(ctx context.Context, query capacityTraceQuery) (json.RawMessage, error) {
	target := bridge.authority
	if query.role == "worker" {
		var exists bool
		target, exists = bridge.workers[query.worker]
		if !exists {
			return nil, errors.New("capacity trace worker unavailable")
		}
	}
	select {
	case bridge.slots <- struct{}{}:
		defer func() { <-bridge.slots }()
	default:
		return nil, errors.New("capacity trace busy")
	}
	requestID, err := controller.RandomID()
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(map[string]any{"version": 1, "request_id": requestID, "command": map[string]any{"command": "get_capacity_trace", "stream_id": query.stream, "after_sequence": query.after, "max_events": query.maximum}})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, errors.New("capacity trace unavailable")
	}
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+target.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := bridge.client.Do(request)
	if err != nil {
		return nil, errors.New("capacity trace unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("Content-Encoding") != "" || response.ContentLength > capacityTracePageBytes {
		return nil, errors.New("capacity trace unavailable")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, capacityTracePageBytes+1))
	if err != nil || len(body) > capacityTracePageBytes || !utf8.Valid(body) {
		return nil, errors.New("invalid capacity trace response")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if sqlJSONValue(ctx, decoder, 0) != nil {
		return nil, errors.New("invalid capacity trace response")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("invalid capacity trace response")
	}
	var envelope struct {
		Version            uint32          `json:"version"`
		RequestID          string          `json:"request_id"`
		OK                 bool            `json:"ok"`
		Result             json.RawMessage `json:"result"`
		Error              *string         `json:"error"`
		PreconditionFailed bool            `json:"precondition_failed"`
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return nil, errors.New("invalid capacity trace response")
	}
	required := []string{"version", "request_id", "ok", "result", "error"}
	if query.role == "authority" {
		required = append(required, "precondition_failed")
	}
	if len(fields) != len(required) {
		return nil, errors.New("invalid capacity trace response")
	}
	for _, key := range required {
		if _, exists := fields[key]; !exists {
			return nil, errors.New("invalid capacity trace response")
		}
	}
	decoder = json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || envelope.Version != 1 || envelope.RequestID != requestID || !envelope.OK || envelope.Error != nil || envelope.PreconditionFailed {
		return nil, errors.New("capacity trace unavailable")
	}
	if query.role == "authority" && string(fields["precondition_failed"]) != "false" {
		return nil, errors.New("invalid capacity trace response")
	}
	var page map[string]json.RawMessage
	var schema int
	if json.Unmarshal(envelope.Result, &page) != nil || json.Unmarshal(page["schemaVersion"], &schema) != nil || schema != 1 {
		return nil, errors.New("invalid capacity trace page")
	}
	var normalized any
	decoder = json.NewDecoder(bytes.NewReader(envelope.Result))
	decoder.UseNumber()
	if decoder.Decode(&normalized) != nil {
		return nil, errors.New("invalid capacity trace page")
	}
	var normalizedBuffer bytes.Buffer
	encoder := json.NewEncoder(&normalizedBuffer)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(normalized)
	decoded := normalizedBuffer.Bytes()
	if err != nil || bytes.Contains(decoded, []byte(bridge.authority.token)) {
		return nil, errors.New("invalid capacity trace page")
	}
	for _, worker := range bridge.workers {
		if bytes.Contains(decoded, []byte(worker.token)) {
			return nil, errors.New("invalid capacity trace page")
		}
	}
	return envelope.Result, nil
}

func (management *Management) capacityTrace(response http.ResponseWriter, request *http.Request, principal Principal) {
	if principal.Role != "admin" || principal.Project != "*" || management.config.Metrics == nil {
		managementReply(response, 403, "", nil, "forbidden")
		return
	}
	query, err := parseCapacityTraceQuery(request.URL.RawQuery)
	if err != nil {
		managementReply(response, 400, "", nil, "invalid_request")
		return
	}
	bridge := management.config.CapacityTrace
	if bridge == nil {
		managementReply(response, 503, "", nil, "unavailable")
		return
	}
	if query.role == "worker" {
		if _, exists := bridge.workers[query.worker]; !exists {
			managementReply(response, 400, "", nil, "invalid_request")
			return
		}
	}
	page, err := bridge.page(request.Context(), query)
	if err != nil {
		managementReply(response, 503, "", nil, "unavailable")
		return
	}
	encoded, err := json.Marshal(map[string]any{"version": 1, "request_id": "", "ok": true, "result": page, "error": nil})
	if err != nil || len(encoded)+1 > capacityTracePageBytes || strings.Contains(string(encoded), "Bearer ") {
		managementReply(response, 503, "", nil, "unavailable")
		return
	}
	response.WriteHeader(200)
	_, _ = response.Write(append(encoded, '\n'))
}
