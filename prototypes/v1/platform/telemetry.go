package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	controller "peri-loom/controller"
)

type Telemetry struct {
	authority controller.ControlTransport
	metadata  controller.MetadataStore
	tokens    map[string]string
	client    *http.Client
}

func NewTelemetry(authority controller.ControlTransport, metadata controller.MetadataStore, tokens map[string]string) (*Telemetry, error) {
	if authority == nil || metadata == nil {
		return nil, errors.New("telemetry dependencies required")
	}
	credentials := map[string]string{}
	for identity, token := range tokens {
		credentials[identity] = token
	}
	return &Telemetry{authority: authority, metadata: metadata, tokens: credentials, client: &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true, ResponseHeaderTimeout: 3 * time.Second}, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (telemetry *Telemetry) Collect(ctx context.Context) map[string]any {
	result := map[string]any{"scope": "actual_process_metrics_not_cluster_HA"}
	identity, err := controller.RandomID()
	var authority any
	if err == nil {
		err = telemetry.authority.Authority(ctx, identity, map[string]string{"command": "get_metrics"}, &authority)
	}
	if err != nil {
		result["authority"] = map[string]string{"status": "unavailable"}
	} else {
		result["authority"] = authority
	}
	metadata, err := telemetry.metadata.Snapshot()
	if err != nil {
		result["workers"] = map[string]string{"status": "metadata_unavailable"}
		return result
	}
	workers := map[string]any{}
	for workerID, worker := range metadata.Workers {
		metrics, err := telemetry.workerMetrics(ctx, worker)
		if err != nil {
			workers[workerID] = map[string]string{"status": "unavailable"}
		} else {
			workers[workerID] = metrics
		}
	}
	result["workers"] = workers
	return result
}

func (telemetry *Telemetry) workerMetrics(ctx context.Context, worker controller.Worker) (any, error) {
	identity, err := controller.RandomID()
	if err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(worker.Address)
	if err != nil || endpoint.Scheme != "http" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Hostname() != "127.0.0.1" && endpoint.Hostname() != "::1") {
		return nil, errors.New("invalid worker telemetry endpoint")
	}
	endpoint.Path = "/control"
	encoded, _ := json.Marshal(map[string]any{"version": 1, "request_id": identity, "command": map[string]string{"command": "get_metrics"}})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.GetBody = nil
	request.Header.Set("Authorization", "Bearer "+telemetry.tokens[worker.ID])
	request.Header.Set("Content-Type", "application/json")
	response, err := telemetry.client.Do(request)
	if err != nil {
		return nil, errors.New("worker telemetry unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, errors.New("worker telemetry rejected")
	}
	var envelope struct {
		Version   uint32  `json:"version"`
		RequestID string  `json:"request_id"`
		OK        bool    `json:"ok"`
		Result    any     `json:"result"`
		Error     *string `json:"error"`
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 32*1024*1024+1))
	if err != nil || len(body) > 32*1024*1024 {
		return nil, errors.New("oversized telemetry response")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if sqlJSONValue(ctx, decoder, 0) != nil {
		return nil, errors.New("invalid telemetry response")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("invalid telemetry response")
	}
	decoder = json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(new(any)) != io.EOF || envelope.Version != 1 || envelope.RequestID != identity || !envelope.OK || envelope.Error != nil {
		return nil, errors.New("invalid telemetry response")
	}
	return envelope.Result, nil
}
