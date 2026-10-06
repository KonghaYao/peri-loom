package sdk

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	endpoint      string
	credential    string
	http          *http.Client
	requestLimit  int64
	responseLimit int64
}

func (client Client) String() string   { return "<peri SDK client>" }
func (client Client) GoString() string { return client.String() }

func NewClient(endpoint, credential string, options Options) (*Client, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.RawPath != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("invalid endpoint")
	}
	if parsed.Scheme != "https" {
		address := net.ParseIP(parsed.Hostname())
		if parsed.Scheme != "http" || address == nil || !address.IsLoopback() {
			return nil, errors.New("HTTPS required outside literal loopback")
		}
	}
	if len(credential) < 32 || len(credential) > 4096 {
		return nil, errors.New("invalid API credential")
	}
	for _, character := range credential {
		if character < 33 || character > 126 {
			return nil, errors.New("invalid API credential")
		}
	}
	if options.Timeout == 0 {
		options.Timeout = 30 * time.Second
	}
	if options.MaxRequestBytes == 0 {
		options.MaxRequestBytes = 1 << 20
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = 8 << 20
	}
	if options.Timeout <= 0 || options.MaxRequestBytes < 1 || options.MaxRequestBytes > 32<<20 || options.MaxResponseBytes < 1 || options.MaxResponseBytes > 32<<20 {
		return nil, errors.New("invalid SDK limits")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: options.RootCAs}
	client := &http.Client{Transport: transport, Timeout: options.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Client{endpoint: strings.TrimSuffix(parsed.String(), "/"), credential: credential, http: client, requestLimit: options.MaxRequestBytes, responseLimit: options.MaxResponseBytes}, nil
}

func (client *Client) call(ctx context.Context, path, identifier string, input any, output any, mayMutate bool) error {
	if !label.MatchString(identifier) {
		return &Error{Code: "invalid_request_id"}
	}
	if ctx.Err() != nil {
		return &Error{Code: "request_cancelled", RequestID: identifier}
	}
	body, err := json.Marshal(input)
	if err != nil || int64(len(body)) > client.requestLimit {
		return &Error{Code: "request_limit", RequestID: identifier}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return &Error{Code: "invalid_request", RequestID: identifier}
	}
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+client.credential)
	response, err := client.http.Do(request)
	if err != nil {
		return &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: mayMutate}
	}
	defer response.Body.Close()
	bytes, err := io.ReadAll(io.LimitReader(response.Body, client.responseLimit+1))
	if err != nil || int64(len(bytes)) > client.responseLimit {
		return &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: mayMutate}
	}
	var reply envelope
	if err := decode(bytes, &reply); err != nil || reply.Version != 1 || reply.RequestID != identifier || reply.OK == nil {
		return &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: mayMutate}
	}
	if !*reply.OK {
		if reply.Error == nil || len(reply.Result) == 0 || string(reply.Result) != "null" {
			return &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: mayMutate}
		}
		code := safeCode(reply.Error.Code)
		if code == "sql_error" && (!reply.Error.ExecutionCompleted || !reply.Error.MayHaveCommitted) {
			return &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: mayMutate}
		}
		return &Error{Code: code, RequestID: identifier, OutcomeUnknown: code == "outcome_unknown", ExecutionCompleted: reply.Error.ExecutionCompleted, MayHaveCommitted: reply.Error.MayHaveCommitted}
	}
	if reply.Error != nil || response.StatusCode < 200 || response.StatusCode >= 300 || len(reply.Result) == 0 {
		return &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: mayMutate}
	}
	if output != nil && decode(reply.Result, output) != nil {
		return &Error{Code: "outcome_unknown", RequestID: identifier, OutcomeUnknown: mayMutate}
	}
	return nil
}

func decode(payload []byte, output any) error {
	if err := validateJSON(payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("invalid response")
	}
	return nil
}

func (client *Client) Management(ctx context.Context, operation string, request ManagementRequest) (json.RawMessage, error) {
	path := "/v1/databases/" + strings.TrimPrefix(operation, "databases/")
	operation = strings.TrimPrefix(operation, "databases/")
	switch operation {
	case "create", "delete", "snapshot", "branch", "clone", "restore", "restore-point", "pitr", "suspend", "resume", "migrate", "status", "checkpoint", "schema", "gc", "gc-inspect", "refs", "compact", "drop-snapshot", "retain-after", "rewrap":
	case "organizations/create", "projects/create", "keys/create", "keys/revoke", "tokens":
		path = "/v1/" + operation
	default:
		return nil, &Error{Code: "invalid_operation"}
	}
	if (strings.HasPrefix(path, "/v1/databases/") || operation == "tokens") && (request.Database == nil || request.Database.Validate() != nil) {
		return nil, &Error{Code: "invalid_database"}
	}
	if request.RequestID == "" {
		identifier, err := RandomID()
		if err != nil {
			return nil, err
		}
		request.RequestID = identifier
	}
	var result json.RawMessage
	err := client.call(ctx, path, request.RequestID, request, &result, true)
	return result, err
}
