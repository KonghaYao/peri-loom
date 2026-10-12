package platform

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"

	controller "peri-loom/controller"
)

type ServiceConfig struct {
	Controller            controller.ServiceConfig `json:"controller"`
	MetadataDirectory     string                   `json:"metadata_directory"`
	StateDirectory        string                   `json:"state_directory"`
	AuditPath             string                   `json:"audit_path"`
	KMSKeyring            string                   `json:"kms_keyring"`
	SigningKeyFile        string                   `json:"signing_key_file"`
	BootstrapKey          string                   `json:"bootstrap_key"`
	GatewayListen         string                   `json:"gateway_listen"`
	BrokerListen          string                   `json:"broker_listen"`
	TLSCertificate        string                   `json:"tls_certificate,omitempty"`
	TLSKey                string                   `json:"tls_key,omitempty"`
	InsecureLoopback      bool                     `json:"insecure_loopback,omitempty"`
	WriterAdmission       bool                     `json:"writer_admission,omitempty"`
	WorkerSQLTokens       map[string]string        `json:"worker_sql_tokens"`
	WorkerKeyTokens       map[string]string        `json:"worker_key_tokens"`
	WorkerAuthorityTokens map[string]string        `json:"worker_authority_tokens"`
	OwnedStopProof        *OwnedStopProofConfig    `json:"owned_stop_proof,omitempty"`
}

func privateBytes(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > maximum {
		return nil, errors.New("private regular file required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("private file unavailable")
	}
	defer file.Close()
	actual, err := file.Stat()
	if err != nil || !os.SameFile(info, actual) {
		return nil, errors.New("private file changed during open")
	}
	return io.ReadAll(io.LimitReader(file, maximum+1))
}

func ReadServiceConfig(path string) (ServiceConfig, error) {
	var config ServiceConfig
	encoded, err := privateBytes(path, 1024*1024)
	if err != nil {
		return config, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		return config, errors.New("invalid private configuration")
	}
	host, _, err := net.SplitHostPort(config.BrokerListen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return config, errors.New("broker listener must be literal loopback")
	}
	host, _, err = net.SplitHostPort(config.GatewayListen)
	if err != nil {
		return config, errors.New("invalid gateway listener")
	}
	if config.TLSCertificate == "" || config.TLSKey == "" {
		if !config.InsecureLoopback || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return config, errors.New("TLS required except explicit loopback development")
		}
	} else if _, err := privateBytes(config.TLSKey, 64*1024); err != nil {
		return config, errors.New("TLS private key unavailable")
	}
	seen := map[string]bool{}
	check := func(token string) error {
		if len(token) < 32 || len(token) > 256 || seen[token] {
			return errors.New("invalid or reused service credential")
		}
		for _, character := range token {
			if character < 33 || character > 126 {
				return errors.New("invalid service credential")
			}
		}
		seen[token] = true
		return nil
	}
	for _, token := range []string{config.Controller.AuthorityToken, config.Controller.APIToken, config.BootstrapKey} {
		if err := check(token); err != nil {
			return config, err
		}
	}
	for _, worker := range config.Controller.Workers {
		if err := check(worker.ControlToken); err != nil {
			return config, err
		}
		for _, credentials := range []map[string]string{config.WorkerSQLTokens, config.WorkerKeyTokens, config.WorkerAuthorityTokens} {
			if err := check(credentials[worker.Worker.ID]); err != nil {
				return config, err
			}
		}
	}
	if len(config.Controller.Workers) == 0 || len(config.WorkerSQLTokens) != len(config.Controller.Workers) || len(config.WorkerKeyTokens) != len(config.Controller.Workers) || len(config.WorkerAuthorityTokens) != len(config.Controller.Workers) {
		return config, errors.New("worker credential registry mismatch")
	}
	return config, nil
}
