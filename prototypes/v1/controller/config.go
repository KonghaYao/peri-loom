package controller

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
)

type ServiceConfig struct {
	AuthorityURL   string             `json:"authority_url"`
	AuthorityToken string             `json:"authority_token"`
	APIToken       string             `json:"api_token"`
	Workers        []ConfiguredWorker `json:"workers"`
}

type ConfiguredWorker struct {
	Worker       Worker `json:"worker"`
	ControlToken string `json:"control_token"`
}

func validateServiceToken(token string) error {
	if len(token) < 32 || len(token) > 256 || strings.TrimSpace(token) != token {
		return errors.New("invalid service token")
	}
	for _, character := range token {
		if character < 33 || character > 126 {
			return errors.New("invalid service token")
		}
	}
	return nil
}

func ReadServiceConfig(path string) (ServiceConfig, error) {
	var config ServiceConfig
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return config, errors.New("configuration must be a private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return config, errors.New("configuration unavailable")
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1024*1024))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		return config, errors.New("invalid configuration")
	}
	tokens := map[string]bool{}
	for _, token := range []string{config.AuthorityToken, config.APIToken} {
		if validateServiceToken(token) != nil || tokens[token] {
			return ServiceConfig{}, errors.New("invalid or reused credential")
		}
		tokens[token] = true
	}
	workerIDs := map[string]bool{}
	for _, worker := range config.Workers {
		if validateServiceToken(worker.ControlToken) != nil || tokens[worker.ControlToken] || workerIDs[worker.Worker.ID] {
			return ServiceConfig{}, errors.New("invalid or reused worker credential")
		}
		tokens[worker.ControlToken] = true
		workerIDs[worker.Worker.ID] = true
	}
	return config, nil
}
