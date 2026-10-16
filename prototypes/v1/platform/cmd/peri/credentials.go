package main

import (
	"crypto/x509"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	sdk "peri-loom/platform/sdk/go"
)

func readCredential(path string) (string, error) {
	if path == "" {
		credential := os.Getenv("PERI_API_KEY")
		if credential == "" {
			return "", errors.New("private credential file or PERI_API_KEY required")
		}
		return credential, nil
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", errors.New("credential file unavailable")
	}
	defer file.Close()
	metadata, err := file.Stat()
	if err != nil {
		return "", errors.New("credential file unavailable")
	}
	owner, ok := metadata.Sys().(*syscall.Stat_t)
	if !metadata.Mode().IsRegular() || metadata.Mode().Perm()&0077 != 0 || !ok || int(owner.Uid) != os.Geteuid() || metadata.Size() > 8192 {
		return "", errors.New("credential file must be private and owner controlled")
	}
	bytes, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(bytes) > 8192 {
		return "", errors.New("credential file unavailable")
	}
	return strings.TrimSpace(string(bytes)), nil
}

func tlsOptions(path string, timeout time.Duration, connectionReuse bool) (sdk.Options, error) {
	options := sdk.Options{Timeout: timeout, ConnectionReuse: connectionReuse}
	if path == "" {
		return options, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return options, errors.New("CA certificate unavailable")
	}
	defer file.Close()
	bytes, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(bytes) > 1<<20 {
		return options, errors.New("invalid CA certificate")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(bytes) {
		return options, errors.New("invalid CA certificate")
	}
	options.RootCAs = roots
	return options, nil
}
