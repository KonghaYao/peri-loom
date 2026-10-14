package faultprobe

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type DatabaseID struct {
	Namespace struct {
		Tenant   string `json:"tenant"`
		Database string `json:"database"`
	} `json:"namespace"`
	Branch string `json:"branch"`
}

type Connection struct {
	Endpoint      string       `json:"endpoint"`
	ProjectAPIKey string       `json:"project_api_key"`
	GlobalAPIKey  string       `json:"global_api_key"`
	Database      DatabaseID   `json:"database"`
	CAFile        string       `json:"ca_file"`
	Databases     []DatabaseID `json:"databases,omitempty"`
}

type Topology struct {
	LauncherPID int `json:"launcher_pid"`
	Processes   map[string]struct {
		PID int `json:"pid"`
	} `json:"processes"`
	Gateway   string            `json:"gateway"`
	Authority string            `json:"authority"`
	Broker    string            `json:"broker"`
	Workers   map[string]string `json:"workers"`
}

type Config struct {
	RunDir            string
	ConnectionFile    string
	TopologyFile      string
	SourceRoot        string
	GateFile          string
	RustBinary        string
	Case              string
	AllowWorkerKill   bool
	ACKs              int
	RequestTimeout    time.Duration
	RecoveryTimeout   time.Duration
	ObservationWindow time.Duration
}

func DefaultConfig() Config {
	return Config{Case: "owner_crash", ACKs: 8, RequestTimeout: 5 * time.Second, RecoveryTimeout: 30 * time.Second, ObservationWindow: 10 * time.Second}
}

func strict(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func readPrivate(path string, target any) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 || info.Mode().Perm()&0077 != 0 {
		return errors.New("private bounded regular JSON file required")
	}
	body, err := os.ReadFile(path)
	if err != nil || strict(body, target) != nil {
		return errors.New("invalid private JSON")
	}
	return nil
}

func cleanAbsolute(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsAny(path, " \t\r\n")
}

func localEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" || parsed.Port() == "" {
		return false
	}
	address := net.ParseIP(parsed.Hostname())
	return address != nil && address.IsLoopback() && (parsed.Scheme == "http" || parsed.Scheme == "https")
}

func component(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func (config Config) Validate() (Connection, Topology, error) {
	var connection Connection
	var topology Topology
	fail := func() (Connection, Topology, error) {
		return connection, topology, errors.New("fixture configuration safety check failed")
	}
	if !cleanAbsolute(config.RunDir) || !strings.HasPrefix(filepath.Base(config.RunDir), "peri-stress-") || len(filepath.Base(config.RunDir)) < 24 {
		return fail()
	}
	if err := safeDirectory(config.RunDir); err != nil {
		return fail()
	}
	if config.Case != "owner_crash" && config.Case != "metadata_starvation" {
		return fail()
	}
	if config.Case == "owner_crash" && !config.AllowWorkerKill {
		return fail()
	}
	if config.ACKs < 1 || config.ACKs > 32 || config.RequestTimeout < time.Second || config.RequestTimeout > 15*time.Second || config.RecoveryTimeout < time.Second || config.RecoveryTimeout > 60*time.Second || config.ObservationWindow < time.Second || config.ObservationWindow > 20*time.Second {
		return fail()
	}
	for _, path := range []string{config.ConnectionFile, config.TopologyFile} {
		if !cleanAbsolute(path) || filepath.Dir(path) != config.RunDir {
			return fail()
		}
	}
	if readPrivate(config.ConnectionFile, &connection) != nil || readPrivate(config.TopologyFile, &topology) != nil {
		return fail()
	}
	if !localEndpoint(connection.Endpoint) || topology.Gateway != connection.Endpoint || !localEndpoint(topology.Authority) || !localEndpoint(topology.Broker) || len(connection.ProjectAPIKey) < 24 || len(connection.GlobalAPIKey) < 24 {
		return fail()
	}
	if connection.ProjectAPIKey == connection.GlobalAPIKey || strings.ContainsAny(connection.ProjectAPIKey+connection.GlobalAPIKey, "\r\n") || len(connection.ProjectAPIKey) > 4096 || len(connection.GlobalAPIKey) > 4096 {
		return fail()
	}
	if !cleanAbsolute(config.RustBinary) || filepath.Base(config.RustBinary) != "peri-loom-v1" || config.SourceRoot == "" || config.GateFile == "" {
		return fail()
	}
	binary, err := os.Lstat(config.RustBinary)
	if err != nil || !binary.Mode().IsRegular() || binary.Mode().Perm()&0111 == 0 {
		return fail()
	}
	if connection.CAFile != "" && (!cleanAbsolute(connection.CAFile) || !strings.HasPrefix(connection.CAFile, config.RunDir+string(os.PathSeparator))) {
		return fail()
	}
	if len(topology.Workers) != 2 || len(topology.Processes) != 4 || topology.LauncherPID <= 1 || topology.LauncherPID == os.Getpid() {
		return fail()
	}
	seen := map[int]bool{topology.LauncherPID: true, os.Getpid(): true}
	for _, name := range []string{"authority", "platform", "worker-a", "worker-b"} {
		process, found := topology.Processes[name]
		if !found || process.PID <= 1 || seen[process.PID] {
			return fail()
		}
		seen[process.PID] = true
	}
	for _, name := range []string{"worker-a", "worker-b"} {
		if !localEndpoint(topology.Workers[name]) {
			return fail()
		}
	}
	dbs := append([]DatabaseID{connection.Database}, connection.Databases...)
	if len(dbs) > 3 || config.Case == "metadata_starvation" && len(dbs) != 3 {
		return fail()
	}
	unique := map[DatabaseID]bool{}
	for _, database := range dbs {
		if !component(database.Namespace.Tenant) || !component(database.Namespace.Database) || database.Branch != "main" || !strings.HasPrefix(database.Namespace.Database, filepath.Base(config.RunDir)+"-") || unique[database] {
			return fail()
		}
		unique[database] = true
	}
	return connection, topology, nil
}
