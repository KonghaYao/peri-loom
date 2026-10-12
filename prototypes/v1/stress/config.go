package stress

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

type Namespace struct {
	Tenant   string `json:"tenant"`
	Database string `json:"database"`
}

type DatabaseID struct {
	Namespace Namespace `json:"namespace"`
	Branch    string    `json:"branch"`
}

type Target struct {
	Endpoint string     `json:"endpoint"`
	Project  string     `json:"project"`
	APIKey   string     `json:"api_key"`
	Database DatabaseID `json:"database"`
}

type Process struct {
	Role string `json:"role"`
	PID  int    `json:"pid"`
}

type ConnectionConfig struct {
	Target
	CAFile             string       `json:"ca_file,omitempty"`
	FixtureOnly        bool         `json:"fixture_only"`
	SourceRoot         string       `json:"source_root"`
	ManagementEndpoint string       `json:"management_endpoint,omitempty"`
	ManagementKey      string       `json:"management_key,omitempty"`
	MetricsKey         string       `json:"metrics_key,omitempty"`
	Databases          []DatabaseID `json:"databases,omitempty"`
	NoisyTenant        *Target      `json:"noisy_tenant,omitempty"`
	Processes          []Process    `json:"processes,omitempty"`
	WorkerIDs          []string     `json:"worker_ids,omitempty"`
	MigrationWorker    string       `json:"migration_worker,omitempty"`
	FaultEndpoint      string       `json:"fault_endpoint,omitempty"`
}

type Settings struct {
	OfferStop         <-chan struct{} `json:"-"`
	Rows              int             `json:"rows"`
	Concurrency       []int           `json:"concurrency"`
	Seed              int64           `json:"seed"`
	Warmup            time.Duration   `json:"warmup_ns"`
	Smoke             time.Duration   `json:"smoke_ns"`
	Measure           time.Duration   `json:"measure_ns"`
	OfferedRate       int             `json:"offered_per_second"`
	RequestTimeout    time.Duration   `json:"request_timeout_ns"`
	MaxGeneratedBytes int64           `json:"max_generated_bytes"`
	MaxOperations     int             `json:"max_operations_per_phase"`
	MaxResponseBytes  int64           `json:"max_response_bytes"`
	MaxArtifactBytes  int64           `json:"max_artifact_bytes"`
	MaxRSSBytes       int64           `json:"max_observed_process_rss_bytes,omitempty"`
	Cases             []string        `json:"cases"`
	VictimEvery       int             `json:"victim_every,omitempty"`
	OfferNumerator    int             `json:"offer_numerator,omitempty"`
	MatrixConcurrency int             `json:"matrix_concurrency,omitempty"`
	OfferDenominator  int             `json:"offer_denominator,omitempty"`
}

func DefaultSettings() Settings {
	return Settings{Rows: 1000, Concurrency: []int{1, 2, 4, 8}, Seed: 20261007,
		Warmup: 5 * time.Second, Smoke: 10 * time.Second, Measure: 30 * time.Second,
		OfferedRate: 50, RequestTimeout: 10 * time.Second, MaxGeneratedBytes: 256 << 20,
		MaxOperations: 50000, MaxResponseBytes: 8 << 20, MaxArtifactBytes: 64 << 20,
		Cases: []string{"all"}}
}

func (settings Settings) Validate() error {
	if settings.VictimEvery != 0 && settings.VictimEvery != 10 {
		return errors.New("victim demand must be equal or one in ten offers")
	}
	if settings.OfferNumerator != 0 || settings.OfferDenominator != 0 {
		if settings.OfferNumerator < 1 || settings.OfferDenominator < settings.OfferNumerator || settings.OfferDenominator > 10 {
			return errors.New("invalid bounded tenant offer fraction")
		}
	}
	if settings.Rows != 1000 && settings.Rows != 10000 {
		return errors.New("rows must be 1000 or 10000")
	}
	if len(settings.Concurrency) == 0 || len(settings.Concurrency) > 4 {
		return errors.New("invalid concurrency sweep")
	}
	seen := map[int]bool{}
	for _, count := range settings.Concurrency {
		if count != 1 && count != 2 && count != 4 && count != 8 || seen[count] {
			return errors.New("concurrency must be unique values from 1/2/4/8")
		}
		seen[count] = true
	}
	if settings.Warmup < 0 || settings.Smoke <= 0 || settings.Measure <= 0 || settings.Measure > time.Minute || settings.Smoke > time.Minute || settings.Warmup > time.Minute {
		return errors.New("invalid bounded phase duration")
	}
	if settings.OfferedRate < 1 || settings.OfferedRate > 1000 || settings.RequestTimeout <= 0 || settings.RequestTimeout > 30*time.Second {
		return errors.New("invalid request/rate bounds")
	}
	if settings.MaxOperations < 1 || settings.MaxOperations > 50000 || settings.MaxResponseBytes < 1024 || settings.MaxResponseBytes > 32<<20 {
		return errors.New("invalid operation/response bounds")
	}
	if settings.MaxGeneratedBytes < 1 || settings.MaxGeneratedBytes > 256<<20 || settings.MaxArtifactBytes < 1024 || settings.MaxArtifactBytes > 64<<20 || settings.MaxRSSBytes < 0 {
		return errors.New("invalid data/artifact/resource bounds")
	}
	if len(settings.Cases) == 0 {
		return errors.New("scenario selection required")
	}
	return nil
}

func ReadConnection(path string) (ConnectionConfig, error) {
	var config ConnectionConfig
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64<<10 || info.Mode().Perm()&0077 != 0 {
		return config, errors.New("connection file must be a private regular file (0600)")
	}
	body, err := os.ReadFile(path)
	if err != nil || len(body) > 64<<10 {
		return config, errors.New("connection file unavailable or oversized")
	}
	if err = decodeStrict(body, &config); err != nil {
		return config, errors.New("invalid connection file")
	}
	return config, config.Validate()
}

func decodeStrict(body []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func Label(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func validateEndpoint(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || parsed.Opaque != "" || parsed.Path != "" && parsed.Path != "/" {
		return errors.New("invalid endpoint")
	}
	if parsed.Scheme == "https" {
		return nil
	}
	address := net.ParseIP(parsed.Hostname())
	if parsed.Scheme != "http" || address == nil || !address.IsLoopback() {
		return errors.New("TLS required except literal loopback HTTP")
	}
	return nil
}

func (config ConnectionConfig) Validate() error {
	if !config.FixtureOnly || config.SourceRoot == "" {
		return errors.New("explicit isolated fixture and source root required")
	}
	if err := validateTarget(config.Target); err != nil {
		return err
	}
	if len(config.Databases) > 16 || len(config.Processes) > 8 || len(config.WorkerIDs) > 2 {
		return errors.New("topology exceeds initial bounds")
	}
	workers, processes := 0, map[int]bool{}
	for _, process := range config.Processes {
		if process.PID <= 1 || !Label(process.Role) || processes[process.PID] {
			return errors.New("invalid process sampling target")
		}
		processes[process.PID] = true
		if strings.HasPrefix(process.Role, "worker") {
			workers++
		}
	}
	if workers > 2 {
		return errors.New("at most two worker PIDs")
	}
	for _, database := range config.Databases {
		if !validDatabase(database) {
			return errors.New("invalid isolated database fixture")
		}
	}
	if config.NoisyTenant != nil {
		if err := validateTarget(*config.NoisyTenant); err != nil {
			return err
		}
		if config.NoisyTenant.Project == config.Project || config.NoisyTenant.Database == config.Database {
			return errors.New("noisy tenant must be independently authorized")
		}
	}
	if config.ManagementEndpoint != "" {
		if err := validateEndpoint(config.ManagementEndpoint); err != nil {
			return err
		}
	}
	if config.FaultEndpoint != "" {
		if err := validateEndpoint(config.FaultEndpoint); err != nil {
			return err
		}
	}
	if config.ManagementKey != "" && (config.ManagementEndpoint == "" || !validKey(config.ManagementKey)) {
		return errors.New("invalid private management connection")
	}
	if config.MetricsKey != "" && (config.ManagementEndpoint == "" || !validKey(config.MetricsKey)) {
		return errors.New("invalid private metrics connection")
	}
	workerIDs := map[string]bool{}
	for _, worker := range config.WorkerIDs {
		if !Label(worker) || workerIDs[worker] {
			return errors.New("invalid worker identity")
		}
		workerIDs[worker] = true
	}
	if config.MigrationWorker != "" {
		found := false
		for _, worker := range config.WorkerIDs {
			found = found || worker == config.MigrationWorker
		}
		if !found {
			return errors.New("migration target must be in the explicit two-worker topology")
		}
	}
	return nil
}

func validateTarget(target Target) error {
	if err := validateEndpoint(target.Endpoint); err != nil {
		return err
	}
	if !Label(target.Project) || !validKey(target.APIKey) || !validDatabase(target.Database) {
		return errors.New("invalid project credential or isolated database identity")
	}
	return nil
}

func validDatabase(database DatabaseID) bool {
	return Label(database.Namespace.Tenant) && Label(database.Namespace.Database) && strings.HasPrefix(database.Namespace.Database, "stress_") && database.Branch == "main"
}

func validKey(value string) bool {
	if len(value) < 32 || len(value) > 4096 {
		return false
	}
	for _, character := range value {
		if character < 33 || character > 126 {
			return false
		}
	}
	return true
}

func (config ConnectionConfig) Redacted() map[string]any {
	output := map[string]any{"endpoint": config.Endpoint, "project": config.Project, "database": config.Database,
		"fixture_only": config.FixtureOnly, "ca_file": config.CAFile, "management_endpoint": config.ManagementEndpoint,
		"management_key": "[REDACTED]", "metrics_key": "[REDACTED]", "api_key": "[REDACTED]", "databases": config.Databases,
		"processes": config.Processes, "worker_ids": config.WorkerIDs, "migration_worker": config.MigrationWorker,
		"fault_endpoint": config.FaultEndpoint, "source_root": config.SourceRoot}
	if config.NoisyTenant != nil {
		output["noisy_tenant"] = map[string]any{"endpoint": config.NoisyTenant.Endpoint, "project": config.NoisyTenant.Project, "database": config.NoisyTenant.Database, "api_key": "[REDACTED]"}
	}
	return output
}
