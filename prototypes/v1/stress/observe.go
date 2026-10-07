package stress

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type ProcessSample struct {
	Role       string   `json:"role"`
	PID        int      `json:"pid"`
	Status     string   `json:"status"`
	CPUPercent *float64 `json:"cpu_percent_ps"`
	RSSBytes   *int64   `json:"rss_bytes"`
	CPUSeconds *float64 `json:"cpu_seconds_ps"`
	Identity   string   `json:"process_start_identity,omitempty"`
}

type HostCapacity struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	LogicalCPUs  int    `json:"logical_cpus"`
	MemoryBytes  *int64 `json:"physical_memory_bytes"`
}

func Capacity(ctx context.Context) HostCapacity {
	result := HostCapacity{OS: runtime.GOOS, Architecture: runtime.GOARCH, LogicalCPUs: runtime.NumCPU()}
	if runtime.GOOS == "darwin" {
		encoded, err := exec.CommandContext(ctx, "sysctl", "-n", "hw.memsize").Output()
		if err == nil {
			memory, err := strconv.ParseInt(strings.TrimSpace(string(encoded)), 10, 64)
			if err == nil && memory > 0 {
				result.MemoryBytes = &memory
			}
		}
	} else if runtime.GOOS == "linux" {
		encoded, err := os.ReadFile("/proc/meminfo")
		if err == nil {
			for _, line := range strings.Split(string(encoded), "\n") {
				fields := strings.Fields(line)
				if len(fields) == 3 && fields[0] == "MemTotal:" {
					memory, err := strconv.ParseInt(fields[1], 10, 64)
					if err == nil && memory > 0 {
						memory *= 1024
						result.MemoryBytes = &memory
					}
					break
				}
			}
		}
	}
	return result
}

func SampleProcess(ctx context.Context, process Process) ProcessSample {
	result := ProcessSample{Role: process.Role, PID: process.PID, Status: "unavailable"}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return result
	}
	encoded, err := exec.CommandContext(ctx, "ps", "-p", strconv.Itoa(process.PID), "-o", "pid=,pcpu=,rss=,time=,lstart=").Output()
	if err != nil {
		return result
	}
	fields := strings.Fields(string(encoded))
	if len(fields) < 9 {
		return result
	}
	identity, err := strconv.Atoi(fields[0])
	if err != nil || identity != process.PID {
		return result
	}
	percent, err := strconv.ParseFloat(fields[1], 64)
	if err != nil || percent < 0 || math.IsNaN(percent) || math.IsInf(percent, 0) {
		return result
	}
	rss, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || rss < 0 {
		return result
	}
	seconds, valid := parseCPUTime(fields[3])
	if !valid {
		return result
	}
	rss *= 1024
	result.Status, result.CPUPercent, result.RSSBytes, result.CPUSeconds = "available", &percent, &rss, &seconds
	result.Identity = strings.Join(fields[4:], " ")
	return result
}

func parseCPUTime(value string) (float64, bool) {
	days := 0.0
	if split := strings.Split(value, "-"); len(split) == 2 {
		parsed, err := strconv.ParseFloat(split[0], 64)
		if err != nil || parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, false
		}
		days, value = parsed, split[1]
	}
	parts := strings.Split(value, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, false
	}
	seconds := 0.0
	for _, part := range parts {
		parsed, err := strconv.ParseFloat(part, 64)
		if err != nil || parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, false
		}
		seconds = seconds*60 + parsed
	}
	return seconds + days*86400, true
}

type metricsSource interface {
	Metrics(context.Context) (json.RawMessage, *Failure)
}

type Observer struct {
	Config        ConnectionConfig
	MetricsClient metricsSource
	Artifacts     *Artifacts
	MaxRSS        int64
	identities    map[int]string
}

func (observer *Observer) Sample(ctx context.Context) error {
	processes := make([]ProcessSample, 0, len(observer.Config.Processes))
	unsafe := false
	for _, process := range observer.Config.Processes {
		sample := SampleProcess(ctx, process)
		if sample.Status == "available" {
			if prior, found := observer.identities[process.PID]; found && prior != sample.Identity {
				sample.Status = "pid_reused"
				sample.CPUPercent, sample.CPUSeconds, sample.RSSBytes = nil, nil, nil
				unsafe = true
			} else {
				observer.identities[process.PID] = sample.Identity
			}
			if observer.MaxRSS > 0 && sample.RSSBytes != nil && *sample.RSSBytes > observer.MaxRSS {
				unsafe = true
			}
		} else {
			unsafe = true
		}
		processes = append(processes, sample)
	}
	var metrics json.RawMessage
	status := "unavailable"
	if observer.MetricsClient != nil {
		if actual, failure := observer.MetricsClient.Metrics(ctx); failure == nil {
			metrics, status = actual, "available"
		}
	}
	processStatus := "available"
	if len(processes) == 0 {
		processStatus = "unavailable_no_pids"
	}
	nativeStatus, scopes := metricsScopes(metrics)
	if err := observer.Artifacts.Record(map[string]any{"type": "resource_sample", "observed_at": time.Now().UTC().Format(time.RFC3339Nano), "processes_status": processStatus, "processes": processes, "management_metrics_status": status, "management_metrics": metrics,
		"native_metrics_status": nativeStatus, "measurement_scopes": scopes, "service_queue_delay": nil, "ps_cpu_semantics": "OS ps percent CPU, not cache accounting; CPU time is cumulative"}); err != nil {
		return err
	}
	if unsafe {
		return &Failure{Class: "resource_safety", Code: "process_missing_reused_or_rss_threshold"}
	}
	return nil
}

func metricsScopes(metrics json.RawMessage) (string, json.RawMessage) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(metrics, &fields) != nil {
		return "unavailable", nil
	}
	status := "unavailable"
	var native map[string]json.RawMessage
	if json.Unmarshal(fields["native"], &native) == nil && native != nil {
		status = "available_reported_map"
	}
	return status, fields["measurement_scopes"]
}
