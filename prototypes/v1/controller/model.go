package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
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

var componentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func (database DatabaseID) Validate() error {
	if !componentPattern.MatchString(database.Namespace.Tenant) || !componentPattern.MatchString(database.Namespace.Database) || !componentPattern.MatchString(database.Branch) {
		return fmt.Errorf("invalid database identifier")
	}
	return nil
}

func (database DatabaseID) Key() string {
	return database.Namespace.Tenant + "/" + database.Namespace.Database + "/" + database.Branch
}

type Binding struct {
	Database          DatabaseID `json:"database"`
	RootIncarnation   string     `json:"root_incarnation"`
	AuthorityTerm     uint64     `json:"authority_term"`
	BranchIncarnation uint64     `json:"branch_incarnation"`
	Epoch             uint64     `json:"epoch"`
	WorkerID          string     `json:"worker_id"`
	LeaseID           string     `json:"lease_id"`
	GrantID           string     `json:"grant_id"`
	RenewalSequence   uint64     `json:"renewal_sequence"`
	Role              string     `json:"role"`
}

type Grant struct {
	Binding        Binding `json:"binding"`
	RemainingTTLMS uint64  `json:"remaining_ttl_ms"`
}

type Actual struct {
	Database       DatabaseID `json:"database"`
	Mode           string     `json:"mode"`
	Binding        *Binding   `json:"binding"`
	Epoch          uint64     `json:"epoch"`
	RemainingTTLMS uint64     `json:"remaining_ttl_ms"`
}

type Worker struct {
	ID              string `json:"id"`
	Address         string `json:"address"`
	Region          string `json:"region"`
	Capacity        int    `json:"capacity"`
	Load            int    `json:"load"`
	Healthy         bool   `json:"healthy"`
	LastHeartbeatMS int64  `json:"last_heartbeat_ms"`
	Draining        bool   `json:"draining"`
}

type Placement struct {
	Database       DatabaseID      `json:"database"`
	DesiredWorker  string          `json:"desired_worker"`
	Region         string          `json:"region,omitempty"`
	Epoch          uint64          `json:"epoch"`
	LeaseID        string          `json:"lease_id"`
	TTLMS          uint64          `json:"ttl_ms"`
	DesiredState   string          `json:"desired_state"`
	Role           string          `json:"role"`
	Phase          string          `json:"phase"`
	PendingID      string          `json:"pending_id,omitempty"`
	PendingCommand json.RawMessage `json:"pending_command,omitempty"`
	Grant          *Grant          `json:"grant,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
}

type Metadata struct {
	Version    uint32               `json:"version"`
	Revision   uint64               `json:"revision"`
	Epoch      uint64               `json:"epoch"`
	Workers    map[string]Worker    `json:"workers"`
	Placements map[string]Placement `json:"placements"`
}

type MetadataStore interface {
	Snapshot() (Metadata, error)
	Update(func(*Metadata) error) error
	Close() error
}

type ControlTransport interface {
	Authority(context.Context, string, any, any) error
	Worker(context.Context, Worker, string, any, any) error
}

type ReconcileConfig struct {
	LeaseTTL         time.Duration
	HeartbeatTimeout time.Duration
	Interval         time.Duration
}
