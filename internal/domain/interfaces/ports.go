// Package interfaces defines the Domain-owned ports the product API depends
// on. Implementations live in internal/infrastructure; the service and the
// presentation handlers depend only on these types, never on raft or sqlite.
package interfaces

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Liapoldus/domain/internal/domain/models"
)

// Sentinel failures produced by the raft-backed adapters. They are defined
// here so the application service maps them to public contract codes without
// coupling to raft or sqlite.
var (
	// ErrForbidden reports an operation the caller's scope may not perform.
	ErrForbidden = errors.New("domain access forbidden")

	// ErrNotFound reports a missing row or entity.
	ErrNotFound = errors.New("domain row not found")

	// ErrConflict reports an existing row, a unique violation, a foreign-key
	// violation or a conflicting model change.
	ErrConflict = errors.New("domain row conflict")

	// ErrInvalid reports a request that the domain model cannot represent.
	ErrInvalid = errors.New("invalid domain request")

	// ErrNotLeader reports that this node does not hold the raft leadership.
	ErrNotLeader = errors.New("domain node is not the raft leader")

	// ErrUnavailable reports a leadership, barrier or transport failure.
	ErrUnavailable = errors.New("domain unavailable")

	// ErrUnknownOutcome reports an apply whose durable outcome is unknown.
	ErrUnknownOutcome = errors.New("domain write outcome unknown")

	// ErrInternal reports any failure the domain cannot classify.
	ErrInternal = errors.New("internal domain failure")

	// ErrEpochMismatch reports a client-provided epoch that does not match the current state.
	ErrEpochMismatch = errors.New("domain epoch mismatch")
)

// Proposer proposes deterministic commands to the raft cluster.
type Proposer interface {
	// Apply proposes one command and returns its durable outcome. Failures are
	// returned as one of the sentinel errors above.
	Apply(ctx context.Context, command models.RaftCommand) (*models.ApplyResult, error)

	// IsLeader reports whether this node currently holds the raft leadership.
	IsLeader() bool

	// LeaderAddress returns the transport address of the known leader, or an
	// empty string when no leader is known.
	LeaderAddress() string
}

// ReadBarrier establishes a leader-confirmed fresh-read fence.
type ReadBarrier interface {
	// Barrier asks the current leader to commit a quorum barrier and returns
	// the leader's durable FSM index.
	Barrier(ctx context.Context) (uint64, error)
}

// Row is one materialized row read from the store.
type Row struct {
	ID   string
	Data json.RawMessage
}

// RowReader reads Domain rows from the materialized store.
type RowReader interface {
	// ReadRow returns the row document and whether it exists.
	ReadRow(ctx context.Context, tenant, site, entity, id string) (json.RawMessage, bool, error)

	// ScanEntity streams every row of one entity's type under the given scope.
	ScanEntity(ctx context.Context, tenant, site, entity string) ([]Row, error)
}

// Status describes the current cluster and FSM position.
type Status struct {
	Ready         bool
	Leader        string
	LeaderAddress string
	Term          uint64
	CommitIndex   uint64
	AppliedIndex  uint64
	Voters        []string
	Quorum        int
	Epoch         int64
}

// ClusterStatus reads the cluster status from any node.
type ClusterStatus interface {
	Status(ctx context.Context) (Status, error)
}

// Revision exposes the active model and its epoch. The raftfsm adapter reads
// them through from the store so a migrate/rollback applied after the adapter
// was built is observed without re-wrapping it; the application still never
// talks to the store itself. The port has no error channel: a store read
// failure serves the adapter's last-known revision.
type Revision interface {
	Model(ctx context.Context) models.Model
	Epoch(ctx context.Context) int64
}
