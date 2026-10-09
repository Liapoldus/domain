package raftfsm

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/hashicorp/raft"
)

const (
	defaultWriteTimeout           = 15 * time.Second
	defaultBarrierDeadline        = 1500 * time.Millisecond
	defaultScanEntityMaximumBytes = int64(512 << 20)
)

// scanRow mirrors the snapshot row object so ScanEntity can walk an export
// stream without importing the private snapshot representation.
type scanRow struct {
	Tenant string          `json:"tenant"`
	Site   string          `json:"site"`
	Entity string          `json:"entity"`
	ID     string          `json:"id"`
	Data   json.RawMessage `json:"data"`
}

// Cluster implements the product-API ports (Proposer, ReadBarrier, RowReader,
// ClusterStatus and Revision) on top of one raft node and its sqlite store.
// One Cluster wraps one node; the leader-facing and read-facing operations
// must only be invoked when this node is the leader (the product service
// checks IsLeader before proposing or serving fresh reads).
type Cluster struct {
	node         *raft.Raft
	store        *modelsqlite.Store
	model        models.Model
	epoch        int64
	revisionMu   sync.Mutex
	writeTimeout time.Duration
}

// NewCluster wraps a running raft node. The model and epoch seed the
// last-known revision: they are served only when the store cannot be read
// (see revision), never as the primary answer.
func NewCluster(node *raft.Raft, store *modelsqlite.Store, model models.Model, epoch int64, writeTimeout time.Duration) *Cluster {
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}
	return &Cluster{
		node:         node,
		store:        store,
		model:        model,
		epoch:        epoch,
		writeTimeout: writeTimeout,
	}
}

// revision reads the durable model and epoch from the store, so a
// migrate/rollback that commits after construction is observed by the product
// API without re-wrapping the adapter. Because Model() and Epoch() cannot
// report an error, a failed store read (store closed, transient SQLite error)
// falls back to the last-known revision: the construction-time values seeded
// by NewCluster, refreshed by every successful read. A read waits for an
// in-flight store transaction instead of serving a revision the store has
// already moved past.
func (cluster *Cluster) revision(ctx context.Context) (models.Model, int64) {
	var revision models.StoredRevision
	var err error
	if cluster.store != nil {
		revision, err = cluster.store.CurrentRevision(ctx)
	}
	cluster.revisionMu.Lock()
	defer cluster.revisionMu.Unlock()
	if err != nil || cluster.store == nil {
		return cluster.model, cluster.epoch
	}
	cluster.model, cluster.epoch = revision.Model, revision.Epoch
	return cluster.model, cluster.epoch
}

// Model returns the durable revision model, falling back to the last-known
// model when the store cannot be read (see revision).
func (cluster *Cluster) Model(ctx context.Context) models.Model {
	model, _ := cluster.revision(ctx)
	return model
}

// Epoch returns the durable revision epoch, falling back to the last-known
// epoch when the store cannot be read (see revision).
func (cluster *Cluster) Epoch(ctx context.Context) int64 {
	_, epoch := cluster.revision(ctx)
	return epoch
}

// IsLeader reports whether this node currently holds the raft leadership.
func (cluster *Cluster) IsLeader() bool {
	if cluster == nil || cluster.node == nil {
		return false
	}
	return cluster.node.State() == raft.Leader
}

// LeaderAddress returns the address of the known leader, or an empty string
// when no leader is known.
func (cluster *Cluster) LeaderAddress() string {
	if cluster == nil || cluster.node == nil {
		return ""
	}
	return string(cluster.node.Leader())
}

// Apply proposes one deterministic command. Outcomes map to the interfaces
// sentinels: not leader, unknown outcome (apply deadline), unavailable
// (leadership lost / quorum / transport) and store-classified failures.
func (cluster *Cluster) Apply(ctx context.Context, command models.RaftCommand) (*models.ApplyResult, error) {
	if cluster == nil || cluster.node == nil {
		return nil, interfaces.ErrInternal
	}
	data, err := json.Marshal(&command)
	if err != nil {
		return nil, interfaces.ErrInternal
	}
	future := cluster.node.Apply(data, cluster.writeTimeout)
	if err := future.Error(); err != nil {
		return nil, mapApplyError(err)
	}
	response := future.Response()
	if result, ok := response.(*models.ApplyResult); ok {
		if result == nil {
			return nil, interfaces.ErrInternal
		}
		return result, nil
	}
	if applyErr, ok := response.(error); ok {
		if applyErr == nil {
			return nil, interfaces.ErrInternal
		}
		return nil, mapStoreError(applyErr)
	}
	if response == nil {
		return nil, interfaces.ErrInternal
	}
	return nil, interfaces.ErrInternal
}

// Barrier establishes a quorum-backed fresh-read fence on the leader and
// returns the leader's durable FSM index.
func (cluster *Cluster) Barrier(ctx context.Context) (uint64, error) {
	if cluster == nil || cluster.node == nil || cluster.store == nil {
		return 0, interfaces.ErrUnavailable
	}
	index, err := CommitReadBarrier(ctx, cluster.node, cluster.store, defaultBarrierDeadline)
	if err != nil {
		return 0, interfaces.ErrUnavailable
	}
	return index, nil
}

// ReadRow reads one row from the materialized store. A missing row returns
// found=false without an error.
func (cluster *Cluster) ReadRow(ctx context.Context, tenant, site, entity, id string) (json.RawMessage, bool, error) {
	if cluster == nil || cluster.store == nil {
		return nil, false, interfaces.ErrInternal
	}
	document, err := cluster.store.ReadRow(ctx, tenant, site, entity, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, interfaces.ErrInternal
	}
	return json.RawMessage(document), true, nil
}

// ScanEntity streams every row of one entity type under the given scope by
// walking the store export stream. The export is bounded by the configured
// maximum snapshot bytes; an over-large export fails the scan rather than
// serving a partial entity.
func (cluster *Cluster) ScanEntity(ctx context.Context, tenant, site, entity string) ([]interfaces.Row, error) {
	if cluster == nil || cluster.store == nil {
		return nil, interfaces.ErrInternal
	}
	buffer := &bytes.Buffer{}
	if err := cluster.store.ExportTo(ctx, buffer, defaultScanEntityMaximumBytes); err != nil {
		return nil, mapStoreError(err)
	}
	decoder := json.NewDecoder(buffer)
	if _, err := decoder.Token(); err != nil {
		return nil, interfaces.ErrInternal
	}
	rows := make([]interfaces.Row, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, interfaces.ErrInternal
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, interfaces.ErrInternal
		}
		if key != "rows" {
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				return nil, interfaces.ErrInternal
			}
			continue
		}
		if _, err := decoder.Token(); err != nil {
			return nil, interfaces.ErrInternal
		}
		for decoder.More() {
			var row scanRow
			if err := decoder.Decode(&row); err != nil {
				return nil, interfaces.ErrInternal
			}
			if row.Tenant == tenant && row.Site == site && row.Entity == entity && len(row.Data) > 0 {
				rows = append(rows, interfaces.Row{ID: row.ID, Data: row.Data})
			}
		}
		if _, err := decoder.Token(); err != nil {
			return nil, interfaces.ErrInternal
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, interfaces.ErrUnavailable
	}
	return rows, nil
}

// Status reads the cluster status from this node. It never requires the
// leadership, so it can be served from any follower.
func (cluster *Cluster) Status(ctx context.Context) (interfaces.Status, error) {
	if cluster == nil || cluster.node == nil || cluster.store == nil {
		return interfaces.Status{}, interfaces.ErrInternal
	}
	state := cluster.node.State()
	status := interfaces.Status{Ready: state != raft.Shutdown}
	leaderAddress, leaderID := cluster.node.LeaderWithID()
	status.Leader = string(leaderID)
	status.LeaderAddress = string(leaderAddress)
	status.Ready = leaderID != ""
	stats := cluster.node.Stats()
	status.Term = parseStatsUint(stats["term"])
	status.CommitIndex = parseStatsUint(stats["commit_index"])
	applied, err := cluster.store.AppliedRaftIndex(context.Background())
	if err != nil {
		return interfaces.Status{}, interfaces.ErrInternal
	}
	status.AppliedIndex = applied
	configurationFuture := cluster.node.GetConfiguration()
	if err := configurationFuture.Error(); err != nil {
		return interfaces.Status{}, interfaces.ErrUnavailable
	}
	for _, server := range configurationFuture.Configuration().Servers {
		if server.Suffrage == raft.Voter {
			status.Voters = append(status.Voters, string(server.ID))
		}
	}
	status.Quorum = len(status.Voters)/2 + 1
	_, status.Epoch = cluster.revision(ctx)
	return status, nil
}

// mapApplyError classifies the raft-level error of an apply future.
func mapApplyError(err error) error {
	if err == nil {
		return interfaces.ErrInternal
	}
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipTransferInProgress) {
		return interfaces.ErrNotLeader
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return interfaces.ErrUnknownOutcome
	}
	if errors.Is(err, raft.ErrEnqueueTimeout) || errors.Is(err, raft.ErrLeadershipLost) || errors.Is(err, raft.ErrRaftShutdown) {
		return interfaces.ErrUnavailable
	}
	return interfaces.ErrInternal
}

// mapStoreError classifies the sqlite <-> application sentinel errors raised
// by the store while applying an entry.
func mapStoreError(err error) error {
	if err == nil {
		return interfaces.ErrInternal
	}
	switch {
	case errors.Is(err, modelsqlite.ErrForbiddenGroup):
		return interfaces.ErrForbidden
	case errors.Is(err, modelsqlite.ErrRowNotFound):
		return interfaces.ErrNotFound
	case errors.Is(err, modelsqlite.ErrRowExists),
		errors.Is(err, modelsqlite.ErrUniqueViolation),
		errors.Is(err, modelsqlite.ErrForeignKeyViolation),
		errors.Is(err, modelsqlite.ErrModelConflict),
		errors.Is(err, modelsqlite.ErrNoSnapshot):
		return interfaces.ErrConflict
	case errors.Is(err, modelsqlite.ErrInvalidRow),
		errors.Is(err, modelsqlite.ErrUnsafeMigration),
		errors.Is(err, modelsqlite.ErrInvalidRaftIndex):
		return interfaces.ErrInvalid
	case errors.Is(err, interfaces.ErrEpochMismatch):
		return interfaces.ErrEpochMismatch
	case errors.Is(err, modelsqlite.ErrSnapshotTooLarge),
		errors.Is(err, modelsqlite.ErrCorruptRaftStorage),
		errors.Is(err, modelsqlite.ErrInvalidRaftLog):
		return interfaces.ErrInternal
	default:
		return interfaces.ErrInternal
	}
}

func parseStatsUint(value string) uint64 {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}
