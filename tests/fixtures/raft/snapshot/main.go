package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/Liapoldus/domain/internal/infrastructure/raftfsm"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/hashicorp/raft"
)

type node struct {
	raft      *raft.Raft
	transport *raft.InmemTransport
	store     *modelsqlite.Store
	address   raft.ServerAddress
}

func main() {
	if len(os.Args) != 3 {
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "prepare":
		err = prepare(os.Args[2])
		if err == nil {
			os.Exit(0) // Simulate abrupt process termination with durable snapshot files.
		}
	case "recover":
		var result map[string]bool
		result, err = recoverSnapshot(os.Args[2])
		if err == nil && json.NewEncoder(os.Stdout).Encode(result) == nil {
			return
		}
	}
	if err != nil {
		check.Value(fmt.Fprintf(os.Stderr, "bounded raft snapshot fixture failed: %v\n", err))
	}
	os.Exit(2)
}

func prepare(root string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}, {Name: "payload", Type: "text"}}}}}
	cluster, err := openCluster(root, model, true)
	if err != nil {
		return err
	}
	leader, err := waitLeader(cluster)
	if err != nil {
		check.Must(closeCluster(cluster))
		return err
	}
	if err := commit(cluster[leader].raft, "snapshot-row"); err != nil {
		check.Must(closeCluster(cluster))
		return err
	}
	if err := waitRow(cluster, "snapshot-row"); err != nil {
		check.Must(closeCluster(cluster))
		return err
	}
	if err := cluster[leader].raft.Snapshot().Error(); err != nil {
		check.Must(closeCluster(cluster))
		return err
	}
	firstIndex, err := cluster[leader].store.FirstIndex()
	lastIndex, lastErr := cluster[leader].store.LastIndex()
	snapshotFiles, snapshotErr := os.ReadDir(filepath.Join(root, fmt.Sprintf("snapshots-%d", leader)))
	if err != nil || lastErr != nil || snapshotErr != nil || len(snapshotFiles) == 0 || firstIndex != 0 || lastIndex != 0 {
		check.Must(closeCluster(cluster))
		return fmt.Errorf("snapshot did not persist and compact the durable Raft log: first=%d last=%d", firstIndex, lastIndex)
	}
	if err := check.WriteFile(filepath.Join(root, "snapshot-leader"), []byte(fmt.Sprintf("%d", leader)), 0o600); err != nil {
		check.Must(closeCluster(cluster))
		return err
	}
	return nil
}

func recoverSnapshot(root string) (map[string]bool, error) {
	leaderBytes, err := check.ReadFile(filepath.Join(root, "snapshot-leader"))
	if err != nil || len(leaderBytes) != 1 || leaderBytes[0] < '0' || leaderBytes[0] > '2' {
		return nil, errors.New("snapshot leader marker is invalid")
	}
	leader := int(leaderBytes[0] - '0')
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}, {Name: "payload", Type: "text"}}}}}
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(root, fmt.Sprintf("snapshots-%d", leader)), 2, io.Discard)
	if err != nil {
		return nil, err
	}
	snapshots, err := snapshotStore.List()
	if err != nil || len(snapshots) == 0 {
		return nil, errors.New("persisted snapshot metadata is missing")
	}
	wantedAppliedIndex := snapshots[0].Index
	// Erase only the FSM materialization; the Raft log metadata and snapshot
	// files remain after the first fixture process has exited abruptly.
	store, err := modelsqlite.Open(context.Background(), filepath.Join(root, fmt.Sprintf("node-%d.sqlite", leader)))
	if err != nil {
		return nil, err
	}
	emptyState := check.Value(json.Marshal(struct {
		Model            json.RawMessage `json:"model"`
		Epoch            int64           `json:"epoch"`
		RaftAppliedIndex uint64          `json:"raftAppliedIndex"`
		Rows             []any           `json:"rows"`
	}{Model: mustJSON(model), Epoch: 1, RaftAppliedIndex: 0, Rows: []any{}}))
	if err := store.Import(context.Background(), emptyState); err != nil {
		check.Must(store.Close())
		return nil, err
	}
	if err := store.Close(); err != nil {
		return nil, err
	}
	restarted, err := openCluster(root, model, false)
	if err != nil {
		return nil, err
	}
	if err := waitRow([]node{restarted[leader]}, "snapshot-row"); err != nil {
		return nil, errors.New("restarted node did not restore its compacted snapshot")
	}
	actualAppliedIndex, err := restarted[leader].store.AppliedRaftIndex(context.Background())
	if err != nil || actualAppliedIndex != wantedAppliedIndex {
		return nil, errors.New("restarted node did not restore the snapshot's applied Raft index")
	}
	if err := closeCluster(restarted); err != nil {
		return nil, err
	}

	boundedStore, err := modelsqlite.Open(context.Background(), filepath.Join(root, "bounded.sqlite"))
	if err != nil {
		return nil, err
	}
	defer func() { check.Must(boundedStore.Close()) }()
	if err := boundedStore.InitModel(context.Background(), model); err != nil {
		return nil, err
	}
	largeRow := []byte(fmt.Sprintf(`{"id":"bounded","payload":%q}`, strings.Repeat("x", 4096)))
	if err := boundedStore.PutRow(context.Background(), "tenant-a", "site-1", "entries", "bounded", largeRow); err != nil {
		return nil, err
	}
	fsm := &raftfsm.FSM{Store: boundedStore, MaximumSnapshotBytes: 1024}
	if snapshot, err := fsm.Snapshot(); err == nil {
		snapshot.Release()
		return nil, errors.New("snapshot larger than its configured bound was accepted")
	} else if !errors.Is(err, raftfsm.ErrSnapshotTooLarge) {
		return nil, errors.New("oversized snapshot returned an unexpected error")
	}
	if err := fsm.Restore(io.NopCloser(&oversizedReader{})); !errors.Is(err, raftfsm.ErrSnapshotTooLarge) {
		return nil, errors.New("oversized restore was not rejected")
	}
	legacyState := check.Value(json.Marshal(struct {
		Model json.RawMessage `json:"model"`
		Epoch int64           `json:"epoch"`
		Rows  []any           `json:"rows"`
	}{Model: mustJSON(model), Epoch: 1, Rows: []any{}}))
	if err := boundedStore.Import(context.Background(), legacyState); err == nil {
		return nil, errors.New("snapshot without durable applied index was accepted")
	}
	row, err := boundedStore.ReadRow(context.Background(), "tenant-a", "site-1", "entries", "bounded")
	if err != nil || len(row) == 0 {
		return nil, errors.New("failed restore modified the active materialization")
	}
	return map[string]bool{
		"snapshotCreated":              true,
		"logsCompacted":                true,
		"restartRestoredSnapshot":      true,
		"snapshotRestoredAppliedIndex": true,
		"snapshotLimitRejected":        true,
		"failedRestorePreservedState":  true,
	}, nil
}

type oversizedReader struct{ remaining int }

func (reader *oversizedReader) Read(buffer []byte) (int, error) {
	if reader.remaining == 0 {
		reader.remaining = 2048
	}
	if len(buffer) > reader.remaining {
		buffer = buffer[:reader.remaining]
	}
	for index := range buffer {
		buffer[index] = 'x'
	}
	reader.remaining -= len(buffer)
	return len(buffer), nil
}

func openCluster(root string, model models.Model, bootstrap bool) ([]node, error) {
	var nodes [3]node
	var snapshotStores [3]raft.SnapshotStore
	for index := range nodes {
		address, transport := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("snapshot-node-%d", index)))
		path := filepath.Join(root, fmt.Sprintf("node-%d.sqlite", index))
		store, err := modelsqlite.Open(context.Background(), path)
		if err != nil {
			return nil, err
		}
		if bootstrap {
			if err := store.InitModel(context.Background(), model); err != nil {
				check.Must(store.Close())
				return nil, err
			}
		}
		nodes[index] = node{transport: transport, store: store, address: address}
		snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(root, fmt.Sprintf("snapshots-%d", index)), 2, io.Discard)
		if err != nil {
			check.Must(closeCluster(nodes[:]))
			return nil, err
		}
		snapshotStores[index] = snapshotStore
	}
	for left := range nodes {
		for right := range nodes {
			if left != right {
				nodes[left].transport.Connect(nodes[right].address, nodes[right].transport)
			}
		}
	}
	servers := make([]raft.Server, 0, len(nodes))
	for index := range nodes {
		servers = append(servers, raft.Server{ID: raft.ServerID(nodes[index].address), Address: nodes[index].address, Suffrage: raft.Voter})
	}
	if bootstrap {
		if err := raft.BootstrapCluster(raftConfig(nodes[0].address), nodes[0].store, nodes[0].store, snapshotStores[0], nodes[0].transport, raft.Configuration{Servers: servers}); err != nil {
			check.Must(closeCluster(nodes[:]))
			return nil, err
		}
	}
	for index := range nodes {
		instance, err := raft.NewRaft(raftConfig(nodes[index].address), &raftfsm.FSM{Store: nodes[index].store}, nodes[index].store, nodes[index].store, snapshotStores[index], nodes[index].transport)
		if err != nil {
			check.Must(closeCluster(nodes[:]))
			return nil, err
		}
		nodes[index].raft = instance
	}
	return nodes[:], nil
}

func raftConfig(address raft.ServerAddress) *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(address)
	config.LogOutput = io.Discard
	config.HeartbeatTimeout = 250 * time.Millisecond
	config.ElectionTimeout = 250 * time.Millisecond
	config.LeaderLeaseTimeout = 250 * time.Millisecond
	config.CommitTimeout = 20 * time.Millisecond
	config.TrailingLogs = 0
	return config
}

func waitLeader(nodes []node) (int, error) {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for index := range nodes {
			if nodes[index].raft != nil && nodes[index].raft.State() == raft.Leader {
				return index, nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1, errors.New("raft leader election timed out")
}

func commit(cluster *raft.Raft, id string) error {
	data, err := json.Marshal(models.RaftCommand{Kind: "put", Tenant: "tenant-a", Site: "site-1", Entity: "entries", ID: id, Row: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))})
	if err != nil {
		return err
	}
	future := cluster.Apply(data, 3*time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	if result, ok := future.Response().(error); ok {
		return result
	}
	return nil
}

func waitRow(nodes []node, id string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		complete := true
		for index := range nodes {
			if _, err := nodes[index].store.ReadRow(context.Background(), "tenant-a", "site-1", "entries", id); err != nil {
				complete = false
				break
			}
		}
		if complete {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("raft peers did not apply the committed row")
}

func closeCluster(nodes []node) error {
	var first error
	for index := range nodes {
		if nodes[index].raft != nil {
			if err := nodes[index].raft.Shutdown().Error(); err != nil && first == nil {
				first = err
			}
		}
	}
	for index := range nodes {
		if nodes[index].transport != nil {
			check.Must(nodes[index].transport.Close())
		}
		if nodes[index].store != nil {
			if err := nodes[index].store.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic("fixture model encoding failed")
	}
	return encoded
}
