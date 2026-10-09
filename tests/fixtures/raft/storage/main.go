package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"io"
	"os"
	"path/filepath"
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

var (
	_ raft.LogStore    = (*modelsqlite.Store)(nil)
	_ raft.StableStore = (*modelsqlite.Store)(nil)
)

func main() {
	result, err := exercise()
	if err != nil {
		check.Value(fmt.Fprintf(os.Stderr, "durable raft storage fixture failed: %v\n", err))
		os.Exit(2)
	}
	if json.NewEncoder(os.Stdout).Encode(result) != nil {
		os.Exit(2)
	}
}

func exercise() (map[string]bool, error) {
	root, err := os.MkdirTemp("", "domain-raft-durable-")
	if err != nil {
		return nil, err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	if err := exerciseStorageOperations(root); err != nil {
		return nil, err
	}
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}}}}}
	first, err := openCluster(root, model, true)
	if err != nil {
		return nil, err
	}
	if err := first[0].store.Set([]byte("probe/stable"), []byte("persisted")); err != nil {
		check.Must(closeCluster(first))
		return nil, err
	}
	if err := first[0].store.SetUint64([]byte("probe/index"), 42); err != nil {
		check.Must(closeCluster(first))
		return nil, err
	}
	leader, err := waitLeader(first, -1)
	if err != nil {
		check.Must(closeCluster(first))
		return nil, err
	}
	replayedLogIndex, err := commitWithIndex(first[leader].raft, "before-restart")
	if err != nil {
		check.Must(closeCluster(first))
		return nil, err
	}
	if err := waitRow(first, "before-restart"); err != nil {
		check.Must(closeCluster(first))
		return nil, err
	}
	if err := closeCluster(first); err != nil {
		return nil, err
	}

	replayStore, err := modelsqlite.Open(context.Background(), filepath.Join(root, "node-0.sqlite"))
	if err != nil {
		return nil, err
	}
	var replayedLog raft.Log
	if err := replayStore.GetLog(replayedLogIndex, &replayedLog); err != nil {
		check.Must(replayStore.Close())
		return nil, err
	}
	// Replaying a committed log after a process restart is an expected recovery
	// path; a materialized command must be acknowledged as a no-op.
	replayFSM := &raftfsm.FSM{Store: replayStore}
	replayResult := replayFSM.Apply(&replayedLog)
	_, replayDidApplyIdempotently := replayResult.(*models.ApplyResult)
	if err := replayStore.Close(); err != nil {
		return nil, err
	}

	restarted, err := openCluster(root, model, false)
	if err != nil {
		return nil, err
	}
	defer func() { check.Must(closeCluster(restarted)) }()
	restartedLeader, err := waitLeader(restarted, -1)
	if err != nil {
		return nil, err
	}
	value, err := restarted[0].store.Get([]byte("probe/stable"))
	if err != nil {
		return nil, err
	}
	stableIndex, err := restarted[0].store.GetUint64([]byte("probe/index"))
	if err != nil {
		return nil, err
	}
	firstRow, err := restarted[0].store.ReadRow(context.Background(), "tenant-a", "site-1", "entries", "before-restart")
	if err != nil || len(firstRow) == 0 {
		return nil, errors.New("restarted node did not restore committed materialized state")
	}
	if err := commit(restarted[restartedLeader].raft, "after-restart"); err != nil {
		return nil, err
	}
	if err := waitRow(restarted, "after-restart"); err != nil {
		return nil, err
	}
	durableFailoverCommit, durableMinorityRefused, err := exerciseDurableFailover(restarted)
	if err != nil {
		return nil, err
	}
	return map[string]bool{
		"firstCommit":                  true,
		"restartRecoveredCommit":       true,
		"restartedNodeAppliedState":    true,
		"stableMetadataPreserved":      string(value) == "persisted" && stableIndex == 42,
		"logStoreOperations":           true,
		"emptyStableValueRoundTrip":    true,
		"corruptStableIntegerRejected": true,
		"durableFailoverCommit":        durableFailoverCommit,
		"durableMinorityRefused":       durableMinorityRefused,
		"replayedRaftLogAcknowledged":  replayDidApplyIdempotently,
	}, nil
}

func exerciseDurableFailover(nodes []node) (bool, bool, error) {
	leader, err := waitLeader(nodes, -1)
	if err != nil {
		return false, false, err
	}
	remaining := make([]int, 0, len(nodes)-1)
	for index := range nodes {
		if index == leader {
			continue
		}
		nodes[leader].transport.Disconnect(nodes[index].address)
		nodes[index].transport.Disconnect(nodes[leader].address)
		remaining = append(remaining, index)
	}
	newLeader, err := waitLeader(nodes, leader)
	if err != nil {
		return false, false, err
	}
	if err := commit(nodes[newLeader].raft, "durable-failover"); err != nil {
		return false, false, err
	}
	if err := waitRowsAt(nodes, remaining, "durable-failover"); err != nil {
		return false, false, err
	}

	for index := range nodes {
		nodes[index].transport.DisconnectAll()
	}
	minorityRefused := commit(nodes[newLeader].raft, "durable-minority") != nil
	if !minorityRefused {
		return true, false, errors.New("SQLite-backed Raft node committed a write after losing its majority")
	}
	for index := range nodes {
		if _, err := nodes[index].store.ReadRow(context.Background(), "tenant-a", "site-1", "entries", "durable-minority"); !errors.Is(err, sql.ErrNoRows) {
			return true, false, errors.New("minority write appeared in the SQLite materialization")
		}
	}
	return true, true, nil
}

func exerciseStorageOperations(root string) error {
	store, err := modelsqlite.Open(context.Background(), filepath.Join(root, "storage-operations.sqlite"))
	if err != nil {
		return err
	}
	defer func() { check.Must(store.Close()) }()
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}}}}}
	if err := store.InitModel(context.Background(), model); err != nil {
		return err
	}
	first, err := store.FirstIndex()
	if err != nil || first != 0 {
		return errors.New("empty Raft store has an unexpected first index")
	}
	last, err := store.LastIndex()
	if err != nil || last != 0 {
		return errors.New("empty Raft store has an unexpected last index")
	}
	if err := store.StoreLogs([]*raft.Log{{Index: 3, Term: 1}, nil}); !errors.Is(err, modelsqlite.ErrInvalidRaftLog) {
		return errors.New("invalid Raft batch was not rejected")
	}
	last, err = store.LastIndex()
	if err != nil || last != 0 {
		return errors.New("invalid Raft batch was partially persisted")
	}
	appendedAt := time.Date(2026, 10, 6, 0, 0, 0, 123, time.UTC)
	logs := []*raft.Log{
		{Index: 5, Term: 2, Type: raft.LogCommand, Data: []byte(`{"payload":true}`), AppendedAt: appendedAt},
		{Index: 7, Term: 3, Type: raft.LogConfiguration, Extensions: []byte("extension")},
	}
	if err := store.StoreLogs(logs); err != nil {
		return err
	}
	var read raft.Log
	if err := store.GetLog(5, &read); err != nil || read.Index != 5 || read.Term != 2 || string(read.Data) != `{"payload":true}` || !read.AppendedAt.Equal(appendedAt) {
		return errors.New("raft log fields did not round-trip")
	}
	if err := store.GetLog(6, &read); !errors.Is(err, raft.ErrLogNotFound) {
		return errors.New("missing Raft log did not return the storage sentinel")
	}
	first, err = store.FirstIndex()
	if err != nil || first != 5 {
		return errors.New("raft first-index metadata did not update")
	}
	last, err = store.LastIndex()
	if err != nil || last != 7 {
		return errors.New("raft last-index metadata did not update")
	}
	if err := store.DeleteRange(5, 6); err != nil {
		return err
	}
	first, err = store.FirstIndex()
	if err != nil || first != 7 {
		return errors.New("inclusive Raft log deletion removed the wrong range")
	}
	if err := store.DeleteRange(7, 7); err != nil {
		return err
	}
	first, err = store.FirstIndex()
	if err != nil || first != 0 {
		return errors.New("deleting all Raft logs did not return an empty index")
	}
	if err := store.Set(nil, nil); err != nil {
		return err
	}
	value, err := store.Get(nil)
	if err != nil || value == nil || len(value) != 0 {
		return errors.New("empty opaque Raft stable value did not round-trip")
	}
	if _, err := store.GetUint64(nil); !errors.Is(err, modelsqlite.ErrCorruptRaftStorage) {
		return errors.New("malformed Raft stable integer was not rejected")
	}
	if value, err := store.Get([]byte("missing")); err != nil || value != nil {
		return errors.New("missing Raft stable value did not return empty")
	}
	if value, err := store.GetUint64([]byte("missing-integer")); err != nil || value != 0 {
		return errors.New("missing Raft stable integer did not return zero")
	}
	command := &models.RaftCommand{Kind: "put", Tenant: "tenant-a", Site: "site-1", Entity: "entries", ID: "fsm-atomic", Row: json.RawMessage(`{"id":"fsm-atomic"}`)}
	if err := store.ApplyRaftEntry(context.Background(), 5, command); err != nil {
		return err
	}
	if err := store.ApplyRaftEntry(context.Background(), 5, command); err != nil {
		return errors.New("replayed durable FSM index was not acknowledged as a no-op")
	}
	if err := store.ApplyRaftEntry(context.Background(), 6, command); err == nil {
		return errors.New("duplicate row command unexpectedly succeeded at a new log index")
	}
	index, err := store.AppliedRaftIndex(context.Background())
	if err != nil || index != 5 {
		return errors.New("failed FSM command advanced the durable applied index")
	}
	command.ID = "fsm-after-failure"
	command.Row = json.RawMessage(`{"id":"fsm-after-failure"}`)
	if err := store.ApplyRaftEntry(context.Background(), 6, command); err != nil {
		return err
	}
	index, err = store.AppliedRaftIndex(context.Background())
	if err != nil || index != 6 {
		return errors.New("successful FSM command did not atomically advance the applied index")
	}
	if _, ok := (&raftfsm.FSM{Store: store}).Apply(&raft.Log{Index: 7, Type: raft.LogBarrier}).(*models.ApplyResult); !ok {
		return errors.New("raft barrier was not durably acknowledged")
	}
	index, err = store.AppliedRaftIndex(context.Background())
	if err != nil || index != 7 {
		return errors.New("raft barrier did not advance the durable applied index")
	}
	return nil
}

func openCluster(root string, model models.Model, bootstrap bool) ([]node, error) {
	var nodes [3]node
	var snapshotStores [3]raft.SnapshotStore
	for index := range nodes {
		address, transport := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node-%d", index)))
		path := filepath.Join(root, fmt.Sprintf("node-%d.sqlite", index))
		_, statErr := os.Stat(path)
		newStore := errors.Is(statErr, os.ErrNotExist)
		if statErr != nil && !newStore {
			return nil, statErr
		}
		store, err := modelsqlite.Open(context.Background(), path)
		if err != nil {
			return nil, err
		}
		if newStore {
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
	configuration := make([]raft.Server, 0, len(nodes))
	for index := range nodes {
		configuration = append(configuration, raft.Server{ID: raft.ServerID(nodes[index].address), Address: nodes[index].address, Suffrage: raft.Voter})
	}
	if bootstrap {
		if err := raft.BootstrapCluster(raftConfig(nodes[0].address), nodes[0].store, nodes[0].store,
			snapshotStores[0], nodes[0].transport, raft.Configuration{Servers: configuration}); err != nil {
			check.Must(closeCluster(nodes[:]))
			return nil, err
		}
	}
	for index := range nodes {
		config := raftConfig(nodes[index].address)
		instance, err := raft.NewRaft(config, &raftfsm.FSM{Store: nodes[index].store}, nodes[index].store, nodes[index].store, snapshotStores[index], nodes[index].transport)
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
	return config
}

func waitLeader(nodes []node, excluded int) (int, error) {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for index := range nodes {
			if index != excluded && nodes[index].raft.State() == raft.Leader {
				return index, nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1, errors.New("raft leader election timed out")
}

func commit(cluster *raft.Raft, id string) error {
	_, err := commitWithIndex(cluster, id)
	return err
}

func commitWithIndex(cluster *raft.Raft, id string) (uint64, error) {
	data, err := json.Marshal(models.RaftCommand{Kind: "put", Tenant: "tenant-a", Site: "site-1", Entity: "entries", ID: id, Row: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))})
	if err != nil {
		return 0, err
	}
	future := cluster.Apply(data, 3*time.Second)
	if err := future.Error(); err != nil {
		return 0, err
	}
	if err, ok := future.Response().(error); ok {
		return 0, err
	}
	return future.Index(), nil
}

func waitRow(nodes []node, id string) error {
	indexes := make([]int, len(nodes))
	for index := range nodes {
		indexes[index] = index
	}
	return waitRowsAt(nodes, indexes, id)
}

func waitRowsAt(nodes []node, indexes []int, id string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		complete := true
		for _, index := range indexes {
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
