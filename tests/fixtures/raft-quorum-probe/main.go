package main

import (
	"context"
	"encoding/json"
	"fmt"
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

func main() {
	result, err := exercise()
	if err != nil {
		_, _ = io.WriteString(os.Stderr, "raft quorum fixture failed\n")
		os.Exit(2)
	}
	if json.NewEncoder(os.Stdout).Encode(result) != nil {
		os.Exit(2)
	}
}

func exercise() (map[string]bool, error) {
	root, err := os.MkdirTemp("", "raft-quorum-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(root)
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}}}}}
	var nodes [3]node
	var logStores [3]*raft.InmemStore
	var snapshots [3]*raft.InmemSnapshotStore
	for index := range nodes {
		address, transport := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node-%d", index)))
		store, err := modelsqlite.Open(context.Background(), filepath.Join(root, fmt.Sprintf("node-%d.sqlite", index)))
		if err != nil {
			return nil, err
		}
		if err := store.InitModel(context.Background(), model); err != nil {
			return nil, err
		}
		nodes[index] = node{transport: transport, store: store, address: address}
		logStores[index] = raft.NewInmemStore()
		snapshots[index] = raft.NewInmemSnapshotStore()
		defer store.Close()
		defer transport.Close()
	}
	for left := range nodes {
		for right := range nodes {
			if left != right {
				nodes[left].transport.Connect(nodes[right].address, nodes[right].transport)
			}
		}
	}
	configurations := make([]raft.Server, 0, len(nodes))
	for index := range nodes {
		configurations = append(configurations, raft.Server{ID: raft.ServerID(nodes[index].address), Address: nodes[index].address, Suffrage: raft.Voter})
	}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodes[0].address)
	config.LogOutput = io.Discard
	config.HeartbeatTimeout = 200 * time.Millisecond
	config.ElectionTimeout = 200 * time.Millisecond
	config.LeaderLeaseTimeout = 200 * time.Millisecond
	config.CommitTimeout = 20 * time.Millisecond
	if err := raft.BootstrapCluster(config, logStores[0], logStores[0], snapshots[0], nodes[0].transport, raft.Configuration{Servers: configurations}); err != nil {
		return nil, err
	}
	for index := range nodes {
		localConfig := *config
		localConfig.LocalID = raft.ServerID(nodes[index].address)
		raftNode, err := raft.NewRaft(&localConfig, &raftfsm.FSM{Store: nodes[index].store}, logStores[index], logStores[index], snapshots[index], nodes[index].transport)
		if err != nil {
			return nil, err
		}
		nodes[index].raft = raftNode
		defer func() { _ = raftNode.Shutdown().Error() }()
	}
	firstLeader, err := waitLeader(nodes[:], -1)
	if err != nil {
		return nil, err
	}
	firstCommit := commit(nodes[firstLeader].raft, "first") == nil
	if !firstCommit {
		return nil, fmt.Errorf("first commit failed")
	}
	for index := range nodes {
		if index == firstLeader {
			continue
		}
		nodes[firstLeader].transport.Disconnect(nodes[index].address)
		nodes[index].transport.Disconnect(nodes[firstLeader].address)
	}
	secondLeader, err := waitLeader(nodes[:], firstLeader)
	if err != nil {
		return nil, err
	}
	failoverCommit := commit(nodes[secondLeader].raft, "second") == nil
	if !failoverCommit {
		return nil, fmt.Errorf("failover commit failed")
	}
	for index := range nodes {
		nodes[index].transport.DisconnectAll()
	}
	time.Sleep(500 * time.Millisecond)
	minorityRefused := commit(nodes[secondLeader].raft, "third") != nil
	return map[string]bool{"firstCommit": firstCommit, "failoverCommit": failoverCommit, "minorityRefused": minorityRefused}, nil
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
	return -1, fmt.Errorf("leader election timeout")
}

func commit(cluster *raft.Raft, id string) error {
	data, err := json.Marshal(models.RaftCommand{Kind: "put", Tenant: "tenant-a", Site: "site-1", Entity: "entries", ID: id, Row: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))})
	if err != nil {
		return err
	}
	future := cluster.Apply(data, 300*time.Millisecond)
	if err := future.Error(); err != nil {
		return err
	}
	if err, ok := future.Response().(error); ok {
		return err
	}
	return nil
}
