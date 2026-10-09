package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"io"
	"os"
	"path/filepath"

	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/Liapoldus/domain/internal/infrastructure/raftfsm"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/hashicorp/raft"
)

type bufferSink struct{ bytes.Buffer }

func (*bufferSink) ID() string    { return "test" }
func (*bufferSink) Cancel() error { return nil }
func (*bufferSink) Close() error  { return nil }

func main() {
	root, err := os.MkdirTemp("", "raft-fsm-")
	if err != nil {
		os.Exit(2)
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	ctx := context.Background()
	store, err := modelsqlite.Open(ctx, filepath.Join(root, "first.sqlite"))
	if err != nil {
		os.Exit(2)
	}
	defer func() { check.Must(store.Close()) }()
	previous := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}, {Name: "title", Type: "text", Required: true}}}}}
	next := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}, {Name: "heading", Type: "text", Required: true}}}}, Migrations: []models.Mapping{{Entity: "entries", From: "title", To: "heading"}}}
	if store.InitModel(ctx, previous) != nil || store.PutRow(ctx, "tenant-a", "site-1", "entries", "a", []byte(`{"id":"a","title":"hello"}`)) != nil {
		os.Exit(2)
	}
	fsm := &raftfsm.FSM{Store: store}
	command := check.Value(json.Marshal(models.RaftCommand{Kind: "migrate", Previous: previous, Next: next}))
	applyResult := fsm.Apply(&raft.Log{Index: 1, Type: raft.LogCommand, Data: command})
	if result, ok := applyResult.(*models.ApplyResult); !ok || result == nil {
		os.Exit(2)
	}
	snapshot, err := fsm.Snapshot()
	if err != nil {
		os.Exit(2)
	}
	sink := &bufferSink{}
	if snapshot.Persist(sink) != nil {
		os.Exit(2)
	}
	snapshot.Release()
	restoredStore, err := modelsqlite.Open(ctx, filepath.Join(root, "restored.sqlite"))
	if err != nil {
		os.Exit(2)
	}
	defer func() { check.Must(restoredStore.Close()) }()
	if (&raftfsm.FSM{Store: restoredStore}).Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))) != nil {
		os.Exit(2)
	}
	row, err := restoredStore.ReadRow(ctx, "tenant-a", "site-1", "entries", "a")
	if err != nil {
		os.Exit(2)
	}
	response := struct {
		Applied  bool            `json:"applied"`
		Restored json.RawMessage `json:"restored"`
	}{Applied: true, Restored: row}
	if json.NewEncoder(os.Stdout).Encode(response) != nil {
		os.Exit(2)
	}
}
