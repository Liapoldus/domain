package raftfsm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/Liapoldus/domain/internal/domain/models"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/hashicorp/raft"
)

var ErrInvalidCommand = errors.New("invalid raft command")

// FSM applies only commands already committed by Raft. It deliberately does
// not expose a direct public write API or a non-quorum fallback.
type FSM struct {
	Store *modelsqlite.Store
}

func (fsm *FSM) Apply(log *raft.Log) interface{} {
	if fsm == nil || fsm.Store == nil || log == nil {
		return ErrInvalidCommand
	}
	var command models.RaftCommand
	if err := json.Unmarshal(log.Data, &command); err != nil {
		return ErrInvalidCommand
	}
	ctx := context.Background()
	switch command.Kind {
	case "migrate":
		return fsm.Store.ApplyMigration(ctx, command.Previous, command.Next)
	case "rollback":
		return fsm.Store.RollbackMigration(ctx, command.Next)
	case "put":
		return fsm.Store.PutRow(ctx, command.Tenant, command.Site, command.Entity, command.ID, command.Row)
	default:
		return ErrInvalidCommand
	}
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	if fsm == nil || fsm.Store == nil {
		return nil, ErrInvalidCommand
	}
	data, err := fsm.Store.Export(context.Background())
	if err != nil {
		return nil, err
	}
	return &snapshot{data: data}, nil
}

func (fsm *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	if fsm == nil || fsm.Store == nil {
		return ErrInvalidCommand
	}
	const maximumSnapshotBytes = 512 << 20
	data, err := io.ReadAll(io.LimitReader(reader, maximumSnapshotBytes+1))
	if err != nil || len(data) > maximumSnapshotBytes {
		return ErrInvalidCommand
	}
	return fsm.Store.Import(context.Background(), data)
}

type snapshot struct{ data []byte }

func (state *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, bytes.NewReader(state.data)); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (state *snapshot) Release() { state.data = nil }
