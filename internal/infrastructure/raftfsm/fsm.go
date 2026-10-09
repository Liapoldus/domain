// Package raftfsm adapts durable Domain state to HashiCorp Raft.
package raftfsm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/Liapoldus/domain/internal/domain/models"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/hashicorp/raft"
)

var (
	ErrInvalidCommand   = errors.New("invalid raft command")
	ErrSnapshotTooLarge = modelsqlite.ErrSnapshotTooLarge
)

const defaultMaximumSnapshotBytes int64 = 512 << 20

// FSM applies only commands already committed by Raft. It deliberately does
// not expose a direct public write API or a non-quorum fallback.
type FSM struct {
	Store                *modelsqlite.Store
	MaximumSnapshotBytes int64
}

func (fsm *FSM) Apply(log *raft.Log) interface{} {
	if fsm == nil || fsm.Store == nil || log == nil || log.Index == 0 {
		return ErrInvalidCommand
	}
	var command *models.RaftCommand
	switch log.Type {
	case raft.LogCommand:
		command = new(models.RaftCommand)
		if len(log.Data) == 0 || !json.Valid(log.Data) || json.Unmarshal(log.Data, command) != nil || command.Kind == "" {
			return ErrInvalidCommand
		}
	case raft.LogBarrier, raft.LogConfiguration:
		// These committed entries advance the durable FSM fence but do not mutate
		// Domain-owned product state.
	default:
		return ErrInvalidCommand
	}
	result, err := fsm.Store.ApplyRaftEntryResult(context.Background(), log.Index, command)
	if err != nil {
		return err
	}
	return result
}

func (fsm *FSM) Snapshot() (ret0 raft.FSMSnapshot, retErr error) {
	if fsm == nil || fsm.Store == nil {
		return nil, ErrInvalidCommand
	}
	file, err := os.CreateTemp("", "liapoldus-domain-snapshot-*")
	if err != nil {
		return nil, err
	}
	path := file.Name()
	if err := fsm.Store.ExportTo(context.Background(), file, fsm.maximumSnapshotBytes()); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return *new(raft.FSMSnapshot), closeErr
		}
		if closeErr := os.Remove(path); closeErr != nil {
			return *new(raft.FSMSnapshot), closeErr
		}
		return nil, err
	}
	if err := file.Sync(); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return *new(raft.FSMSnapshot), closeErr
		}
		if closeErr := os.Remove(path); closeErr != nil {
			return *new(raft.FSMSnapshot), closeErr
		}
		return nil, err
	}
	if err := file.Close(); err != nil {
		if closeErr := os.Remove(path); closeErr != nil {
			return *new(raft.FSMSnapshot), closeErr
		}
		return nil, err
	}
	return &snapshot{path: path}, nil
}

func (fsm *FSM) maximumSnapshotBytes() int64 {
	if fsm.MaximumSnapshotBytes > 0 && fsm.MaximumSnapshotBytes < defaultMaximumSnapshotBytes {
		return fsm.MaximumSnapshotBytes
	}
	return defaultMaximumSnapshotBytes
}

func (fsm *FSM) Restore(reader io.ReadCloser) (retErr error) {
	if reader != nil {
		defer func() { retErr = errors.Join(retErr, reader.Close()) }()
	}
	if fsm == nil || fsm.Store == nil || reader == nil {
		return ErrInvalidCommand
	}
	maximum := fsm.maximumSnapshotBytes()
	file, err := os.CreateTemp("", "liapoldus-domain-restore-*")
	if err != nil {
		return ErrInvalidCommand
	}
	path := file.Name()
	defer func() { retErr = errors.Join(retErr, os.Remove(path)) }()
	written, err := io.Copy(file, io.LimitReader(reader, maximum+1))
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return closeErr
		}
		return ErrInvalidCommand
	}
	if written > maximum {
		if closeErr := file.Close(); closeErr != nil {
			return closeErr
		}
		return ErrSnapshotTooLarge
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return closeErr
		}
		return ErrInvalidCommand
	}
	err = fsm.Store.ImportFrom(context.Background(), file)
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return ErrInvalidCommand
	}
	return nil
}

type snapshot struct{ path string }

func (state *snapshot) Persist(sink raft.SnapshotSink) (retErr error) {
	file, err := os.Open(state.path)
	if err != nil {
		if closeErr := sink.Cancel(); closeErr != nil {
			return closeErr
		}
		return err
	}
	if _, err := io.Copy(sink, file); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return closeErr
		}
		if closeErr := sink.Cancel(); closeErr != nil {
			return closeErr
		}
		return err
	}
	if err := file.Close(); err != nil {
		if closeErr := sink.Cancel(); closeErr != nil {
			return closeErr
		}
		return err
	}
	if err := sink.Close(); err != nil {
		if closeErr := sink.Cancel(); closeErr != nil {
			return closeErr
		}
		return err
	}
	return nil
}

func (state *snapshot) Release() {
	if state.path == "" {
		return
	}
	if err := os.Remove(state.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return // retain the path so cleanup can be retried
	}
	state.path = ""
}
