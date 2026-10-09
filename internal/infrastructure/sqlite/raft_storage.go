package sqlite

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/hashicorp/raft"
)

var (
	ErrInvalidRaftLog     = errors.New("invalid raft log")
	ErrCorruptRaftStorage = errors.New("corrupt raft storage")
)

var (
	_ raft.LogStore    = (*Store)(nil)
	_ raft.StableStore = (*Store)(nil)
)

// FirstIndex returns the lowest durable Raft log index, or zero when empty.
func (store *Store) FirstIndex() (uint64, error) {
	first, _, err := store.logBounds()
	return first, err
}

// LastIndex returns the highest durable Raft log index, or zero when empty.
func (store *Store) LastIndex() (uint64, error) {
	_, last, err := store.logBounds()
	return last, err
}

func (store *Store) logBounds() (uint64, uint64, error) {
	query := queryRaftLogBounds
	var first, last int64
	if err := store.db.QueryRowContext(context.Background(), query).Scan(&first, &last); err != nil {
		return 0, 0, err
	}
	if first < 0 || last < 0 {
		return 0, 0, ErrCorruptRaftStorage
	}
	return uint64(first), uint64(last), nil
}

// GetLog reads and copies one durable log entry into the caller-owned value.
func (store *Store) GetLog(index uint64, target *raft.Log) error {
	if target == nil {
		return ErrInvalidRaftLog
	}
	key, err := raftUint64(index)
	if err != nil || key == 0 {
		return ErrInvalidRaftLog
	}
	query := queryRaftLogGet
	var storedIndex, term, kind int64
	var data, extensions []byte
	var appendedAt string
	err = store.db.QueryRowContext(context.Background(), query, key).
		Scan(&storedIndex, &term, &kind, &data, &extensions, &appendedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return raft.ErrLogNotFound
	}
	if err != nil {
		return err
	}
	if storedIndex <= 0 || term < 0 || kind < 0 || kind > math.MaxUint8 {
		return ErrCorruptRaftStorage
	}
	parsedTime := time.Time{}
	if appendedAt != "" {
		parsedTime, err = time.Parse(time.RFC3339Nano, appendedAt)
		if err != nil {
			return ErrCorruptRaftStorage
		}
	}
	*target = raft.Log{
		Index:      uint64(storedIndex),
		Term:       uint64(term),
		Type:       raft.LogType(kind),
		Data:       cloneBytes(data),
		Extensions: cloneBytes(extensions),
		AppendedAt: parsedTime,
	}
	return nil
}

// StoreLog persists one log entry using the same transaction semantics as a batch.
func (store *Store) StoreLog(log *raft.Log) error {
	return store.StoreLogs([]*raft.Log{log})
}

// StoreLogs atomically persists a batch, replacing conflicting suffix entries.
func (store *Store) StoreLogs(logs []*raft.Log) (retErr error) {
	if store == nil || store.db == nil {
		return ErrInvalidRaftLog
	}
	query := queryRaftLogStore
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()
	for _, log := range logs {
		if log == nil {
			return ErrInvalidRaftLog
		}
		index, indexErr := raftUint64(log.Index)
		term, termErr := raftUint64(log.Term)
		if indexErr != nil || termErr != nil || index == 0 {
			return ErrInvalidRaftLog
		}
		appendedAt := ""
		if !log.AppendedAt.IsZero() {
			appendedAt = log.AppendedAt.UTC().Format(time.RFC3339Nano)
		}
		data := cloneBytes(log.Data)
		extensions := cloneBytes(log.Extensions)
		if _, err := tx.ExecContext(context.Background(), query, index, term, uint8(log.Type),
			data, data, data, extensions, extensions, extensions, appendedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteRange removes an inclusive index range.
func (store *Store) DeleteRange(minimum, maximum uint64) error {
	if minimum > maximum {
		return nil
	}
	minIndex, minErr := raftUint64(minimum)
	maxIndex, maxErr := raftUint64(maximum)
	if minErr != nil || maxErr != nil {
		return ErrInvalidRaftLog
	}
	query := queryRaftLogDelete
	_, err := store.db.ExecContext(context.Background(), query, minIndex, maxIndex)
	return err
}

// Set stores a byte value under an opaque Raft stable-store key.
func (store *Store) Set(key, value []byte) error {
	query := queryRaftStableSet
	keyBytes, valueBytes := cloneBytes(key), cloneBytes(value)
	_, err := store.db.ExecContext(context.Background(), query,
		keyBytes, keyBytes, keyBytes, valueBytes, valueBytes, valueBytes)
	return err
}

// Get returns nil for a missing key as required by HashiCorp Raft's StableStore.
func (store *Store) Get(key []byte) ([]byte, error) {
	query := queryRaftStableGet
	var value []byte
	keyBytes := cloneBytes(key)
	err := store.db.QueryRowContext(context.Background(), query, keyBytes, keyBytes, keyBytes).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cloneBytes(value), nil
}

// SetUint64 stores a Raft integer as an exact eight-byte big-endian value.
func (store *Store) SetUint64(key []byte, value uint64) error {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return store.Set(key, encoded)
}

// GetUint64 returns zero for a missing key and rejects malformed stored values.
func (store *Store) GetUint64(key []byte) (uint64, error) {
	value, err := store.Get(key)
	if err != nil || value == nil {
		return 0, err
	}
	if len(value) != 8 {
		return 0, ErrCorruptRaftStorage
	}
	return binary.BigEndian.Uint64(value), nil
}

func raftUint64(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, ErrInvalidRaftLog
	}
	return int64(value), nil
}

func cloneBytes(source []byte) []byte {
	if source == nil {
		return []byte{}
	}
	return append([]byte(nil), source...)
}
