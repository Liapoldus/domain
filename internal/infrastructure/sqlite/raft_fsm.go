package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Liapoldus/domain/internal/application/migration"
	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
)

var ErrInvalidRaftIndex = errors.New("invalid raft FSM index")

// ApplyRaftEntry applies one committed FSM entry and advances its durable
// index in the same SQLite transaction. Replayed entries are acknowledged as
// no-ops. A nil command records a Raft barrier entry without changing product
// state. It delegates to ApplyRaftEntryResult and only surfaces failures.
func (store *Store) ApplyRaftEntry(ctx context.Context, index uint64, command *models.RaftCommand) error {
	_, err := store.ApplyRaftEntryResult(ctx, index, command)
	return err
}

// ApplyRaftEntryResult applies one committed FSM entry and returns a non-nil
// result on success. Duplicate is true when the index was already applied or
// when the scoped writeId already has a durable ledger record; such entries
// advance the applied index without changing product state. Any failure
// returns a nil result and an error, leaving the applied index untouched.
func (store *Store) ApplyRaftEntryResult(ctx context.Context, index uint64, command *models.RaftCommand) (ret0 *models.ApplyResult, retErr error) {
	if store == nil || store.db == nil || index == 0 || index > uint64(^uint64(0)>>1) {
		return nil, ErrInvalidRaftIndex
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()

	readQuery := queryRaftFsmAppliedGet
	var applied int64
	if err := tx.QueryRowContext(ctx, readQuery).Scan(&applied); err != nil {
		return nil, err
	}
	if applied < 0 {
		return nil, ErrCorruptRaftStorage
	}
	if index <= uint64(applied) {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &models.ApplyResult{Duplicate: true}, nil
	}
	if command == nil {
		if err := store.commitAppliedIndex(ctx, tx, index); err != nil {
			return nil, err
		}
		return &models.ApplyResult{}, nil
	}
	ledgerScoped := command.WriteID != "" && command.Tenant != "" && command.Site != ""
	if ledgerScoped {
		recorded, err := ledgerContainsTx(ctx, tx, command.Tenant, command.Site, command.WriteID)
		if err != nil {
			return nil, err
		}
		if recorded {
			if err := store.commitAppliedIndex(ctx, tx, index); err != nil {
				return nil, err
			}
			return &models.ApplyResult{Duplicate: true}, nil
		}
	}
	// FSM-level epoch fencing for write commands
	switch command.Kind {
	case "create", "update", "delete", "batch", "migrate", "rollback":
		currentEpoch, epochErr := store.EpochTx(ctx, tx)
		if epochErr != nil {
			return nil, epochErr
		}
		// If command carries explicit non-zero epoch, enforce match
		if command.Epoch != 0 && command.Epoch != currentEpoch {
			return nil, interfaces.ErrEpochMismatch
		}
	}
	appliedCount, err := applyRaftCommandTx(ctx, tx, *command)
	if err != nil {
		return nil, err
	}
	if ledgerScoped {
		outcome := ledgerOutcome{Kind: command.Kind, Entity: command.Entity, ID: command.ID, Applied: appliedCount}
		if err := ledgerRecordTx(ctx, tx, command.Tenant, command.Site, command.WriteID, outcome, int64(index)); err != nil {
			return nil, err
		}
	}
	if err := store.commitAppliedIndex(ctx, tx, index); err != nil {
		return nil, err
	}
	return &models.ApplyResult{}, nil
}

func (store *Store) commitAppliedIndex(ctx context.Context, tx *sql.Tx, index uint64) error {
	if index > uint64(1<<63-1) {
		return ErrInvalidRaftIndex
	}
	writeQuery := queryRaftFsmAppliedSet
	if _, err := tx.ExecContext(ctx, writeQuery, int64(index)); err != nil {
		return err
	}
	return tx.Commit()
}

// AppliedRaftIndex returns the last committed FSM log index materialized here.
func (store *Store) AppliedRaftIndex(ctx context.Context) (uint64, error) {
	if store == nil || store.db == nil {
		return 0, ErrInvalidRaftIndex
	}
	query := queryRaftFsmAppliedGet
	var index int64
	if err := store.db.QueryRowContext(ctx, query).Scan(&index); err != nil {
		return 0, err
	}
	if index < 0 {
		return 0, ErrCorruptRaftStorage
	}
	return uint64(index), nil
}

func applyRaftCommandTx(ctx context.Context, tx *sql.Tx, command models.RaftCommand) (int, error) {
	switch command.Kind {
	case "migrate":
		plan := migration.Plan(command.Previous, command.Next)
		if !plan.Safe {
			return 0, ErrUnsafeMigration
		}
		if err := applyMigrationTx(ctx, tx, command.Previous, command.Next, plan, auditMetaFor(command)); err != nil {
			return 0, err
		}
		return 1, nil
	case "rollback":
		if err := rollbackMigrationTx(ctx, tx, command.Next, auditMetaFor(command)); err != nil {
			return 0, err
		}
		return 1, nil
	case "put":
		if err := putRowTx(ctx, tx, command.Tenant, command.Site, command.Entity, command.ID, command.Row); err != nil {
			return 0, err
		}
		return 1, nil
	case "create", "update", "delete", "batch":
		return applyProductCommandTx(ctx, tx, command)
	default:
		return 0, ErrInvalidRow
	}
}
