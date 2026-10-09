// Package sqlite persists Domain state and migration history.
package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/Liapoldus/domain/internal/application/migration"
	"github.com/Liapoldus/domain/internal/domain/models"
	_ "modernc.org/sqlite"
)

var (
	ErrUnsafeMigration     = errors.New("unsafe migration")
	ErrInvalidRow          = errors.New("invalid row")
	ErrModelConflict       = errors.New("model changed")
	ErrSnapshotTooLarge    = errors.New("snapshot exceeds configured size")
	ErrRowNotFound         = errors.New("row not found")
	ErrRowExists           = errors.New("row already exists")
	ErrUniqueViolation     = errors.New("unique constraint violation")
	ErrForeignKeyViolation = errors.New("foreign key violation")
	ErrForbiddenGroup      = errors.New("group may not write this entity")
	ErrNoSnapshot          = errors.New("no migration snapshot")
)

// Store is one deterministic SQLite materialization target for a future Raft
// FSM. It is not itself a consensus or peer admission mechanism.
type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (ret0 *Store, retErr error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(ctx, querySchema)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return *new(*Store), closeErr
		}
		return nil, err
	}
	return &Store{db: db}, nil
}

// Epoch returns the current model epoch from the model_state table.
func (store *Store) Epoch(ctx context.Context) (int64, error) {
	if store == nil || store.db == nil {
		return 0, ErrInvalidRow
	}
	query := queryExportModel
	var model []byte
	var epoch int64
	if err := store.db.QueryRowContext(ctx, query).Scan(&model, &epoch); err != nil {
		return 0, err
	}
	if epoch <= 0 {
		return 0, ErrInvalidRow
	}
	return epoch, nil
}

// EpochTx returns the current model epoch using the provided transaction.
func (store *Store) EpochTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	query := queryExportModel
	var model []byte
	var epoch int64
	if err := tx.QueryRowContext(ctx, query).Scan(&model, &epoch); err != nil {
		return 0, err
	}
	if epoch <= 0 {
		return 0, ErrInvalidRow
	}
	return epoch, nil
}

func (store *Store) Close() error { return store.db.Close() }

func (store *Store) InitModel(ctx context.Context, model models.Model) error {
	if !migration.Plan(models.Model{}, model).Safe {
		return ErrUnsafeMigration
	}
	data, err := json.Marshal(model)
	if err != nil {
		return err
	}
	query := queryInsertModel
	_, err = store.db.ExecContext(ctx, query, data)
	return err
}

func (store *Store) PutRow(ctx context.Context, tenant, site, entity, id string, row []byte) (retErr error) {
	if store == nil || store.db == nil {
		return ErrInvalidRow
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()
	if err := putRowTx(ctx, tx, tenant, site, entity, id, row); err != nil {
		return err
	}
	return tx.Commit()
}

func putRowTx(ctx context.Context, tx *sql.Tx, tenant, site, entity, id string, row []byte) error {
	normalized, err := validateAndNormalizeRowTx(ctx, tx, tenant, site, entity, id, row)
	if err != nil {
		return err
	}
	query := queryInsertRow
	_, err = tx.ExecContext(ctx, query, tenant, site, entity, id, normalized)
	return err
}

func (store *Store) ReadRow(ctx context.Context, tenant, site, entity, id string) ([]byte, error) {
	query := queryReadRow
	var row []byte
	if err := store.db.QueryRowContext(ctx, query, tenant, site, entity, id).Scan(&row); err != nil {
		return nil, err
	}
	return row, nil
}

// ApplyMigration preflights and transforms all existing rows inside one SQLite
// transaction. A failure rolls the entire local materialization back. The
// caller must order this operation with a committed Raft log entry in v1.
func (store *Store) ApplyMigration(ctx context.Context, previous, next models.Model) (retErr error) {
	plan := migration.Plan(previous, next)
	if !plan.Safe {
		return ErrUnsafeMigration
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()
	if err := applyMigrationTx(ctx, tx, previous, next, plan, nil); err != nil {
		return err
	}
	return tx.Commit()
}

func applyMigrationTx(ctx context.Context, tx *sql.Tx, previous, next models.Model, plan models.MigrationPlan, meta *migrationAuditMeta) (retErr error) {
	currentQuery := queryExportModel
	var current []byte
	var currentEpoch int64
	if err := tx.QueryRowContext(ctx, currentQuery).Scan(&current, &currentEpoch); err != nil {
		return err
	}
	expected, err := json.Marshal(previous)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, expected) {
		return ErrModelConflict
	}
	captureQuery := queryCaptureSnapshot
	if _, err := tx.ExecContext(ctx, captureQuery); err != nil {
		return err
	}
	saveSnapshotQuery := querySaveSnapshotModel
	if _, err := tx.ExecContext(ctx, saveSnapshotQuery, current); err != nil {
		return err
	}
	selectQuery := querySelectEntityRows
	updateQuery := queryUpdateRow
	var allUpdates []rowUpdate
	for _, entity := range next.Entities {
		rows, err := tx.QueryContext(ctx, selectQuery, entity.Name)
		if err != nil {
			return err
		}
		for rows.Next() {
			var entry rowUpdate
			if err := rows.Scan(&entry.tenant, &entry.site, &entry.id, &entry.document); err != nil {
				if closeErr := rows.Close(); closeErr != nil {
					return closeErr
				}
				return err
			}
			entry.document, err = transform(entry.document, entity, plan.Steps)
			if err != nil {
				if closeErr := rows.Close(); closeErr != nil {
					return closeErr
				}
				return err
			}
			entry.entity = entity.Name
			allUpdates = append(allUpdates, entry)
		}
		if err := rows.Err(); err != nil {
			if closeErr := rows.Close(); closeErr != nil {
				return closeErr
			}
			return err
		}
		if closeErr := rows.Close(); closeErr != nil {
			return closeErr
		}
	}
	if err := validateMigratedRows(next, allUpdates); err != nil {
		return err
	}
	for _, entry := range allUpdates {
		if _, err := tx.ExecContext(ctx, updateQuery, entry.document, entry.tenant, entry.site, entry.entity, entry.id); err != nil {
			return err
		}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	updateModelQuery := queryUpdateModel
	if _, err := tx.ExecContext(ctx, updateModelQuery, data); err != nil {
		return err
	}
	return insertMigrationAuditTx(ctx, tx, meta, currentEpoch, currentEpoch+1, current, data)
}

// RollbackMigration restores the exact pre-migration snapshot. Rows committed
// after that snapshot are removed from the active dataset. The epoch advances
// so callers cannot mistake the restored state for an old read revision.
func (store *Store) RollbackMigration(ctx context.Context, target models.Model) (retErr error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, rollback(tx)) }()
	if err := rollbackMigrationTx(ctx, tx, target, nil); err != nil {
		return err
	}
	return tx.Commit()
}

func rollbackMigrationTx(ctx context.Context, tx *sql.Tx, target models.Model, meta *migrationAuditMeta) error {
	readQuery := queryReadSnapshotModel
	var snapshotModel []byte
	if err := tx.QueryRowContext(ctx, readQuery).Scan(&snapshotModel); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoSnapshot
		}
		return err
	}
	expected, err := json.Marshal(target)
	if err != nil {
		return err
	}
	if !bytes.Equal(snapshotModel, expected) {
		return ErrModelConflict
	}
	currentQuery := queryExportModel
	var current []byte
	var currentEpoch int64
	if err := tx.QueryRowContext(ctx, currentQuery).Scan(&current, &currentEpoch); err != nil {
		return err
	}
	for _, query := range []string{queryRestoreRows, queryRestoreModel, queryClearSnapshot} {
		if query == queryRestoreModel {
			_, err = tx.ExecContext(ctx, query, snapshotModel)
		} else {
			_, err = tx.ExecContext(ctx, query)
		}
		if err != nil {
			return err
		}
	}
	return insertMigrationAuditTx(ctx, tx, meta, currentEpoch, currentEpoch+1, current, snapshotModel)
}

type rowUpdate struct {
	tenant, site, entity, id string
	document                 []byte
}

func transform(raw []byte, entity models.Entity, steps []models.MigrationStep) ([]byte, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil || record == nil {
		return nil, ErrInvalidRow
	}
	for _, step := range steps {
		if step.Entity != entity.Name {
			continue
		}
		switch step.Kind {
		case "renameField":
			value, exists := record[step.Field]
			if exists {
				if _, collision := record[step.Target]; collision {
					return nil, ErrInvalidRow
				}
				record[step.Target] = value
				delete(record, step.Field)
			}
		case "convertField":
			value, exists := record[step.Field]
			if !exists {
				continue
			}
			var target models.Field
			for _, field := range entity.Fields {
				if field.Name == step.Target {
					target = field
					break
				}
			}
			converted, err := convertValue(value, target.Type)
			if err != nil {
				return nil, err
			}
			record[step.Target] = converted
		case "addField":
			for _, field := range entity.Fields {
				if field.Name == step.Field && len(field.Default) != 0 {
					record[field.Name] = field.Default
				}
			}
		}
	}
	return json.Marshal(record)
}

func convertValue(value json.RawMessage, target string) (json.RawMessage, error) {
	switch target {
	case "int64":
		var text string
		if err := json.Unmarshal(value, &text); err != nil {
			return nil, ErrInvalidRow
		}
		number, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, ErrInvalidRow
		}
		return json.Marshal(number)
	case "text":
		var number int64
		if err := json.Unmarshal(value, &number); err != nil {
			return nil, ErrInvalidRow
		}
		return json.Marshal(strconv.FormatInt(number, 10))
	default:
		return nil, ErrInvalidRow
	}
}
