package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strconv"

	"github.com/Liapoldus/domain/internal/application"
	"github.com/Liapoldus/domain/internal/domain/models"
	_ "modernc.org/sqlite"
)

//go:embed sql/*.sql
var statements embed.FS

var (
	ErrUnsafeMigration = errors.New("unsafe migration")
	ErrInvalidRow      = errors.New("invalid row")
	ErrModelConflict   = errors.New("model changed")
)

// Store is one deterministic SQLite materialization target for a future Raft
// FSM. It is not itself a consensus or peer admission mechanism.
type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	schema, err := statement("schema")
	if err == nil {
		_, err = db.ExecContext(ctx, schema)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (store *Store) Close() error { return store.db.Close() }

func (store *Store) InitModel(ctx context.Context, model models.Model) error {
	if !application.PlanMigration(models.Model{}, model).Safe {
		return ErrUnsafeMigration
	}
	data, err := json.Marshal(model)
	if err != nil {
		return err
	}
	query, err := statement("insert_model")
	if err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, query, data)
	return err
}

func (store *Store) PutRow(ctx context.Context, tenant, site, entity, id string, row []byte) error {
	if tenant == "" || site == "" || entity == "" || id == "" || !json.Valid(row) {
		return ErrInvalidRow
	}
	query, err := statement("insert_row")
	if err != nil {
		return err
	}
	_, err = store.db.ExecContext(ctx, query, tenant, site, entity, id, row)
	return err
}

func (store *Store) ReadRow(ctx context.Context, tenant, site, entity, id string) ([]byte, error) {
	query, err := statement("read_row")
	if err != nil {
		return nil, err
	}
	var row []byte
	if err := store.db.QueryRowContext(ctx, query, tenant, site, entity, id).Scan(&row); err != nil {
		return nil, err
	}
	return row, nil
}

// ApplyMigration preflights and transforms all existing rows inside one SQLite
// transaction. A failure rolls the entire local materialization back. The
// caller must order this operation with a committed Raft log entry in v1.
func (store *Store) ApplyMigration(ctx context.Context, previous, next models.Model) error {
	plan := application.PlanMigration(previous, next)
	if !plan.Safe {
		return ErrUnsafeMigration
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	currentQuery, err := statement("read_model")
	if err != nil {
		return err
	}
	var current []byte
	if err := tx.QueryRowContext(ctx, currentQuery).Scan(&current); err != nil {
		return err
	}
	expected, err := json.Marshal(previous)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, expected) {
		return ErrModelConflict
	}
	captureQuery, err := statement("capture_snapshot")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, captureQuery); err != nil {
		return err
	}
	saveSnapshotQuery, err := statement("save_snapshot_model")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, saveSnapshotQuery, current); err != nil {
		return err
	}
	selectQuery, err := statement("select_entity_rows")
	if err != nil {
		return err
	}
	updateQuery, err := statement("update_row")
	if err != nil {
		return err
	}
	for _, entity := range next.Entities {
		rows, err := tx.QueryContext(ctx, selectQuery, entity.Name)
		if err != nil {
			return err
		}
		var updates []rowUpdate
		for rows.Next() {
			var entry rowUpdate
			if err := rows.Scan(&entry.tenant, &entry.site, &entry.id, &entry.document); err != nil {
				rows.Close()
				return err
			}
			entry.document, err = transform(entry.document, entity, plan.Steps)
			if err != nil {
				rows.Close()
				return err
			}
			updates = append(updates, entry)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, entry := range updates {
			if _, err := tx.ExecContext(ctx, updateQuery, entry.document, entry.tenant, entry.site, entity.Name, entry.id); err != nil {
				return err
			}
		}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	updateModelQuery, err := statement("update_model")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, updateModelQuery, data); err != nil {
		return err
	}
	return tx.Commit()
}

// RollbackMigration restores the exact pre-migration snapshot. Rows committed
// after that snapshot are removed from the active dataset. The epoch advances
// so callers cannot mistake the restored state for an old read revision.
func (store *Store) RollbackMigration(ctx context.Context, target models.Model) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	readQuery, err := statement("read_snapshot_model")
	if err != nil {
		return err
	}
	var snapshotModel []byte
	if err := tx.QueryRowContext(ctx, readQuery).Scan(&snapshotModel); err != nil {
		return err
	}
	expected, err := json.Marshal(target)
	if err != nil {
		return err
	}
	if !bytes.Equal(snapshotModel, expected) {
		return ErrModelConflict
	}
	for _, name := range []string{"restore_rows", "restore_model", "clear_snapshot"} {
		query, err := statement(name)
		if err != nil {
			return err
		}
		if name == "restore_model" {
			_, err = tx.ExecContext(ctx, query, snapshotModel)
		} else {
			_, err = tx.ExecContext(ctx, query)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

type rowUpdate struct {
	tenant, site, id string
	document         []byte
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

func statement(name string) (string, error) {
	data, err := fs.ReadFile(statements, "sql/"+name+".sql")
	if err != nil {
		return "", fmt.Errorf("load SQL statement: %w", err)
	}
	return string(data), nil
}
