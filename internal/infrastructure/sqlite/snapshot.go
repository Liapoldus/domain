package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type storedRow struct {
	Tenant string          `json:"tenant"`
	Site   string          `json:"site"`
	Entity string          `json:"entity"`
	ID     string          `json:"id"`
	Data   json.RawMessage `json:"data"`
}

type persistedState struct {
	Model         json.RawMessage `json:"model"`
	Epoch         int64           `json:"epoch"`
	Rows          []storedRow     `json:"rows"`
	SnapshotModel json.RawMessage `json:"snapshotModel,omitempty"`
	SnapshotRows  []storedRow     `json:"snapshotRows,omitempty"`
}

// Export captures a consistent materialized state, including the rollback
// snapshot, for a Raft FSM snapshot. It must be bounded or streamed before
// large production datasets are enabled.
func (store *Store) Export(ctx context.Context) ([]byte, error) {
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	state := persistedState{}
	modelQuery, err := statement("export_model")
	if err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, modelQuery).Scan(&state.Model, &state.Epoch); err != nil {
		return nil, err
	}
	rowsQuery, err := statement("export_rows")
	if err != nil {
		return nil, err
	}
	state.Rows, err = readSnapshotRows(ctx, tx, rowsQuery)
	if err != nil {
		return nil, err
	}
	modelQuery, err = statement("read_snapshot_model")
	if err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, modelQuery).Scan(&state.SnapshotModel); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	snapshotRowsQuery, err := statement("export_snapshot_rows")
	if err != nil {
		return nil, err
	}
	state.SnapshotRows, err = readSnapshotRows(ctx, tx, snapshotRowsQuery)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return json.Marshal(state)
}

func readSnapshotRows(ctx context.Context, tx *sql.Tx, query string) ([]storedRow, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []storedRow
	for rows.Next() {
		var entry storedRow
		if err := rows.Scan(&entry.Tenant, &entry.Site, &entry.Entity, &entry.ID, &entry.Data); err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, rows.Err()
}

// Import replaces a local materialization atomically. The input must be an
// authenticated Raft snapshot; callers cannot use this as a public data API.
func (store *Store) Import(ctx context.Context, data []byte) error {
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil || !json.Valid(state.Model) || state.Epoch <= 0 {
		return ErrInvalidRow
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	resetQuery, err := statement("import_reset")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, resetQuery); err != nil {
		return err
	}
	modelQuery, err := statement("import_model")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, modelQuery, []byte(state.Model), state.Epoch); err != nil {
		return err
	}
	rowQuery, err := statement("insert_row")
	if err != nil {
		return err
	}
	for _, row := range state.Rows {
		if err := importRow(ctx, tx, rowQuery, row); err != nil {
			return err
		}
	}
	if len(state.SnapshotModel) != 0 {
		if !json.Valid(state.SnapshotModel) {
			return ErrInvalidRow
		}
		snapshotModelQuery, err := statement("save_snapshot_model")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, snapshotModelQuery, []byte(state.SnapshotModel)); err != nil {
			return err
		}
	}
	snapshotRowQuery, err := statement("import_snapshot_row")
	if err != nil {
		return err
	}
	for _, row := range state.SnapshotRows {
		if err := importRow(ctx, tx, snapshotRowQuery, row); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func importRow(ctx context.Context, tx *sql.Tx, query string, row storedRow) error {
	if row.Tenant == "" || row.Site == "" || row.Entity == "" || row.ID == "" || !json.Valid(row.Data) {
		return ErrInvalidRow
	}
	_, err := tx.ExecContext(ctx, query, row.Tenant, row.Site, row.Entity, row.ID, []byte(row.Data))
	return err
}
