package sqlite

import (
	"database/sql"
	"errors"
)

// rollback reports cleanup failures while accepting an already committed or
// rolled-back transaction. Callers join its result with the primary failure.
func rollback(tx *sql.Tx) error {
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}
