package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Liapoldus/domain/internal/application/migration"
	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
)

func (store *Store) CurrentRevision(ctx context.Context) (models.StoredRevision, error) {
	query := queryExportModel
	var document []byte
	var epoch int64
	if err := store.db.QueryRowContext(ctx, query).Scan(&document, &epoch); err != nil {
		return models.StoredRevision{}, err
	}
	if !json.Valid(document) || epoch <= 0 {
		return models.StoredRevision{}, ErrInvalidRow
	}
	var model models.Model
	if err := json.Unmarshal(document, &model); err != nil {
		return models.StoredRevision{}, err
	}
	return models.StoredRevision{Model: model, Epoch: epoch, Document: document}, nil
}

func (store *Store) SnapshotRevision(ctx context.Context) (models.StoredRevision, bool, error) {
	query := queryReadSnapshotModel
	var document []byte
	if err := store.db.QueryRowContext(ctx, query).Scan(&document); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.StoredRevision{}, false, nil
		}
		return models.StoredRevision{}, false, err
	}
	if !json.Valid(document) {
		return models.StoredRevision{}, false, ErrInvalidRow
	}
	var model models.Model
	if err := json.Unmarshal(document, &model); err != nil {
		return models.StoredRevision{}, false, err
	}
	return models.StoredRevision{Model: model, Document: document}, true, nil
}

func (store *Store) PreflightMigration(ctx context.Context, previous models.StoredRevision, next models.Model) (retErr error) {
	plan := migration.Plan(previous.Model, next)
	if !plan.Safe {
		return interfaces.ErrInvalid
	}
	currentQuery := queryExportModel
	var current []byte
	var epoch int64
	if err := store.db.QueryRowContext(ctx, currentQuery).Scan(&current, &epoch); err != nil {
		return err
	}
	expected, err := json.Marshal(previous.Model)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, expected) {
		return interfaces.ErrConflict
	}
	selectQuery := querySelectEntityRows
	var allUpdates []rowUpdate
	for _, entity := range next.Entities {
		rows, err := store.db.QueryContext(ctx, selectQuery, entity.Name)
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
				return &models.PreflightFailure{Reason: "row_preflight"}
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
		switch {
		case errors.Is(err, ErrUniqueViolation):
			return &models.PreflightFailure{Reason: "row_unique_violation"}
		case errors.Is(err, ErrForeignKeyViolation):
			return &models.PreflightFailure{Reason: "row_foreign_key_violation"}
		default:
			return &models.PreflightFailure{Reason: "row_preflight"}
		}
	}
	return nil
}

func (store *Store) RecentMigrations(ctx context.Context, tenant, site string, limit int) (ret0 []models.MigrationAudit, retErr error) {
	if limit <= 0 {
		return nil, ErrInvalidRow
	}
	query := queryMigrationAuditRecent
	rows, err := store.db.QueryContext(ctx, query, tenant, site, limit)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	audit := []models.MigrationAudit{}
	for rows.Next() {
		var entry models.MigrationAudit
		if err := rows.Scan(&entry.Kind, &entry.WriteID, &entry.EpochBefore, &entry.EpochAfter, &entry.FingerprintBefore, &entry.FingerprintAfter); err != nil {
			return nil, err
		}
		audit = append(audit, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return audit, nil
}
