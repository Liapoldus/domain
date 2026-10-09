package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Liapoldus/domain/internal/application/row"
	"github.com/Liapoldus/domain/internal/domain/models"
)

// applyProductCommandTx applies one create, update, delete or batch command
// inside the caller's transaction. All product writes enforce the entity's
// owner-group ACL before any existence or row validation runs.
func applyProductCommandTx(ctx context.Context, tx *sql.Tx, command models.RaftCommand) (int, error) {
	if command.Kind == "batch" {
		return applyBatchTx(ctx, tx, command)
	}
	if err := applyScopedCommandTx(ctx, tx, command); err != nil {
		return 0, err
	}
	return 1, nil
}

// applyBatchTx applies every nested operation atomically. Nested operations
// inherit the outer tenant, site and group; nested writeIds are ignored.
func applyBatchTx(ctx context.Context, tx *sql.Tx, command models.RaftCommand) (int, error) {
	if len(command.Ops) == 0 || len(command.Ops) > 100 {
		return 0, ErrInvalidRow
	}
	for _, operation := range command.Ops {
		scoped := operation
		scoped.Tenant = command.Tenant
		scoped.Site = command.Site
		scoped.Group = command.Group
		if err := applyScopedCommandTx(ctx, tx, scoped); err != nil {
			return 0, err
		}
	}
	return len(command.Ops), nil
}

func applyScopedCommandTx(ctx context.Context, tx *sql.Tx, command models.RaftCommand) error {
	if command.Tenant == "" || command.Site == "" || command.Entity == "" || command.ID == "" {
		return ErrInvalidRow
	}
	entity, err := requireEntityTx(ctx, tx, command.Entity, command.Group)
	if err != nil {
		return err
	}
	switch command.Kind {
	case "create":
		present, err := rowExistsTx(ctx, tx, command.Tenant, command.Site, command.Entity, command.ID)
		if err != nil {
			return err
		}
		if present {
			return ErrRowExists
		}
		normalized, err := validateAndNormalizeRowTx(ctx, tx, command.Tenant, command.Site, command.Entity, command.ID, command.Row)
		if err != nil {
			return err
		}
		query := queryInsertRow
		if _, err := tx.ExecContext(ctx, query, command.Tenant, command.Site, command.Entity, command.ID, normalized); err != nil {
			return err
		}
		return nil
	case "update":
		if _, err := requireRowTx(ctx, tx, command.Tenant, command.Site, command.Entity, command.ID); err != nil {
			return err
		}
		normalized, err := validateAndNormalizeRowTx(ctx, tx, command.Tenant, command.Site, command.Entity, command.ID, command.Row)
		if err != nil {
			return err
		}
		query := queryUpdateRow
		if _, err := tx.ExecContext(ctx, query, normalized, command.Tenant, command.Site, command.Entity, command.ID); err != nil {
			return err
		}
		return nil
	case "delete":
		stored, err := requireRowTx(ctx, tx, command.Tenant, command.Site, command.Entity, command.ID)
		if err != nil {
			return err
		}
		if err := ensureNoDependentRowsTx(ctx, tx, command.Tenant, command.Site, entity, stored); err != nil {
			return err
		}
		query := queryDeleteRow
		if _, err := tx.ExecContext(ctx, query, command.Tenant, command.Site, command.Entity, command.ID); err != nil {
			return err
		}
		return nil
	default:
		return ErrInvalidRow
	}
}

// requireEntityTx resolves the scoped entity and enforces that the command's
// group matches the entity owner group.
func requireEntityTx(ctx context.Context, tx *sql.Tx, entityName, group string) (models.Entity, error) {
	model, err := readModelTx(ctx, tx)
	if err != nil {
		return models.Entity{}, err
	}
	for _, entity := range model.Entities {
		if entity.Name != entityName {
			continue
		}
		if entity.OwnerGroup != group {
			return models.Entity{}, ErrForbiddenGroup
		}
		return entity, nil
	}
	return models.Entity{}, ErrInvalidRow
}

func rowExistsTx(ctx context.Context, tx *sql.Tx, tenant, site, entity, id string) (bool, error) {
	query := queryReadRow
	var document []byte
	err := tx.QueryRowContext(ctx, query, tenant, site, entity, id).Scan(&document)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func requireRowTx(ctx context.Context, tx *sql.Tx, tenant, site, entity, id string) (map[string]json.RawMessage, error) {
	query := queryReadRow
	var document []byte
	err := tx.QueryRowContext(ctx, query, tenant, site, entity, id).Scan(&document)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrRowNotFound
	}
	if err != nil {
		return nil, err
	}
	values := make(map[string]json.RawMessage)
	if json.Unmarshal(document, &values) != nil {
		return nil, ErrInvalidRow
	}
	return values, nil
}

// ensureNoDependentRowsTx rejects a delete while any row in the same scope
// still references the stored row through a declared reference field.
func ensureNoDependentRowsTx(ctx context.Context, tx *sql.Tx, tenant, site string, entity models.Entity, stored map[string]json.RawMessage) error {
	model, err := readModelTx(ctx, tx)
	if err != nil {
		return err
	}
	for _, candidate := range model.Entities {
		for _, field := range candidate.Fields {
			if field.References == nil || field.References.Entity != entity.Name {
				continue
			}
			target, present := stored[field.References.Field]
			if !present {
				continue
			}
			targetKey, err := row.ValueKey(field.Type, target)
			if err != nil {
				return ErrInvalidRow
			}
			dependentRows, err := loadScopedRowsTx(ctx, tx, tenant, site, candidate.Name)
			if err != nil {
				return err
			}
			for _, dependent := range dependentRows {
				value, exists := dependent.Values[field.Name]
				if !exists {
					continue
				}
				dependentKey, err := row.ValueKey(field.Type, value)
				if err != nil {
					return ErrInvalidRow
				}
				if dependentKey == targetKey {
					return ErrForeignKeyViolation
				}
			}
		}
	}
	return nil
}
