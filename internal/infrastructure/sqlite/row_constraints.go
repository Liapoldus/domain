package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Liapoldus/domain/internal/application/row"
	"github.com/Liapoldus/domain/internal/domain/models"
)

type scopedStoredRow struct {
	ID     string
	Values map[string]json.RawMessage
}

type rowConstraintKey struct {
	tenant string
	site   string
	entity string
	field  string
	value  string
}

// validateMigratedRows validates every transformed row against one candidate
// model before the transaction writes any row or activates that model.
func validateMigratedRows(model models.Model, updates []rowUpdate) error {
	entities := make(map[string]models.Entity, len(model.Entities))
	for _, entity := range model.Entities {
		entities[entity.Name] = entity
	}
	values := make([]map[string]json.RawMessage, len(updates))
	uniqueValues := make(map[rowConstraintKey]string)
	allValues := make(map[rowConstraintKey]struct{})
	for index := range updates {
		update := &updates[index]
		if update.tenant == "" || update.site == "" || update.id == "" {
			return ErrInvalidRow
		}
		entity, exists := entities[update.entity]
		if !exists {
			return ErrInvalidRow
		}
		normalized, err := row.Normalize(model, update.entity, update.id, update.document)
		if err != nil {
			return ErrInvalidRow
		}
		update.document = normalized
		values[index] = make(map[string]json.RawMessage)
		if json.Unmarshal(normalized, &values[index]) != nil {
			return ErrInvalidRow
		}
		for _, field := range entity.Fields {
			raw, present := values[index][field.Name]
			if !present {
				continue
			}
			value, err := row.ValueKey(field.Type, raw)
			if err != nil {
				return ErrInvalidRow
			}
			key := rowConstraintKey{tenant: update.tenant, site: update.site, entity: update.entity, field: field.Name, value: value}
			if field.Unique || field.PrimaryKey {
				if previousID, duplicate := uniqueValues[key]; duplicate && previousID != update.id {
					return ErrUniqueViolation
				}
				uniqueValues[key] = update.id
			}
			allValues[key] = struct{}{}
		}
	}
	for index, update := range updates {
		entity := entities[update.entity]
		for _, field := range entity.Fields {
			if field.References == nil {
				continue
			}
			raw, present := values[index][field.Name]
			if !present {
				continue
			}
			value, err := row.ValueKey(field.Type, raw)
			if err != nil {
				return ErrInvalidRow
			}
			key := rowConstraintKey{tenant: update.tenant, site: update.site, entity: field.References.Entity, field: field.References.Field, value: value}
			if _, found := allValues[key]; !found {
				return ErrForeignKeyViolation
			}
		}
	}
	return nil
}

// readModelTx loads the active model inside the caller's transaction.
func readModelTx(ctx context.Context, tx *sql.Tx) (models.Model, error) {
	modelQuery := queryReadModel
	var document []byte
	if err := tx.QueryRowContext(ctx, modelQuery).Scan(&document); err != nil {
		return models.Model{}, ErrInvalidRow
	}
	var model models.Model
	if json.Unmarshal(document, &model) != nil {
		return models.Model{}, ErrInvalidRow
	}
	return model, nil
}

// loadScopedRowsTx reads every stored row of one entity inside the caller's
// tenant and site scope.
func loadScopedRowsTx(ctx context.Context, tx *sql.Tx, tenant, site, entity string) (ret0 []scopedStoredRow, retErr error) {
	query := querySelectScopedEntityRows
	rows, err := tx.QueryContext(ctx, query, tenant, site, entity)
	if err != nil {
		return nil, ErrInvalidRow
	}
	defer func() { retErr = errors.Join(retErr, rows.Close()) }()
	var result []scopedStoredRow
	for rows.Next() {
		var stored scopedStoredRow
		var storedJSON []byte
		if rows.Scan(&stored.ID, &storedJSON) != nil || json.Unmarshal(storedJSON, &stored.Values) != nil {
			return nil, ErrInvalidRow
		}
		result = append(result, stored)
	}
	if rows.Err() != nil {
		return nil, ErrInvalidRow
	}
	return result, nil
}

func validateAndNormalizeRowTx(ctx context.Context, tx *sql.Tx, tenant, site, entityName, id string, raw []byte) ([]byte, error) {
	if tenant == "" || site == "" || entityName == "" || id == "" || !json.Valid(raw) {
		return nil, ErrInvalidRow
	}
	model, err := readModelTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	normalized, err := row.Normalize(model, entityName, id, raw)
	if err != nil {
		return nil, ErrInvalidRow
	}
	values := make(map[string]json.RawMessage)
	if json.Unmarshal(normalized, &values) != nil {
		return nil, ErrInvalidRow
	}
	var entity *models.Entity
	for index := range model.Entities {
		if model.Entities[index].Name == entityName {
			entity = &model.Entities[index]
			break
		}
	}
	if entity == nil {
		return nil, ErrInvalidRow
	}
	rowsByEntity := make(map[string][]scopedStoredRow)
	loadRows := func(name string) ([]scopedStoredRow, error) {
		if rows, ok := rowsByEntity[name]; ok {
			return rows, nil
		}
		result, err := loadScopedRowsTx(ctx, tx, tenant, site, name)
		if err != nil {
			return nil, err
		}
		rowsByEntity[name] = result
		return result, nil
	}

	for _, field := range entity.Fields {
		candidate, exists := values[field.Name]
		if !exists {
			continue
		}
		candidateKey, err := row.ValueKey(field.Type, candidate)
		if err != nil {
			return nil, ErrInvalidRow
		}
		if field.Unique || field.PrimaryKey {
			rows, err := loadRows(entityName)
			if err != nil {
				return nil, err
			}
			for _, stored := range rows {
				if stored.ID == id {
					continue
				}
				existing, exists := stored.Values[field.Name]
				if !exists {
					continue
				}
				existingKey, err := row.ValueKey(field.Type, existing)
				if err != nil {
					return nil, ErrInvalidRow
				}
				if existingKey == candidateKey {
					return nil, ErrUniqueViolation
				}
			}
		}
		if field.References != nil {
			targetRows, err := loadRows(field.References.Entity)
			if err != nil {
				return nil, err
			}
			found := false
			for _, stored := range targetRows {
				target, exists := stored.Values[field.References.Field]
				if !exists {
					continue
				}
				targetKey, err := row.ValueKey(field.Type, target)
				if err != nil {
					return nil, ErrInvalidRow
				}
				if targetKey == candidateKey {
					found = true
					break
				}
			}
			if !found {
				return nil, ErrForeignKeyViolation
			}
		}
	}
	return normalized, nil
}
