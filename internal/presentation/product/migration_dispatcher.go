package product

import (
	"context"
	"encoding/json"

	"github.com/Liapoldus/domain/internal/application/call"
	"github.com/Liapoldus/domain/internal/application/migration"
	"github.com/Liapoldus/domain/internal/application/row"
	"github.com/Liapoldus/domain/internal/domain/models"
)

type migrationPlanRequest struct {
	Next  json.RawMessage `json:"next"`
	Epoch int64           `json:"epoch,omitempty"`
}

// WithMigration attaches the migration service to this handler. Handlers
// built without it still expose only the read-only plan and status methods,
// returning a fixed internal error instead of nil-panicking.
func (handler *Handler) WithMigration(migration *migration.Service) *Handler {
	handler.migration = migration
	return handler
}

func (handler *Handler) registerMigrationMethods() {
	handler.methods["domain.migration.plan"] = methodSpec{defaultMaxPayloadBytes, defaultWriteTimeout, handler.callMigrationPlan}
	handler.methods["domain.migration.status"] = methodSpec{defaultMaxPayloadBytes, defaultReadTimeout, handler.callMigrationStatus}
}

func (handler *Handler) callMigrationPlan(ctx context.Context, _ *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	if handler.migration == nil {
		return callOutcome{productErr: &models.ProductError{
			Code:    call.CodeInternal,
			Message: "domain migration service is not available",
		}}
	}
	request := new(migrationPlanRequest)
	if productErr := decodeStrict(payload, request); productErr != nil {
		return callOutcome{productErr: productErr}
	}
	next, productErr := decodeMigrationModel(request.Next)
	if productErr != nil {
		return callOutcome{productErr: productErr}
	}
	data, productErr := handler.migration.Plan(ctx, scope, next, request.Epoch)
	return callOutcome{data: data, productErr: productErr}
}

func (handler *Handler) callMigrationStatus(ctx context.Context, _ *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	if handler.migration == nil {
		return callOutcome{productErr: &models.ProductError{
			Code:    call.CodeInternal,
			Message: "domain migration service is not available",
		}}
	}
	request := new(statusRequest)
	if productErr := decodeStrict(payload, request); productErr != nil {
		return callOutcome{productErr: productErr}
	}
	data, productErr := handler.migration.Status(ctx, scope)
	return callOutcome{data: data, productErr: productErr}
}

func decodeMigrationModel(raw json.RawMessage) (models.Model, *models.ProductError) {
	var next models.Model
	if len(raw) == 0 || json.Unmarshal(raw, &next) != nil {
		return models.Model{}, invalidRequest("request payload does not match the method schema")
	}
	return next, nil
}
