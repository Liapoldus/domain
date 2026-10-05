package application

import (
	"github.com/Liapoldus/domain/internal/domain/models"
)

// PlanMigration only describes transformations that preserve existing rows.
// Execution is deliberately separate: a rejected plan must never mutate data.
func PlanMigration(previous, next models.Model) models.MigrationPlan {
	if err := validateModel(next); err != "" {
		return models.MigrationPlan{Error: err}
	}
	plan := models.MigrationPlan{Safe: true}
	oldEntities := make(map[string]models.Entity, len(previous.Entities))
	newEntities := make(map[string]models.Entity, len(next.Entities))
	for _, entity := range previous.Entities {
		oldEntities[entity.Name] = entity
	}
	for _, entity := range next.Entities {
		newEntities[entity.Name] = entity
	}
	for _, entity := range previous.Entities {
		if _, ok := newEntities[entity.Name]; !ok {
			return models.MigrationPlan{Error: "destructive_change"}
		}
	}
	for _, entity := range next.Entities {
		old, exists := oldEntities[entity.Name]
		if !exists {
			plan.Steps = append(plan.Steps, models.MigrationStep{Kind: "addEntity", Entity: entity.Name})
			continue
		}
		if old.OwnerGroup != entity.OwnerGroup {
			return models.MigrationPlan{Error: "ownership_change"}
		}
		oldFields := make(map[string]models.Field, len(old.Fields))
		newFields := make(map[string]models.Field, len(entity.Fields))
		for _, field := range old.Fields {
			oldFields[field.Name] = field
		}
		for _, field := range entity.Fields {
			newFields[field.Name] = field
		}
		for _, field := range old.Fields {
			if _, unchanged := newFields[field.Name]; unchanged {
				continue
			}
			mapping, found := mappingFor(next.Migrations, entity.Name, field.Name)
			if !found {
				return models.MigrationPlan{Error: "mapping_required"}
			}
			target, exists := newFields[mapping.To]
			if !exists || oldFields[mapping.To].Name != "" {
				return models.MigrationPlan{Error: "invalid_mapping"}
			}
			if field.Type != target.Type && !validConversion(mapping.Conversion, field.Type, target.Type) {
				return models.MigrationPlan{Error: "invalid_conversion"}
			}
			if field.PrimaryKey != target.PrimaryKey {
				return models.MigrationPlan{Error: "primary_key_change"}
			}
			plan.Steps = append(plan.Steps, models.MigrationStep{Kind: "renameField", Entity: entity.Name, Field: field.Name, Target: mapping.To})
		}
		for _, field := range entity.Fields {
			oldField, existed := oldFields[field.Name]
			if existed {
				if oldField.PrimaryKey != field.PrimaryKey {
					return models.MigrationPlan{Error: "primary_key_change"}
				}
				if !oldField.Required && field.Required {
					return models.MigrationPlan{Error: "mapping_required"}
				}
				if oldField.Type != field.Type {
					mapping, found := mappingFor(next.Migrations, entity.Name, field.Name)
					if !found || mapping.To != field.Name {
						return models.MigrationPlan{Error: "mapping_required"}
					}
					if !validConversion(mapping.Conversion, oldField.Type, field.Type) {
						return models.MigrationPlan{Error: "invalid_conversion"}
					}
					plan.Steps = append(plan.Steps, models.MigrationStep{Kind: "convertField", Entity: entity.Name, Field: field.Name, Target: field.Name})
				}
				continue
			}
			if mappedTarget(next.Migrations, entity.Name, field.Name) {
				continue
			}
			if field.Required && len(field.Default) == 0 {
				return models.MigrationPlan{Error: "default_required"}
			}
			plan.Steps = append(plan.Steps, models.MigrationStep{Kind: "addField", Entity: entity.Name, Field: field.Name})
		}
	}
	return plan
}

func validConversion(name, from, to string) bool {
	return (name == "parseInt64" && from == "text" && to == "int64") ||
		(name == "formatInt64" && from == "int64" && to == "text")
}

func mappingFor(mappings []models.Mapping, entity, from string) (models.Mapping, bool) {
	for _, mapping := range mappings {
		if mapping.Entity == entity && mapping.From == from {
			return mapping, true
		}
	}
	return models.Mapping{}, false
}

func mappedTarget(mappings []models.Mapping, entity, target string) bool {
	for _, mapping := range mappings {
		if mapping.Entity == entity && mapping.To == target {
			return true
		}
	}
	return false
}

func validateModel(model models.Model) string {
	if model.SchemaVersion != "1" || len(model.Entities) == 0 {
		return "invalid_model"
	}
	seenEntities := make(map[string]struct{}, len(model.Entities))
	for _, entity := range model.Entities {
		if entity.Name == "" || entity.OwnerGroup == "" || len(entity.Fields) == 0 {
			return "invalid_model"
		}
		if _, duplicate := seenEntities[entity.Name]; duplicate {
			return "duplicate_entity"
		}
		seenEntities[entity.Name] = struct{}{}
		seenFields := make(map[string]struct{}, len(entity.Fields))
		primary := 0
		for _, field := range entity.Fields {
			if field.Name == "" || field.Type == "" {
				return "invalid_model"
			}
			if _, duplicate := seenFields[field.Name]; duplicate {
				return "duplicate_field"
			}
			seenFields[field.Name] = struct{}{}
			if field.PrimaryKey {
				primary++
			}
		}
		if primary != 1 {
			return "invalid_primary_key"
		}
	}
	return ""
}
