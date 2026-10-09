// Package migration plans and applies deterministic model transitions.
package migration

import (
	"encoding/json"

	"github.com/Liapoldus/domain/internal/domain/models"
)

// Plan only describes transformations that preserve existing rows.
// Execution is deliberately separate: a rejected plan must never mutate data.
func Plan(previous, next models.Model) models.MigrationPlan {
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
	if model.SchemaVersion != "1" || len(model.Entities) == 0 || len(model.Entities) > 256 || len(model.Migrations) > 512 {
		return "invalid_model"
	}
	entities := make(map[string]map[string]models.Field, len(model.Entities))
	for _, entity := range model.Entities {
		if !validIdentifier(entity.Name) || !validIdentifier(entity.OwnerGroup) || len(entity.Fields) == 0 || len(entity.Fields) > 128 {
			return "invalid_model"
		}
		if _, duplicate := entities[entity.Name]; duplicate {
			return "duplicate_entity"
		}
		fields := make(map[string]models.Field, len(entity.Fields))
		primary := 0
		for _, field := range entity.Fields {
			if !validIdentifier(field.Name) || !validFieldType(field.Type) || !isJSONScalar(field.Default) {
				return "invalid_model"
			}
			if _, duplicate := fields[field.Name]; duplicate {
				return "duplicate_field"
			}
			fields[field.Name] = field
			if field.PrimaryKey {
				primary++
			}
		}
		if primary != 1 {
			return "invalid_primary_key"
		}
		entities[entity.Name] = fields
	}
	for _, entity := range model.Entities {
		for _, field := range entity.Fields {
			if field.References == nil {
				continue
			}
			targetFields, exists := entities[field.References.Entity]
			if !validIdentifier(field.References.Entity) || !validIdentifier(field.References.Field) || !exists {
				return "invalid_reference"
			}
			target, exists := targetFields[field.References.Field]
			if !exists || (!target.PrimaryKey && !target.Unique) || target.Type != field.Type {
				return "invalid_reference"
			}
		}
	}
	mappedSources := make(map[string]struct{}, len(model.Migrations))
	mappedTargets := make(map[string]struct{}, len(model.Migrations))
	for _, mapping := range model.Migrations {
		if !validIdentifier(mapping.Entity) || !validIdentifier(mapping.From) || !validIdentifier(mapping.To) {
			return "invalid_model"
		}
		source := mapping.Entity + "\x00" + mapping.From
		target := mapping.Entity + "\x00" + mapping.To
		if _, exists := mappedSources[source]; exists {
			return "invalid_mapping"
		}
		if _, exists := mappedTargets[target]; exists {
			return "invalid_mapping"
		}
		mappedSources[source] = struct{}{}
		mappedTargets[target] = struct{}{}
		if mapping.Conversion != "" && mapping.Conversion != "parseInt64" && mapping.Conversion != "formatInt64" {
			return "invalid_conversion"
		}
	}
	return ""
}

func validIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 64 || !isASCIILetter(value[0]) {
		return false
	}
	for i := 1; i < len(value); i++ {
		character := value[i]
		if !isASCIILetter(character) && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func isASCIILetter(character byte) bool {
	return (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z')
}

func validFieldType(value string) bool {
	switch value {
	case "text", "int64", "decimal", "bool", "timestamp", "bytes":
		return true
	default:
		return false
	}
}

func isJSONScalar(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	if !json.Valid(raw) {
		return false
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	switch value.(type) {
	case nil, string, float64, bool:
		return true
	default:
		return false
	}
}
