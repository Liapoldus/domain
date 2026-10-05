package models

type MigrationPlan struct {
	Safe  bool            `json:"safe"`
	Steps []MigrationStep `json:"steps,omitempty"`
	Error string          `json:"error,omitempty"`
}
