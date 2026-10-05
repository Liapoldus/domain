package models

type MigrationStep struct {
	Kind   string `json:"kind"`
	Entity string `json:"entity"`
	Field  string `json:"field,omitempty"`
	Target string `json:"target,omitempty"`
}
