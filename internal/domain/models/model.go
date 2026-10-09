// Package models defines Domain data and command models.
package models

type Model struct {
	SchemaVersion string    `json:"schemaVersion"`
	Entities      []Entity  `json:"entities"`
	Migrations    []Mapping `json:"migrations,omitempty"`
}
