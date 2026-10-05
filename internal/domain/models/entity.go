package models

import "encoding/json"

type Entity struct {
	Name       string  `json:"name"`
	OwnerGroup string  `json:"ownerGroup"`
	Fields     []Field `json:"fields"`
}

type Field struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	PrimaryKey bool            `json:"primaryKey,omitempty"`
	Required   bool            `json:"required,omitempty"`
	Default    json.RawMessage `json:"default,omitempty"`
}
