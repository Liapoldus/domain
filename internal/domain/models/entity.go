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
	Unique     bool            `json:"unique,omitempty"`
	Default    json.RawMessage `json:"default,omitempty"`
	References *Reference      `json:"references,omitempty"`
}

type Reference struct {
	Entity string `json:"entity"`
	Field  string `json:"field"`
}
