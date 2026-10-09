// Package contracts owns typed, immutable Domain contract definitions.
// JSON files in v1 are deterministic exports, never runtime inputs.
package contracts

import "encoding/json"

// Schema is the JSON Schema vocabulary used by Domain's public contracts.
type Schema struct {
	Schema               string            `json:"$schema,omitzero"`
	ID                   string            `json:"$id,omitzero"`
	Ref                  string            `json:"$ref,omitzero"`
	Title                string            `json:"title,omitzero"`
	Description          string            `json:"description,omitzero"`
	Type                 Types             `json:"type,omitzero"`
	AdditionalProperties *Additional       `json:"additionalProperties,omitzero"`
	Required             []string          `json:"required,omitzero"`
	Properties           map[string]Schema `json:"properties,omitzero"`
	Defs                 map[string]Schema `json:"$defs,omitzero"`
	Items                *Schema           `json:"items,omitzero"`
	Enum                 []string          `json:"enum,omitzero"`
	Const                *Literal          `json:"const,omitzero"`
	Pattern              string            `json:"pattern,omitzero"`
	Minimum              *int              `json:"minimum,omitzero"`
	Maximum              *int              `json:"maximum,omitzero"`
	MinItems             *int              `json:"minItems,omitzero"`
	MaxItems             *int              `json:"maxItems,omitzero"`
	MinLength            *int              `json:"minLength,omitzero"`
	MaxLength            *int              `json:"maxLength,omitzero"`
	AllOf                []Schema          `json:"allOf,omitzero"`
	If                   *Schema           `json:"if,omitzero"`
	Then                 *Schema           `json:"then,omitzero"`
	Else                 *Schema           `json:"else,omitzero"`
	Not                  *Schema           `json:"not,omitzero"`
}

// Types preserves the distinction between a type name and an array of types.
type Types struct {
	Name  string
	Names []string
}

func (t Types) IsZero() bool { return t.Name == "" && t.Names == nil }
func (t Types) MarshalJSON() ([]byte, error) {
	if t.Names != nil {
		return json.Marshal(t.Names)
	}
	return json.Marshal(t.Name)
}

// Additional encodes either an explicit boolean or a nested schema.
type Additional struct {
	Allowed bool
	Schema  *Schema
}

func (a Additional) MarshalJSON() ([]byte, error) {
	if a.Schema != nil {
		return json.Marshal(a.Schema)
	}
	return json.Marshal(a.Allowed)
}

// Literal encodes the string or boolean constants present in these schemas.
type Literal struct {
	Text    *string
	Boolean *bool
}

func (l Literal) MarshalJSON() ([]byte, error) {
	if l.Text != nil {
		return json.Marshal(*l.Text)
	}
	return json.Marshal(l.Boolean)
}

func integer(v int) *int      { return &v }
func text(v string) *Literal  { return &Literal{Text: &v} }
func boolean(v bool) *Literal { return &Literal{Boolean: &v} }
