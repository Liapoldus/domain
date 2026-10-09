package contracts

import (
	"encoding/json"
	"fmt"
)

//go:generate go run ../cmd/contracts -out .

// Documents returns fresh schema definitions without reading exported files.
func Documents() map[string]Schema {
	result := modelSchemas()
	for name, schema := range requestsSchemas() {
		result[name] = schema
	}
	for name, schema := range responsesSchemas() {
		result[name] = schema
	}
	return result
}

// Document serializes one code-owned runtime schema.
func Document(name string) ([]byte, error) {
	schema, ok := Documents()[name]
	if !ok {
		return nil, fmt.Errorf("unknown Domain schema: %s", name)
	}
	return json.Marshal(schema)
}

// Artifacts deterministically exports all public contracts from typed code.
func Artifacts() (map[string][]byte, error) {
	definitions := make(map[string]any)
	for name, schema := range Documents() {
		definitions[name] = schema
	}
	definitions["v1/plugin.json"] = Product()
	definitions["v1/raft-peer.json"] = Raft()
	result := make(map[string][]byte, len(definitions))
	for name, definition := range definitions {
		data, err := json.MarshalIndent(definition, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("export %s: %w", name, err)
		}
		result[name] = append(data, '\n')
	}
	return result, nil
}
