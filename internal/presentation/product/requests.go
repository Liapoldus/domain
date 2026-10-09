package product

import (
	"encoding/json"

	"github.com/Liapoldus/domain/internal/domain/models"
)

// Request documents mirroring the domain v1 request schemas. The wire
// contract itself (types, required fields, additionalProperties:false,
// patterns, enums, limits) is enforced by the runtime schema validation in
// schema.go before decoding; row content is validated by the application
// service against the configured model.
type writeRequest struct {
	Entity  string          `json:"entity"`
	ID      string          `json:"id"`
	Row     json.RawMessage `json:"row"`
	WriteID string          `json:"writeId"`
	Epoch   int64           `json:"epoch,omitempty"`
}

type deleteRequest struct {
	Entity  string `json:"entity"`
	ID      string `json:"id"`
	WriteID string `json:"writeId"`
	Epoch   int64  `json:"epoch,omitempty"`
}

type idRequest struct {
	Entity string `json:"entity"`
	ID     string `json:"id"`
	Epoch  int64  `json:"epoch,omitempty"`
}

type queryRequest struct {
	SQL     string            `json:"sql"`
	Params  []json.RawMessage `json:"params"`
	MaxRows uint32            `json:"maxRows"`
	Epoch   int64             `json:"epoch,omitempty"`
}

type batchRequest struct {
	WriteID string           `json:"writeId"`
	Epoch   int64            `json:"epoch,omitempty"`
	Ops     []batchOpRequest `json:"ops"`
}

type batchOpRequest struct {
	Op     string          `json:"op"`
	Entity string          `json:"entity"`
	ID     string          `json:"id"`
	Row    json.RawMessage `json:"row"`
}

type statusRequest struct{}

// decodeStrict decodes an already schema-validated payload into the typed
// request document. Failures are reported as a business invalid_request with
// a fixed message that never echoes payload content.
func decodeStrict(payload json.RawMessage, target any) *models.ProductError {
	if err := json.Unmarshal(payload, target); err != nil {
		return invalidRequest("request payload does not match the method schema")
	}
	return nil
}
