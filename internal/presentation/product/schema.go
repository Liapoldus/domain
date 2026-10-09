package product

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Liapoldus/domain/contracts"
	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// requestSchemaFiles maps every product method to its embedded domain v1
// request schema, mirroring the payloadSchema refs in contracts/v1/plugin.json.
// This is the set of schemas executed at runtime for incoming calls.
func requestSchemas() map[string]string {
	result := make(map[string]string)
	for _, method := range contracts.Product().Capabilities {
		result[method.Name] = strings.TrimPrefix(method.PayloadSchema, "contracts/")
	}
	return result
}

// maxReportedViolations caps how many leaf violations appear in one message.
const maxReportedViolations = 8

// schemaRegistry holds the compiled request schemas of one dispatcher.
// Compiled schemas are safe for concurrent validation.
type schemaRegistry struct {
	requests map[string]*jsonschema.Schema
}

// newSchemaRegistry compiles every embedded domain v1 contract schema once.
// It panics on a malformed asset: contracts are build-time inputs, so a bad
// schema must fail fast at startup instead of surfacing per request.
func newSchemaRegistry() *schemaRegistry {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020
	compiled := make(map[string]*jsonschema.Schema, len(contracts.Documents()))
	for path := range contracts.Documents() {
		document, err := contracts.Document(path)
		if err != nil {
			panic(fmt.Sprintf("domain product: embedded contract schema %s is unreadable: %v", path, err))
		}
		resource := "mem://contract/" + path
		if err := compiler.AddResource(resource, bytes.NewReader(document)); err != nil {
			panic(fmt.Sprintf("domain product: embedded contract schema %s cannot be registered: %v", path, err))
		}
		if idURL := schemaDocumentID(resource, document); idURL != "" {
			if err := compiler.AddResource(idURL, bytes.NewReader(document)); err != nil {
				panic(fmt.Sprintf("domain product: embedded contract schema %s cannot be registered under its $id: %v", path, err))
			}
		}
		schema, err := compiler.Compile(resource)
		if err != nil {
			panic(fmt.Sprintf("domain product: embedded contract schema %s does not compile: %v", path, err))
		}
		compiled[path] = schema
	}
	requests := make(map[string]*jsonschema.Schema, len(requestSchemas()))
	for method, path := range requestSchemas() {
		schema, ok := compiled[path]
		if !ok {
			panic(fmt.Sprintf("domain product: no compiled request schema for %s (%s)", method, path))
		}
		requests[method] = schema
	}
	return &schemaRegistry{requests: requests}
}

// validateRequest checks one method payload against its compiled contract
// schema before any typed decode. The reported message only carries the
// failing instance pointer and constraint keyword; submitted values never
// reach the envelope.
func (registry *schemaRegistry) validateRequest(method string, payload []byte) *models.ProductError {
	var document any
	if err := json.Unmarshal(payload, &document); err != nil {
		return invalidRequest("request payload must be a valid JSON object")
	}
	if hasDuplicateObjectKeys(payload) {
		return invalidRequest("request payload must not contain duplicate object keys")
	}
	schema := registry.requests[method]
	if schema == nil {
		return invalidRequest("request payload does not match the method schema")
	}
	if err := schema.Validate(document); err != nil {
		return invalidRequest(violationMessage(err))
	}
	return nil
}

// hasDuplicateObjectKeys walks the original token stream before JSON decoding
// collapses object members into a map. Decoder.Token returns decoded key text,
// so escaped spellings of the same key are treated as duplicates too.
func hasDuplicateObjectKeys(payload []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return false // validateRequest has already rejected malformed JSON.
	}
	duplicate, err := scanJSONValue(decoder, first)
	if err != nil {
		return false // defensive: malformed input is rejected by json.Unmarshal.
	}
	if duplicate {
		return true
	}
	_, err = decoder.Token()
	return err == nil // a second top-level token is invalid, fail closed.
}

func scanJSONValue(decoder *json.Decoder, token json.Token) (bool, error) {
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return false, nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return false, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return false, errors.New("invalid JSON object key")
			}
			if _, exists := seen[key]; exists {
				return true, nil
			}
			seen[key] = struct{}{}
			value, err := decoder.Token()
			if err != nil {
				return false, err
			}
			duplicate, err := scanJSONValue(decoder, value)
			if err != nil || duplicate {
				return duplicate, err
			}
		}
		_, err := decoder.Token() // consume '}'
		return false, err
	case '[':
		for decoder.More() {
			value, err := decoder.Token()
			if err != nil {
				return false, err
			}
			duplicate, err := scanJSONValue(decoder, value)
			if err != nil || duplicate {
				return duplicate, err
			}
		}
		_, err := decoder.Token() // consume ']'
		return false, err
	default:
		return false, errors.New("invalid JSON delimiter")
	}
}

// violationMessage formats the leaf causes of a validation failure as
// "<instance pointer>: <keyword>" joined with "; ". Intermediate $ref
// wrapper causes carry no keyword of their own and are skipped.
// ValidationError.Message is deliberately never used: it embeds submitted
// values (row content, identifiers, unknown field names).
func violationMessage(err error) string {
	var violation *jsonschema.ValidationError
	if !errors.As(err, &violation) {
		return "request payload does not match the method schema"
	}
	leaves := make([]*jsonschema.ValidationError, 0, 4)
	var collect func(node *jsonschema.ValidationError)
	collect = func(node *jsonschema.ValidationError) {
		if len(node.Causes) == 0 {
			leaves = append(leaves, node)
			return
		}
		for _, cause := range node.Causes {
			collect(cause)
		}
	}
	collect(violation)
	parts := make([]string, 0, len(leaves))
	for index, leaf := range leaves {
		if index == maxReportedViolations {
			parts = append(parts, "...")
			break
		}
		pointer := leaf.InstanceLocation
		if pointer == "" {
			pointer = "/"
		}
		parts = append(parts, pointer+": "+keywordOf(leaf.KeywordLocation))
	}
	return strings.Join(parts, "; ")
}

// keywordOf reduces a schema keyword location such as
// "/properties/writeId/$ref/pattern" to its declaring keyword ("pattern").
func keywordOf(location string) string {
	if location == "" {
		return "schema"
	}
	segments := strings.Split(location, "/")
	for index := len(segments) - 1; index >= 0; index-- {
		segment := segments[index]
		if segment == "" || segment == "$ref" {
			continue
		}
		return segment
	}
	return "schema"
}

// schemaDocumentID returns the absolute $id of a schema document, resolved
// against its registration URL. Cross-file $ref targets resolve to this URL,
// so the same document is registered under both keys.
func schemaDocumentID(base string, document []byte) string {
	var header struct {
		ID string `json:"$id"`
	}
	if err := json.Unmarshal(document, &header); err != nil || header.ID == "" {
		return ""
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}
	idURL, err := url.Parse(header.ID)
	if err != nil {
		return ""
	}
	return baseURL.ResolveReference(idURL).String()
}
