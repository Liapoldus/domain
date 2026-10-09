package row

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/Liapoldus/domain/internal/domain/models"
)

var ErrInvalidDataRow = errors.New("invalid Domain row")

// Normalize validates the product row against its configured entity and
// applies declared defaults deterministically before the row is replicated.
func Normalize(model models.Model, entityName, recordID string, document []byte) ([]byte, error) {
	if entityName == "" || recordID == "" || !json.Valid(document) {
		return nil, ErrInvalidDataRow
	}
	values, err := decodeRowObject(document)
	if err != nil {
		return nil, ErrInvalidDataRow
	}
	var entity *models.Entity
	for index := range model.Entities {
		if model.Entities[index].Name == entityName {
			entity = &model.Entities[index]
			break
		}
	}
	if entity == nil {
		return nil, ErrInvalidDataRow
	}
	fields := make(map[string]models.Field, len(entity.Fields))
	for _, field := range entity.Fields {
		fields[field.Name] = field
	}
	for name := range values {
		if _, exists := fields[name]; !exists {
			return nil, ErrInvalidDataRow
		}
	}
	for _, field := range entity.Fields {
		value, exists := values[field.Name]
		if !exists && len(field.Default) > 0 {
			value = append(json.RawMessage(nil), field.Default...)
			values[field.Name] = value
			exists = true
		}
		if !exists {
			if field.Required || field.PrimaryKey {
				return nil, ErrInvalidDataRow
			}
			continue
		}
		if _, err := ValueKey(field.Type, value); err != nil {
			return nil, ErrInvalidDataRow
		}
		if field.PrimaryKey {
			primaryID, err := RecordID(field.Type, value)
			if err != nil || primaryID != recordID {
				return nil, ErrInvalidDataRow
			}
		}
	}
	normalized, err := json.Marshal(values)
	if err != nil || !json.Valid(normalized) {
		return nil, ErrInvalidDataRow
	}
	return normalized, nil
}

// RecordID maps a typed primary-key value to the storage identifier used by
// the row APIs. Text IDs remain unchanged; other scalar IDs use the
// type-prefixed canonical comparison key to avoid cross-type ambiguity.
func RecordID(valueType string, raw json.RawMessage) (string, error) {
	key, err := ValueKey(valueType, raw)
	if err != nil {
		return "", err
	}
	if valueType != "text" {
		return key, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", ErrInvalidDataRow
	}
	return text, nil
}

// ValueKey produces a type-aware, deterministic comparison key for a
// validated scalar field value. It is used only for unique/FK checks.
func ValueKey(valueType string, raw json.RawMessage) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", ErrInvalidDataRow
	}
	switch valueType {
	case "text":
		text, ok := value.(string)
		if !ok {
			return "", ErrInvalidDataRow
		}
		return "text:" + text, nil
	case "timestamp":
		text, ok := value.(string)
		if !ok {
			return "", ErrInvalidDataRow
		}
		parsed, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return "", ErrInvalidDataRow
		}
		return "timestamp:" + parsed.UTC().Format(time.RFC3339Nano), nil
	case "int64":
		number, ok := value.(json.Number)
		if !ok {
			return "", ErrInvalidDataRow
		}
		integer, err := strconv.ParseInt(string(number), 10, 64)
		if err != nil {
			return "", ErrInvalidDataRow
		}
		return "int64:" + strconv.FormatInt(integer, 10), nil
	case "decimal":
		number, ok := value.(json.Number)
		if !ok {
			return "", ErrInvalidDataRow
		}
		rational, ok := new(big.Rat).SetString(string(number))
		if !ok {
			return "", ErrInvalidDataRow
		}
		return "decimal:" + rational.RatString(), nil
	case "bool":
		boolean, ok := value.(bool)
		if !ok {
			return "", ErrInvalidDataRow
		}
		return fmt.Sprintf("bool:%t", boolean), nil
	case "bytes":
		text, ok := value.(string)
		if !ok {
			return "", ErrInvalidDataRow
		}
		decoded, err := base64.RawURLEncoding.DecodeString(text)
		if err != nil {
			return "", ErrInvalidDataRow
		}
		return "bytes:" + base64.RawURLEncoding.EncodeToString(decoded), nil
	default:
		return "", ErrInvalidDataRow
	}
}

func decodeRowObject(document []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(document))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrInvalidDataRow
	}
	values := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok {
			return nil, ErrInvalidDataRow
		}
		if _, duplicate := values[key]; duplicate {
			return nil, ErrInvalidDataRow
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, ErrInvalidDataRow
		}
		values[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, ErrInvalidDataRow
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, ErrInvalidDataRow
	}
	return values, nil
}
