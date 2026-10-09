package main

import (
	"encoding/json"
	"errors"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"os"

	"github.com/Liapoldus/domain/internal/application/query"
	"github.com/Liapoldus/domain/internal/domain/models"
)

// Query planner probe. Reads one JSON document from stdin:
//
//	{"model":<Model>,"rows":{entity:[[row...]]},"sql":"...","params":[...],"maxRows":N}
//
// and prints either {"columns":[...],"rows":[[...]]} or
// {"error":{"code":"query_rejected","message":"..."}}.
// Rows map values are arrays of row documents; each row is either a JSON
// object keyed by field name or a positional array following the entity's
// field order. Errors never embed row data or resolved parameter values.
func main() {
	var request struct {
		Model   models.Model                 `json:"model"`
		Rows    map[string][]json.RawMessage `json:"rows"`
		SQL     string                       `json:"sql"`
		Params  []any                        `json:"params"`
		MaxRows int                          `json:"maxRows"`
	}
	decoder := json.NewDecoder(os.Stdin)
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		os.Exit(2)
	}
	result, err := query.Execute(request.Model, request.Rows, request.SQL, request.Params, request.MaxRows)
	if err != nil {
		var queryErr *query.Error
		if !errors.As(err, &queryErr) {
			queryErr = &query.Error{Code: "internal", Message: "internal query failure"}
		}
		check.Must(json.NewEncoder(os.Stdout).Encode(struct {
			Error *query.Error `json:"error"`
		}{Error: queryErr}))
		return
	}
	check.Must(json.NewEncoder(os.Stdout).Encode(result))
}
