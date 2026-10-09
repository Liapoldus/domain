// Package query implements the domain's use-case layer. This file
// exposes the SELECT query planner/executor defined in the design spec (§6):
// a pure function that turns a declarative model, materialized row documents
// and a SELECT statement into result rows.
//
// Execute performs no I/O, reads no clock, draws no randomness and
// never logs: every rejection is returned as a Error whose message
// contains identifiers and counts only — never row values, SQL parameter
// values or tenant credentials. Tenant/site scoping happens BEFORE the
// planner: the caller is responsible for handing in only the rows that the
// current tenant/site may read (the RowReader enforcement point sits behind
// the domain barrier), so the planner itself performs no authorization.
//
// The statement surface is deliberately small: single SELECT, equality
// joins backed by declared references, WHERE/GROUP BY/HAVING/ORDER BY,
// LIMIT/OFFSET. Subqueries, set operations, DML and DDL are rejected before
// any row is decoded.
//
// Pipeline: length check → lex → forbidden-token pre-pass → parse →
// parameter binding → model validation → scan-count check → row decode →
// FROM/JOIN tuple construction → WHERE → GROUP BY/HAVING → projection and
// sort keys → sort → DISTINCT → OFFSET → LIMIT.
//
// Division of labour with glinq (github.com/CreateLab/glinq): glinq supplies
// the streaming operators — Where (WHERE and HAVING filters), GroupBy (both
// GROUP BY bucketing and the hash index for the right side of equijoins),
// OrderBy (total-order sort), DistinctBy (DISTINCT), Skip/Take
// (OFFSET/LIMIT) and the free Select mapper where mapping is used. All
// statement semantics are hand-written: the lexer, the parser, the
// validation pass, the nested-loop equijoin itself (glinq has no join
// operator), aggregate folds over dynamic values (glinq's Sum/Min/Max are
// constrained to static numeric types), three-valued logic and the total
// value ordering used for comparisons and sorts. glinq's GroupBy iterates
// its buckets in Go map order, which is nondeterministic; the executor is
// therefore built so that the final sort is total (sort keys → full output
// row → item index), which makes identical rows the only possible
// tie-bounds, and interchangeable rows cannot be distinguished by any
// observable output.
package query

import (
	"encoding/json"
	"fmt"

	"github.com/Liapoldus/domain/internal/domain/models"
)

// Planner bounds. These are statement-shape limits, not authorization:
// they bound the work a single query may ask for before authorization is
// re-checked by the caller.
const (
	maxSQLBytes  = 8192   // maximum length of a statement in bytes
	maxParams    = 64     // maximum number of ? placeholders per statement
	maxScanRows  = 200000 // maximum rows scanned across FROM and JOIN sources
	maxLimitRows = 10000  // maximum value accepted for LIMIT and OFFSET
	defaultLimit = 100    // rows returned when the statement omits LIMIT
)

// Result is the JSON shape of a successful domain.query response:
// column labels in projection order, then row values in sort order.
type Result struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// Error is the only error type Execute returns to callers.
// Code is always "query_rejected" for statement-level rejections; the probe
// maps any unexpected non-Error to "internal" with a generic message.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

func rejectQuery(message string) error {
	return &Error{Code: "query_rejected", Message: message}
}

// Execute plans and executes one SELECT statement against the given
// model and materialized rows. The rows map holds one entry per entity name
// with that entity's row documents; a row may be a JSON object keyed by
// field name (unknown keys are rejected) or a positional JSON array whose
// arity must match the entity's field order. Missing fields read as null.
// maxRows is an optional caller cap: 0 or less means unspecified, a
// positive value lowers the effective row limit to min(LIMIT or 100,
// maxRows) and never raises it.
//
// The function is pure: identical inputs always produce identical outputs,
// including row order (the planner always sorts with a total ordering).
func Execute(model models.Model, rows map[string][]json.RawMessage, sql string, params []any, maxRows int) (Result, error) {
	if len(sql) > maxSQLBytes {
		return Result{}, rejectQuery(fmt.Sprintf("SQL exceeds the maximum length of %d bytes", maxSQLBytes))
	}
	tokens, err := lexQuery(sql)
	if err != nil {
		return Result{}, err
	}
	if err := precheckTokens(tokens); err != nil {
		return Result{}, err
	}
	stmt, err := parseQuery(tokens)
	if err != nil {
		return Result{}, err
	}
	if stmt.paramCount > maxParams {
		return Result{}, rejectQuery(fmt.Sprintf("too many parameters: maximum is %d", maxParams))
	}
	if stmt.paramCount != len(params) {
		return Result{}, rejectQuery(fmt.Sprintf(
			"parameter count mismatch: query has %d placeholders but %d parameters were provided",
			stmt.paramCount, len(params)))
	}
	for _, param := range params {
		if !isScalar(param) {
			return Result{}, rejectQuery("non-scalar query parameter")
		}
	}
	bound, err := validateQuery(model, stmt)
	if err != nil {
		return Result{}, err
	}
	scanRows := 0
	for _, source := range bound.sources {
		scanRows += len(rows[source.entity.Name])
		if scanRows > maxScanRows {
			return Result{}, rejectQuery(fmt.Sprintf("scan row limit of %d exceeded", maxScanRows))
		}
	}
	decoded := make([][][]any, len(bound.sources))
	for i, source := range bound.sources {
		decoded[i], err = decodeEntityRows(source, rows[source.entity.Name])
		if err != nil {
			return Result{}, err
		}
	}
	return executeBoundQuery(bound, decoded, params, maxRows)
}
