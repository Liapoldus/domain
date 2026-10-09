package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/CreateLab/glinq/pkg/glinq"
)

// Execution layer: row decoding, tuple construction (FROM + equijoins),
// aggregate folding, projection, the total-order sort, DISTINCT and
// pagination. Streaming operators come from glinq; join semantics,
// aggregate folds and the comparator are hand-written (package comment).

type queryTuple struct {
	cells [][]any // one []any per source, in source order
}

type resultItem struct {
	out       []any
	orderKeys []any
	idx       int
}

// decodeEntityRows turns one entity's raw row documents into cell rows
// following the entity's field order. An object row may omit fields (they
// read as null) but may not introduce unknown keys; a positional array must
// match the field count exactly. Rejection messages carry identifiers and
// counts only — never row contents.
func decodeEntityRows(source sourceDef, rawRows []json.RawMessage) ([][]any, error) {
	fields := source.entity.Fields
	decoded := make([][]any, 0, len(rawRows))
	for _, raw := range rawRows {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || (trimmed[0] != '[' && trimmed[0] != '{') {
			return nil, rejectQuery(fmt.Sprintf("row for entity %q is not a JSON object or array", source.entity.Name))
		}
		if trimmed[0] == '[' {
			var items []json.RawMessage
			if err := json.Unmarshal(trimmed, &items); err != nil {
				return nil, rejectQuery(fmt.Sprintf("row for entity %q is not a JSON object or array", source.entity.Name))
			}
			if len(items) != len(fields) {
				return nil, rejectQuery(fmt.Sprintf(
					"positional row for entity %q has %d values, expected %d",
					source.entity.Name, len(items), len(fields)))
			}
			cells := make([]any, len(fields))
			for i, item := range items {
				value, err := decodeRowValue(item, source.entity.Name)
				if err != nil {
					return nil, err
				}
				cells[i] = value
			}
			decoded = append(decoded, cells)
			continue
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &object); err != nil {
			return nil, rejectQuery(fmt.Sprintf("row for entity %q is not a JSON object or array", source.entity.Name))
		}
		cells := make([]any, len(fields))
		for key, item := range object {
			index := fieldIndexOf(source.entity, key)
			if index < 0 {
				return nil, rejectQuery(fmt.Sprintf("unknown field %q in row for entity %q", key, source.entity.Name))
			}
			value, err := decodeRowValue(item, source.entity.Name)
			if err != nil {
				return nil, err
			}
			cells[index] = value
		}
		decoded = append(decoded, cells)
	}
	return decoded, nil
}

func decodeRowValue(raw json.RawMessage, entityName string) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, rejectQuery(fmt.Sprintf("row value for entity %q is not valid JSON", entityName))
	}
	return value, nil
}

// buildJoinTuples materializes FROM as tuples and applies each equijoin as
// a hash-indexed nested loop: the joined source is bucketed by its join key
// with glinq.GroupBy, then every left tuple probes the bucket for its key.
// Null keys never match (equality with null is unknown); unmatched LEFT
// JOIN rows get a null-filled cell row.
func buildJoinTuples(bound *boundQuery, decoded [][][]any) []queryTuple {
	sourceCount := len(bound.sources)
	tuples := make([]queryTuple, 0, len(decoded[0]))
	for _, row := range decoded[0] {
		cells := make([][]any, sourceCount)
		cells[0] = row
		tuples = append(tuples, queryTuple{cells: cells})
	}
	for _, join := range bound.joins {
		type indexedRow struct {
			row []any
		}
		rightRows := decoded[join.sourceIdx]
		bucketed := make([]indexedRow, len(rightRows))
		for i, row := range rightRows {
			bucketed[i] = indexedRow{row: row}
		}
		buckets := glinq.GroupBy(glinq.From(bucketed), func(item indexedRow) string {
			return valueKey(item.row[join.rightFld])
		})
		index := make(map[string][]indexedRow)
		for {
			pair, ok := buckets.Next()
			if !ok {
				break
			}
			index[pair.Key] = pair.Value
		}
		nullRow := make([]any, len(bound.sources[join.sourceIdx].entity.Fields))
		next := make([]queryTuple, 0, len(tuples))
		for _, left := range tuples {
			leftKey := left.cells[join.leftSrc][join.leftFld]
			var matches []indexedRow
			if leftKey != nil {
				matches = index[valueKey(leftKey)]
			}
			if len(matches) > 0 {
				for _, match := range matches {
					cells := make([][]any, sourceCount)
					copy(cells, left.cells)
					cells[join.sourceIdx] = match.row
					next = append(next, queryTuple{cells: cells})
				}
			} else if join.leftJoin {
				cells := make([][]any, sourceCount)
				copy(cells, left.cells)
				cells[join.sourceIdx] = nullRow
				next = append(next, queryTuple{cells: cells})
			}
		}
		tuples = next
	}
	return tuples
}

// computeAggregates folds every declared aggregate over one group's tuples.
// COUNT over no rows is 0; SUM, AVG, MIN and MAX over no non-null values
// are null. SUM and AVG reject non-numeric values with a statement-shape
// error that names the function only, never the offending row value.
func computeAggregates(defs []aggDef, tuples []queryTuple) ([]any, error) {
	results := make([]any, len(defs))
	for i, def := range defs {
		switch def.fn {
		case "COUNT":
			if def.star {
				results[i] = len(tuples)
				continue
			}
			count := 0
			for _, tuple := range tuples {
				if tuple.cells[def.src][def.fld] != nil {
					count++
				}
			}
			results[i] = count
		case "SUM", "AVG":
			sum := new(big.Rat)
			seen := 0
			for _, tuple := range tuples {
				value := tuple.cells[def.src][def.fld]
				if value == nil {
					continue
				}
				rational, ok := toRat(value)
				if !ok {
					return nil, rejectQuery(def.fn + " requires numeric values")
				}
				sum.Add(sum, rational)
				seen++
			}
			if seen == 0 {
				results[i] = nil
				continue
			}
			if def.fn == "AVG" {
				sum = new(big.Rat).Quo(sum, big.NewRat(int64(seen), 1))
			}
			results[i] = renderRat(sum)
		case "MIN", "MAX":
			var best any
			found := false
			for _, tuple := range tuples {
				value := tuple.cells[def.src][def.fld]
				if value == nil {
					continue
				}
				if !found {
					best, found = value, true
					continue
				}
				order := compareValues(value, best)
				if (def.fn == "MIN" && order < 0) || (def.fn == "MAX" && order > 0) {
					best = value
				}
			}
			if !found {
				results[i] = nil
			} else {
				results[i] = best
			}
		}
	}
	return results, nil
}

// renderRat renders an exact rational as a JSON number with at most 12
// fractional digits: 12 digits round non-terminating quotients (AVG over
// 8.75/3 → 2.916666666667) deterministically while whole and short results
// stay exact (160 → 160, 4.5 → 4.5).
func renderRat(r *big.Rat) json.Number {
	rendered := r.FloatString(12)
	rendered = strings.TrimRight(rendered, "0")
	rendered = strings.TrimRight(rendered, ".")
	if rendered == "" || rendered == "-" {
		rendered = "0"
	}
	return json.Number(rendered)
}

func projectFlat(bound *boundQuery, tuples []queryTuple) []resultItem {
	items := make([]resultItem, 0, len(tuples))
	for index, tuple := range tuples {
		out := make([]any, len(bound.selectTerms))
		for i, term := range bound.selectTerms {
			out[i] = tuple.cells[term.src][term.fld]
		}
		keys := make([]any, len(bound.orderBy))
		for i, order := range bound.orderBy {
			keys[i] = tuple.cells[order.src][order.fld]
		}
		items = append(items, resultItem{out: out, orderKeys: keys, idx: index})
	}
	return items
}

// projectGrouped buckets tuples by their GROUP BY key with glinq.GroupBy,
// evaluates HAVING per group with Kleene logic, then projects group-key
// slots and aggregate results. A whole-table aggregate with no GROUP BY
// yields exactly one group even when no rows survive WHERE; an explicit
// GROUP BY over no rows yields none.
func projectGrouped(bound *boundQuery, tuples []queryTuple, params []any) ([]resultItem, error) {
	type keyedTuple struct {
		key   string
		tuple queryTuple
	}
	keyed := make([]keyedTuple, len(tuples))
	for i, tuple := range tuples {
		keyValues := make([]any, len(bound.groupBy))
		for slot, group := range bound.groupBy {
			keyValues[slot] = tuple.cells[group.src][group.fld]
		}
		keyed[i] = keyedTuple{key: valuesKey(keyValues), tuple: tuple}
	}
	bucketStream := glinq.GroupBy(glinq.From(keyed), func(item keyedTuple) string { return item.key })
	type groupBucket struct {
		tuples []queryTuple
	}
	buckets := make([]groupBucket, 0)
	for {
		pair, ok := bucketStream.Next()
		if !ok {
			break
		}
		group := groupBucket{tuples: make([]queryTuple, 0, len(pair.Value))}
		for _, item := range pair.Value {
			group.tuples = append(group.tuples, item.tuple)
		}
		buckets = append(buckets, group)
	}
	if len(tuples) == 0 && len(bound.groupBy) == 0 {
		buckets = append(buckets, groupBucket{})
	}

	items := make([]resultItem, 0, len(buckets))
	for index, bucket := range buckets {
		keyValues := make([]any, len(bound.groupBy))
		if len(bucket.tuples) > 0 {
			for slot, group := range bound.groupBy {
				keyValues[slot] = bucket.tuples[0].cells[group.src][group.fld]
			}
		}
		aggValues, err := computeAggregates(bound.aggs, bucket.tuples)
		if err != nil {
			return nil, err
		}
		if bound.having != nil {
			env := &evalEnv{group: keyValues, aggs: aggValues, params: params, grouped: true}
			if evalState(bound.having, env) != tsTrue {
				continue
			}
		}
		out := make([]any, len(bound.selectTerms))
		for i, term := range bound.selectTerms {
			if term.isAgg {
				out[i] = aggValues[term.aggID]
			} else {
				out[i] = keyValues[term.slot]
			}
		}
		keys := make([]any, len(bound.orderBy))
		for i, order := range bound.orderBy {
			if order.isAgg {
				keys[i] = aggValues[order.aggID]
			} else {
				keys[i] = keyValues[order.slot]
			}
		}
		items = append(items, resultItem{out: out, orderKeys: keys, idx: index})
	}
	return items, nil
}

// compareItems is the total sort order: ORDER BY keys (each with its ASC or
// DESC direction), then the full output row ascending, then the item's
// construction index. The trailing tie-breaks make the comparator total, so
// glinq's unstable OrderBy and map-ordered GroupBy cannot affect output.
func compareItems(a, b resultItem, bound *boundQuery) int {
	for i, key := range a.orderKeys {
		if order := compareValues(key, b.orderKeys[i]) * bound.orderDirs[i]; order != 0 {
			return order
		}
	}
	for i, value := range a.out {
		if order := compareValues(value, b.out[i]); order != 0 {
			return order
		}
	}
	return a.idx - b.idx
}

// executeBoundQuery runs a validated, bound statement over decoded rows:
// tuples → WHERE → grouping/HAVING → projection → total-order sort →
// DISTINCT → OFFSET → LIMIT.
func executeBoundQuery(bound *boundQuery, decoded [][][]any, params []any, maxRows int) (Result, error) {
	tuples := buildJoinTuples(bound, decoded)

	if bound.where != nil {
		env := &evalEnv{params: params}
		tuples = glinq.From(tuples).Where(func(tuple queryTuple) bool {
			env.cells = tuple.cells
			return evalState(bound.where, env) == tsTrue
		}).ToSlice()
	}

	var items []resultItem
	if bound.grouped {
		grouped, err := projectGrouped(bound, tuples, params)
		if err != nil {
			return Result{}, err
		}
		items = grouped
	} else {
		items = projectFlat(bound, tuples)
	}

	sorted := glinq.From(items).OrderBy(func(a, b resultItem) int {
		return compareItems(a, b, bound)
	}).ToSlice()
	if bound.distinct {
		sorted = glinq.From(sorted).DistinctBy(func(item resultItem) any {
			return rowKey(item.out)
		}).ToSlice()
	}

	limit := int64(defaultLimit)
	if bound.limitSet {
		limit = bound.limit
	}
	if maxRows > 0 && int64(maxRows) < limit {
		limit = int64(maxRows)
	}
	page := glinq.From(sorted).Skip(int(bound.offset)).Take(int(limit)).ToSlice()

	columns := make([]string, len(bound.selectTerms))
	for i, term := range bound.selectTerms {
		columns[i] = term.label
	}
	rows := make([][]any, 0, len(page))
	for _, item := range page {
		rows = append(rows, item.out)
	}
	return Result{Columns: columns, Rows: rows}, nil
}
