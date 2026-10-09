package query

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Evaluation layer: dynamic values (JSON scalars and query parameters),
// three-valued logic, the total value ordering used by comparisons and
// sorts, and the canonical keys used for GROUP BY bucketing, join probing
// and DISTINCT. Everything here is hand-written (see the package comment
// for the glinq division of labour).

// evalEnv carries the two evaluation scopes. In tuple scope (WHERE) group
// and aggs are nil and columns resolve through cells; in group scope
// (HAVING) columns resolve through group slots and aggregates through aggs.
type evalEnv struct {
	cells   [][]any
	group   []any
	aggs    []any
	params  []any
	grouped bool
}

type tristate int

const (
	tsUnknown tristate = iota
	tsFalse
	tsTrue
)

// isScalar reports whether a parameter value is admissible: null, bool,
// string or a number. Arrays and objects are refused so parameters can only
// ever participate in scalar comparisons.
func isScalar(v any) bool {
	switch v.(type) {
	case nil, bool, string, json.Number, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}

// toRat converts a numeric value to an exact rational. json.Number decimals
// and float exponents are accepted; failure only ever happens for NaN or
// Inf, which JSON rows cannot contain.
func toRat(v any) (*big.Rat, bool) {
	switch x := v.(type) {
	case json.Number:
		return new(big.Rat).SetString(x.String())
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, false
		}
		return new(big.Rat).SetString(strconv.FormatFloat(x, 'g', -1, 64))
	case float32:
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		return new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 32))
	case int:
		return new(big.Rat).SetInt64(int64(x)), true
	case int8:
		return new(big.Rat).SetInt64(int64(x)), true
	case int16:
		return new(big.Rat).SetInt64(int64(x)), true
	case int32:
		return new(big.Rat).SetInt64(int64(x)), true
	case int64:
		return new(big.Rat).SetInt64(x), true
	case uint:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(x))), true
	case uint8:
		return new(big.Rat).SetInt64(int64(x)), true
	case uint16:
		return new(big.Rat).SetInt64(int64(x)), true
	case uint32:
		return new(big.Rat).SetInt64(int64(x)), true
	case uint64:
		r := new(big.Rat).SetInt(new(big.Int).SetUint64(x))
		return r, true
	default:
		return nil, false
	}
}

// valueRank orders the type classes of the total value ordering:
// null < bool < number < string < other.
func valueRank(v any) int {
	switch v.(type) {
	case nil:
		return 0
	case bool:
		return 1
	case json.Number, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return 2
	case string:
		return 3
	default:
		return 4
	}
}

// compareValues is the total ordering over dynamic values. It orders by
// type class first, then within a class: numbers numerically (exact
// rationals), bools false < true, strings lexicographically.
func compareValues(a, b any) int {
	ra, rb := valueRank(a), valueRank(b)
	if ra != rb {
		return ra - rb
	}
	switch ra {
	case 0:
		return 0
	case 1:
		av, aok := a.(bool)
		bv, bok := b.(bool)
		if !aok || !bok {
			return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
		}
		switch {
		case av == bv:
			return 0
		case !av:
			return -1
		default:
			return 1
		}
	case 2:
		ar, aok := toRat(a)
		br, bok := toRat(b)
		if aok && bok {
			return ar.Cmp(br)
		}
		return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
	case 3:
		av, aok := a.(string)
		bv, bok := b.(string)
		if !aok || !bok {
			return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
		}
		return strings.Compare(av, bv)
	default:
		return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
	}
}

// evalCompare applies a comparison operator under three-valued logic: any
// null operand makes the result unknown (nil), comparisons across type
// classes are false for = and true for !=, and ordering across type
// classes is false (never an error, never a panic).
func evalCompare(op string, left, right any) any {
	if left == nil || right == nil {
		return nil
	}
	if valueRank(left) != valueRank(right) {
		switch op {
		case "=":
			return false
		case "!=", "<>":
			return true
		default:
			return false
		}
	}
	order := compareValues(left, right)
	switch op {
	case "=":
		return order == 0
	case "!=", "<>":
		return order != 0
	case "<":
		return order < 0
	case "<=":
		return order <= 0
	case ">":
		return order > 0
	case ">=":
		return order >= 0
	default:
		return false
	}
}

// evalValue evaluates an expression to a dynamic value.
func evalValue(e expr, env *evalEnv) any {
	switch node := e.(type) {
	case litExpr:
		return node.value
	case paramExpr:
		if node.index >= 0 && node.index < len(env.params) {
			return env.params[node.index]
		}
		return nil
	case *colExpr:
		if env.grouped && node.slot >= 0 && node.slot < len(env.group) {
			return env.group[node.slot]
		}
		if node.src >= 0 && node.src < len(env.cells) && node.fld >= 0 && node.fld < len(env.cells[node.src]) {
			return env.cells[node.src][node.fld]
		}
		return nil
	case *aggExpr:
		if node.id >= 0 && node.id < len(env.aggs) {
			return env.aggs[node.id]
		}
		return nil
	case notExpr, boolExpr, cmpExpr:
		switch evalState(e, env) {
		case tsTrue:
			return true
		case tsFalse:
			return false
		default:
			return nil
		}
	default:
		return nil
	}
}

func truthState(v any) tristate {
	switch x := v.(type) {
	case nil:
		return tsUnknown
	case bool:
		if x {
			return tsTrue
		}
		return tsFalse
	default:
		return tsFalse
	}
}

// evalState evaluates an expression in boolean context. WHERE and HAVING
// keep exactly the rows whose predicate is TRUE: unknown behaves like
// false for filtering but NOT and the AND/OR chains use Kleene logic so
// that unknowns never leak a spurious TRUE.
func evalState(e expr, env *evalEnv) tristate {
	switch node := e.(type) {
	case litExpr:
		return truthState(node.value)
	case paramExpr:
		return truthState(evalValue(node, env))
	case *colExpr, *aggExpr:
		return truthState(evalValue(e, env))
	case cmpExpr:
		result := evalCompare(node.op, evalValue(node.left, env), evalValue(node.right, env))
		if result == nil {
			return tsUnknown
		}
		if b, ok := result.(bool); ok && b {
			return tsTrue
		}
		return tsFalse
	case notExpr:
		switch evalState(node.arg, env) {
		case tsTrue:
			return tsFalse
		case tsFalse:
			return tsTrue
		default:
			return tsUnknown
		}
	case boolExpr:
		left := evalState(node.left, env)
		right := evalState(node.right, env)
		if node.op == "AND" {
			if left == tsFalse || right == tsFalse {
				return tsFalse
			}
			if left == tsUnknown || right == tsUnknown {
				return tsUnknown
			}
			return tsTrue
		}
		if left == tsTrue || right == tsTrue {
			return tsTrue
		}
		if left == tsUnknown || right == tsUnknown {
			return tsUnknown
		}
		return tsFalse
	default:
		return tsUnknown
	}
}

// valueKey is the canonical equality key of a value: tag byte plus payload,
// length-prefixed for strings so that concatenating keys of several columns
// cannot produce collisions. Numbers key on their exact rational form, so
// 80 and 80.0 bucket together.
func valueKey(v any) string {
	switch x := v.(type) {
	case nil:
		return "\x00"
	case bool:
		if x {
			return "\x01t"
		}
		return "\x01f"
	case json.Number, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if r, ok := toRat(v); ok {
			return "\x02" + r.RatString()
		}
		return "\x02s:" + fmt.Sprint(v)
	case string:
		return "\x03" + strconv.Itoa(len(x)) + ":" + x
	default:
		return "\x04" + fmt.Sprintf("%T", v) + ":" + fmt.Sprint(v)
	}
}

// valuesKey concatenates per-column keys into one bucket/projection key.
func valuesKey(values []any) string {
	var b strings.Builder
	for _, v := range values {
		if _, err := b.WriteString(valueKey(v)); err != nil {
			panic(err)
		}
	}
	return b.String()
}

// rowKey is the canonical key of a whole output row, used by DISTINCT.
func rowKey(row []any) string {
	return valuesKey(row)
}
