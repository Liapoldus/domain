package query

import (
	"fmt"

	"github.com/Liapoldus/domain/internal/domain/models"
)

// A boundQuery is a statement whose identifiers have been resolved against
// the model: every column reference carries a source and field index, every
// grouped reference additionally carries its GROUP BY slot, and every
// aggregate carries an id into the shared aggregate table.

type sourceDef struct {
	entity models.Entity
	alias  string
}

type boundColumn struct {
	src int
	fld int
}

type aggDef struct {
	fn   string // COUNT, SUM, MIN, MAX, AVG (upper case)
	star bool
	src  int
	fld  int
}

type boundJoin struct {
	leftJoin  bool
	sourceIdx int // index of the joined source
	leftSrc   int // join key column on the left side
	leftFld   int
	rightFld  int // join key column on the joined source
}

type boundSelectTerm struct {
	isAgg bool
	aggID int
	slot  int // group-key slot when the query is grouped, -1 otherwise
	src   int
	fld   int
	label string
}

type boundOrderTerm struct {
	desc  bool
	isAgg bool
	aggID int
	slot  int
	src   int
	fld   int
}

type boundQuery struct {
	distinct    bool
	sources     []sourceDef
	joins       []boundJoin
	where       expr // tuple-level binding (slot -1)
	having      expr // group-level binding (slot >= 0)
	grouped     bool
	groupBy     []boundColumn
	selectTerms []boundSelectTerm
	orderBy     []boundOrderTerm
	orderDirs   []int // 1 ascending, -1 descending; parallel to orderBy
	aggs        []aggDef
	limitSet    bool
	limit       int64
	offset      int64
}

func findEntity(model models.Model, name string) (models.Entity, bool) {
	for _, entity := range model.Entities {
		if entity.Name == name {
			return entity, true
		}
	}
	return models.Entity{}, false
}

func fieldIndexOf(entity models.Entity, name string) int {
	for i, field := range entity.Fields {
		if field.Name == name {
			return i
		}
	}
	return -1
}

// declaredReference reports whether the equality left == right is backed by
// a references constraint, in either direction. Joins that are not declared
// in the model are refused so that query shape cannot invent relationships.
func declaredReference(model models.Model, leftEntity, leftField, rightEntity, rightField string) bool {
	if entity, ok := findEntity(model, leftEntity); ok {
		if idx := fieldIndexOf(entity, leftField); idx >= 0 {
			ref := entity.Fields[idx].References
			if ref != nil && ref.Entity == rightEntity && ref.Field == rightField {
				return true
			}
		}
	}
	if entity, ok := findEntity(model, rightEntity); ok {
		if idx := fieldIndexOf(entity, rightField); idx >= 0 {
			ref := entity.Fields[idx].References
			if ref != nil && ref.Entity == leftEntity && ref.Field == leftField {
				return true
			}
		}
	}
	return false
}

func exprHasAggregate(e expr) bool {
	switch node := e.(type) {
	case *aggExpr:
		return true
	case *colExpr:
		return false
	case litExpr, paramExpr:
		return false
	case notExpr:
		return exprHasAggregate(node.arg)
	case boolExpr:
		return exprHasAggregate(node.left) || exprHasAggregate(node.right)
	case cmpExpr:
		return exprHasAggregate(node.left) || exprHasAggregate(node.right)
	default:
		return false
	}
}

// validateQuery resolves the parsed statement against the model and binds
// every identifier. All rejections are statement-shape errors: they name
// identifiers and structural counts, never row or parameter values.
func validateQuery(model models.Model, stmt *query) (*boundQuery, error) {
	sources := make([]sourceDef, 0, len(stmt.joins)+1)
	aliasSeen := make(map[string]bool, len(stmt.joins)+1)
	addSource := func(ref tableRef) error {
		entity, ok := findEntity(model, ref.entity)
		if !ok {
			return rejectQuery(fmt.Sprintf("unknown entity %q", ref.entity))
		}
		if aliasSeen[ref.alias] {
			return rejectQuery(fmt.Sprintf("duplicate source alias %q", ref.alias))
		}
		aliasSeen[ref.alias] = true
		sources = append(sources, sourceDef{entity: entity, alias: ref.alias})
		return nil
	}
	if err := addSource(stmt.from); err != nil {
		return nil, err
	}
	for _, join := range stmt.joins {
		if err := addSource(join.table); err != nil {
			return nil, err
		}
	}

	resolve := func(ref columnRef) (int, int, error) {
		if ref.alias != "" {
			src := -1
			for i, source := range sources {
				if source.alias == ref.alias {
					src = i
					break
				}
			}
			if src < 0 {
				return 0, 0, rejectQuery(fmt.Sprintf("unknown source alias %q", ref.alias))
			}
			fld := fieldIndexOf(sources[src].entity, ref.name)
			if fld < 0 {
				return 0, 0, rejectQuery(fmt.Sprintf("unknown field %q for entity %q", ref.name, sources[src].entity.Name))
			}
			return src, fld, nil
		}
		firstSrc, firstFld := -1, -1
		matches := 0
		for i, source := range sources {
			if fld := fieldIndexOf(source.entity, ref.name); fld >= 0 {
				matches++
				if matches == 1 {
					firstSrc, firstFld = i, fld
				}
			}
		}
		if matches == 0 {
			entityName := ""
			if len(sources) > 0 {
				entityName = sources[0].entity.Name
			}
			return 0, 0, rejectQuery(fmt.Sprintf("unknown field %q for entity %q", ref.name, entityName))
		}
		if matches > 1 {
			return 0, 0, rejectQuery(fmt.Sprintf("ambiguous column reference %q", ref.name))
		}
		return firstSrc, firstFld, nil
	}

	bound := &boundQuery{
		distinct:  stmt.distinct,
		sources:   sources,
		limitSet:  stmt.limitSet,
		limit:     stmt.limit,
		offset:    stmt.offset,
		groupBy:   make([]boundColumn, 0, len(stmt.groupBy)),
		orderDirs: make([]int, 0, len(stmt.orderBy)),
	}

	// Joins: each ON condition must be a single equality between one column
	// of the accumulated left side and one column of the joined source,
	// and it must be backed by a declared reference.
	for i, join := range stmt.joins {
		joinIdx := i + 1
		cmp, ok := join.on.(cmpExpr)
		if !ok || cmp.op != "=" {
			return nil, rejectQuery("join condition must be an equality between two columns")
		}
		left, leftOK := cmp.left.(*colExpr)
		right, rightOK := cmp.right.(*colExpr)
		if !leftOK || !rightOK {
			return nil, rejectQuery("join condition must be an equality between two columns")
		}
		leftSrc, leftFld, err := resolve(left.ref)
		if err != nil {
			return nil, err
		}
		rightSrc, rightFld, err := resolve(right.ref)
		if err != nil {
			return nil, err
		}
		left.src, left.fld = leftSrc, leftFld
		right.src, right.fld = rightSrc, rightFld
		var keyLeftSrc, keyLeftFld, keyRightSrc, keyRightFld int
		switch {
		case leftSrc < joinIdx && rightSrc == joinIdx:
			keyLeftSrc, keyLeftFld, keyRightSrc, keyRightFld = leftSrc, leftFld, rightSrc, rightFld
		case rightSrc < joinIdx && leftSrc == joinIdx:
			keyLeftSrc, keyLeftFld, keyRightSrc, keyRightFld = rightSrc, rightFld, leftSrc, leftFld
		default:
			return nil, rejectQuery("join condition must compare one column from each side")
		}
		if !declaredReference(model,
			sources[keyLeftSrc].entity.Name, sources[keyLeftSrc].entity.Fields[keyLeftFld].Name,
			sources[keyRightSrc].entity.Name, sources[keyRightSrc].entity.Fields[keyRightFld].Name,
		) {
			return nil, rejectQuery("join condition is not backed by a declared reference")
		}
		bound.joins = append(bound.joins, boundJoin{
			leftJoin:  join.leftJoin,
			sourceIdx: joinIdx,
			leftSrc:   keyLeftSrc,
			leftFld:   keyLeftFld,
			rightFld:  keyRightFld,
		})
	}

	// WHERE runs before grouping: bindings are tuple-level and aggregates
	// are refused outright.
	if stmt.where != nil {
		if err := bindTupleExpr(stmt.where, resolve); err != nil {
			return nil, err
		}
		bound.where = stmt.where
	}

	// GROUP BY slots. Duplicate group columns collapse onto one slot.
	for _, ref := range stmt.groupBy {
		src, fld, err := resolve(ref)
		if err != nil {
			return nil, err
		}
		duplicate := false
		for _, existing := range bound.groupBy {
			if existing == (boundColumn{src: src, fld: fld}) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			bound.groupBy = append(bound.groupBy, boundColumn{src: src, fld: fld})
		}
	}
	groupSlot := func(bc boundColumn) int {
		for i, existing := range bound.groupBy {
			if existing == bc {
				return i
			}
		}
		return -1
	}

	selectHasAgg := false
	for _, term := range stmt.selectList {
		if term.isAgg {
			selectHasAgg = true
			break
		}
	}
	havingHasAgg := stmt.having != nil && exprHasAggregate(stmt.having)
	bound.grouped = len(bound.groupBy) > 0 || selectHasAgg || havingHasAgg

	hasStar := false
	for _, term := range stmt.selectList {
		if term.isStar {
			hasStar = true
		}
	}
	if hasStar && bound.grouped {
		return nil, rejectQuery("star projection cannot be combined with GROUP BY or aggregates")
	}

	labels := make(map[string]bool, len(stmt.selectList))
	addTerm := func(term boundSelectTerm) {
		bound.selectTerms = append(bound.selectTerms, term)
	}
	bindAggregate := func(fn string, star bool, src, fld int) (int, error) {
		if star && fn != "COUNT" {
			return 0, rejectQuery("only COUNT accepts * as its argument")
		}
		id := len(bound.aggs)
		bound.aggs = append(bound.aggs, aggDef{fn: fn, star: star, src: src, fld: fld})
		return id, nil
	}

	for _, term := range stmt.selectList {
		if term.isStar {
			sourcesToExpand := sources
			if term.starAlias != "" {
				idx := -1
				for i, source := range sources {
					if source.alias == term.starAlias {
						idx = i
						break
					}
				}
				if idx < 0 {
					return nil, rejectQuery(fmt.Sprintf("unknown source alias %q", term.starAlias))
				}
				sourcesToExpand = sources[idx : idx+1]
			}
			for _, source := range sourcesToExpand {
				for _, field := range source.entity.Fields {
					label := source.alias + "." + field.Name
					if labels[label] {
						return nil, rejectQuery(fmt.Sprintf("duplicate column label %q", label))
					}
					labels[label] = true
					addTerm(boundSelectTerm{src: indexOfSource(sources, source.alias), fld: fieldIndexOf(source.entity, field.Name), label: label})
				}
			}
			continue
		}
		if term.isAgg {
			src, fld := -1, -1
			if !term.aggStar {
				var err error
				src, fld, err = resolve(term.aggArg)
				if err != nil {
					return nil, err
				}
			}
			id, err := bindAggregate(term.aggFn, term.aggStar, src, fld)
			if err != nil {
				return nil, err
			}
			label := term.aggFn + "(" + term.aggRaw + ")"
			if labels[label] {
				return nil, rejectQuery(fmt.Sprintf("duplicate column label %q", label))
			}
			labels[label] = true
			addTerm(boundSelectTerm{isAgg: true, aggID: id, label: label})
			continue
		}
		src, fld, err := resolve(term.col)
		if err != nil {
			return nil, err
		}
		label := term.col.written()
		if labels[label] {
			return nil, rejectQuery(fmt.Sprintf("duplicate column label %q", label))
		}
		labels[label] = true
		if bound.grouped {
			slot := groupSlot(boundColumn{src: src, fld: fld})
			if slot < 0 {
				return nil, rejectQuery(fmt.Sprintf(
					"column %q must appear in the GROUP BY list or be used in an aggregate", label))
			}
			addTerm(boundSelectTerm{slot: slot, label: label})
			continue
		}
		addTerm(boundSelectTerm{src: src, fld: fld, label: label})
	}

	// HAVING binds against the group: aggregates get ids, columns must be
	// part of the grouping.
	if stmt.having != nil {
		if !bound.grouped {
			return nil, rejectQuery("HAVING requires GROUP BY or aggregates")
		}
		if err := bindGroupExpr(stmt.having, resolve, groupSlot, bindAggregate); err != nil {
			return nil, err
		}
		bound.having = stmt.having
	}

	for _, term := range stmt.orderBy {
		boundTerm := boundOrderTerm{desc: term.desc}
		if term.isAgg {
			if !bound.grouped {
				return nil, rejectQuery("aggregate functions in ORDER BY require a grouped query")
			}
			src, fld := -1, -1
			if !term.aggStar {
				var err error
				src, fld, err = resolve(term.aggArg)
				if err != nil {
					return nil, err
				}
			}
			id, err := bindAggregate(term.aggFn, term.aggStar, src, fld)
			if err != nil {
				return nil, err
			}
			boundTerm.isAgg = true
			boundTerm.aggID = id
		} else {
			src, fld, err := resolve(term.col)
			if err != nil {
				return nil, err
			}
			if bound.grouped {
				slot := groupSlot(boundColumn{src: src, fld: fld})
				if slot < 0 {
					return nil, rejectQuery(fmt.Sprintf(
						"column %q must appear in the GROUP BY list or be used in an aggregate",
						term.col.written()))
				}
				boundTerm.slot = slot
			} else {
				boundTerm.src, boundTerm.fld = src, fld
			}
		}
		bound.orderBy = append(bound.orderBy, boundTerm)
		direction := 1
		if term.desc {
			direction = -1
		}
		bound.orderDirs = append(bound.orderDirs, direction)
	}

	return bound, nil
}

func indexOfSource(sources []sourceDef, alias string) int {
	for i, source := range sources {
		if source.alias == alias {
			return i
		}
	}
	return -1
}

// bindTupleExpr walks an expression that is evaluated per row before
// grouping: columns resolve to source/field indexes, parameters and literals
// pass through, and aggregates are rejected.
func bindTupleExpr(e expr, resolve func(columnRef) (int, int, error)) error {
	switch node := e.(type) {
	case *aggExpr:
		return rejectQuery("aggregate functions are not allowed in WHERE")
	case *colExpr:
		src, fld, err := resolve(node.ref)
		if err != nil {
			return err
		}
		node.src, node.fld, node.slot = src, fld, -1
		return nil
	case notExpr:
		return bindTupleExpr(node.arg, resolve)
	case boolExpr:
		if err := bindTupleExpr(node.left, resolve); err != nil {
			return err
		}
		return bindTupleExpr(node.right, resolve)
	case cmpExpr:
		if err := bindTupleExpr(node.left, resolve); err != nil {
			return err
		}
		return bindTupleExpr(node.right, resolve)
	default:
		return nil
	}
}

// bindGroupExpr walks an expression evaluated after grouping: aggregates get
// shared ids and columns must belong to the GROUP BY list.
func bindGroupExpr(
	e expr,
	resolve func(columnRef) (int, int, error),
	groupSlot func(boundColumn) int,
	bindAggregate func(fn string, star bool, src, fld int) (int, error),
) error {
	switch node := e.(type) {
	case *aggExpr:
		src, fld := -1, -1
		if !node.star {
			var err error
			src, fld, err = resolve(node.ref)
			if err != nil {
				return err
			}
		}
		id, err := bindAggregate(node.fn, node.star, src, fld)
		if err != nil {
			return err
		}
		node.id = id
		return nil
	case *colExpr:
		src, fld, err := resolve(node.ref)
		if err != nil {
			return err
		}
		slot := groupSlot(boundColumn{src: src, fld: fld})
		if slot < 0 {
			return rejectQuery(fmt.Sprintf(
				"column %q must appear in the GROUP BY list or be used in an aggregate",
				node.ref.written()))
		}
		node.src, node.fld, node.slot = src, fld, slot
		return nil
	case notExpr:
		return bindGroupExpr(node.arg, resolve, groupSlot, bindAggregate)
	case boolExpr:
		if err := bindGroupExpr(node.left, resolve, groupSlot, bindAggregate); err != nil {
			return err
		}
		return bindGroupExpr(node.right, resolve, groupSlot, bindAggregate)
	case cmpExpr:
		if err := bindGroupExpr(node.left, resolve, groupSlot, bindAggregate); err != nil {
			return err
		}
		return bindGroupExpr(node.right, resolve, groupSlot, bindAggregate)
	default:
		return nil
	}
}
