package query

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// AST. Nodes that carry a column reference or an aggregate are mutated by
// the validation pass to record their binding (source index, field index,
// group-key slot, aggregate id); Execute builds a fresh AST per call,
// so this mutation is confined to one invocation.

type columnRef struct {
	alias string // source alias, "" when unqualified
	name  string
}

func (r columnRef) written() string {
	if r.alias == "" {
		return r.name
	}
	return r.alias + "." + r.name
}

type expr interface{}

type litExpr struct {
	value any // nil, bool, json.Number or string
}

type paramExpr struct {
	index int
}

type colExpr struct {
	ref  columnRef
	src  int // bound source index, -1 before validation
	fld  int // bound field index within the source, -1 before validation
	slot int // bound group-key slot when the query is grouped, -1 otherwise
}

type aggExpr struct {
	fn   string // upper case: COUNT, SUM, MIN, MAX, AVG
	star bool
	ref  columnRef // argument column when !star
	id   int       // bound aggregate id, -1 before validation
}

type notExpr struct {
	arg expr
}

type boolExpr struct {
	op    string // AND, OR
	left  expr
	right expr
}

type cmpExpr struct {
	op    string // =, !=, <>, <, <=, >, >=
	left  expr
	right expr
}

type selectTerm struct {
	isStar    bool
	starAlias string // set for a.* expansion, "" for bare *
	isAgg     bool
	aggFn     string // upper case
	aggStar   bool
	aggArg    columnRef // argument column when !aggStar
	aggRaw    string    // label material: argument as written, or "*"
	col       columnRef // when !isAgg
}

type orderTerm struct {
	desc    bool
	isAgg   bool
	aggFn   string
	aggStar bool
	aggArg  columnRef
	aggRaw  string
	col     columnRef
}

type tableRef struct {
	entity string
	alias  string
}

type joinClause struct {
	leftJoin bool
	table    tableRef
	on       expr
}

type query struct {
	distinct   bool
	selectList []selectTerm
	from       tableRef
	joins      []joinClause
	where      expr
	groupBy    []columnRef
	having     expr
	orderBy    []orderTerm
	limitSet   bool
	limit      int64
	offset     int64
	paramCount int
}

type parser struct {
	tokens     []token
	pos        int
	paramCount int
}

func (p *parser) peek() token {
	return p.tokens[p.pos]
}

func (p *parser) next() token {
	tok := p.tokens[p.pos]
	if tok.kind != tokenEOF {
		p.pos++
	}
	return tok
}

func (p *parser) atKeyword(word string) bool {
	tok := p.peek()
	return tok.kind == tokenKeyword && tok.text == word
}

func (p *parser) acceptKeyword(word string) bool {
	if p.atKeyword(word) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) atPunct(symbol string) bool {
	tok := p.peek()
	return tok.kind == tokenPunct && tok.text == symbol
}

func (p *parser) acceptPunct(symbol string) bool {
	if p.atPunct(symbol) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expectKeyword(word string) error {
	if !p.acceptKeyword(word) {
		return syntaxError()
	}
	return nil
}

func (p *parser) expectPunct(symbol string) error {
	if !p.acceptPunct(symbol) {
		return syntaxError()
	}
	return nil
}

// syntaxError is deliberately generic: a diagnostic must never echo token
// text, because tokens may contain string-literal contents (row or tenant
// data supplied by the caller).
func syntaxError() error {
	return rejectQuery("syntax error in query")
}

func isAggregateWord(text string) (string, bool) {
	upper := toUpper(text)
	if aggregateNames[upper] {
		return upper, true
	}
	return "", false
}

// parseQuery parses a pre-checked token stream into a statement. Parameter
// placeholders are numbered in appearance order; LIMIT/OFFSET must be
// non-negative integer literals and are bounded here.
func parseQuery(tokens []token) (*query, error) {
	p := &parser{tokens: tokens}
	stmt := &query{}

	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	stmt.distinct = p.acceptKeyword("DISTINCT")

	for {
		term, err := p.parseSelectTerm()
		if err != nil {
			return nil, err
		}
		stmt.selectList = append(stmt.selectList, term)
		if !p.acceptPunct(",") {
			break
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	from, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt.from = from
	for p.atKeyword("LEFT") || p.atKeyword("INNER") || p.atKeyword("JOIN") {
		leftJoin := false
		if p.acceptKeyword("LEFT") {
			leftJoin = true
			if err := p.expectKeyword("JOIN"); err != nil {
				return nil, err
			}
		} else if p.acceptKeyword("INNER") {
			if err := p.expectKeyword("JOIN"); err != nil {
				return nil, err
			}
		} else {
			if err := p.expectKeyword("JOIN"); err != nil {
				return nil, err
			}
		}
		table, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("ON"); err != nil {
			return nil, err
		}
		on, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		stmt.joins = append(stmt.joins, joinClause{leftJoin: leftJoin, table: table, on: on})
	}
	if p.acceptKeyword("WHERE") {
		where, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		stmt.where = where
	}
	if p.acceptKeyword("GROUP") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			ref, err := p.parseColumnRef()
			if err != nil {
				return nil, err
			}
			stmt.groupBy = append(stmt.groupBy, ref)
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	if p.acceptKeyword("HAVING") {
		having, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		stmt.having = having
	}
	if p.acceptKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			term, err := p.parseOrderTerm()
			if err != nil {
				return nil, err
			}
			stmt.orderBy = append(stmt.orderBy, term)
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	if p.acceptKeyword("LIMIT") {
		limit, err := p.parseIntegerLiteral("LIMIT")
		if err != nil {
			return nil, err
		}
		if limit > maxLimitRows {
			return nil, rejectQuery(fmt.Sprintf("LIMIT exceeds the maximum of %d", maxLimitRows))
		}
		stmt.limitSet = true
		stmt.limit = limit
	}
	if p.acceptKeyword("OFFSET") {
		offset, err := p.parseIntegerLiteral("OFFSET")
		if err != nil {
			return nil, err
		}
		if offset > maxLimitRows {
			return nil, rejectQuery(fmt.Sprintf("OFFSET exceeds the maximum of %d", maxLimitRows))
		}
		stmt.offset = offset
	}
	if p.peek().kind != tokenEOF {
		return nil, syntaxError()
	}
	stmt.paramCount = p.paramCount
	return stmt, nil
}

func (p *parser) parseIntegerLiteral(word string) (int64, error) {
	tok := p.next()
	if tok.kind != tokenNumber {
		return 0, rejectQuery(word + " must be a non-negative integer")
	}
	for _, c := range tok.text {
		if c < '0' || c > '9' {
			return 0, rejectQuery(word + " must be a non-negative integer")
		}
	}
	value, err := strconv.ParseInt(tok.text, 10, 64)
	if err != nil {
		return 0, rejectQuery(fmt.Sprintf("%s exceeds the maximum of %d", word, maxLimitRows))
	}
	return value, nil
}

func (p *parser) parseTableRef() (tableRef, error) {
	first := p.next()
	if first.kind != tokenIdent {
		return tableRef{}, syntaxError()
	}
	ref := tableRef{entity: first.text, alias: first.text}
	if p.peek().kind == tokenIdent {
		ref.alias = p.next().text
	}
	return ref, nil
}

func (p *parser) parseColumnRef() (columnRef, error) {
	first := p.next()
	if first.kind != tokenIdent {
		return columnRef{}, syntaxError()
	}
	if p.acceptPunct(".") {
		second := p.next()
		if second.kind != tokenIdent {
			return columnRef{}, syntaxError()
		}
		return columnRef{alias: first.text, name: second.text}, nil
	}
	return columnRef{name: first.text}, nil
}

func (p *parser) parseSelectTerm() (selectTerm, error) {
	tok := p.peek()
	if p.acceptPunct("*") {
		return selectTerm{isStar: true}, nil
	}
	if tok.kind != tokenIdent {
		return selectTerm{}, syntaxError()
	}
	// a.* or a.column
	if p.tokens[p.pos+1].kind == tokenPunct && p.tokens[p.pos+1].text == "." {
		aliasTok := p.next()
		p.next() // "."
		if p.acceptPunct("*") {
			return selectTerm{isStar: true, starAlias: aliasTok.text}, nil
		}
		nameTok := p.next()
		if nameTok.kind != tokenIdent {
			return selectTerm{}, syntaxError()
		}
		return selectTerm{col: columnRef{alias: aliasTok.text, name: nameTok.text}}, nil
	}
	identTok := p.next()
	if fn, ok := isAggregateWord(identTok.text); ok && p.atPunct("(") {
		return p.parseAggregateTail(fn)
	}
	return selectTerm{col: columnRef{name: identTok.text}}, nil
}

func (p *parser) parseAggregateTail(fn string) (selectTerm, error) {
	p.next() // "("
	if p.acceptPunct("*") {
		if err := p.expectPunct(")"); err != nil {
			return selectTerm{}, err
		}
		return selectTerm{isAgg: true, aggFn: fn, aggStar: true, aggRaw: "*"}, nil
	}
	ref, err := p.parseColumnRef()
	if err != nil {
		return selectTerm{}, err
	}
	if err := p.expectPunct(")"); err != nil {
		return selectTerm{}, err
	}
	return selectTerm{isAgg: true, aggFn: fn, aggArg: ref, aggRaw: ref.written()}, nil
}

func (p *parser) parseOrderTerm() (orderTerm, error) {
	tok := p.peek()
	if tok.kind != tokenIdent {
		return orderTerm{}, syntaxError()
	}
	var term orderTerm
	if fn, ok := isAggregateWord(tok.text); ok && p.tokens[p.pos+1].kind == tokenPunct && p.tokens[p.pos+1].text == "(" {
		p.next()
		agg, err := p.parseAggregateTail(fn)
		if err != nil {
			return orderTerm{}, err
		}
		term = orderTerm{isAgg: true, aggFn: agg.aggFn, aggStar: agg.aggStar, aggArg: agg.aggArg, aggRaw: agg.aggRaw}
	} else {
		ref, err := p.parseColumnRef()
		if err != nil {
			return orderTerm{}, err
		}
		term = orderTerm{col: ref}
	}
	if p.acceptKeyword("ASC") {
		term.desc = false
	} else if p.acceptKeyword("DESC") {
		term.desc = true
	}
	return term, nil
}

func (p *parser) parseOr() (expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = boolExpr{op: "OR", left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("AND") {
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = boolExpr{op: "AND", left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (expr, error) {
	if p.acceptKeyword("NOT") {
		arg, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notExpr{arg: arg}, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (expr, error) {
	left, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	tok := p.peek()
	if tok.kind == tokenOperator {
		p.next()
		right, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		return cmpExpr{op: tok.text, left: left, right: right}, nil
	}
	return left, nil
}

func (p *parser) parseAtom() (expr, error) {
	tok := p.peek()
	switch {
	case tok.kind == tokenParam:
		p.next()
		expr := paramExpr{index: p.paramCount}
		p.paramCount++
		return expr, nil
	case tok.kind == tokenNumber:
		p.next()
		return litExpr{value: json.Number(tok.text)}, nil
	case tok.kind == tokenString:
		p.next()
		return litExpr{value: tok.text}, nil
	case tok.kind == tokenPunct && tok.text == "(":
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if err := p.expectPunct(")"); err != nil {
			return nil, err
		}
		return inner, nil
	case tok.kind == tokenKeyword && tok.text == "NULL":
		p.next()
		return litExpr{value: nil}, nil
	case tok.kind == tokenKeyword && tok.text == "TRUE":
		p.next()
		return litExpr{value: true}, nil
	case tok.kind == tokenKeyword && tok.text == "FALSE":
		p.next()
		return litExpr{value: false}, nil
	case tok.kind == tokenIdent:
		if fn, ok := isAggregateWord(tok.text); ok && p.tokens[p.pos+1].kind == tokenPunct && p.tokens[p.pos+1].text == "(" {
			p.next()
			agg, err := p.parseAggregateTail(fn)
			if err != nil {
				return nil, err
			}
			return &aggExpr{fn: agg.aggFn, star: agg.aggStar, ref: agg.aggArg, id: -1}, nil
		}
		ref, err := p.parseColumnRef()
		if err != nil {
			return nil, err
		}
		return &colExpr{ref: ref, src: -1, fld: -1, slot: -1}, nil
	default:
		return nil, syntaxError()
	}
}
