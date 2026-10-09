package query

import "fmt"

type tokenKind int

const (
	tokenEOF tokenKind = iota
	tokenIdent
	tokenKeyword
	tokenNumber
	tokenString
	tokenOperator
	tokenPunct
	tokenParam
)

type token struct {
	kind tokenKind
	text string // ident: as written; keyword: upper case; operator/punct: symbol
	pos  int    // byte offset, used only for syntax diagnostics
}

// parserKeywords are the words the grammar itself understands. They are
// recognised case-insensitively and always stored upper cased.
var parserKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "GROUP": true, "BY": true,
	"HAVING": true, "ORDER": true, "LIMIT": true, "OFFSET": true,
	"DISTINCT": true, "JOIN": true, "INNER": true, "LEFT": true, "ON": true,
	"AND": true, "OR": true, "NOT": true, "NULL": true, "TRUE": true,
	"FALSE": true, "ASC": true, "DESC": true,
}

// forbiddenWords are rejected anywhere in the statement (see the spec
// surface). They are matched on identifiers too, so a column literally
// named "window" or "like" is refused rather than silently misparsed.
var forbiddenWords = map[string]bool{
	"UNION": true, "EXCEPT": true, "INTERSECT": true, "INSERT": true,
	"UPDATE": true, "DELETE": true, "DROP": true, "ALTER": true,
	"PRAGMA": true, "ATTACH": true, "CREATE": true, "REPLACE": true,
	"INTO": true, "VALUES": true, "TABLE": true, "WITH": true,
	"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"WINDOW": true, "OVER": true, "LIKE": true, "IN": true,
	"BETWEEN": true, "IS": true, "EXISTS": true, "AS": true,
}

var aggregateNames = map[string]bool{
	"COUNT": true, "SUM": true, "MIN": true, "MAX": true, "AVG": true,
}

// precheckTokens runs the statement-shape gate before parsing: exactly one
// statement, starting with SELECT, with no set operations, DML, DDL or
// subqueries anywhere. Rejection messages name keywords only.
func precheckTokens(tokens []token) error {
	if len(tokens) == 0 || tokens[0].kind != tokenKeyword || tokens[0].text != "SELECT" {
		return rejectQuery("query must start with SELECT")
	}
	for _, tok := range tokens[1:] {
		switch {
		case tok.kind == tokenPunct && tok.text == ";":
			return rejectQuery("multiple statements are not allowed")
		case tok.kind == tokenKeyword && tok.text == "SELECT":
			return rejectQuery("subqueries are not allowed")
		case forbiddenWords[upperOf(tok)]:
			return rejectQuery(fmt.Sprintf("keyword %s is not allowed", upperOf(tok)))
		}
	}
	return nil
}

func upperOf(tok token) string {
	if tok.kind == tokenKeyword {
		return tok.text
	}
	if tok.kind == tokenIdent {
		return toUpper(tok.text)
	}
	return ""
}

func toUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
	}
	return string(b)
}

func isIdentifierStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentifierPart(c byte) bool {
	return isIdentifierStart(c) || (c >= '0' && c <= '9')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// lexQuery turns a statement into tokens. Strings never surface in error
// messages: a lexical problem reports only its byte position.
func lexQuery(sql string) ([]token, error) {
	tokens := make([]token, 0, 16)
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case isIdentifierStart(c):
			start := i
			for i < len(sql) && isIdentifierPart(sql[i]) {
				i++
			}
			word := sql[start:i]
			if parserKeywords[toUpper(word)] {
				tokens = append(tokens, token{kind: tokenKeyword, text: toUpper(word), pos: start})
			} else {
				tokens = append(tokens, token{kind: tokenIdent, text: word, pos: start})
			}
		case isDigit(c):
			start := i
			for i < len(sql) && isDigit(sql[i]) {
				i++
			}
			if i+1 < len(sql) && sql[i] == '.' && isDigit(sql[i+1]) {
				i++
				for i < len(sql) && isDigit(sql[i]) {
					i++
				}
			}
			if i < len(sql) && (sql[i] == 'e' || sql[i] == 'E') {
				expStart := i
				i++
				if i < len(sql) && (sql[i] == '+' || sql[i] == '-') {
					i++
				}
				if i < len(sql) && isDigit(sql[i]) {
					for i < len(sql) && isDigit(sql[i]) {
						i++
					}
				} else {
					i = expStart
				}
			}
			tokens = append(tokens, token{kind: tokenNumber, text: sql[start:i], pos: start})
		case c == '\'':
			start := i
			i++
			var value []byte
			closed := false
			for i < len(sql) {
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						value = append(value, '\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				value = append(value, sql[i])
				i++
			}
			if !closed {
				return nil, rejectQuery(fmt.Sprintf("syntax error at byte %d: unterminated string literal", start))
			}
			tokens = append(tokens, token{kind: tokenString, text: string(value), pos: start})
		case c == '?':
			tokens = append(tokens, token{kind: tokenParam, text: "?", pos: i})
			i++
		case c == '=' || c == '<' || c == '>':
			start := i
			op := string(c)
			if c == '<' && i+1 < len(sql) && (sql[i+1] == '>' || sql[i+1] == '=') {
				op = sql[i : i+2]
				i += 2
			} else if c == '>' && i+1 < len(sql) && sql[i+1] == '=' {
				op = sql[i : i+2]
				i += 2
			} else {
				i++
			}
			tokens = append(tokens, token{kind: tokenOperator, text: op, pos: start})
		case c == '!' && i+1 < len(sql) && sql[i+1] == '=':
			tokens = append(tokens, token{kind: tokenOperator, text: "!=", pos: i})
			i += 2
		case c == '(' || c == ')' || c == ',' || c == '*' || c == '.' || c == ';' || c == '-':
			tokens = append(tokens, token{kind: tokenPunct, text: string(c), pos: i})
			i++
		default:
			return nil, rejectQuery(fmt.Sprintf("syntax error at byte %d: unexpected character", i))
		}
	}
	tokens = append(tokens, token{kind: tokenEOF, text: "", pos: len(sql)})
	return tokens, nil
}
