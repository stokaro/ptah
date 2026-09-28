package parser

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
)

// CockroachDB hides an index from the optimizer with a clause at the end of the
// index: last in CREATE INDEX, after the key parts, STORING or INCLUDE, and
// WHERE, and after the parts of an index a CREATE TABLE declares. Measured on
// v26.3.2:
//
//	CREATE INDEX k ON t (a) NOT VISIBLE                  hidden
//	CREATE INDEX k ON t (a) INVISIBLE                    hidden; MySQL's word
//	CREATE INDEX k ON t (a) VISIBLE                      shown
//	CREATE INDEX k ON t (a) WHERE a > 0 NOT VISIBLE      hidden, condition a > 0
//	CREATE INDEX k ON t (a) NOT VISIBLE WHERE a > 0      syntax error
//	CREATE INDEX k ON t (a) WHERE NOT visible            shown, condition NOT visible
//	CREATE INDEX k ON t (a) VISIBILITY 0.5               shown to half the queries
//	ALTER INDEX t@k NOT VISIBLE                          hides k
//	ALTER TABLE t ALTER INDEX k NOT VISIBLE              syntax error
//
// VISIBILITY 0.0 and 1.0 print back as NOT VISIBLE and as nothing, so they are
// read as the two states. Any other share is refused: the model holds one of
// the two, and either reading moves the index on the next apply.

// cockroachVisibilityAt reads a visibility clause at the cursor. It answers
// whether one was there, and leaves the cursor where it was when not.
func (p *Parser) cockroachVisibilityAt() (read bool, err error) {
	p.skipWhitespace()
	start := p.current.Start
	switch {
	case p.current.MatchIdentifierValue("NOT") && p.nextIsWord("VISIBLE"):
		p.advance()
		p.skipWhitespace()
		p.advance()
		p.keyOptions.invisible = true
	case p.current.MatchIdentifierValue("INVISIBLE"), p.current.MatchIdentifierValue("VISIBLE"):
		p.keyOptions.invisible = strings.EqualFold(p.current.Value, "INVISIBLE")
		p.advance()
	case p.current.MatchIdentifierValue("VISIBILITY"):
		p.advance()
		p.skipWhitespace()
		invisible, err := visibilityShare(p.current.Value, start)
		if err != nil {
			return false, err
		}
		p.advance()
		p.keyOptions.invisible = invisible
	default:
		return false, nil
	}
	return true, nil
}

// readTableElementOptions reads the options after the parts of a CREATE TABLE
// element: the MySQL family's (see [Parser.readKeyOptions]), and the
// visibility clause that may end a CockroachDB index. Any other dialect, and
// any other element, reads no visibility here; the table element reports what
// it cannot read.
//
// CREATE INDEX does not come here: CockroachDB takes the clause there after
// INCLUDE and WHERE, so reading it after the parts would accept `NOT VISIBLE
// WHERE ...`, which the server refuses.
func (p *Parser) readTableElementOptions(kind, prefix string) error {
	if err := p.readKeyOptions(kind, prefix); err != nil {
		return err
	}
	if p.dialect != platform.CockroachDB || kind != indexElement {
		return nil
	}
	_, err := p.cockroachVisibilityAt()
	return err
}

// visibilityShare reads the number after VISIBILITY. It answers whether the
// index is hidden, for the two shares that are one of the two states.
func visibilityShare(value string, start int) (bool, error) {
	share, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	switch {
	case err == nil && share == 0:
		return true, nil
	case err == nil && share == 1:
		return false, nil
	}
	return false, fmt.Errorf("VISIBILITY %s at position %d: a partially visible index is not modeled; "+
		"write VISIBLE or NOT VISIBLE", value, start)
}

// splitCockroachConditionVisibility separates a visibility clause from the end
// of a CREATE INDEX condition on CockroachDB, whose WHERE runs to the end of the
// statement. tokens are the condition's tokens without whitespace.
//
// The words are a clause only when the token before them can end an
// expression: `WHERE (visible) NOT VISIBLE` hides the index, and `WHERE NOT
// visible` and `WHERE a AND visible` are conditions on a column named visible.
// It answers how many tokens the condition keeps.
func (p *Parser) splitCockroachConditionVisibility(tokens []lexer.Token) (int, error) {
	n := len(tokens)
	word := func(i int) string { return strings.ToUpper(tokens[i].Value) }
	isWord := func(i int, want string) bool {
		return tokens[i].Type == lexer.TokenIdentifier && word(i) == want
	}
	clauseLength := 0
	switch {
	case n >= 3 && isWord(n-2, "NOT") && isWord(n-1, "VISIBLE"):
		clauseLength = 2
	case n >= 3 && isWord(n-2, "VISIBILITY"):
		clauseLength = 2
	case n >= 2 && (isWord(n-1, "VISIBLE") || isWord(n-1, "INVISIBLE")):
		clauseLength = 1
	}
	if clauseLength == 0 || !endsOperand(tokens[n-clauseLength-1]) {
		return n, nil
	}
	first := n - clauseLength
	switch word(first) {
	case "VISIBILITY":
		invisible, err := visibilityShare(tokens[n-1].Value, tokens[first].Start)
		if err != nil {
			return 0, err
		}
		p.keyOptions.invisible = invisible
	default:
		p.keyOptions.invisible = word(first) != "VISIBLE"
	}
	return first, nil
}

// expressionOperators are the words after which an expression continues, so
// a word that follows one is an operand rather than a clause.
var expressionOperators = map[string]struct{}{
	"AND": {}, "OR": {}, "NOT": {}, "IS": {}, "WHERE": {}, "LIKE": {}, "ILIKE": {},
	"SIMILAR": {}, "IN": {}, "BETWEEN": {}, "ESCAPE": {}, "CASE": {}, "WHEN": {},
	"THEN": {}, "ELSE": {}, "DISTINCT": {}, "FROM": {}, "ANY": {}, "ALL": {},
	"SOME": {}, "EXISTS": {}, "ARRAY": {},
}

// endsOperand reports whether token can be the last token of an expression.
func endsOperand(token lexer.Token) bool {
	switch token.Type {
	case lexer.TokenString:
		return true
	case lexer.TokenOperator:
		return token.Value == ")" || token.Value == "]"
	case lexer.TokenIdentifier:
		_, operator := expressionOperators[strings.ToUpper(token.Value)]
		return !operator
	default:
		return false
	}
}

// parseCockroachAlterIndex reads CockroachDB's `ALTER INDEX table@index
// VISIBLE | NOT VISIBLE | INVISIBLE`, with ALTER consumed and INDEX current,
// as the ALTER TABLE operation the MySQL family spells for the same change.
//
// The index is named through its table, the way CockroachDB addresses one and
// the way Ptah writes the statement. A bare index name is refused: the schema
// file names no table for it, and the change belongs to one.
func (p *Parser) parseCockroachAlterIndex() (ast.Node, error) {
	p.advance()
	p.skipWhitespace()
	start := p.current.Start
	table, err := p.parseQualifiedIdentifier("table name")
	if err != nil {
		return nil, fmt.Errorf("ALTER INDEX at position %d: %w", start, err)
	}
	p.skipWhitespace()
	if !p.current.MatchOperatorValue("@") {
		return nil, fmt.Errorf("ALTER INDEX %s at position %d: name the index through its table, as table@index",
			table, start)
	}
	p.advance()
	index, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected the index name after ALTER INDEX %s@: %w", table, err)
	}
	p.keyOptions = keyOptions{}
	read, err := p.cockroachVisibilityAt()
	if err != nil {
		return nil, err
	}
	if !read {
		return nil, fmt.Errorf(
			"ALTER INDEX %s@%s %s at position %d: only VISIBLE and NOT VISIBLE are read; "+
				"an index's other settings are not part of a schema file",
			table, index, p.current.Value, p.current.Start)
	}
	return &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.AlterIndexVisibilityOperation{
			IndexName: index, Invisible: p.keyOptions.invisible,
		}},
	}, nil
}
