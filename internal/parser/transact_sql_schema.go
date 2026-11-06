package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

// parseGuardedCreateSchema reads the statement Ptah writes for a schema on SQL
// Server, which has no CREATE SCHEMA IF NOT EXISTS and takes CREATE SCHEMA only as
// the first statement of a batch:
//
//	IF SCHEMA_ID('app') IS NULL
//	    EXEC('CREATE SCHEMA [app]');
//
// A description of a database writes it, so a schema file reads back what the
// description says. Any other IF is not schema DDL, and stays refused.
func (p *Parser) parseGuardedCreateSchema() (ast.Node, error) {
	unsupported := fmt.Errorf("unsupported SQL statement: IF at position %d", p.current.Start)
	if err := p.expectWords("IF", "SCHEMA_ID"); err != nil {
		return nil, unsupported
	}
	name, ok := p.parenthesizedString()
	if !ok {
		return nil, unsupported
	}
	if err := p.expectWords("IS", "NULL", "EXEC"); err != nil {
		return nil, unsupported
	}
	created, ok := p.parenthesizedString()
	if !ok || !strings.EqualFold(created, "CREATE SCHEMA ["+strings.ReplaceAll(name, "]", "]]")+"]") {
		return nil, unsupported
	}
	return &ast.CreateSchemaNode{Name: name, IfNotExists: true}, nil
}

// parenthesizedString consumes `('text')` or `(N'text')` and answers the text.
func (p *Parser) parenthesizedString() (string, bool) {
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return "", false
	}
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("N") {
		p.advance()
	}
	if p.current.Type != lexer.TokenString || !strings.HasPrefix(p.current.Value, "'") {
		return "", false
	}
	text := strings.ReplaceAll(strings.Trim(p.current.Value, "'"), "''", "'")
	p.advance()
	if err := p.expect(lexer.TokenOperator, ")"); err != nil {
		return "", false
	}
	return text, true
}
