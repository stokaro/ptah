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
// description says, the guarded comment that may follow it included; see
// [Parser.parseGuardedSchemaComment]. Any other IF is not schema DDL, and stays
// refused.
func (p *Parser) parseGuardedCreateSchema() (ast.Node, error) {
	unsupported := fmt.Errorf("unsupported SQL statement: IF at position %d", p.current.Start)
	if err := p.expectWords("IF"); err != nil {
		return nil, unsupported
	}
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("NOT") {
		return p.parseGuardedSchemaComment(unsupported)
	}
	if err := p.expectWords("SCHEMA_ID"); err != nil {
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

// parseGuardedSchemaComment reads what a description writes for the comment of a
// schema it may find holding one:
//
//	IF NOT EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 3 AND
//	    major_id = SCHEMA_ID('app') AND minor_id = 0 AND name = N'MS_Description')
//	    EXEC sp_addextendedproperty @name = N'MS_Description', ...;
//
// Any other condition stays refused, because the comment would then depend on
// something the model does not hold.
func (p *Parser) parseGuardedSchemaComment(unsupported error) (ast.Node, error) {
	if err := p.expectWords("NOT", "EXISTS"); err != nil {
		return nil, unsupported
	}
	if err := p.expect(lexer.TokenOperator, "("); err != nil {
		return nil, unsupported
	}
	start := p.current.Start
	for depth := 1; depth > 0; p.advance() {
		switch {
		case p.isAtEnd():
			return nil, unsupported
		case p.current.MatchOperatorValue("("):
			depth++
		case p.current.MatchOperatorValue(")"):
			depth--
		}
	}
	condition := strings.Join(strings.Fields(p.rawStatementFragment(start, p.previous.Start)), " ")
	p.skipWhitespace()
	schema, comment, ok := p.schemaCommentCall()
	if !ok || condition != schemaCommentAbsent(schema) {
		return nil, unsupported
	}
	return schemaCommentNode(schema, comment)
}

// parseSchemaCommentCall reads the statement that sets a schema's comment on SQL
// Server. Any other EXEC runs something the model does not hold, and stays refused.
func (p *Parser) parseSchemaCommentCall() (ast.Node, error) {
	unsupported := fmt.Errorf("unsupported SQL statement: EXEC at position %d", p.current.Start)
	schema, comment, ok := p.schemaCommentCall()
	if !ok {
		return nil, unsupported
	}
	return schemaCommentNode(schema, comment)
}

// schemaCommentCall consumes `EXEC sp_addextendedproperty` addressing a schema's
// MS_Description property, and answers the schema and the comment.
func (p *Parser) schemaCommentCall() (schema, comment string, ok bool) {
	if err := p.expectWords("EXEC", "sp_addextendedproperty"); err != nil {
		return "", "", false
	}
	arguments := make(map[string]string, 4)
	for {
		p.skipWhitespace()
		if err := p.expect(lexer.TokenOperator, "@"); err != nil {
			return "", "", false
		}
		name := strings.ToLower(p.current.Value)
		p.advance()
		if err := p.expect(lexer.TokenOperator, "="); err != nil {
			return "", "", false
		}
		p.skipWhitespace()
		if p.current.MatchIdentifierValue("N") {
			p.advance()
		}
		value, err := p.stringConstant("the extended property's value")
		if err != nil {
			return "", "", false
		}
		arguments[name] = value
		p.skipWhitespace()
		if !p.current.MatchOperatorValue(",") {
			break
		}
		p.advance()
	}
	if len(arguments) != 4 || arguments["name"] != "MS_Description" || arguments["level0type"] != "SCHEMA" {
		return "", "", false
	}
	return arguments["level0name"], arguments["value"], arguments["level0name"] != "" && arguments["value"] != ""
}

// schemaCommentAbsent is the condition a description guards a schema's comment
// with.
func schemaCommentAbsent(schema string) string {
	return "SELECT 1 FROM sys.extended_properties WHERE class = 3 AND major_id = SCHEMA_ID('" +
		strings.ReplaceAll(schema, "'", "''") + "') AND minor_id = 0 AND name = N'MS_Description'"
}

// schemaCommentNode is the COMMENT ON SCHEMA the statement means, which the model
// reads the way it reads the PostgreSQL family's.
func schemaCommentNode(schema, comment string) (ast.Node, error) {
	return constraintCommentNode("SCHEMA ["+strings.ReplaceAll(schema, "]", "]]")+"]", comment, quotedStringConstant(comment))
}
