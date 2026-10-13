package parser

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

// constraintCommentNode gives a table constraint's comment an executable AST
// operation. Other targets retain their source form for the schema reader.
func constraintCommentNode(target, text, literal string) (ast.Node, error) {
	source := ast.NewComment("COMMENT ON " + target + " IS " + literal)
	p := NewParser(target)
	if !p.current.MatchIdentifierValue("CONSTRAINT") {
		return source, nil
	}
	p.advance()
	p.skipWhitespace()
	name, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("COMMENT ON CONSTRAINT: %w", err)
	}
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "ON"); err != nil {
		return nil, fmt.Errorf("COMMENT ON CONSTRAINT requires ON: %w", err)
	}
	p.skipWhitespace()
	if p.current.MatchIdentifierValue("DOMAIN") {
		return source, nil
	}
	table, err := p.parseQualifiedIdentifier("constraint's table")
	if err != nil {
		return nil, fmt.Errorf("COMMENT ON CONSTRAINT: %w", err)
	}
	p.skipWhitespace()
	if !p.isAtEnd() {
		return nil, fmt.Errorf("COMMENT ON CONSTRAINT has an unexpected token %q", p.current.Value)
	}
	return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{&ast.SetConstraintCommentOperation{Constraint: name, Comment: text}}}, nil
}
