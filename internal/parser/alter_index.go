package parser

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

// parseAlterIndex reads PostgreSQL's `ALTER INDEX [IF EXISTS] name RENAME TO
// new_name`, with ALTER consumed and INDEX current.
//
// RENAME TO is the one action read. The others -- SET TABLESPACE, SET and
// RESET of storage parameters, ATTACH PARTITION, ALTER COLUMN ... SET
// STATISTICS, DEPENDS ON EXTENSION, and ALTER INDEX ALL IN TABLESPACE -- change
// nothing a schema model holds, so a schema file that states one is refused by
// name rather than read as nothing.
func (p *Parser) parseAlterIndex() (*ast.AlterIndexNode, error) {
	if err := p.expect(lexer.TokenIdentifier, "INDEX"); err != nil {
		return nil, err
	}
	p.skipWhitespace()

	node := &ast.AlterIndexNode{}
	if p.current.MatchIdentifierValue("IF") {
		p.advance()
		p.skipWhitespace()
		if err := p.expect(lexer.TokenIdentifier, "EXISTS"); err != nil {
			return nil, fmt.Errorf("expected EXISTS after ALTER INDEX IF: %w", err)
		}
		p.skipWhitespace()
		node.IfExists = true
	}

	name, err := p.parseQualifiedIdentifier("index name")
	if err != nil {
		return nil, err
	}
	node.Name = name
	p.skipWhitespace()

	if !p.current.MatchIdentifierValue("RENAME") {
		return nil, fmt.Errorf(
			"ALTER INDEX %s %s at position %d: only RENAME TO is read; an index's other settings are not part of a schema file",
			name, p.current.Value, p.current.Start)
	}
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "TO"); err != nil {
		return nil, fmt.Errorf("expected TO after ALTER INDEX %s RENAME: %w", name, err)
	}
	p.skipWhitespace()
	newName, err := p.expectIdentifier()
	if err != nil {
		return nil, fmt.Errorf("expected the new name after ALTER INDEX %s RENAME TO: %w", name, err)
	}
	if p.current.MatchOperatorValue(".") {
		return nil, fmt.Errorf(
			"ALTER INDEX %s RENAME TO %s. at position %d: the new name is bare, because the index stays in its schema",
			name, newName, p.current.Start)
	}
	node.NewName = newName
	return node, nil
}
