package parser

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
)

// parseAlterIndex reads PostgreSQL's `ALTER INDEX [IF EXISTS] name RENAME TO
// new_name` and `ALTER INDEX name ATTACH PARTITION partition_index`, with ALTER
// consumed and INDEX current.
//
// pg_dump writes ATTACH PARTITION for each index of a partition that copies an
// index of its parent. The other actions -- SET TABLESPACE, SET and RESET of
// storage parameters, ALTER COLUMN ... SET STATISTICS, DEPENDS ON EXTENSION,
// and ALTER INDEX ALL IN TABLESPACE -- change nothing a schema model holds, so
// a schema file that states one is refused by name rather than read as nothing.
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

	if p.current.MatchIdentifierValue("ATTACH") {
		if node.IfExists {
			return nil, ptaherr.At(p.current.Start, fmt.Errorf(
				"ALTER INDEX IF EXISTS %s ATTACH PARTITION: PostgreSQL takes no IF EXISTS here", name))
		}
		return p.parseAttachIndexPartition(node)
	}
	if !p.current.MatchIdentifierValue("RENAME") {
		return nil, ptaherr.At(p.current.Start, fmt.Errorf(
			"ALTER INDEX %s %s: only RENAME TO and ATTACH PARTITION are read; an index's other settings are not part of a schema file",
			name, p.current.Value))
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
		return nil, ptaherr.At(p.current.Start, fmt.Errorf(
			"ALTER INDEX %s RENAME TO %s.: the new name is bare, because the index stays in its schema",
			name, newName))
	}
	node.NewName = newName
	return node, nil
}

// parseAttachIndexPartition reads `ATTACH PARTITION partition_index` after the
// index name, with ATTACH current. PostgreSQL takes no IF EXISTS here.
func (p *Parser) parseAttachIndexPartition(node *ast.AlterIndexNode) (*ast.AlterIndexNode, error) {
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "PARTITION"); err != nil {
		return nil, fmt.Errorf("expected PARTITION after ALTER INDEX %s ATTACH: %w", node.Name, err)
	}
	p.skipWhitespace()
	partition, err := p.parseQualifiedIdentifier("partition index name")
	if err != nil {
		return nil, err
	}
	node.AttachPartition = partition
	return node, nil
}
