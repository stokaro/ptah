package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/lexer"
)

// parseAlterSequence reads ALTER SEQUENCE with ALTER consumed and SEQUENCE
// current: `ALTER SEQUENCE name OWNED BY table.column` and `OWNED BY NONE`,
// into an [ast.AlterSequenceNode] that carries the owner, and `OWNER TO` as
// [Parser.parseAlterOwnerTo] reads it.
//
// pg_dump writes OWNED BY for the sequence of every serial column, after the
// table and the sequence, so a dump that is read has to take it. It is read
// where the dialect's columns own sequences, as the descriptor's
// Conversion.OwnedSequences states, and by a document of no dialect. Every
// other action keeps its refusal, as on a dialect without owned sequences.
func (p *Parser) parseAlterSequence() (ast.Node, error) {
	_, after, named := p.namedWords(1, 1)
	ownedBy := named && len(after) == 1 && strings.EqualFold(after[0], "OWNED")
	if !ownedBy || (p.target.known && !p.target.ownedSequences) {
		return p.parseAlterOwnerTo("SEQUENCE")
	}
	p.advance()
	p.skipWhitespace()
	schema, name, err := p.parseSchemaQualifiedName("sequence name")
	if err != nil {
		return nil, err
	}
	p.skipWhitespace()
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "BY"); err != nil {
		return nil, fmt.Errorf("expected BY after ALTER SEQUENCE %s OWNED: %w", name, err)
	}
	p.skipWhitespace()
	owner, err := p.parseQualifiedIdentifier("OWNED BY target")
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(owner, "NONE") {
		owner = "NONE"
	}
	if err := p.endStatement("ALTER SEQUENCE OWNED BY"); err != nil {
		return nil, err
	}
	return ast.NewAlterSequence(name).SetSchema(schema).SetOwnedBy(owner), nil
}
