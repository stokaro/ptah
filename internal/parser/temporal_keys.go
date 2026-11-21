package parser

import (
	"errors"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
)

// The temporal key clauses of PostgreSQL 18: WITHOUT OVERLAPS after the last
// column of a PRIMARY KEY or UNIQUE, and PERIOD before the last column of both
// lists of a FOREIGN KEY. They are read on every dialect and recorded on the
// node, as NULLS [NOT] DISTINCT is; a render decides them by the target's
// [capability.TemporalKeys] (stokaro/ptah#6330).

var (
	errWithoutOverlapsNotLast = errors.New("WITHOUT OVERLAPS applies to the last column of a key")
	errPeriodNotLast          = errors.New("PERIOD applies to the last column of a foreign key column list")
	errPeriodOneSide          = errors.New("PERIOD must be written before the last column of both the referencing and the referenced column list")
	errPeriodOnColumn         = errors.New("PERIOD belongs to a table-level FOREIGN KEY, where both column lists can carry it")
)

// readWithoutOverlaps reads an optional WITHOUT OVERLAPS after a key column.
// It answers whether the clause was there.
func (p *Parser) readWithoutOverlaps() bool {
	if p.current.Type != lexer.TokenIdentifier || !p.current.MatchIdentifierValue("WITHOUT") {
		return false
	}
	next := p.peekSignificantToken()
	if next.Type != lexer.TokenIdentifier || !next.MatchIdentifierValue("OVERLAPS") {
		return false
	}
	p.advance()
	p.skipWhitespace()
	p.advance()
	p.skipWhitespace()
	return true
}

// readPeriodMarker reads an optional PERIOD before a foreign key column. It
// answers whether the marker was there. PERIOD is also an ordinary column
// name, so it is the marker only where a column name follows it: a bare
// `period` ahead of a comma or a parenthesis is the column.
func (p *Parser) readPeriodMarker() bool {
	if p.current.Type != lexer.TokenIdentifier || !p.current.MatchIdentifierValue("PERIOD") {
		return false
	}
	next := p.peekSignificantToken()
	if next.Type != lexer.TokenIdentifier && !isDoubleQuotedIdentifierToken(next) {
		return false
	}
	p.advance()
	p.skipWhitespace()
	return true
}

// refuseKeyTail refuses a list that goes on after the column a temporal clause
// qualified: both clauses belong to the last column.
func (p *Parser) refuseKeyTail(err error) error {
	if p.current.MatchOperatorValue(",") {
		return ptaherr.At(p.current.Start, err)
	}
	return nil
}

// checkPeriodSides refuses a foreign key that writes PERIOD on one of its two
// column lists only: [Parser.localPeriod] records the referencing list.
func (p *Parser) checkPeriodSides(reference *ast.ForeignKeyRef, at int) error {
	if reference == nil || p.localPeriod == reference.Period {
		return nil
	}
	return ptaherr.At(at, errPeriodOneSide)
}
