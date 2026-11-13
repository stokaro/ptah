package parser

import (
	"fmt"
	"slices"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
)

// readsOwnerTo reports whether the document's dialect reads `ALTER kind name
// OWNER TO role`. A document of no dialect takes any engine's spelling.
func (p *Parser) readsOwnerTo(kind string) bool {
	return !p.target.known || slices.Contains(p.target.grammar.Forms.OwnerTo, kind)
}

// parseAlterOwnerTo reads `ALTER kind name [(arguments)] OWNER TO role` for a
// kind other than a table, with ALTER consumed and the kind's first word
// current. It contributes no node.
//
// pg_dump writes one such statement after every object it dumps, unless it is
// run with --no-owner. Ptah renders no OWNER TO and keeps ownership for
// analysis only, so a desired schema has nothing to plan from the statement,
// and the same statement in a migration changes no object a rule models.
//
// Every other action on the kind keeps the refusal the kind had before it was
// read at all, so a RENAME TO or SET SCHEMA is not taken for ownership.
func (p *Parser) parseAlterOwnerTo(target string) error {
	start := p.current.Start
	kind := target
	if target == "MATERIALIZED" {
		if !p.nextIsWord("VIEW") {
			return p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
		}
		kind = descriptor.OwnerKindMaterializedView
	}
	if !p.readsOwnerTo(kind) {
		return p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
	}
	p.advance()
	if target == "MATERIALIZED" {
		p.skipWhitespace()
		p.advance()
	}
	p.skipWhitespace()

	if _, err := p.parseTableName("name"); err != nil {
		return p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
	}
	p.skipWhitespace()
	if kind == descriptor.OwnerKindFunction || kind == descriptor.OwnerKindProcedure {
		if p.current.MatchOperatorValue("(") {
			if _, err := p.collectParenthesizedBody("routine arguments"); err != nil {
				return err
			}
			p.skipWhitespace()
		}
	}
	if !p.current.MatchIdentifierValue("OWNER") {
		return p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
	}
	return p.readOwnerTo("ALTER " + kind)
}

// readOwnerTo reads `OWNER TO role` and the end of the statement, with OWNER
// current. The role is a name, CURRENT_ROLE, CURRENT_USER or SESSION_USER; it
// is read and dropped.
func (p *Parser) readOwnerTo(statement string) error {
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "TO"); err != nil {
		return fmt.Errorf("expected TO after %s OWNER: %w", statement, err)
	}
	p.skipWhitespace()
	if _, err := p.expectIdentifier(); err != nil {
		return ptaherr.At(p.current.Start, fmt.Errorf("expected the role after %s OWNER TO: %w", statement, err))
	}
	return p.endStatement(statement + " OWNER TO")
}
