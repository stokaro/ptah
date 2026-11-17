package parser

import (
	"fmt"
	"slices"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
)

// readsOwnerTo reports whether the document's dialect reads `ALTER kind name
// OWNER TO role`. A document of no dialect takes any engine's spelling.
func (p *Parser) readsOwnerTo(kind string) bool {
	return !p.target.known || slices.Contains(p.target.grammar.Forms.OwnerTo, kind)
}

// managesOwner reports whether the document's dialect manages the owner of an
// object of kind, so the statement that changes it is a node. A document of no
// dialect manages the kinds any engine does, and a target decides later.
func (p *Parser) managesOwner(kind string) bool {
	if !p.target.known {
		return kind == descriptor.OwnerKindView || kind == descriptor.OwnerKindMaterializedView
	}
	return slices.Contains(p.target.grammar.Forms.ManagedOwners, kind)
}

// parseAlterOwnerTo reads `ALTER kind name [(arguments)] OWNER TO role` for a
// kind other than a table, with ALTER consumed and the kind's first word
// current.
//
// For a kind the dialect manages the owner of
// ([descriptor.StatementForms.ManagedOwners]), the statement is an
// [ast.AlterOwnerNode] naming the object and the role as written. For any
// other kind it contributes no node: pg_dump writes one such statement after
// every object it dumps, unless it is run with --no-owner, and Ptah keeps the
// owner of that kind for analysis only, so a desired schema has nothing to plan
// from it and the same statement in a migration changes no object a rule
// models.
//
// Every other action on the kind keeps the refusal the kind had before it was
// read at all, so a RENAME TO or SET SCHEMA is not taken for ownership.
func (p *Parser) parseAlterOwnerTo(target string) (ast.Node, error) {
	start := p.current.Start
	kind := target
	if target == "MATERIALIZED" {
		if !p.nextIsWord("VIEW") {
			return nil, p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
		}
		kind = descriptor.OwnerKindMaterializedView
	}
	if !p.readsOwnerTo(kind) {
		return nil, p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
	}
	p.advance()
	if target == "MATERIALIZED" {
		p.skipWhitespace()
		p.advance()
	}
	p.skipWhitespace()

	name, err := p.parseTableName("name")
	if err != nil {
		return nil, p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
	}
	p.skipWhitespace()
	if kind == descriptor.OwnerKindFunction || kind == descriptor.OwnerKindProcedure {
		if p.current.MatchOperatorValue("(") {
			if _, err := p.collectParenthesizedBody("routine arguments"); err != nil {
				return nil, err
			}
			p.skipWhitespace()
		}
	}
	if !p.current.MatchIdentifierValue("OWNER") {
		return nil, p.refuseHead(start, "ALTER", target, "unsupported ALTER target: %s")
	}
	role, err := p.readOwnerTo("ALTER " + kind)
	if err != nil || !p.managesOwner(kind) {
		return nil, err
	}
	return ast.NewAlterOwner(kind, name, role), nil
}

// readOwnerTo reads `OWNER TO role` and the end of the statement, with OWNER
// current, and returns the role as written: a name, double-quoted or not, or
// CURRENT_ROLE, CURRENT_USER or SESSION_USER.
func (p *Parser) readOwnerTo(statement string) (string, error) {
	p.advance()
	p.skipWhitespace()
	if err := p.expect(lexer.TokenIdentifier, "TO"); err != nil {
		return "", fmt.Errorf("expected TO after %s OWNER: %w", statement, err)
	}
	p.skipWhitespace()
	role := p.current
	if !isRoleName(role) {
		return "", ptaherr.At(role.Start, fmt.Errorf("expected the role after %s OWNER TO, got %q", statement, role.Value))
	}
	p.advance()
	return role.Value, p.endStatement(statement + " OWNER TO")
}

// isRoleName reports a token PostgreSQL reads as the role of OWNER TO: a
// double-quoted name, or an unquoted one that starts with a letter or an
// underscore. CURRENT_USER, CURRENT_ROLE and SESSION_USER are such names. A
// number, or a name in SQL Server's brackets, is a syntax error on the server,
// and a reader that took it would accept a file the server refuses.
func isRoleName(token lexer.Token) bool {
	if isDoubleQuotedIdentifierToken(token) {
		return true
	}
	if token.Type != lexer.TokenIdentifier || token.Value == "" {
		return false
	}
	first, _ := utf8.DecodeRuneInString(token.Value)
	return first == '_' || unicode.IsLetter(first)
}
