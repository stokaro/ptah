package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/lexer"
)

// parseAlterColumnAddIdentity reads `ADD GENERATED {ALWAYS | BY DEFAULT} AS
// IDENTITY [(options)]` after ALTER COLUMN name, with GENERATED current, as
// the identity clause of a column definition is read.
//
// pg_dump writes an identity column this way: the table first, with the
// column NOT NULL and no identity, then this statement, which names the
// sequence the column owns and every option of it. It is read where ALTER
// COLUMN adds an identity, as the descriptor's Comparison.IdentityColumns
// states, and by a document of no dialect.
func (p *Parser) parseAlterColumnAddIdentity(columnName string) (ast.AlterOperation, error) {
	start := p.current.Start
	identity := &ast.ColumnNode{Name: columnName}
	if err := p.handleGenerated(identity); err != nil {
		return nil, err
	}
	if identity.IdentityGeneration == "" {
		return nil, ptaherr.At(start, fmt.Errorf("expected AS IDENTITY after ALTER COLUMN %s ADD GENERATED", columnName))
	}
	return &ast.AlterColumnOperation{
		ColumnName:         columnName,
		Action:             ast.AlterColumnAddIdentity,
		IdentityGeneration: identity.IdentityGeneration,
		IdentityStart:      identity.IdentityStart,
		IdentityIncrement:  identity.IdentityIncrement,
		IdentityOptions:    identity.IdentityOptions,
	}, nil
}

// refuseReplicaIdentity refuses `ALTER TABLE ... REPLICA IDENTITY`, with
// REPLICA current, naming the statement as written.
//
// pg_dump writes it for a table whose replica identity is not the default,
// and has no option that leaves it out. The schema model has no place for it:
// no table of the model or of a catalog read carries a replica identity, and
// the PostgreSQL reader does not read pg_class.relreplident. Read and dropped,
// the statement would declare something no plan creates and no drift check
// compares. The words are PostgreSQL's alone, so the refusal says the same on
// every dialect.
func (p *Parser) refuseReplicaIdentity(table string) error {
	start, end := p.current.Start, p.current.End
	for !p.isAtEnd() && p.current.Type != lexer.TokenSemicolon && !p.current.MatchOperatorValue(",") {
		end = p.current.End
		p.advance()
	}
	clause := strings.Join(strings.Fields(p.input[start:end]), " ")
	return ptaherr.At(start, fmt.Errorf(
		"unsupported ALTER TABLE %s %s: Ptah does not model a table's replica identity; delete the statement "+
			"from the schema file, and set the replica identity in a migration where a database needs it",
		table, clause))
}
