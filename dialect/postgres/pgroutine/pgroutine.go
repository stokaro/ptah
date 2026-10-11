// Package pgroutine reads PostgreSQL routine bodies for the hosts that analyze
// them, as the routine body contract ptah.run/core/routineext describes: what a
// body writes and reads, and where it builds SQL at run time.
//
// A PL/pgSQL body is split into its statements: an optional DECLARE section,
// then the statements of the outer block, each IF, CASE, loop, nested block and
// EXCEPTION section carrying the statements it runs. A body in any other
// language is one statement, its whole text. The bundled runtime
// ptah.run/engine/builtin registers [Reader] on every target of the PostgreSQL
// family.
package pgroutine

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
)

// Language is the language of a PostgreSQL routine that declares none, which
// is the language the server assumes for one written by hand.
const Language = "plpgsql"

// Security is the security mode of a PostgreSQL routine that states none, as
// the reader reports it: the routine runs with its caller's rights.
const Security = "INVOKER"

// Reader reads the routine bodies of the PostgreSQL family: postgres,
// cockroachdb, yugabytedb and spanner. Its zero value is ready to use.
type Reader struct{}

// ReadBodies reads every body of the request. PostgreSQL resolves a name in a
// body that a variable and a column share as an error unless the routine says
// otherwise (plpgsql.variable_conflict), so every reading reports column reads
// as attributable. A target outside the family wraps
// ptaherr.ErrUnsupportedDialect, and cancellation returns the context's error;
// either returns no result.
func (Reader) ReadBodies(ctx context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	if ctx == nil {
		return routineext.ReadResult{}, fmt.Errorf("%w: reading routine bodies requires a context", routineext.ErrInvalid)
	}
	if !platform.IsPostgresFamily(request.Target) {
		return routineext.ReadResult{}, fmt.Errorf("%w: the PostgreSQL routine body reader on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := routineext.ReadResult{Bodies: make([]routineext.Reading, 0, len(request.Bodies))}
	for _, body := range request.Bodies {
		result.Bodies = append(result.Bodies, routineext.Reading{Statements: read(body), ColumnReads: true})
	}
	if err := ctx.Err(); err != nil {
		return routineext.ReadResult{}, err
	}
	return result, nil
}

// read splits one body: a PL/pgSQL body into its statements, and a body in any
// other language into one statement holding all of it.
func read(body routineext.Body) []routineext.Statement {
	if strings.EqualFold(body.Language, "plpgsql") {
		return contract(parsePLpgSQL(body.SQL))
	}
	if sql := strings.TrimSpace(body.SQL); sql != "" {
		return []routineext.Statement{{Kind: routineext.StatementSQL, Label: string(plRaw), SQL: sql}}
	}
	return nil
}

// contract states PL/pgSQL statements in the contract's terms.
//
// Only a control-flow statement carries statements. The splitter finds a THEN
// inside a CASE expression too, so a SELECT, RETURN or PERFORM that holds one
// comes back with fragments of the expression as its contents. Those fragments
// are not statements: read as such, a SELECT with a CASE column lost the reads
// of its FROM, because a host took the fragments in place of the SELECT.
func contract(parsed []plStatement) []routineext.Statement {
	statements := make([]routineext.Statement, 0, len(parsed))
	for _, statement := range parsed {
		converted := routineext.Statement{Label: string(statement.kind), SQL: statement.sql}
		switch statement.kind {
		case plBlock, plException, plIf, plCase, plLoop:
			converted.Kind = routineext.StatementControl
			converted.Opaque = len(statement.statements) == 0
			if !converted.Opaque {
				converted.Statements = contract(statement.statements)
			}
		case plDeclaration, plReturn, plRaise, plPerform:
			converted.Kind = routineext.StatementInert
		case plExecute:
			converted.Kind = routineext.StatementDynamic
		default:
			converted.Kind = routineext.StatementSQL
		}
		statements = append(statements, converted)
	}
	return statements
}
