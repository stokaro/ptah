// Package mssqlroutine reads SQL Server routine bodies for the hosts that
// analyze them, as the routine body contract ptah.run/core/routineext
// describes: what a body writes, what it reads, and where it builds SQL at run
// time.
//
// A body is split into the top-level statements of its outer BEGIN ... END, or
// of the body itself when it has none. The statements an IF, a WHILE or a
// nested block runs are not split out of it, so each of those is reported as
// opaque. The bundled runtime ptah.run/engine/builtin registers [Reader] on the
// sqlserver target.
package mssqlroutine

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
)

// Language is the language of every SQL Server routine. T-SQL has no LANGUAGE
// clause, so a routine declares none, and the reader reports this one.
const Language = "sql"

// Security is the security mode of a SQL Server routine that states none, as
// the reader reports it: the routine runs as EXECUTE AS CALLER, with its
// caller's rights.
const Security = "INVOKER"

// Volatility is the volatility the reader reports for every SQL Server
// routine. T-SQL has no volatility clause: the engine judges a function
// deterministic from its body and SCHEMABINDING, and OBJECTPROPERTY reports
// that verdict, which no declaration states. Reported as volatility, a
// schema-bound function would differ from its own declaration on every
// comparison.
const Volatility = "VOLATILE"

// Reader reads the routine bodies of SQL Server. Its zero value is ready to
// use.
type Reader struct{}

// ReadBodies reads every body of the request. A T-SQL variable carries an @
// prefix and cannot share a name with a column, so every reading reports its
// column reads as attributable. A target other than sqlserver wraps
// ptaherr.ErrUnsupportedDialect, and cancellation returns the context's
// error; either returns no result.
func (Reader) ReadBodies(ctx context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	if ctx == nil {
		return routineext.ReadResult{}, fmt.Errorf("%w: reading routine bodies requires a context", routineext.ErrInvalid)
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return routineext.ReadResult{}, fmt.Errorf("%w: the SQL Server routine body reader on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := routineext.ReadResult{Bodies: make([]routineext.Reading, 0, len(request.Bodies))}
	for _, body := range request.Bodies {
		result.Bodies = append(result.Bodies, routineext.Reading{Statements: contract(parseBody(body.SQL)), ColumnReads: true})
	}
	if err := ctx.Err(); err != nil {
		return routineext.ReadResult{}, err
	}
	return result, nil
}

// contract states the statements of a body in the contract's terms.
//
// A declaration, an assignment, a RETURN, a SELECT and BEGIN TRANSACTION
// write nothing. EXEC and EXECUTE that run a string, or sp_executesql, are
// dynamic; EXEC calling a procedure by name runs as written, and a host cannot
// see what it writes. A control-flow statement carries the statements it runs
// in its text alone, so it is opaque.
func contract(parsed []statement) []routineext.Statement {
	statements := make([]routineext.Statement, 0, len(parsed))
	for _, parsedStatement := range parsed {
		converted := routineext.Statement{Label: string(parsedStatement.kind), SQL: parsedStatement.sql}
		switch parsedStatement.kind {
		case kindRaw, kindInsert:
			converted.Kind = routineext.StatementSQL
		case kindExec, kindExecute:
			converted.Kind = routineext.StatementDynamic
		case kindDeclaration, kindAssignment, kindReturn, kindSelect, kindTransaction:
			converted.Kind = routineext.StatementInert
		default:
			converted.Kind, converted.Opaque = routineext.StatementControl, true
		}
		statements = append(statements, converted)
	}
	return statements
}
