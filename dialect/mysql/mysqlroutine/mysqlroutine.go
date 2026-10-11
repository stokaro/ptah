// Package mysqlroutine reads MySQL and MariaDB routine bodies for the hosts
// that analyze them, as the routine body contract ptah.run/core/routineext
// describes: what a body writes, and where it builds SQL at run time.
//
// A compound body is split into the top-level statements of its outer
// BEGIN ... END; a function's RETURN body is one statement. The statements a
// control-flow statement runs are not split out of it, so it is reported as
// opaque. The bundled runtime ptah.run/engine/builtin registers [Reader] on the
// mysql and mariadb targets.
package mysqlroutine

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
)

// Reader reads the routine bodies of MySQL and MariaDB. Its zero value is
// ready to use.
type Reader struct{}

// ReadBodies reads every body of the request. MySQL resolves a name a declared
// variable and a column share to the variable, so a name in a body does not
// say which it is, and no reading reports column reads as attributable. A
// target other than mysql or mariadb wraps ptaherr.ErrUnsupportedDialect, and
// cancellation returns the context's error; either returns no result.
func (Reader) ReadBodies(ctx context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	if ctx == nil {
		return routineext.ReadResult{}, fmt.Errorf("%w: reading routine bodies requires a context", routineext.ErrInvalid)
	}
	if target := platform.NormalizeDialect(request.Target); target != platform.MySQL && target != platform.MariaDB {
		return routineext.ReadResult{}, fmt.Errorf("%w: the MySQL routine body reader on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := routineext.ReadResult{Bodies: make([]routineext.Reading, 0, len(request.Bodies))}
	for _, body := range request.Bodies {
		result.Bodies = append(result.Bodies, routineext.Reading{Statements: contract(parseBody(body.SQL))})
	}
	if err := ctx.Err(); err != nil {
		return routineext.ReadResult{}, err
	}
	return result, nil
}

// contract states the statements of a body in the contract's terms.
//
// PREPARE builds a statement from text and EXECUTE runs what PREPARE built, so
// both are dynamic. Releasing a prepared statement writes nothing, and CLOSE is
// inert as OPEN and FETCH are. A control-flow statement, and a handler, carries
// the statements it runs in its text alone, so it is opaque.
func contract(parsed []statement) []routineext.Statement {
	statements := make([]routineext.Statement, 0, len(parsed))
	for _, parsedStatement := range parsed {
		converted := routineext.Statement{Label: string(parsedStatement.kind), SQL: parsedStatement.sql}
		switch parsedStatement.kind {
		case kindRaw, kindInsert, kindUpdate, kindDelete:
			converted.Kind = routineext.StatementSQL
		case kindPrepare, kindExecute:
			converted.Kind = routineext.StatementDynamic
		case kindDeclaration, kindCursor, kindReturn, kindSet, kindSelect, kindOpen, kindFetch, kindClose,
			kindLeave, kindIterate, kindDeallocate:
			converted.Kind = routineext.StatementInert
		default:
			converted.Kind, converted.Opaque = routineext.StatementControl, true
		}
		statements = append(statements, converted)
	}
	return statements
}
