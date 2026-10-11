package schemalineage

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
)

// deriveSQLServerWrites resolves the writes a T-SQL routine body performs.
//
// The T-SQL parser names insert and select and leaves update and delete in the
// raw kind, which the leading-word reader handles. It models no nesting, so a
// branch's contents are unresolved rather than empty.
func deriveSQLServerWrites(routine schemamodel.Function, kind string) (writes []RoutineWrite, unresolved []string) {
	for _, statement := range parser.ParseSQLServerRoutineBody(routine.Body).Statements {
		stmtWrites, stmtUnresolved := classifySQLServerStatement(statement, routine.Name, kind)
		writes = append(writes, stmtWrites...)
		unresolved = append(unresolved, stmtUnresolved...)
	}
	return writes, unresolved
}

// classifySQLServerStatement gives one of three answers about a statement: the
// writes it performs, that it cannot write, or that this analysis cannot tell.
func classifySQLServerStatement(statement ast.SQLServerRoutineStatement, routine, kind string) (writes []RoutineWrite, unresolved []string) {
	switch statement.Kind {
	case ast.SQLServerRoutineStatementInsert, ast.SQLServerRoutineStatementRaw:
		return classifyRawStatement(statement.SQL, routine, kind)
	case ast.SQLServerRoutineStatementDeclaration, ast.SQLServerRoutineStatementAssignment,
		ast.SQLServerRoutineStatementReturn, ast.SQLServerRoutineStatementSelect:
		return nil, nil
	default:
		return nil, []string{unreadableContents(string(statement.Kind))}
	}
}

// unreadableContents is the sentence a control-flow statement gets when the
// parser models no statements inside it.
//
// The T-SQL body model carries no nested statements, so every branch lands
// here. That is the honest answer: crediting a routine
// with writing nothing inside an IF nobody opened is the confident wrong answer
// this package exists to avoid.
func unreadableContents(kind string) string {
	return fmt.Sprintf("the %s statement's contents could not be read", kind)
}

// proceduralWritesFor selects the body analysis the routines of a dialect
// whose owner reads no bodies yet get. A PostgreSQL or MySQL body is read by
// its owner and never reaches here.
//
// SQL Server parses a routine body with its own grammar, and there is no
// shared one to fall back on: a T-SQL body read by the PL/pgSQL splitter is
// not a conservative answer, it is a wrong one. A dialect with no
// routine-body parser is reported as having none rather than analyzed by
// whichever parser happened to be reachable.
func proceduralWritesFor(dialect string, routine schemamodel.Function, kind string) (writes []RoutineWrite, unresolved []string, analyzed bool) {
	switch {
	case isSQLServerFamily(dialect):
		writes, unresolved = deriveSQLServerWrites(routine, kind)
		return writes, unresolved, true
	default:
		return nil, nil, false
	}
}

// isSQLServerFamily names the dialect whose routine bodies have a parser here.
// platform carries no predicate for it, and inventing one there would put a
// classification in a package that has no routine bodies in it.
func isSQLServerFamily(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.SQLServer
}
