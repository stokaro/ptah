package schemalineage

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
)

// deriveMySQLWrites resolves the writes a MySQL routine body performs.
//
// The MySQL parser classifies a body statement by kind, so insert, update and
// delete arrive named rather than having to be read off the leading word. What
// it does not model is nesting: a MySQLRoutineStatement carries no statements
// of its own, so the contents of an IF or a loop are not split and are reported
// as unresolved rather than credited with writing nothing (stokaro/ptah#2394).
func deriveMySQLWrites(routine schemamodel.Function, kind string) (writes []RoutineWrite, unresolved []string) {
	for _, statement := range parser.ParseMySQLRoutineBody(routine.Body).Statements {
		stmtWrites, stmtUnresolved := classifyMySQLStatement(statement, routine.Name, kind)
		writes = append(writes, stmtWrites...)
		unresolved = append(unresolved, stmtUnresolved...)
	}
	return writes, unresolved
}

// classifyMySQLStatement answers one of three things about a statement: the
// writes it performs, that it cannot write, or that this analysis cannot tell.
func classifyMySQLStatement(statement ast.MySQLRoutineStatement, routine, kind string) (writes []RoutineWrite, unresolved []string) {
	switch statement.Kind {
	case ast.MySQLRoutineStatementInsert, ast.MySQLRoutineStatementUpdate,
		ast.MySQLRoutineStatementDelete, ast.MySQLRoutineStatementRaw:
		return classifyRawStatement(statement.SQL, routine, kind)
	case ast.MySQLRoutineStatementDeclaration, ast.MySQLRoutineStatementCursor,
		ast.MySQLRoutineStatementReturn, ast.MySQLRoutineStatementSet,
		ast.MySQLRoutineStatementSelect, ast.MySQLRoutineStatementOpen,
		ast.MySQLRoutineStatementFetch, ast.MySQLRoutineStatementLeave,
		ast.MySQLRoutineStatementIterate:
		return nil, nil
	default:
		return nil, []string{unreadableContents(string(statement.Kind))}
	}
}

// deriveSQLServerWrites resolves the writes a T-SQL routine body performs.
//
// The T-SQL parser names insert and select and leaves update and delete in the
// raw kind, which the leading-word reader handles. Like MySQL it models no
// nesting, so a branch's contents are unresolved rather than empty.
func deriveSQLServerWrites(routine schemamodel.Function, kind string) (writes []RoutineWrite, unresolved []string) {
	for _, statement := range parser.ParseSQLServerRoutineBody(routine.Body).Statements {
		stmtWrites, stmtUnresolved := classifySQLServerStatement(statement, routine.Name, kind)
		writes = append(writes, stmtWrites...)
		unresolved = append(unresolved, stmtUnresolved...)
	}
	return writes, unresolved
}

// classifySQLServerStatement is the T-SQL counterpart of
// [classifyMySQLStatement].
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
// Neither the MySQL nor the T-SQL body model carries nested statements today,
// so every branch lands here. That is the honest answer: crediting a routine
// with writing nothing inside an IF nobody opened is the confident wrong answer
// this package exists to avoid.
func unreadableContents(kind string) string {
	return fmt.Sprintf("the %s statement's contents could not be read", kind)
}

// proceduralWritesFor selects the body analysis a dialect's routines get.
//
// PostgreSQL, MySQL and SQL Server each parse a routine body with their own
// grammar, and there is no shared one to fall back on: a T-SQL body read by the
// PL/pgSQL splitter is not a conservative answer, it is a wrong one. A dialect
// with no routine-body parser is reported as having none rather than analyzed
// by whichever parser happened to be reachable.
func proceduralWritesFor(dialect string, routine schemamodel.Function, kind string) (writes []RoutineWrite, unresolved []string, analyzed bool) {
	switch {
	case platform.IsPostgresFamily(dialect):
		writes, unresolved = deriveProceduralWrites(routine, kind)
		return writes, unresolved, true
	case isMySQLFamily(dialect):
		writes, unresolved = deriveMySQLWrites(routine, kind)
		return writes, unresolved, true
	case isSQLServerFamily(dialect):
		writes, unresolved = deriveSQLServerWrites(routine, kind)
		return writes, unresolved, true
	default:
		return nil, nil, false
	}
}

// isMySQLFamily and isSQLServerFamily name the dialects whose routine bodies
// have a parser here. platform carries a predicate for the PostgreSQL family
// and none for these two, and inventing one there would put a classification in
// a package that has no routine bodies in it.
func isMySQLFamily(dialect string) bool {
	normalized := platform.NormalizeDialect(dialect)
	return normalized == platform.MySQL || normalized == platform.MariaDB
}

func isSQLServerFamily(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.SQLServer
}
