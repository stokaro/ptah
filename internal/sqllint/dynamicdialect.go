package sqllint

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
)

// dialectDynamicStatements returns the statements in a MySQL or T-SQL routine
// body that build SQL at run time, with the label naming the routine.
//
// PL/pgSQL has a statement kind for it, so the PostgreSQL half of this rule
// asks the parser. Neither of the other two models one: MySQL's PREPARE and
// T-SQL's sp_executesql arrive in the raw kind, and the leading words are what
// separate them from an ordinary statement. Until this, both answered with
// SQL002 alone -- "not modelled" -- which says nothing about the one property
// that makes a body unanalyzable (stokaro/ptah#1270, criterion 6).
func dialectDynamicStatements(stmt ast.Node) (label string, dynamic []string) {
	switch node := stmt.(type) {
	case *ast.MySQLRoutineNode:
		return routineLabel(string(node.Kind), node.Name), mysqlDynamicStatements(node.Body.Statements)
	case *ast.SQLServerRoutineNode:
		return routineLabel(string(node.Kind), node.Name), sqlServerDynamicStatements(node.Body.Statements)
	default:
		return "", nil
	}
}

// routineLabel names the routine the way the PostgreSQL half does.
func routineLabel(kind, name string) string {
	return fmt.Sprintf("%s %s", strings.ToLower(kind), name)
}

// mysqlDynamicStatements finds MySQL's prepared-statement mechanism.
//
// PREPARE takes the text from a variable, and EXECUTE runs what PREPARE built:
// both halves are reported, because a body carrying only one of them is still a
// body whose SQL is not in the file.
func mysqlDynamicStatements(statements []ast.MySQLRoutineStatement) []string {
	dynamic := make([]string, 0)
	for _, statement := range statements {
		if dynamicLeadingWord(statement.SQL, "PREPARE", "EXECUTE", "DEALLOCATE") {
			dynamic = append(dynamic, statement.SQL)
		}
	}
	return dynamic
}

// sqlServerDynamicStatements finds T-SQL's two spellings.
//
// `EXEC sp_executesql` and `EXEC('...')` build their statement; `EXEC a_proc`
// calls one that is already written down, and reporting it would say a routine
// is unanalyzable because it calls another routine.
func sqlServerDynamicStatements(statements []ast.SQLServerRoutineStatement) []string {
	dynamic := make([]string, 0)
	for _, statement := range statements {
		if sqlServerDynamicStatement(statement.SQL) {
			dynamic = append(dynamic, statement.SQL)
		}
	}
	return dynamic
}

// sqlServerDynamicStatement reports whether one T-SQL statement builds its SQL.
//
// What follows the keyword decides it: a parenthesis means the statement is a
// string being executed, and sp_executesql is the procedure that runs one.
// `EXEC(...)` written without a space is the same thing, so the parenthesis is
// looked for in the keyword's own field too.
func sqlServerDynamicStatement(sql string) bool {
	fields := strings.Fields(strings.TrimSpace(sql))
	if !dynamicLeadingWord(sql, "EXEC", "EXECUTE") {
		return false
	}
	if strings.Contains(fields[0], "(") {
		return true
	}
	if len(fields) < 2 {
		return false
	}
	return strings.HasPrefix(fields[1], "(") ||
		strings.HasPrefix(strings.ToLower(fields[1]), "sp_executesql")
}

// dynamicLeadingWord reports whether a statement begins with one of the words.
func dynamicLeadingWord(sql string, words ...string) bool {
	fields := strings.Fields(strings.TrimSpace(sql))
	if len(fields) == 0 {
		return false
	}
	leading := strings.ToUpper(strings.TrimLeft(fields[0], "("))
	for _, word := range words {
		if leading == word || strings.HasPrefix(leading, word+"(") {
			return true
		}
	}
	return false
}
