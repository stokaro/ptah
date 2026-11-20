package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// A # banner between an IF condition and its THEN is a comment on MySQL and
// MariaDB, so the IF opens a block that END IF closes and the routine ends at
// its own END. Read as IF(...) the call form, the block never closes and the
// body runs into the end of the text.
func TestParse_MySQLRoutineIfBlockAfterHashBanner_HappyPath(t *testing.T) {
	const sql = "CREATE PROCEDURE p() BEGIN IF (x > 1) #=== banner\n THEN SET y = 1; END IF; END;\n"
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			routine, ok := statements.Statements[0].(*ast.RoutineNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(routine.Body.SQL, qt.Contains, "END IF")
		})
	}
}

// The SQL-standard function body reader reads the same fragment with the
// dialect's options. ClickHouse lists # as a line comment and has no routine
// grammar of its own, so it reaches that reader with a dialect whose # rule
// differs from the heuristic's.
func TestParse_AtomicFunctionBodyIfBlockAfterHashBanner_HappyPath(t *testing.T) {
	c := qt.New(t)
	const sql = "CREATE FUNCTION f() RETURNS int LANGUAGE sql BEGIN ATOMIC IF (x > 1) #=== banner\n THEN SET y = 1; END IF; SELECT 1; END;\n"

	statements, err := parser.NewParser(sql, parser.WithDialect("clickhouse")).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	function, ok := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(function.Body, qt.Contains, "END IF")
}
