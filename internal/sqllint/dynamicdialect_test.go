package sqllint_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/sqllint"
)

// dynamicCodes lists the rules a lint run reported for one routine.
func dynamicCodes(c *qt.C, dialect, sql string) []string {
	c.Helper()

	findings, err := sqllint.LintSource(sqllint.Source{Name: "x.sql", SQL: sql},
		sqllint.Options{Dialect: dialect})
	c.Assert(err, qt.IsNil)
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	return codes
}

// TestLintSource_DynamicSQLIsNamedOnEveryDialect is #1270's criterion 6, in the
// exact form it records: MySQL PREPARE and T-SQL sp_executesql yielded only
// SQL002.
//
// SQL002 says the linter does not model the routine. It says nothing about the
// one property that makes a body unanalyzable however well it is modelled --
// that its SQL is not in the file. PL/pgSQL said so and the other two did not.
func TestLintSource_DynamicSQLIsNamedOnEveryDialect(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
	}{
		{
			name:    "mysql prepares a statement from a variable",
			dialect: "mysql",
			sql:     "CREATE PROCEDURE p() BEGIN SET @s = 'DROP TABLE t'; PREPARE stmt FROM @s; END",
		},
		{
			name:    "t-sql calls sp_executesql",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p AS EXEC sp_executesql N'DROP TABLE t';",
		},
		{
			name:    "t-sql executes a string",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p AS EXEC('DROP TABLE t');",
		},
		{
			name:    "pl/pgsql executes a string",
			dialect: "postgres",
			sql:     "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'DROP TABLE t'; END; $$;",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			codes := dynamicCodes(c, row.dialect, row.sql)

			c.Assert(codes, qt.Contains, "SQL003")
		})
	}
}

// TestLintSource_AnOrdinaryRoutineIsNotCalledDynamic is the control.
//
// A rule that reported every routine would satisfy the test above and mean
// nothing. `EXEC other_proc` is the case that matters most: it calls a routine
// that is written down, and reporting it would say a body is unanalyzable
// because it calls another one.
func TestLintSource_AnOrdinaryRoutineIsNotCalledDynamic(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		sql     string
	}{
		{
			name:    "t-sql calls a named procedure",
			dialect: "sqlserver",
			sql:     "CREATE PROCEDURE p AS EXEC other_proc;",
		},
		{
			name:    "mysql updates a table",
			dialect: "mysql",
			sql:     "CREATE PROCEDURE p() BEGIN UPDATE t SET c = 1; END",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			codes := dynamicCodes(c, row.dialect, row.sql)

			c.Assert(codes, qt.Not(qt.Contains), "SQL003")
		})
	}
}

// TestLintSource_MySQLNamesBothHalvesOfThePreparedStatement keeps the report
// complete.
//
// PREPARE builds the text and EXECUTE runs it. A body carrying either is a body
// whose SQL is not in the file, and reporting one spelling would leave the
// other silent.
func TestLintSource_MySQLNamesBothHalvesOfThePreparedStatement(t *testing.T) {
	c := qt.New(t)

	codes := dynamicCodes(c, "mysql",
		"CREATE PROCEDURE p() BEGIN SET @s = 'DROP TABLE t'; PREPARE stmt FROM @s; EXECUTE stmt; END")

	c.Assert(countCode(codes, "SQL003"), qt.Equals, 2)
}

// countCode counts how many findings carried one identifier.
func countCode(codes []string, want string) int {
	found := 0
	for _, code := range codes {
		found += len(strings.Split(code, want)) - 1
	}
	return found
}
