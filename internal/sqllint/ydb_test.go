package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqllint"
)

// Every caller of the rules is refused for YDB, the agent gate as well as
// `ptah sql lint`, before a statement is read: the rules read a double-quoted
// "a;b" as a name, and the parser has no YQL grammar.
func TestLintSource_RefusesYDB(t *testing.T) {
	for _, dialect := range []string{"ydb", "ydbs"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(
				sqllint.Source{Name: "x.sql", SQL: "CREATE TABLE t (id Int64, PRIMARY KEY (id));\nSELECT \"a;b\";"},
				sqllint.Options{Dialect: dialect},
			)

			c.Assert(err, qt.ErrorMatches, `linting YQL for YDB is not implemented yet \(stokaro/ptah#4015, phase 8\)`)
			c.Assert(findings, qt.IsNil)
		})
	}
}

// Every other name is left to the checks that already judge it, so the YDB
// refusal widens nothing.
func TestValidateDialect_LeavesOtherDialectsAlone(t *testing.T) {
	for _, dialect := range []string{"", "postgres", "oracle", "clickhouse", "db2"} {
		c := qt.New(t)
		c.Assert(sqllint.ValidateDialect(dialect), qt.IsNil, qt.Commentf("dialect %q", dialect))
	}
}
