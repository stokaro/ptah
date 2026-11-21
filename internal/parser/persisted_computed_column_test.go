package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
)

// TestParse_PersistedComputedColumn_HappyPath reads a T-SQL computed column
// with the kind SQL Server gives it: `AS (expr)` is computed on read, which
// the model names VIRTUAL, and `AS (expr) PERSISTED` is stored. Read with no
// kind, a bare `AS (expr)` would take the target's default, PERSISTED, and a
// file `ptah db read` wrote would build a stored column where the database
// holds one computed on read. Every other dialect reads a bare AS with no kind.
func TestParse_PersistedComputedColumn_HappyPath(t *testing.T) {
	rows := []struct {
		name     string
		dialect  string
		column   string
		wantKind string
		wantExpr string
	}{
		{name: "bare AS on SQL Server", dialect: platform.SQLServer, column: "x AS ([a]+(1))", wantKind: "VIRTUAL", wantExpr: "[a]+(1)"},
		{name: "PERSISTED on SQL Server", dialect: platform.SQLServer, column: "x AS ([a]+(1)) PERSISTED", wantKind: "PERSISTED", wantExpr: "[a]+(1)"},
		{name: "persisted in lower case on SQL Server", dialect: platform.SQLServer, column: "x AS (a) persisted", wantKind: "PERSISTED", wantExpr: "a"},
		{name: "bare AS on MySQL", dialect: platform.MySQL, column: "x INT AS (a + 1)", wantKind: "", wantExpr: "a + 1"},
		{name: "STORED on MySQL", dialect: platform.MySQL, column: "x INT AS (a + 1) STORED", wantKind: "STORED", wantExpr: "a + 1"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			sql := "CREATE TABLE t (a INT NOT NULL, " + row.column + ");"

			column := parseComputedColumn(c, row.dialect, sql)

			c.Assert(column.GeneratedKind, qt.Equals, row.wantKind)
			c.Assert(column.GeneratedExpression, qt.Equals, row.wantExpr)
		})
	}
}

// TestParse_PersistedComputedColumn_FailurePath refuses PERSISTED on a
// dialect whose grammar has no persisted computed column.
func TestParse_PersistedComputedColumn_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("CREATE TABLE t (a INT NOT NULL, x INT AS (a + 1) PERSISTED);",
		parser.WithDialect(platform.MySQL), parser.WithSQL(builtintest.Runtime().SQL())).Parse()

	c.Assert(err, qt.ErrorMatches, `line 1, column 50: unsupported column attribute: PERSISTED`)
	c.Assert(statements, qt.IsNil)
}
