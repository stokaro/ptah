package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

func addColumnIfNotExists(ifNotExists bool) ast.Node {
	alter := &ast.AlterTableNode{Name: "users"}
	alter.Operations = append(alter.Operations, &ast.AddColumnOperation{
		Column:      ast.NewColumn("note", "text"),
		IfNotExists: ifNotExists,
	})
	return alter
}

// ADD COLUMN IF NOT EXISTS is written by the PostgreSQL family's renderer, and
// the unguarded form is written as it always was.
func TestAddColumnIfNotExists_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		ifNotExists bool
		wantSQL     string
	}{
		{name: "postgres writes the guard", dialect: "postgres", ifNotExists: true, wantSQL: `ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "note" text;`},
		{name: "cockroachdb writes the guard", dialect: "cockroachdb", ifNotExists: true, wantSQL: `ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "note" text;`},
		{name: "yugabytedb writes the guard", dialect: "yugabytedb", ifNotExists: true, wantSQL: `ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "note" text;`},
		{name: "spanner writes the guard", dialect: "spanner", ifNotExists: true, wantSQL: `ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "note" text;`},
		{name: "postgres without the guard", dialect: "postgres", wantSQL: `ALTER TABLE "users" ADD COLUMN "note" text;`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(test.dialect, addColumnIfNotExists(test.ifNotExists))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.wantSQL)
		})
	}
}

// A dialect whose renderer does not write the guard refuses the operation
// rather than render a plain ADD COLUMN, which fails on a table that has the
// column where the author asked it not to.
func TestAddColumnIfNotExists_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb", "sqlite", "sqlserver", "clickhouse", "oracle"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, addColumnIfNotExists(true))

			c.Assert(err, qt.ErrorMatches, `.*does not render ADD COLUMN IF NOT EXISTS`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// STRICT is written beside the routine's other attributes, and a routine that
// states nothing about NULL input renders no clause, because CALLED ON NULL
// INPUT is the server's default.
func TestFunctionStrict_Renders(t *testing.T) {
	tests := []struct {
		name    string
		strict  bool
		wantSQL string
	}{
		{name: "strict", strict: true, wantSQL: "LANGUAGE sql IMMUTABLE STRICT;"},
		{name: "not strict", wantSQL: "LANGUAGE sql IMMUTABLE;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			function := ast.NewCreateFunction("twice").
				SetParameters("a integer").
				SetReturns("integer").
				SetLanguage("sql").
				SetVolatility("IMMUTABLE").
				SetBody("SELECT a * 2")
			function.Strict = test.strict

			sql, err := renderer.RenderSQL("postgres", function)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.wantSQL)
		})
	}
}
