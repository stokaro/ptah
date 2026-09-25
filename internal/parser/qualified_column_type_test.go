package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// A column type may be written with its schema and in quotes, as pg_dump
// writes every type outside the search path (stokaro/ptah#3620). Read as one
// word, the name stopped at the dot and the column list failed on the rest.
func TestParse_QualifiedColumnType_HappyPath(t *testing.T) {
	rows := []struct {
		name     string
		column   string
		wantType string
	}{
		{name: "an enum in another schema", column: "m app.mood", wantType: "app.mood"},
		{name: "quoted parts", column: `m "app"."Mood"`, wantType: `"app"."Mood"`},
		{name: "an array", column: "m app.mood[]", wantType: "app.mood[]"},
		{name: "a modifier list", column: "g public.geometry(Point, 4326)", wantType: "public.geometry(Point, 4326)"},
		{name: "followed by a constraint", column: "m public.citext NOT NULL", wantType: "public.citext"},
		{name: "a built-in type in pg_catalog", column: "n pg_catalog.int4", wantType: "pg_catalog.int4"},
		{name: "a multi-word built-in, as before", column: "v character varying(5)", wantType: "character varying(5)"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements := parsePostgres(c, "CREATE TABLE t (id int, "+row.column+");")

			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 2)
			c.Assert(table.Columns[1].Type, qt.Equals, row.wantType)
		})
	}
}

// The same type is read where a column is added later and where a domain is
// based on it.
func TestParse_QualifiedTypeInAlterAndDomain(t *testing.T) {
	c := qt.New(t)

	statements := parsePostgres(c, "ALTER TABLE t ADD COLUMN m app.mood; CREATE DOMAIN app.d AS app.mood;")

	c.Assert(statements.Statements, qt.HasLen, 2)
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	add, ok := alter.Operations[0].(*ast.AddColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(add.Column.Type, qt.Equals, "app.mood")
}

// A dot with nothing after it is refused, rather than read as a type ending in
// a dot.
func TestParse_QualifiedColumnType_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("CREATE TABLE t (id int, m app.);", parser.WithDialect("postgres")).Parse()

	c.Assert(err, qt.ErrorMatches, `.*expected identifier after '\.' in column type: .*`)
	c.Assert(statements, qt.IsNil)
}
