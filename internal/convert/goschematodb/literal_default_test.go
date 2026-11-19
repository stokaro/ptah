package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// A literal default is held as a catalog reports it: a string in quotes on a
// text column, so a description of the literal `1 + 2` does not read like the
// expression `(1 + 2)` a catalog reports. An expression keeps its text, and a
// literal on another column keeps the value as written.
func TestToDBSchema_HoldsALiteralDefaultAsACatalogReportsIt(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		want  string
	}{
		{name: "a bare literal on a varchar", field: schemamodel.Field{Type: "VARCHAR(30)", Default: "1 + 2", DefaultSet: true}, want: "'1 + 2'"},
		{name: "a quote inside the literal", field: schemamodel.Field{Type: "TEXT", Default: "it's", DefaultSet: true}, want: "'it''s'"},
		{name: "an empty literal", field: schemamodel.Field{Type: "VARCHAR(8)", DefaultSet: true}, want: "''"},
		{name: "a literal the SQL parser kept quoted", field: schemamodel.Field{Type: "VARCHAR(30)", Default: "'x'", DefaultSet: true}, want: "'x'"},
		{name: "a number on an integer", field: schemamodel.Field{Type: "INTEGER", Default: "7", DefaultSet: true}, want: "7"},
		{name: "an expression", field: schemamodel.Field{Type: "VARCHAR(30)", DefaultExpr: "(1 + 2)"}, want: "(1 + 2)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			field := test.field
			field.StructName, field.Name, field.Nullable = "Note", "c", true
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Note", Name: "notes"}},
				Fields: []schemamodel.Field{field},
			}

			got, err := goschematodb.ToDBSchema(t.Context(), db, platform.Postgres, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(got.Tables, qt.HasLen, 1)
			c.Assert(got.Tables[0].Columns, qt.HasLen, 1)
			c.Assert(got.Tables[0].Columns[0].ColumnDefault, qt.DeepEquals, new(test.want))
		})
	}
}

// Where a backslash in a string is an escape, a literal is held with its
// backslashes doubled, the form MySQL and ClickHouse report the characters
// in. PostgreSQL reads a backslash in a standard string as itself.
func TestToDBSchema_HoldsABackslashAsTheDialectReportsIt(t *testing.T) {
	tests := []struct {
		name, dialect string
		want          string
	}{
		{name: "MySQL", dialect: platform.MySQL, want: `'a\\b'`},
		{name: "MariaDB", dialect: platform.MariaDB, want: `'a\\b'`},
		{name: "ClickHouse", dialect: platform.ClickHouse, want: `'a\\b'`},
		{name: "PostgreSQL", dialect: platform.Postgres, want: `'a\b'`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			key := schemamodel.Field{StructName: "Note", Name: "id", Type: "INT", Primary: true}
			field := schemamodel.Field{StructName: "Note", Name: "c", Type: "VARCHAR(30)", Nullable: true, Default: `a\b`, DefaultSet: true}
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Note", Name: "notes"}},
				Fields: []schemamodel.Field{key, field},
			}

			got, err := goschematodb.ToDBSchema(t.Context(), db, test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(got.Tables, qt.HasLen, 1)
			c.Assert(got.Tables[0].Columns, qt.HasLen, 2)
			c.Assert(got.Tables[0].Columns[1].ColumnDefault, qt.DeepEquals, new(test.want))
		})
	}
}
