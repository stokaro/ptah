package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// tableWithChecks is one table carrying the `checks` attribute the parser fills
// from `//ptah:schema:table checks="..."`.
func tableWithChecks(checks ...string) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "P", Name: "products", Checks: checks}},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "P", Name: "price", Type: "NUMERIC(10,2)"},
		},
	}
	schemamodel.Finalize(database)
	return database
}

// TestFromTable_DeclaredChecksReachTheDDL covers item 1 of stokaro/ptah#2590.
//
// `checks` is declared in internal/annotationmeta, filled by the parser and
// written back by the HCL renderer. SQL rendering read it nowhere, so an author
// who wrote `checks="price > 0"` got a table with no CHECK and exit 0 -- a
// constraint that never reached the database and was never reported.
func TestFromTable_DeclaredChecksReachTheDDL(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "postgres", dialect: platform.Postgres, want: "CHECK (price > 0)"},
		{name: "mysql", dialect: platform.MySQL, want: "CHECK (price > 0)"},
		{name: "sqlite", dialect: platform.SQLite, want: "CHECK (price > 0)"},
		// ClickHouse refuses an unnamed table CHECK, so its renderer names one
		// from the table. The declaration carries an expression and no name,
		// which is the same path a column's `check=` already takes there.
		{name: "clickhouse", dialect: platform.ClickHouse, want: "CONSTRAINT products_check CHECK (price > 0)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(tableWithChecks("price > 0"), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}

// TestFromTable_EveryDeclaredCheckReachesTheDDL keeps the conversion from
// stopping at the first expression.
//
// The attribute is a comma-separated list, so a table declaring two checks and
// getting one is the same silent loss one step smaller.
func TestFromTable_EveryDeclaredCheckReachesTheDDL(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		tableWithChecks("price > 0", "id > 0"), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements[0], qt.Contains, "CHECK (price > 0)")
	c.Assert(statements[0], qt.Contains, "CHECK (id > 0)")
}

// TestFromTable_NoDeclaredChecksAddsNothing is the control.
//
// Without it a conversion that emitted a CHECK unconditionally, or one that
// invented an empty constraint from a trailing comma in the attribute, would
// pass the assertions above.
func TestFromTable_NoDeclaredChecksAddsNothing(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(tableWithChecks(), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements[0], qt.Not(qt.Contains), "CHECK")

	blank, err := renderer.GetOrderedCreateStatements(tableWithChecks("  "), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(blank[0], qt.Not(qt.Contains), "CHECK")
}
