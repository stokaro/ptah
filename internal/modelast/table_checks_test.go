package modelast_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/modelast"
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

// tableWithChecksAndDeclaredConstraint is the collision: a `checks` entry whose
// generated name is the one an explicit constraint already answers to, over a
// different expression.
func tableWithChecksAndDeclaredConstraint(checkName string) *schemamodel.Database {
	database := tableWithChecks("price > 0")
	database.Constraints = []schemamodel.Constraint{{
		StructName:      "P",
		Name:            checkName,
		Type:            "CHECK",
		Table:           "products",
		CheckExpression: "stock >= 0",
	}}
	database.Fields = append(database.Fields, schemamodel.Field{
		StructName: "P", Name: "stock", Type: "BIGINT",
	})
	schemamodel.Finalize(database)
	return database
}

// TestFromTable_ADeclaredCheckRendersOnce is the count, which nothing else
// asserted.
//
// Two passes over table.Checks stood in the conversion at once -- an unnamed one
// and a named one -- so every declared check rendered twice, as
// `CHECK (price > 0)` and `CONSTRAINT "products_check" CHECK (price > 0)` in the
// same CREATE TABLE. Every existing test asked whether the check was PRESENT,
// which both copies satisfy.
func TestFromTable_ADeclaredCheckRendersOnce(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(tableWithChecks("price > 0"), platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Count(sql, "price > 0"), qt.Equals, 1)
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check" CHECK (price > 0)`)
}

// TestFromTable_AGeneratedCheckNameSkipsADeclaredOne covers the collision.
//
// Supersession is keyed by EXPRESSION, so it cannot settle this: the two
// constraints say different things and only share a name. Measured on
// PostgreSQL 18.6, the colliding pair is refused outright --
// `ERROR: check constraint "products_check" already exists` -- so this is DDL
// the server will not take rather than a cosmetic clash.
func TestFromTable_AGeneratedCheckNameSkipsADeclaredOne(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		tableWithChecksAndDeclaredConstraint("products_check"), platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check1" CHECK (price > 0)`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check" CHECK (stock >= 0)`)
	c.Assert(strings.Count(sql, `CONSTRAINT "products_check" `), qt.Equals, 1)
}

// TestFromTable_AGeneratedCheckNameIsUnchangedWithoutACollision is the control.
// Without it, a namer that always skipped to `_check1` would pass the test
// above.
func TestFromTable_AGeneratedCheckNameIsUnchangedWithoutACollision(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		tableWithChecksAndDeclaredConstraint("products_stock_positive"), platform.Postgres)

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check" CHECK (price > 0)`)
	c.Assert(sql, qt.Not(qt.Contains), "products_check1")
}

// checkConstraintNames is the names of the table-level CHECK constraints a node
// carries, in the order it carries them.
//
// The filtering lives here rather than in a test body because the style rule
// forbids a conditional in a test function, and this is data extraction rather
// than a choice about how to assert.
func checkConstraintNames(node *ast.CreateTableNode) []string {
	names := make([]string, 0, len(node.Constraints))
	for _, constraint := range node.Constraints {
		if constraint.Type != ast.CheckConstraint {
			continue
		}
		names = append(names, constraint.Name)
	}
	return names
}

// TestFromTable_NodeCarriesOneConstraintPerDeclaredCheck counts on the node
// itself, which is the only place the count is visible.
//
// The whole-schema path re-derives the synthesized checks in addTableConstraints
// once the declared constraint list is visible, so a duplicate emission inside
// FromTable is cleaned up before any statement is rendered and a test driving
// GetOrderedCreateStatements cannot see it. The planners do not take that path:
// they build a CREATE from FromTable/FromTableWithConstraints and add explicit
// constraints as their own ALTER statements, so a duplicate here would ship in
// every migration they plan.
//
// Established by mutation: restoring the second, unnamed pass over table.Checks
// leaves every test that goes through the renderer green and reddens this one.
func TestFromTable_NodeCarriesOneConstraintPerDeclaredCheck(t *testing.T) {
	tests := []struct {
		name   string
		checks []string
		want   int
	}{
		{name: "one check", checks: []string{"price > 0"}, want: 1},
		{name: "two checks", checks: []string{"price > 0", "stock >= 0"}, want: 2},
		{name: "no checks", checks: nil, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := tableWithChecks(test.checks...)

			node := modelast.FromTable(
				database.Tables[0], database.Fields, database.Enums, platform.Postgres)

			c.Assert(checkConstraintNames(node), qt.HasLen, test.want)
		})
	}
}

// TestFromTableWithConstraints_NodeSkipsADeclaredName is the planner's half of
// the collision.
//
// A planner adds the explicit constraint through its own ALTER, so the CREATE it
// builds must still leave that name free. Measured before the fix:
// `schema apply` emitted the colliding pair and died on
// `ERROR: constraint "products_check" for relation "products" already exists`
// while `schema render` was already correct — the two paths had drifted apart.
func TestFromTableWithConstraints_NodeSkipsADeclaredName(t *testing.T) {
	c := qt.New(t)
	database := tableWithChecksAndDeclaredConstraint("products_check")

	node := modelast.FromTableWithConstraints(
		database.Tables[0], database.Fields, database.Enums, platform.Postgres, database.Constraints)

	c.Assert(checkConstraintNames(node), qt.DeepEquals, []string{"products_check1"})
}
