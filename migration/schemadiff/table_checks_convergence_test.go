package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// checksDesired is a table declaring its check expressions through the `checks`
// table attribute.
func checksDesired(checks ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Product", Name: "products", Checks: checks}},
		Fields: []schemamodel.Field{
			{StructName: "Product", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Product", Name: "price", Type: "INTEGER"},
			{StructName: "Product", Name: "stock", Type: "INTEGER"},
		},
	}
}

// checksCurrent is that table as a PostgreSQL catalog reports it once the
// rendered CREATE TABLE has been applied: the CHECKs come back under the names
// the DDL gave them, with the parentheses the server adds.
func checksCurrent(constraints ...catalog.Constraint) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "products", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsNullable: "NO"},
			{Name: "price", DataType: "integer", IsNullable: "NO"},
			{Name: "stock", DataType: "integer", IsNullable: "NO"},
		}}},
		Constraints: append([]catalog.Constraint{{
			Name: "products_pkey", TableName: "products", Type: "PRIMARY KEY",
			ColumnNames: []string{"id"},
		}}, constraints...),
	}
}

func checkConstraint(name, clause string) catalog.Constraint {
	return catalog.Constraint{
		Name:        name,
		TableName:   "products",
		Type:        "CHECK",
		CheckClause: &clause,
	}
}

// TestTableChecks_RenderAndCompareAgree is the property the whole `checks` fix
// turns on: what the renderer creates, the comparison recognizes.
//
// The two halves are separate code and they disagreed by construction. Rendered
// alone -- which is what stokaro/ptah#2590 first fixed -- the comparison saw a
// CHECK on the server that no desired constraint claimed, so the next plan
// emitted `ALTER TABLE "products" DROP CONSTRAINT IF EXISTS "products_check"`
// and a second apply deleted the check the first one created. Measured, before
// the synthesis existed.
//
// The renderer's own tests cannot see this: they end at the SQL. The
// comparator's own tests cannot see it either, because a fixture written by
// hand agrees with whatever the comparator expects. Only driving both from one
// declaration does.
func TestTableChecks_RenderAndCompareAgree(t *testing.T) {
	c := qt.New(t)

	desired := checksDesired("price > 0", "stock >= 0")

	statements, err := renderer.GetOrderedCreateStatements(desired, "postgres")
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check" CHECK (price > 0)`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check1" CHECK (stock >= 0)`)

	diff := schemadiff.CompareWithDialect(desired, checksCurrent(
		checkConstraint("products_check", "((price > 0))"),
		checkConstraint("products_check1", "((stock >= 0))"),
	), "postgres")

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

// TestTableChecks_AMissingCheckIsStillReported is the acceptance control for the
// test above. A synthesis that produced nothing, or one the comparison ignored,
// would make every database look converged -- including one where the check the
// author declared is absent.
func TestTableChecks_AMissingCheckIsStillReported(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		checksDesired("price > 0"),
		checksCurrent(),
		"postgres",
	)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].Name, qt.Equals, "products_check")
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

// TestTableChecks_ANewTableSynthesizesNothing keeps the synthesis off a table
// the database does not have yet.
//
// Such a table's checks ship inline in its CREATE TABLE, so an ADD CONSTRAINT
// beside it would create the same constraint twice in one migration. It is the
// rule the field-level `check=` synthesis already follows for absent columns.
func TestTableChecks_ANewTableSynthesizesNothing(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		checksDesired("price > 0"),
		&catalog.Database{},
		"postgres",
	)

	c.Assert(diff.TablesAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
}

// TestTableChecks_AnExplicitConstraintIsCompared covers the overlap: the entry
// an explicit CHECK already spells is neither rendered nor synthesized, so the
// server's single constraint has exactly one desired counterpart.
func TestTableChecks_AnExplicitConstraintIsCompared(t *testing.T) {
	c := qt.New(t)

	desired := checksDesired("price > 0")
	desired.Constraints = []schemamodel.Constraint{{
		StructName:      "Product",
		Name:            "products_price_positive",
		Type:            "CHECK",
		Table:           "products",
		CheckExpression: "price > 0",
	}}

	diff := schemadiff.CompareWithDialect(desired, checksCurrent(
		checkConstraint("products_price_positive", "((price > 0))"),
	), "postgres")

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}
