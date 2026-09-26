package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// columnCheckDesired declares table e the way a YAML file or a Go annotation
// does: a column CHECK over two columns and one over its own column, neither
// named, and a `checks` entry.
func columnCheckDesired() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "E", Name: "e", Checks: []string{"a > 0"}}},
		Fields: []schemamodel.Field{
			{StructName: "E", Name: "a", Type: "INTEGER", Nullable: true, Check: "a < b"},
			{StructName: "E", Name: "b", Type: "INTEGER", Nullable: true, Check: "b > 0"},
		},
	}
}

// columnCheckCurrent is table e as PostgreSQL 18.6 reports it after running the
// CREATE TABLE the renderer writes for [columnCheckDesired]: the server named
// the column CHECKs, and the `checks` entry carries the name the render gave it.
func columnCheckCurrent() *catalog.Database {
	check := func(name, clause string) catalog.Constraint {
		return catalog.Constraint{Name: name, TableName: "e", Type: "CHECK", CheckClause: new(clause)}
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "e", Columns: []catalog.Column{
			{Name: "a", DataType: "integer", IsNullable: "YES"},
			{Name: "b", DataType: "integer", IsNullable: "YES"},
		}}},
		Constraints: []catalog.Constraint{
			check("e_check", "((a < b))"),
			check("e_b_check", "((b > 0))"),
			check("e_check1", "((a > 0))"),
		},
	}
}

// TestCompare_AnUnnamedColumnCheckTakesTheServersName compares the table with
// the database the rendered table built: nothing is planned. Named
// `<table>_<column>_check`, the CHECK over two columns was looked for as
// `e_a_check`, and every apply renamed the server's `e_check`
// (stokaro/ptah#3750).
func TestCompare_AnUnnamedColumnCheckTakesTheServersName(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(columnCheckDesired(), columnCheckCurrent(), platform.Postgres)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

// TestCompare_AnUnnamedColumnCheckThatChangedIsPlanned is the control for the
// test above: a column CHECK the database holds with another condition is
// still planned, under the server's name.
func TestCompare_AnUnnamedColumnCheckThatChangedIsPlanned(t *testing.T) {
	c := qt.New(t)
	desired := columnCheckDesired()
	desired.Fields[0].Check = "a <= b"

	diff := schemadiff.CompareWithDialect(desired, columnCheckCurrent(), platform.Postgres)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].Name, qt.Equals, "e_check")
	c.Assert(diff.ConstraintsAdded[0].CheckExpression, qt.Equals, "a <= b")
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemoved[0].Name, qt.Equals, "e_check")
}

// TestCompareSchemas_AnUnnamedColumnCheckPairsWithItself compares the table
// with itself, as `schema diff` between two copies of one file does: both sides
// name the column CHECKs and the `checks` entry by one rule, so nothing is
// planned.
func TestCompareSchemas_AnUnnamedColumnCheckPairsWithItself(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.MySQL} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareSchemas(columnCheckDesired(), columnCheckDesired(), dialect)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestColumnCheck_RenderAndPlanNameTheChecksEntryPastIt renders the table and
// plans it for an empty database: both write the `checks` entry as `e_check1`.
// The column's CHECK over two columns is written unnamed and takes `e_check`
// from the server first, so an entry also named `e_check` is refused with
// `check constraint "e_check" already exists`. The render and the plan lower
// the table through two paths, and each must see the columns.
func TestColumnCheck_RenderAndPlanNameTheChecksEntryPastIt(t *testing.T) {
	c := qt.New(t)
	desired := columnCheckDesired()

	statements, err := renderer.GetOrderedCreateStatements(desired, platform.Postgres)
	c.Assert(err, qt.IsNil)
	plan, err := planner.GenerateSchemaDiffSQLStatements(
		schemadiff.CompareWithDialect(desired, &catalog.Database{}, platform.Postgres), platform.Postgres)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Join(statements, "\n"), qt.Contains, `CONSTRAINT "e_check1" CHECK (a > 0)`)
	c.Assert(strings.Join(plan, "\n"), qt.Contains, `CONSTRAINT "e_check1" CHECK (a > 0)`)
}
