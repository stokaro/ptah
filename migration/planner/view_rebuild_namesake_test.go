package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// namesakeRebuildDiff compares a declaration that retypes reporting.item_skus.qty
// with the database. reporting.sku_report reads that table, and public.sku_list
// reads public.item_skus by its bare name. semantics is what the comparison's
// connection reported, nil for a comparison made without one.
func namesakeRebuildDiff(c *qt.C, semantics *identifier.Semantics) *difftypes.SchemaDiff {
	c.Helper()
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "item_skus", Schema: "public", StructName: "PublicSku"},
			{Name: "item_skus", Schema: "reporting", StructName: "ReportingSku"},
		},
		Fields: []schemamodel.Field{
			{StructName: "PublicSku", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "PublicSku", Name: "qty", Type: "INTEGER", Nullable: true},
			{StructName: "ReportingSku", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "ReportingSku", Name: "qty", Type: "BIGINT", Nullable: true},
		},
		Views: []schemamodel.View{
			{Name: "public.sku_list", Body: "SELECT id, qty FROM item_skus"},
			{Name: "reporting.sku_report", Body: "SELECT id, qty FROM reporting.item_skus"},
		},
	}
	columns := []catalog.Column{
		{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		{Name: "qty", DataType: "integer", IsNullable: "YES", OrdinalPosition: 2},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "public", Name: "item_skus", Type: "BASE TABLE", Columns: columns},
			{Schema: "reporting", Name: "item_skus", Type: "BASE TABLE", Columns: columns},
		},
		Views: []catalog.View{
			{Schema: "public", Name: "sku_list", Body: "SELECT id, qty FROM item_skus"},
			{Schema: "reporting", Name: "sku_report", Body: "SELECT id, qty FROM reporting.item_skus"},
		},
	}
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	diff.IdentifierSemantics = semantics
	return diff
}

// TestGenerateSchemaDiffSQL_RebuildLeavesANamesakeReader_HappyPath pins the
// readers a column type change rebuilds: reporting.sku_report, and not
// public.sku_list, whose bare item_skus is public's under the path public. A
// view the plan drops and creates again loses what the schema does not declare,
// such as a grant, and a policy on it refuses the plan.
func TestGenerateSchemaDiffSQL_RebuildLeavesANamesakeReader_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
		namesakeRebuildDiff(c, sessionSemantics("public")), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP VIEW IF EXISTS "reporting"."sku_report"`)
	c.Assert(sql, qt.Not(qt.Contains), "sku_list")
}

// TestGenerateSchemaDiffSQL_RebuildTakesANamesakeReaderWithoutASession_HappyPath
// is the control: a comparison made without a connection does not know the
// path, so the bare item_skus may be reporting's, and public.sku_list is
// rebuilt rather than left to the CASCADE.
func TestGenerateSchemaDiffSQL_RebuildTakesANamesakeReaderWithoutASession_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
		namesakeRebuildDiff(c, nil), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP VIEW IF EXISTS "public"."sku_list"`)
}

// namesakeViewDropDiff compares a declaration that swaps the columns of the
// view reporting.totals, a change CREATE OR REPLACE VIEW cannot make, so the
// plan drops it and creates it again. public.summary reads public.totals by
// its bare name.
func namesakeViewDropDiff(c *qt.C, semantics *identifier.Semantics) *difftypes.SchemaDiff {
	c.Helper()
	desired := &schemamodel.Database{Views: []schemamodel.View{
		{Name: "public.totals", Body: "SELECT 1 AS a, 2 AS b"},
		{Name: "public.summary", Body: "SELECT a FROM totals"},
		{Name: "reporting.totals", Body: "SELECT 2 AS b, 1 AS a"},
	}}
	current := &catalog.Database{Views: []catalog.View{
		{Schema: "public", Name: "totals", Body: "SELECT 1 AS a, 2 AS b"},
		{Schema: "public", Name: "summary", Body: "SELECT a FROM totals"},
		{Schema: "reporting", Name: "totals", Body: "SELECT 1 AS a, 2 AS b"},
	}}
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	diff.IdentifierSemantics = semantics
	return diff
}

// TestGenerateSchemaDiffSQL_ViewDropLeavesANamesakeReader_HappyPath pins what a
// view drop takes with it: reporting.totals is dropped and created again, and
// public.summary, whose bare totals is public's under the path public, is
// left alone. Taken as a reader, it is dropped and created again too.
func TestGenerateSchemaDiffSQL_ViewDropLeavesANamesakeReader_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
		namesakeViewDropDiff(c, sessionSemantics("public")), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `"reporting"."totals"`)
	c.Assert(sql, qt.Not(qt.Contains), "summary")
}

// TestGenerateSchemaDiffSQL_ViewDropTakesANamesakeReaderWithoutASession_HappyPath
// is the control: without a connection, the bare totals may be reporting's,
// and public.summary is created again after the drop.
func TestGenerateSchemaDiffSQL_ViewDropTakesANamesakeReaderWithoutASession_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
		namesakeViewDropDiff(c, nil), platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `"public"."summary"`)
}
