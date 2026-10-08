//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func TestYDBIndexAndFeatureRollbackPreservesSkippedIndex(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, changefeedSchemas)
			c.Cleanup(func() { dropTables(c, conn, changefeedSchemas) })
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			stream := ydbschema.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT24H"}
			before := changefeedDeclaration(stream)
			category := schemamodel.Field{StructName: "Event", Name: "category", Type: "TEXT", Nullable: true}
			before.Fields = append(before.Fields, category)
			before.Indexes = []schemamodel.Index{
				{Name: "retained", StructName: "Event", Fields: []string{"payload"}, Comment: "retained comment"},
				{Name: "old_name", StructName: "Event", Fields: []string{"payload", "category"}, Comment: "old comment"},
			}
			apply(c, conn, planAgainst(c, conn, before, changefeedSchemas))
			current := readScoped(c, conn, changefeedSchemas)
			stream.RetentionPeriod = "PT12H"
			after := changefeedDeclaration(stream)
			after.Fields = append(after.Fields, category)
			after.Indexes = []schemamodel.Index{{Name: "new_name", StructName: "Event", Fields: []string{"payload", "category"}, Comment: "new comment"}}
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), after, current, conn.Info(), nil, runtime)
			c.Assert(err, qt.IsNil)
			filtered, skipped := diffpolicy.ApplyForDialect(diff, diffpolicy.NewSkipSet(diffpolicy.DropIndex), "ydb")
			c.Assert(skipped, qt.HasLen, 1)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: filtered, DesiredSchema: after, CurrentSchema: current, Dialect: "ydb", Capabilities: conn.Info().Capabilities,
			})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities("ydb", conn.Info().Capabilities, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := builtin.RenderSQLWithCapabilities("ydb", conn.Info().Capabilities, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			applyScript(c, conn, forward)
			live := readScoped(c, conn, changefeedSchemas)
			c.Assert(indexNamed(c, live, "retained").Comment, qt.Equals, "retained comment")
			c.Assert(indexNamed(c, live, "new_name").Comment, qt.Equals, "new comment")
			c.Assert(changefeedsOf(c, conn), qt.DeepEquals, []ydbschema.ChangefeedSpec{stream})
			applyScript(c, conn, reverse)
			c.Assert(planAgainst(c, conn, before, changefeedSchemas), qt.HasLen, 0)
			c.Assert(indexNamed(c, readScoped(c, conn, changefeedSchemas), "retained").Comment, qt.Equals, "retained comment")
		})
	}
}
