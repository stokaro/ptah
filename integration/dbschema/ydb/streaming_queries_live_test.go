//go:build integration

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

const streamingSchema = "ptah_ydb_streaming"

func streamingDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Topics: []schemamodel.Topic{{Name: "input", Schema: streamingSchema}, {Name: "output", Schema: streamingSchema}},
		StreamingQueries: []schemamodel.StreamingQuery{{Name: "copy", Schema: streamingSchema,
			Spec: ast.StreamingQuerySpec{Text: "INSERT INTO `ptah_ydb_streaming/output` SELECT * FROM `ptah_ydb_streaming/input`;", Run: new(false)}}},
	}
}

// The full integration contour changes cluster flags serially. Cleanup removes
// the query before its topics while both flags remain enabled.
func TestYDBStreamingQueries_RoundTripAndRollback(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, externalSourcesOn, clusterFlag{yaml: "enable_streaming_queries", page: "EnableStreamingQueries", on: true})
	conn := openYDB(c, line)
	c.Assert(conn.Info().Capabilities.Has(capability.StreamingQueries), qt.IsTrue)
	schemas := []string{streamingSchema}
	declared := streamingDeclaration()
	first := planAgainst(c, conn, declared, schemas)
	c.Assert(first, qt.HasLen, 3)
	apply(c, conn, first)
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(context.Context, string) error
	})
	c.Assert(ok, qt.IsTrue)
	c.Cleanup(func() { c.Check(dropper.DropDirectory(context.Background(), streamingSchema), qt.IsNil) })
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	live := readScoped(c, conn, schemas)
	c.Assert(live.StreamingQueries, qt.HasLen, 1)
	c.Assert(*live.StreamingQueries[0].Spec.Run, qt.IsFalse)
	c.Assert(live.StreamingQueries[0].Spec.ResourcePool, qt.Equals, "default")

	declared.StreamingQueries[0].Spec.Run = new(true)
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	declared.StreamingQueries[0].Spec.Run = new(false)
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)

	current := readScoped(c, conn, schemas)
	declared.StreamingQueries[0].Spec.Text += " /* changed */"
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, current, conn.Info(), nil)
	c.Assert(err, qt.IsNil)
	_, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, "ydb", planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.ErrorMatches, `(?s).*allow_state_reset=true.*`)
	declared.StreamingQueries[0].AllowStateReset = true
	diff, err = schemadiff.CompareWithDatabaseInfo(declared, current, conn.Info(), nil)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{Diff: diff, DesiredSchema: declared, CurrentSchema: current, Dialect: "ydb", Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)
	forward, err := renderer.RenderSQLWithCapabilities("ydb", conn.Info().Capabilities, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := renderer.RenderSQLWithCapabilities("ydb", conn.Info().Capabilities, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	applyScript(c, conn, forward)
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	applyScript(c, conn, reverse)
	c.Assert(planAgainst(c, conn, streamingDeclaration(), schemas), qt.HasLen, 0)

	removal := planAgainst(c, conn, &schemamodel.Database{}, schemas)
	c.Assert(removal, qt.HasLen, 3)
	c.Assert(removal[0], qt.Contains, "DROP STREAMING QUERY")
	apply(c, conn, removal)
	c.Assert(readScoped(c, conn, schemas).StreamingQueries, qt.HasLen, 0)
}
