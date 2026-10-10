//go:build integration

package ydb_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/ydbsource"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

const streamingSchema = "ptah_ydb_streaming"

func streamingDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		FeatureCoverage: must.Must(ydbsource.Coverage(ydbsource.Limits{})),
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbtopic.DesiredObject(streamingSchema, "input", "", ydbtopic.Spec{}),
			ydbtopic.DesiredObject(streamingSchema, "output", "", ydbtopic.Spec{}),
			ydbstreaming.DesiredObject(streamingSchema, "copy", "", ydbstreaming.Spec{Text: "INSERT INTO `ptah_ydb_streaming/output` SELECT * FROM `ptah_ydb_streaming/input`;", Run: new(false)}, false))),
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
	c.Assert(live.FeatureObjects.Select(isStreamingQuery).Len(), qt.Equals, 1)
	object, found, err := live.FeatureObjects.Get(ydbstreaming.Ref(streamingSchema, "copy"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	query, ok := object.Value.(*ydbstreaming.Observed)
	c.Assert(ok, qt.IsTrue)
	c.Assert(*query.Spec.Run, qt.IsFalse)
	c.Assert(query.Spec.ResourcePool, qt.Equals, "default")

	editStreamingDeclaration(c, declared, func(query *ydbstreaming.Desired) { query.Spec.Run = new(true) })
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	editStreamingDeclaration(c, declared, func(query *ydbstreaming.Desired) { query.Spec.Run = new(false) })
	apply(c, conn, planAgainst(c, conn, declared, schemas))
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)

	current := readScoped(c, conn, schemas)
	editStreamingDeclaration(c, declared, func(query *ydbstreaming.Desired) {
		query.Spec.Text = "INSERT INTO `ptah_ydb_streaming/output` SELECT * FROM `ptah_ydb_streaming/input` WHERE TRUE; /* changed */"
	})
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, conn.Info(), nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	_, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, "ydb", planner.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.ErrorMatches, `(?s).*allow_state_reset=true.*`)
	editStreamingDeclaration(c, declared, func(query *ydbstreaming.Desired) { query.AllowStateReset = true })
	diff, err = schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, conn.Info(), nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{Diff: diff, DesiredSchema: declared, CurrentSchema: current, Dialect: "ydb", Capabilities: conn.Info().Capabilities, Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQLWithCapabilities("ydb", conn.Info().Capabilities, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQLWithCapabilities("ydb", conn.Info().Capabilities, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	applyScript(c, conn, forward)
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	applyScript(c, conn, reverse)
	c.Assert(planAgainst(c, conn, streamingDeclaration(), schemas), qt.HasLen, 0)

	removal := planAgainst(c, conn, &schemamodel.Database{FeatureCoverage: must.Must(ydbsource.Coverage(ydbsource.Limits{}))}, schemas)
	c.Assert(removal, qt.HasLen, 3)
	c.Assert(removal[0], qt.Contains, "DROP STREAMING QUERY")
	apply(c, conn, removal)
	c.Assert(readScoped(c, conn, schemas).FeatureObjects.Select(isStreamingQuery).Len(), qt.Equals, 0)
}

func isStreamingQuery(ref objectidentity.ID) bool {
	return ref.Kind == objectidentity.Kind(ydbstreaming.Kind)
}

func editStreamingDeclaration(c *qt.C, declared *schemamodel.Database, edit func(*ydbstreaming.Desired)) {
	c.Helper()
	object, found, err := declared.FeatureObjects.Get(ydbstreaming.Ref(streamingSchema, "copy"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	query, ok := object.Value.(*ydbstreaming.Desired)
	c.Assert(ok, qt.IsTrue)
	edit(query)
	declared.FeatureObjects, err = declared.FeatureObjects.Replace(object)
	c.Assert(err, qt.IsNil)
}

// TestYDBStreamingQueries_RenderCreatesTheTopicsFirst writes a whole schema of
// two topics and a running query that reads one and writes the other, and runs
// the script: YDB compiles the query when it creates it, so the query has to
// follow both topics. The schema has no table, so nothing else orders the two
// owners. 25.1 has no streaming queries, so only 26.2 runs it.
func TestYDBStreamingQueries_RenderCreatesTheTopicsFirst(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, externalSourcesOn, clusterFlag{yaml: "enable_streaming_queries", page: "EnableStreamingQueries", on: true})
	conn := openYDB(c, line)
	schemas := []string{streamingSchema}
	declared := streamingDeclaration()
	editStreamingDeclaration(c, declared, func(query *ydbstreaming.Desired) { query.Spec.Run = nil })
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(context.Context, string) error
	})
	c.Assert(ok, qt.IsTrue)
	// Cleanup runs after the test's context ends. The running query goes
	// before the directory that holds the topics it reads.
	c.Cleanup(func() { c.Check(dropper.DropDirectory(context.Background(), streamingSchema), qt.IsNil) })

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(declared, platform.YDB, conn.Info().Capabilities)
	c.Assert(err, qt.IsNil)
	applyScript(c, conn, strings.Join(statements, ""))
	c.Cleanup(func() {
		c.Check(conn.Writer().ExecuteSQL(context.Background(), "DROP STREAMING QUERY `ptah_ydb_streaming/copy`"), qt.IsNil)
	})

	c.Assert(statements, qt.HasLen, 3)
	c.Assert(statements[:2], qt.DeepEquals, []string{"CREATE TOPIC `ptah_ydb_streaming/input`;\n", "CREATE TOPIC `ptah_ydb_streaming/output`;\n"})
	c.Assert(statements[2], qt.Contains, "CREATE STREAMING QUERY `ptah_ydb_streaming/copy`")
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
}
