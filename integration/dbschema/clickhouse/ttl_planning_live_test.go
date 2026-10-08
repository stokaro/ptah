//go:build integration

package clickhouse_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A TTL column cannot disappear until the rule stops using it. Both directions
// execute the native generated plan; independent reads verify its final state.
func TestTTLPlanReplacesReferencedColumnAndRestoresRuleLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	name := uniqueClickHouseRBACName("ttl_plan")
	c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC") })
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE "+name+" (id UInt64, old_time DateTime) ENGINE = MergeTree ORDER BY tuple() TTL old_time + toIntervalDay(1)"), qt.IsNil)
	current := readTTLTable(c, conn, name)
	before, found, err := schemaext.FacetAs[*chschema.ObservedTable](current.Tables[0].Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	after := before.Desired()
	after.TTL.Value = "new_time + toIntervalDay(7)"
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: name, Schema: current.Tables[0].Schema, StructName: "Event", Facets: must.Must(schemaext.NewFacets(after))}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64"}, {Name: "new_time", StructName: "Event", Type: "DateTime"}},
	}
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "clickhouse", Capabilities: conn.Info().Capabilities,
	})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQL("clickhouse", plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, sqlutil.SplitStatementsForDialect("clickhouse", forward))
	described := readTTLTable(c, conn, name)
	noChange, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, described, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(noChange.HasChanges(), qt.IsFalse)
	reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, sqlutil.SplitStatementsForDialect("clickhouse", reverse))
	restored := readTTLTable(c, conn, name)
	restoredStorage, found, err := schemaext.FacetAs[*chschema.ObservedTable](restored.Tables[0].Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(restoredStorage, qt.DeepEquals, before)
	c.Assert(restored.Tables[0].Columns, qt.DeepEquals, current.Tables[0].Columns)
}

func readTTLTable(c *qt.C, conn *dbschema.DatabaseConnection, name string) *catalog.Database {
	c.Helper()
	live := readLive(c, conn)
	index := slices.IndexFunc(live.Tables, func(table catalog.Table) bool { return table.Name == name })
	c.Assert(index, qt.Not(qt.Equals), -1)
	table := live.Tables[index]
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts(table.Schema, table.Name)
	coverage := live.FeatureCoverage.SelectSubjects(func(id objectidentity.ID) bool { return id.Key() == subject.Key() })
	return &catalog.Database{Tables: []catalog.Table{table}, FeatureCoverage: coverage}
}
