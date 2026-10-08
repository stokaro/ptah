//go:build integration

package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// The server decides the difference between an omitted primary key and an
// explicit empty one. Reading both catalog keys catches accidental inheritance.
func TestTypedTableFacetsPreserveKeyIntentLive(t *testing.T) {
	for _, test := range []struct {
		name        string
		order       chschema.Setting
		primary     chschema.Setting
		wantOrder   string
		wantPrimary []string
	}{
		{"inherited", chschema.Setting{State: chschema.Explicit, Value: "tenant, id"}, chschema.Setting{}, "tenant, id", []string{"tenant, id"}},
		{"default", chschema.Setting{State: chschema.Explicit, Value: "tenant, id"}, chschema.Setting{State: chschema.Default}, "tenant, id", []string{"tenant, id"}},
		{"empty_primary", chschema.Setting{State: chschema.Explicit, Value: "tenant, id"}, chschema.Setting{State: chschema.Explicit}, "tenant, id", []string{""}},
		// The 24.10 catalog removes single-key parentheses; 26.9 retains them.
		{"prefix", chschema.Setting{State: chschema.Explicit, Value: "tenant, id"}, chschema.Setting{State: chschema.Explicit, Value: "tenant"}, "tenant, id", []string{"tenant", "(tenant)"}},
		{"empty_both", chschema.Setting{State: chschema.Explicit}, chschema.Setting{State: chschema.Explicit}, "", []string{""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openLiveClickHouseRBACTarget(c)
			name := uniqueClickHouseRBACName("facet_" + test.name)
			c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC") })
			value := &chschema.DesiredTable{OrderBy: test.order, PrimaryKey: test.primary}
			schema := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: name, StructName: "Event", Facets: must.Must(schemaext.NewFacets(value))}},
				Fields: []schemamodel.Field{{Name: "id", Type: "UInt64", StructName: "Event", Primary: true}, {Name: "tenant", Type: "UInt64", StructName: "Event"}},
			}
			runtime := must.Must(builtin.New())
			current := &catalog.Database{}
			diff, err := schemadiff.CompareWithDialect(c.Context(), schema, current, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: schema, CurrentSchema: current, Dialect: "clickhouse",
			})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQL("clickhouse", plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), forward), qt.IsNil)
			var got [2]string
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT sorting_key, primary_key FROM system.tables WHERE database = currentDatabase() AND name = ?", name,
			).Scan(&got[0], &got[1]), qt.IsNil)
			c.Assert(got[0], qt.Equals, test.wantOrder)
			c.Assert(test.wantPrimary, qt.Contains, got[1])
			reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), reverse), qt.IsNil)
			var remaining uint64
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?", name,
			).Scan(&remaining), qt.IsNil)
			c.Assert(remaining, qt.Equals, uint64(0))
		})
	}
}

func TestTypedTableFacetsPreserveStorageClausesLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	name := uniqueClickHouseRBACName("facet_storage")
	c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC") })
	value := (&chschema.ObservedTable{
		Engine: "ReplacingMergeTree(version)", OrderBy: "tenant, id", PrimaryKey: "tenant",
		PartitionBy: "toYYYYMM(created_at)", SampleBy: "tenant", TTL: "created_at + toIntervalDay(30)",
		Settings: "index_granularity = 4096",
	}).Desired()
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: name, StructName: "Event", Facets: must.Must(schemaext.NewFacets(value))}},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "UInt64", StructName: "Event"}, {Name: "tenant", Type: "UInt64", StructName: "Event"},
			{Name: "created_at", Type: "DateTime", StructName: "Event"}, {Name: "version", Type: "UInt64", StructName: "Event"},
		},
	}
	statements, err := builtin.GetOrderedCreateStatements(schema, "clickhouse")
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, statements)
	var engine, partition, sample string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT engine_full, partition_key, sampling_key FROM system.tables WHERE database = currentDatabase() AND name = ?", name,
	).Scan(&engine, &partition, &sample), qt.IsNil)
	c.Assert(partition, qt.Equals, "toYYYYMM(created_at)")
	c.Assert(sample, qt.Equals, "tenant")
	for _, clause := range []string{"ReplacingMergeTree(version)", "TTL created_at + toIntervalDay(30)", "index_granularity = 4096"} {
		c.Assert(engine, qt.Contains, clause)
	}
}
