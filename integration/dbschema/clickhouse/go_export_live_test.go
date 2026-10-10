//go:build integration

package clickhouse_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematogo"
)

// A successful Go export must recreate the storage clauses the server actually
// reports. Comparing catalogs catches an exporter that writes valid Go while
// dropping its platform properties.
func TestGoExportPreservesClickHouseStorageLive(t *testing.T) {
	for _, test := range []struct {
		name       string
		definition string
		want       chschema.ObservedTable
	}{
		{name: "all clauses", definition: " (tenant UInt64, id UInt64, created_at DateTime, version UInt64) " +
			"ENGINE = ReplacingMergeTree(version) PARTITION BY toYYYYMM(created_at) PRIMARY KEY tenant ORDER BY (tenant, id) " +
			"SAMPLE BY tenant TTL created_at + toIntervalDay(30) SETTINGS index_granularity = 4096",
			want: chschema.ObservedTable{Engine: "ReplacingMergeTree(version)", OrderBy: "tenant, id", PrimaryKey: "tenant",
				PartitionBy: "toYYYYMM(created_at)", SampleBy: "tenant", TTL: "created_at + toIntervalDay(30)", Settings: "index_granularity = 4096"}},
		{name: "explicit empty primary key", definition: " (id UInt64) ENGINE = MergeTree PRIMARY KEY tuple() ORDER BY id SETTINGS index_granularity = 4096",
			want: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", Settings: "index_granularity = 4096"}},
		{name: "inherited primary key", definition: " (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4096",
			want: chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id", Settings: "index_granularity = 4096"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openLiveClickHouseRBACTarget(c)
			name := uniqueClickHouseRBACName("go_export")
			copyName := name + "_copy"
			c.Cleanup(func() {
				cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+copyName+" SYNC")
				cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC")
			})
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE "+name+test.definition), qt.IsNil)
			live := readLive(c, conn)
			index := slices.IndexFunc(live.Tables, func(table catalog.Table) bool { return table.Name == name })
			c.Assert(index, qt.Not(qt.Equals), -1)
			table := live.Tables[index]
			subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts(table.Schema, table.Name)
			c.Assert(live.FeatureCoverage.Lookup(chschema.TableKind, subject).State, qt.Equals, schemaext.Complete)
			assertExportStorage(c, conn, table, &test.want)
			coverage := live.FeatureCoverage.SelectSubjects(func(id objectidentity.ID) bool { return id.Key() == subject.Key() })
			runtime := must.Must(builtin.New())
			model, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), &catalog.Database{Tables: []catalog.Table{table}, FeatureCoverage: coverage}, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			files, err := goschematogo.Render(c.Context(), model, goschematogo.Options{SingleFile: true, Dialect: "clickhouse", Runtime: runtime})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			reparsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", files[0].Data)
			c.Assert(err, qt.IsNil)
			reparsed.Tables[0].Name = copyName
			statements, err := builtin.GetOrderedCreateStatements(&reparsed, "clickhouse")
			c.Assert(err, qt.IsNil)
			applyStatements(c, conn, statements)
			copied := readLive(c, conn)
			copyIndex := slices.IndexFunc(copied.Tables, func(table catalog.Table) bool { return table.Name == copyName })
			c.Assert(copyIndex, qt.Not(qt.Equals), -1)
			assertExportStorage(c, conn, copied.Tables[copyIndex], &test.want)
		})
	}
}

// Compare every storage property with the fixture's intent. The owner accepts
// equivalent key parentheses, whose catalog spelling differs across versions.
// Independent catalog queries still pin the reader's exact key observations.
func assertExportStorage(c *qt.C, conn *dbschema.DatabaseConnection, table catalog.Table, want *chschema.ObservedTable) {
	c.Helper()
	observed, found, err := schemaext.FacetAs[*chschema.ObservedTable](table.Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	raw := readStorageClauses(c, conn, table.Name)
	c.Assert(observed.PartitionBy, qt.Equals, raw[1])
	c.Assert(observed.SampleBy, qt.Equals, raw[2])
	c.Assert(observed.OrderBy, qt.Equals, raw[3])
	c.Assert(observed.PrimaryKey, qt.Equals, raw[4])
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts(table.Schema, table.Name)
	result, err := (chcompare.Service{}).CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
		Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind},
		Owners:  []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(want.Desired()))}}},
		Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: table.Facets}}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0, qt.Commentf("catalog engine: %s", raw[0]))
}

func readStorageClauses(c *qt.C, conn *dbschema.DatabaseConnection, name string) [5]string {
	c.Helper()
	var clauses [5]string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT engine_full, partition_key, sampling_key, sorting_key, primary_key FROM system.tables WHERE database = currentDatabase() AND name = ?", name).
		Scan(&clauses[0], &clauses[1], &clauses[2], &clauses[3], &clauses[4]), qt.IsNil)
	return clauses
}
