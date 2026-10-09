//go:build integration

package clickhouse_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/parser"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The catalog independently checks the default's unit: granules per index
// block, not rows per table granule.
func TestSkippingIndexRenderingPreservesServerGranularityLive(t *testing.T) {
	for _, test := range []struct {
		name, source string
		granularity  uint64
	}{
		{name: "omitted granularity", source: "ALTER TABLE %s ADD INDEX idx_value value TYPE minmax;", granularity: 1},
		{name: "explicit granularity", source: "ALTER TABLE %s ADD INDEX idx_value value TYPE minmax GRANULARITY 4;", granularity: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, name := skippingIndexFixture(c)
			parsed, err := parser.NewParser(fmt.Sprintf(test.source, name), parser.WithDialect("clickhouse")).Parse()
			c.Assert(err, qt.IsNil)
			assertSkippingIndex(c, conn, name, parsed.Statements, test.granularity)
		})
	}
}

func TestCommonIndexRenderingUsesServerGranularityDefaultLive(t *testing.T) {
	c := qt.New(t)
	conn, name := skippingIndexFixture(c)
	node := &ast.IndexNode{Name: "idx_value", Table: name, Columns: []string{"value"}}
	assertSkippingIndex(c, conn, name, []ast.Node{node}, 1)
}

func TestSkippingIndexInspectionPreservesTypeParametersLive(t *testing.T) {
	for _, indexType := range []string{"set(100)", "bloom_filter(0.01)"} {
		t.Run(indexType, func(t *testing.T) {
			c := qt.New(t)
			conn, name := skippingIndexFixture(c)
			c.Assert(conn.Writer().ExecuteSQL(c.Context(), "ALTER TABLE "+name+" ADD INDEX idx_value value TYPE "+indexType+" GRANULARITY 4"), qt.IsNil)
			readback, err := conn.Reader().ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)
			position := slices.IndexFunc(readback.Indexes, func(index catalog.Index) bool {
				return index.TableName == name && index.Name == "idx_value"
			})
			c.Assert(position >= 0, qt.IsTrue)
			settings, found, err := schemaext.FacetAs[*chschema.ObservedIndex](readback.Indexes[position].Facets, chschema.IndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(settings, qt.DeepEquals, &chschema.ObservedIndex{IndexType: indexType, Granularity: 4})
			c.Assert(readback.Indexes[position].Definition, qt.Contains, "TYPE "+indexType+" GRANULARITY 4")
		})
	}
}

func skippingIndexFixture(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn := openLiveClickHouseRBACTarget(c)
	name := uniqueClickHouseRBACName("index_render")
	c.Cleanup(func() { cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC") })
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE "+name+" (id UInt64, value UInt64) ENGINE = MergeTree ORDER BY id"), qt.IsNil)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "INSERT INTO "+name+" VALUES (1, 10), (2, 20)"), qt.IsNil)
	return conn, name
}

func assertSkippingIndex(c *qt.C, conn *dbschema.DatabaseConnection, name string, nodes []ast.Node, wantGranularity uint64) {
	c.Helper()
	sql, err := builtin.RenderSQL("clickhouse", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), sql), qt.IsNil)
	var expression, indexType string
	var granularity uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT expr, type, granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = ? AND name = 'idx_value'", name).
		Scan(&expression, &indexType, &granularity), qt.IsNil)
	c.Assert(expression, qt.Equals, "value")
	c.Assert(indexType, qt.Equals, "minmax")
	c.Assert(granularity, qt.Equals, wantGranularity)
	var total uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT sum(value) FROM "+name).Scan(&total), qt.IsNil)
	c.Assert(total, qt.Equals, uint64(30))
}

// ClickHouse changes neither the type nor the granularity of a skipping index
// in place. Both directions execute the native generated plan, and independent
// reads verify that the index carries the settings each direction asked for
// while the table's rows survive.
func TestSkippingIndexSettingsPlanReplacesAndRestoresLive(t *testing.T) {
	c := qt.New(t)
	conn, name := skippingIndexFixture(c)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "ALTER TABLE "+name+" ADD INDEX idx_value value TYPE minmax GRANULARITY 1"), qt.IsNil)
	current := readIndexedTable(c, conn, name)
	before := skippingIndexSettings(c, current, "idx_value")
	c.Assert(before, qt.DeepEquals, &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1})
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: name, Schema: current.Tables[0].Schema, StructName: "Event"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64", Primary: true}, {Name: "value", StructName: "Event", Type: "UInt64"}},
		Indexes: []schemamodel.Index{{
			Name: "idx_value", StructName: "Event", TableName: name, Fields: []string{"value"},
			Overrides: map[string]map[string]string{"clickhouse": {"type": "set(100)", "granularity": "4"}},
		}},
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
	c.Assert(forward, qt.Contains, "DROP INDEX `idx_value`")
	applyStatements(c, conn, sqlutil.SplitStatementsForDialect("clickhouse", forward))
	described := readIndexedTable(c, conn, name)
	c.Assert(skippingIndexSettings(c, described, "idx_value"), qt.DeepEquals, &chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4})
	noChange, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, described, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(noChange.HasChanges(), qt.IsFalse)
	reverse, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(reverse, qt.Contains, "does not restore materialized index data")
	applyStatements(c, conn, sqlutil.SplitStatementsForDialect("clickhouse", reverse))
	restored := readIndexedTable(c, conn, name)
	c.Assert(skippingIndexSettings(c, restored, "idx_value"), qt.DeepEquals, before)
	var total uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT sum(value) FROM "+name).Scan(&total), qt.IsNil)
	c.Assert(total, qt.Equals, uint64(30))
}

// A new index declared with ClickHouse source properties is created with them,
// including a granularity the creation default would not choose.
func TestSkippingIndexDeclaredSettingsCreateTheIndexLive(t *testing.T) {
	c := qt.New(t)
	conn, name := skippingIndexFixture(c)
	current := readIndexedTable(c, conn, name)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: name, Schema: current.Tables[0].Schema, StructName: "Event"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64", Primary: true}, {Name: "value", StructName: "Event", Type: "UInt64"}},
		Indexes: []schemamodel.Index{{
			Name: "idx_value", StructName: "Event", TableName: name, Fields: []string{"value"}, Type: "bloom_filter(0.01)",
			Overrides: map[string]map[string]string{"clickhouse": {"granularity": "8"}},
		}},
	}
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, statements)
	described := readIndexedTable(c, conn, name)
	c.Assert(skippingIndexSettings(c, described, "idx_value"), qt.DeepEquals, &chschema.ObservedIndex{IndexType: "bloom_filter(0.01)", Granularity: 8})
	noChange, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, described, conn.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(noChange.HasChanges(), qt.IsFalse)
}

// readIndexedTable keeps one table, its indexes, and their coverage, so a
// comparison against a shared server does not plan other tests' objects.
func readIndexedTable(c *qt.C, conn *dbschema.DatabaseConnection, name string) *catalog.Database {
	c.Helper()
	live := readLive(c, conn)
	position := slices.IndexFunc(live.Tables, func(table catalog.Table) bool { return table.Name == name })
	c.Assert(position, qt.Not(qt.Equals), -1)
	table := live.Tables[position]
	builder := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	subject := builder.TableParts(table.Schema, table.Name)
	coverage := live.FeatureCoverage.SelectSubjects(func(id objectidentity.ID) bool { return id.Key() == subject.Key() })
	result := &catalog.Database{Tables: []catalog.Table{table}, FeatureCoverage: coverage}
	for _, index := range live.Indexes {
		if index.TableName == name {
			result.Indexes = append(result.Indexes, index)
		}
	}
	return result
}

func skippingIndexSettings(c *qt.C, db *catalog.Database, name string) *chschema.ObservedIndex {
	c.Helper()
	position := slices.IndexFunc(db.Indexes, func(index catalog.Index) bool { return index.Name == name })
	c.Assert(position, qt.Not(qt.Equals), -1)
	settings, found, err := schemaext.FacetAs[*chschema.ObservedIndex](db.Indexes[position].Facets, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return settings
}
