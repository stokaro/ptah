//go:build integration

package clickhouse_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/parser"
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
			c.Assert(readback.Indexes[position].Type, qt.Equals, indexType)
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
