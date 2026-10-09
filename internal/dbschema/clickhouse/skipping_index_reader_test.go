package clickhouse_test

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/dbschema/dbtest"
)

func parameterizedIndexQuery(indexType string) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "engine LIKE '%MergeTree'") {
			return settingsReaderQuery("MergeTree ORDER BY id")(query, args)
		}
		if !strings.Contains(query, "FROM system.data_skipping_indices") {
			return clickHouseIndexPresentReaderQuery(query, args)
		}
		// The short type column cannot reconstruct parameterized index DDL.
		if !strings.Contains(query, "expr, type_full, granularity") {
			return dbtest.QueryResult{}, fmt.Errorf("index inspection must read the complete type expression")
		}
		return dbtest.QueryResult{
			Columns: []string{"table", "name", "expr", "type_full", "granularity"},
			Rows:    [][]driver.Value{{"events", "idx_value", "value", indexType, uint64(64)}},
		}, nil
	}
}

func TestSkippingIndexReaderPreservesTypeParameters(t *testing.T) {
	for _, indexType := range []string{"set(100)", "bloom_filter(0.01)", "tokenbf_v1(256, 2, 0)"} {
		t.Run(indexType, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, parameterizedIndexQuery(indexType))
			reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")
			schema, err := reader.ReadSchemaContext(t.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(schema.Indexes, qt.HasLen, 1)
			c.Assert(schema.Indexes[0].Type, qt.Equals, indexType)
			c.Assert(schema.Indexes[0].Definition, qt.Equals, "INDEX idx_value value TYPE "+indexType+" GRANULARITY 64")
		})
	}
}
