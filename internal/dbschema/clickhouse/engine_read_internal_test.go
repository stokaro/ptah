package clickhouse

// White-box testing required: readTables owns the system-table query.
// This fixture isolates its property extraction from unrelated catalog reads.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dbschema/dbtest"
)

const engineFullFixture = "ReplacingMergeTree(ver) PARTITION BY toYYYYMM(day) " +
	"ORDER BY (day, id) SAMPLE BY id TTL day + toIntervalDay(90) SETTINGS index_granularity = 4096"

func TestReadTables_CarriesEveryEngineClause(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, engineTableServer)
	reader := NewClickHouseReader(db.SQL, "default")

	tables, err := reader.readTables(t.Context(), "default")

	c.Assert(err, qt.IsNil)
	c.Assert(tables, qt.HasLen, 1)
	value, found, err := schemaext.FacetAs[*chschema.ObservedTable](tables[0].Facets, chschema.TableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, &chschema.ObservedTable{
		Engine: "ReplacingMergeTree(ver)", OrderBy: "day, id", PrimaryKey: "day, id",
		PartitionBy: "toYYYYMM(day)", SampleBy: "id", TTL: "day + toIntervalDay(90)", Settings: "index_granularity = 4096",
	})
	c.Assert(tables[0].Facets.TargetScope(chschema.TableKind), qt.DeepEquals, []string{"clickhouse"})
}

// engineTableServer answers the two reads readTables makes.
//
// The table arm is matched on the MergeTree predicate rather than on
// `system.tables` alone: the table query excludes a materialized view's inner
// storage by name, and that subquery mentions system.tables too.
func engineTableServer(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "FROM system.columns"):
		return dbtest.QueryResult{
			Columns: []string{
				"table", "name", "type", "default_kind", "default_expression",
				"position", "comment", "is_in_primary_key",
			},
			Rows: [][]driver.Value{
				{"events", "day", "Date", "", "", uint64(1), "", uint8(1)},
				{"events", "id", "UInt64", "", "", uint64(2), "", uint8(1)},
			},
		}, nil
	case strings.Contains(query, "engine LIKE '%MergeTree'"):
		return dbtest.QueryResult{
			Columns: []string{
				"name", "comment", "sorting_key", "primary_key",
				"engine_full", "partition_key", "sampling_key",
			},
			Rows: [][]driver.Value{
				{"events", "", "day, id", "day, id", engineFullFixture, "toYYYYMM(day)", "id"},
			},
		}, nil
	default:
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
}
