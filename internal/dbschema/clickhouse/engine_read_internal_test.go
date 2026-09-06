package clickhouse

// White-box testing required: readTables is package-local and the five fields
// under test are filled from columns of its own query.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

const engineFullFixture = "ReplacingMergeTree(ver) PARTITION BY toYYYYMM(day) " +
	"ORDER BY (day, id) SAMPLE BY id TTL day + toIntervalDay(90) SETTINGS index_granularity = 4096"

// TestReadTables_CarriesEveryEngineClause pins the five facts a table IS.
//
// The read asked only for the sorting key and the primary key, so everything
// else fell to the renderer's defaults: a ReplacingMergeTree replayed as a
// MergeTree -- losing the deduplicating merge the table exists for -- and the
// partition key, the sampling key, the TTL and the settings replayed absent. The
// TTL is the one that changes what the data does: a table replayed without it
// keeps rows it was configured to delete (stokaro/ptah#2198).
func TestReadTables_CarriesEveryEngineClause(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, engineTableServer)
	reader := NewClickHouseReader(db.SQL, "default")

	tables, err := reader.readTables(t.Context(), "default")

	c.Assert(err, qt.IsNil)
	c.Assert(tables, qt.HasLen, 1)
	c.Assert(tables[0].ClickHouseEngine, qt.Equals, "ReplacingMergeTree(ver)")
	c.Assert(tables[0].ClickHousePartitionKey, qt.Equals, "toYYYYMM(day)")
	c.Assert(tables[0].ClickHouseSamplingKey, qt.Equals, "id")
	c.Assert(tables[0].ClickHouseTTL, qt.Equals, "day + toIntervalDay(90)")
	c.Assert(tables[0].ClickHouseSettings, qt.Equals, "index_granularity = 4096")
	// The raw ORDER BY, kept even though it equals the primary key: the renderer
	// derives the columns from the key but not their order, and `(day, id)` came
	// back `(id, day)`.
	c.Assert(tables[0].ClickHouseOrderBy, qt.Equals, "day, id")
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
