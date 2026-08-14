package clickhouse_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// innerStorageNameExpression is the expression the reader subtracts by, spelled
// out here rather than imported so the test pins SQL a reader would have to
// change on purpose.
//
// One row of system.tables, one storage name. Which spelling that is follows the
// database engine, and the materialized view's own row says which: an Atomic
// database gives the view a UUID and names its storage ".inner_id.<that uuid>",
// an Ordinary database leaves the UUID at all zeros and names it
// ".inner.<view name>". Measured on 26.7.3.19 and on 24.10.4.191, the two ends
// of the range this preset covers.
//
// Emitting both spellings for every view subtracts a name no view owns. A
// leading dot is legal in a quoted ClickHouse name, so a real table can be
// called ".inner.user_counts" in an Atomic database where the view "user_counts"
// stores its rows elsewhere -- and that table then disappears from the table
// read, from the index read, and from what DropAllTables destroys.
//
// The Atomic half of that is pinned live, in
// TestInnerTableSubtractionKeepsADeclaredDotNameLive. The Ordinary half is not:
// Ordinary is deprecated and needs allow_deprecated_database_ordinary to create
// at all, so this text is what holds that arm in place.
const innerStorageNameExpression = `if(
		         uuid = toUUID('00000000-0000-0000-0000-000000000000'),
		         concat('.inner.', name),
		         concat('.inner_id.', toString(uuid))
		       )`

// clickHouseIndexPresentReaderQuery answers like clickHouseViewReaderQuery but
// reports system.data_skipping_indices as present, so the index read is issued
// and its SQL can be inspected.
func clickHouseIndexPresentReaderQuery(
	query string,
	args []driver.NamedValue,
) (dbtest.QueryResult, error) {
	switch {
	case strings.Contains(query, "name = 'data_skipping_indices'"):
		return dbtest.QueryResult{
			Columns: []string{"count()"},
			Rows:    [][]driver.Value{{uint64(1)}},
		}, nil
	case strings.Contains(query, "FROM system.data_skipping_indices"):
		return dbtest.QueryResult{
			Columns: []string{"table", "name", "expr", "type", "granularity"},
		}, nil
	default:
		return clickHouseViewReaderQuery(query, args)
	}
}

func recordingClickHouseReaderQuery(recorded *[]string) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		*recorded = append(*recorded, query)
		return clickHouseIndexPresentReaderQuery(query, args)
	}
}

func clickHouseQueriesContaining(recorded []string, needle string) []string {
	var matched []string
	for _, query := range recorded {
		if !strings.Contains(query, needle) {
			continue
		}
		matched = append(matched, query)
	}
	return matched
}

// TestReaderReadSchema_SubtractsInnerStorageByOneSpellingPerView pins that both
// reads which subtract materialized-view storage pick the spelling from the
// view's own row rather than subtracting every spelling a view might have had.
func TestReaderReadSchema_SubtractsInnerStorageByOneSpellingPerView(t *testing.T) {
	c := qt.New(t)
	var recorded []string
	db := dbtest.Open(t, recordingClickHouseReaderQuery(&recorded))
	reader := clickhouse.NewClickHouseReader(db.SQL, "analytics")

	_, err := reader.ReadSchema()

	c.Assert(err, qt.IsNil)
	tableQueries := clickHouseQueriesContaining(recorded, "engine LIKE '%MergeTree'")
	c.Assert(tableQueries, qt.HasLen, 1)
	c.Assert(tableQueries[0], qt.Contains, innerStorageNameExpression)

	indexQueries := clickHouseQueriesContaining(recorded, "FROM system.data_skipping_indices")
	c.Assert(indexQueries, qt.HasLen, 1)
	c.Assert(indexQueries[0], qt.Contains, innerStorageNameExpression)
}
