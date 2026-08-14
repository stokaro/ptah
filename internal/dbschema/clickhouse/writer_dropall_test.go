package clickhouse_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// The two inventories DropAllTables takes, spelled out here rather than
// imported so the test pins the SQL a reader would have to change on purpose.
const cleanupViewsQuery = `
	SELECT name FROM system.tables
	WHERE database = currentDatabase()
	  AND is_temporary = 0
	  AND engine IN ('View', 'MaterializedView')
	ORDER BY name
`

const cleanupTablesQuery = `
	SELECT name FROM system.tables
	WHERE database = currentDatabase()
	  AND is_temporary = 0
	  AND (
	    engine LIKE '%MergeTree'
	    OR engine = 'Memory'
	    OR engine = 'Log'
	    OR engine = 'TinyLog'
	    OR engine = 'StripeLog'
	  )
	  AND engine NOT LIKE '%View'
	  AND name NOT IN (
		SELECT concat('.inner_id.', toString(uuid))
		FROM system.tables
		WHERE database = currentDatabase() AND engine = 'MaterializedView'
		UNION ALL
		SELECT concat('.inner.', name)
		FROM system.tables
		WHERE database = currentDatabase() AND engine = 'MaterializedView'
)
	ORDER BY name
`

// TestWriterDropAllTables_DropsViewsBeforeTables pins what a ClickHouse reset
// destroys and in which order.
//
// Every caller of DropAllTables replays DDL into the database afterwards -- the
// shadow replay in migration/generator, the dev-database cleanup in
// internal/atlasschema, the integration harness -- so a view or materialized
// view left standing makes the next CREATE fail with "already exists" rather
// than being a merely smaller destructive scope.
//
// Both inventories are read before anything is dropped, because the table query
// subtracts materialized-view storage by asking the materialized views
// themselves, and those are gone once the view drops begin.
func TestWriterDropAllTables_DropsViewsBeforeTables(t *testing.T) {
	c := qt.New(t)
	queries := []sqlMockQuery{
		{
			sql: cleanupViewsQuery,
			result: dbtest.QueryResult{
				Columns: []string{"name"},
				Rows:    [][]driver.Value{{"mv_users"}, {"v_users"}},
			},
		},
		{
			sql: cleanupTablesQuery,
			result: dbtest.QueryResult{
				Columns: []string{"name"},
				Rows:    [][]driver.Value{{"users"}},
			},
		},
	}
	execs := []sqlMockExec{
		{sql: "DROP VIEW IF EXISTS `mv_users` SYNC"},
		{sql: "DROP VIEW IF EXISTS `v_users` SYNC"},
		{sql: "DROP TABLE IF EXISTS `users` SYNC"},
	}
	db := openClickHouseSQLMock(t, c, queries, execs)

	err := clickhouse.NewClickHouseWriter(db.SQL, "ptah_test").DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	assertClickHouseSQLMockComplete(c, db, queries, execs)
}

// TestWriterDropAllTables_RefusesBacktickedNames keeps the identifier guard on
// both halves of the cleanup. quoteIdent doubles an embedded backtick, so this
// is defense in depth, but a name that cannot appear in a real deployment is
// refused rather than quoted.
func TestWriterDropAllTables_RefusesBacktickedNames(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		views   [][]driver.Value
		tables  [][]driver.Value
		wantErr string
	}{
		{
			name:    "view",
			views:   [][]driver.Value{{"v`_users"}},
			tables:  [][]driver.Value{{"users"}},
			wantErr: "clickhouse: refusing to drop view \"v`_users\": name contains a backtick",
		},
		{
			name:    "table",
			views:   [][]driver.Value{},
			tables:  [][]driver.Value{{"us`ers"}},
			wantErr: "clickhouse: refusing to drop table \"us`ers\": name contains a backtick",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			queries := []sqlMockQuery{
				{
					sql:    cleanupViewsQuery,
					result: dbtest.QueryResult{Columns: []string{"name"}, Rows: test.views},
				},
				{
					sql:    cleanupTablesQuery,
					result: dbtest.QueryResult{Columns: []string{"name"}, Rows: test.tables},
				},
			}
			db := openClickHouseSQLMock(t, c, queries, nil)

			err := clickhouse.NewClickHouseWriter(db.SQL, "ptah_test").DropAllTables(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantErr)
		})
	}
}
