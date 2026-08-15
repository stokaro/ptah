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
// The table inventory is read AFTER the views are dropped. Deriving the storage
// names first and subtracting them is a guess, and in an Ordinary database a
// materialized view created with TO owns no storage while ".inner.<view name>"
// is still what a storage-owning view of that name would be called -- so a real
// table spelled that way, the view's own target included, was left standing by
// the reset. Asking after the drops needs no guess: what is still there is a
// table, and the query below subtracts nothing.
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
	db := openClickHouseSQLMock(t, c.TB, queries, execs)

	err := clickhouse.NewClickHouseWriter(db.SQL, "ptah_test").DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	assertClickHouseSQLMockComplete(c.TB, db, queries, execs)
}

// TestWriterDropAllTables_RefusesBacktickedNames keeps the identifier guard on
// both halves of the cleanup. quoteIdent doubles an embedded backtick, so this
// is defense in depth, but a name that cannot appear in a real deployment is
// refused rather than quoted.
func TestWriterDropAllTables_RefusesBacktickedNames(t *testing.T) {
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
		t.Run(test.name, func(t2 *testing.T) {
			c := qt.New(t2)
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
			db := openClickHouseSQLMock(t, c.TB, queries, nil)

			err := clickhouse.NewClickHouseWriter(db.SQL, "ptah_test").DropAllTables(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantErr)
		})
	}
}
