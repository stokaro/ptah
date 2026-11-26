package clickhouse_test

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/dbschema/dbtest"
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
	db := openClickHouseSQLMock(t, c, queries, execs)

	err := clickhouse.NewClickHouseWriter(db.SQL, "ptah_test").DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	assertClickHouseSQLMockComplete(c, db, queries, execs)
}

// TestWriterDropAllTables_QuotesEveryName pins the cleanup of objects whose
// names hold a backtick or a backslash. ClickHouse holds such tables, and the
// cleanup quotes each name the way the renderer created it, so the database is
// emptied rather than refused. Written without the backslash doubled, `a\b`
// names a table that does not exist and IF EXISTS hides it.
func TestWriterDropAllTables_QuotesEveryName(t *testing.T) {
	c := qt.New(t)
	queries := []sqlMockQuery{
		{
			sql:    cleanupViewsQuery,
			result: dbtest.QueryResult{Columns: []string{"name"}, Rows: [][]driver.Value{{"v`_users"}}},
		},
		{
			sql:    cleanupTablesQuery,
			result: dbtest.QueryResult{Columns: []string{"name"}, Rows: [][]driver.Value{{`a\b`}, {"us`ers"}}},
		},
	}
	execs := []sqlMockExec{
		{sql: "DROP VIEW IF EXISTS `v``_users` SYNC"},
		{sql: "DROP TABLE IF EXISTS `a\\\\b` SYNC"},
		{sql: "DROP TABLE IF EXISTS `us``ers` SYNC"},
	}
	db := openClickHouseSQLMock(t, c, queries, execs)

	err := clickhouse.NewClickHouseWriter(db.SQL, "ptah_test").DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	assertClickHouseSQLMockComplete(c, db, queries, execs)
}
