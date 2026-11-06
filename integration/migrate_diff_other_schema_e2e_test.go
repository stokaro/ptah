//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// migrateDiffOtherSchemaCases are databases a URL connects to one schema of, with a
// directory that puts tables in another. tables reads back what the database holds.
var migrateDiffOtherSchemaCases = []struct {
	name    string
	dialect string
	target  func(c *qt.C) string
	tables  string
}{
	{
		name:    "sqlserver",
		dialect: platform.SQLServer,
		target: func(c *qt.C) string {
			return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
				"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
		},
		tables: "SELECT STRING_AGG(CONCAT(table_schema, '.', table_name), ',') WITHIN GROUP (ORDER BY table_schema, table_name) FROM information_schema.tables",
	},
	{
		name:    "postgres with a search_path",
		dialect: platform.Postgres,
		target:  func(c *qt.C) string { return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_other_schema"), "public") },
		tables:  "SELECT string_agg(table_schema || '.' || table_name, ',' ORDER BY table_schema, table_name) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')",
	},
}

// TestCompatMigrateDiffE2E_ReadsTheSchemasADirectoryPutsTablesIn writes three files over
// tables in a schema the URL does not name. Read at the connection's schema alone, the
// directory seems to hold none of them, so the second file creates them all again and
// never drops the one that went. Here the second file changes only what changed, the
// third finds nothing to do, and the files run both ways on another database.
func TestCompatMigrateDiffE2E_ReadsTheSchemasADirectoryPutsTablesIn(t *testing.T) {
	const (
		first  = "CREATE SCHEMA held;\nCREATE TABLE held.kept (id int PRIMARY KEY);\nCREATE TABLE held.gone (id int PRIMARY KEY);\n"
		second = "CREATE SCHEMA held;\nCREATE TABLE held.kept (id int PRIMARY KEY);\nCREATE TABLE held.fresh (id int PRIMARY KEY);\n"
	)
	for _, test := range migrateDiffOtherSchemaCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.target(c)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", test.target(c), []string{first, second, second}))

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, test.dialect, pairs[0].Up)
			c.Assert(heldColumns(c, target, test.tables), qt.Equals, "held.gone,held.kept")
			applyEngineFile(c, target, test.dialect, pairs[1].Up)
			c.Assert(heldColumns(c, target, test.tables), qt.Equals, "held.fresh,held.kept")
			applyEngineFile(c, target, test.dialect, pairs[1].Down)
			c.Assert(heldColumns(c, target, test.tables), qt.Equals, "held.gone,held.kept")
		})
	}
}
